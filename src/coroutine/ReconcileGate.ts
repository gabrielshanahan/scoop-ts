/**
 * Decides, per worker, whether a given tick should run the reconciliation step (the EMITTED→SEEN
 * / ROLLBACK_EMITTED→ROLLING_BACK anti-joins over `message_event`) AND whether it should run the
 * drain (the pending-coroutine-run query — candidate_seens et al., by far the most expensive
 * statement Scoop issues).
 *
 * Reconciliation only has work to do when a new EMITTED or ROLLBACK_EMITTED row appeared on this
 * worker's topic — every such row is accompanied by a Postgres NOTIFY. The DRAIN only has work
 * when a notification arrived, a reconcile pass inserted continuations, or the previous drain
 * actually resumed something (a resumed run can suspend and become ready again). This gate turns
 * both from "every tick" into "only when a signal says there might be work, plus a rare safety
 * sweep" — on an idle fleet of dozens of workers the per-tick candidate_seens scans were the
 * database's single biggest steady load (abo-uat 2026-07-17: 3.5 average active sessions).
 *
 * ## Drain, don't reconcile-once
 * A single notification can require several reconcile passes to fully drain (sibling handlers
 * contend on the parent's SEEN row via FOR UPDATE SKIP LOCKED). Once woken, the gate keeps
 * reconciling every tick until it observes QUIET_TICKS consecutive passes that inserted nothing.
 * The drain keeps its own quiet accounting the same way.
 *
 * ## Safety net
 * Independently of notifications, a reconcile AND a drain are forced at least every
 * [forceReconcileEvery] — correctness never depends on notification delivery, only latency does.
 * The drain sweep is what bounds the two signals Postgres does NOT notify about: a parent
 * suspended on children whose commits land on OTHER topics, and time-based wakeups.
 *
 * Monotonic time is measured in nanoseconds via `process.hrtime.bigint()` (converted to Number —
 * safe for relative comparisons over any realistic process lifetime).
 */

/**
 * Consecutive zero-insert reconcile passes required before a woken worker stops reconciling.
 */
const QUIET_TICKS = 3

/**
 * The drain's quiet tail is ONE pass, not three: the drain already loops until empty WITHIN a
 * tick (whileISaySo), so the only cross-tick race a retry must cover is a run hidden behind a
 * sibling's FOR UPDATE SKIP LOCKED — one follow-up pass covers it, and the notify latch + safety
 * sweep re-arm everything else. At three, an idle fleet paid 3 extra candidate_seens scans per
 * kick (abo-uat 2026-07-17: the residual 2-3 scans/s were mostly these confirm-empty passes).
 */
const DRAIN_QUIET_TICKS = 1

export class ReconcileGate {
    // Set by NOTIFY, consumed in shouldReconcile. Starts armed so the first tick after
    // subscribe/startup reconciles, catching messages emitted while this worker was down.
    private dirtySignal = true

    // Remaining ticks to keep reconciling before going idle; reset whenever a notification
    // arrives or a reconcile pass is productive.
    private quietTicksRemaining = QUIET_TICKS

    // Monotonic instant of the next forced safety-net reconcile.
    private nextSweepNanos: number

    // Drain-side twin state: its own dirty latch and quiet tail (a reconcile consumes the shared
    // dirtySignal, so the drain latches its copy at markDirty time), and its own sweep timer.
    private drainDirtySignal = true
    private drainQuietTicksRemaining = DRAIN_QUIET_TICKS
    private nextDrainSweepNanos: number

    private constructor(
        private readonly intervalNanos: number,
        seedNanos: number,
    ) {
        this.nextSweepNanos = seedNanos
        this.nextDrainSweepNanos = seedNanos
    }

    /** Records that a notification arrived for this worker's topic. */
    markDirty(): void {
        this.dirtySignal = true
        this.drainDirtySignal = true
    }

    /**
     * Returns whether this tick should reconcile, atomically consuming the dirty signal. A gate
     * built with a non-positive interval ([ALWAYS]) never gates.
     */
    shouldReconcile(nowNanos: number = monotonicNanos()): boolean {
        if (this.intervalNanos <= 0) {
            return true
        }
        if (this.dirtySignal) {
            this.dirtySignal = false
            this.quietTicksRemaining = QUIET_TICKS
        }
        return this.quietTicksRemaining > 0 || nowNanos - this.nextSweepNanos >= 0
    }

    /**
     * Records the outcome of a committed reconcile pass and pushes the safety-net timer forward.
     * A productive pass keeps the drain going; an empty pass counts towards the quiet tail.
     */
    reconcileSucceeded(insertedRows: number, nowNanos: number = monotonicNanos()): void {
        if (this.intervalNanos <= 0) {
            return
        }
        this.quietTicksRemaining =
            insertedRows > 0 ? QUIET_TICKS : Math.max(this.quietTicksRemaining - 1, 0)
        // Fresh continuations are exactly what the drain runs — arm it.
        if (insertedRows > 0) {
            this.drainQuietTicksRemaining = DRAIN_QUIET_TICKS
        }
        this.nextSweepNanos = nowNanos + this.intervalNanos
    }

    /** Keeps the drain armed so a failed/rolled-back reconcile is retried on the next tick. */
    reconcileFailed(): void {
        if (this.intervalNanos > 0) {
            this.quietTicksRemaining = QUIET_TICKS
            this.drainQuietTicksRemaining = DRAIN_QUIET_TICKS
        }
    }

    /**
     * Returns whether this tick should run the drain (the pending-coroutine-run query),
     * atomically consuming the drain's dirty latch. A gate built with a non-positive interval
     * ([ALWAYS]) never gates.
     */
    shouldDrain(nowNanos: number = monotonicNanos()): boolean {
        if (this.intervalNanos <= 0) {
            return true
        }
        if (this.drainDirtySignal) {
            this.drainDirtySignal = false
            this.drainQuietTicksRemaining = DRAIN_QUIET_TICKS
        }
        return this.drainQuietTicksRemaining > 0 || nowNanos - this.nextDrainSweepNanos >= 0
    }

    /**
     * Records the outcome of a completed drain and pushes its safety-net timer forward. A drain
     * that resumed at least one run stays armed (a resumed run can suspend and become ready
     * again without any notification); an empty drain counts towards the quiet tail.
     */
    drainCompleted(resumedAnything: boolean, nowNanos: number = monotonicNanos()): void {
        if (this.intervalNanos <= 0) {
            return
        }
        this.drainQuietTicksRemaining = resumedAnything
            ? DRAIN_QUIET_TICKS
            : Math.max(this.drainQuietTicksRemaining - 1, 0)
        this.nextDrainSweepNanos = nowNanos + this.intervalNanos
    }

    /** Keeps the drain armed so a failed drain is retried on the next tick. */
    drainFailed(): void {
        if (this.intervalNanos > 0) {
            this.drainQuietTicksRemaining = DRAIN_QUIET_TICKS
        }
    }

    /** A non-gating gate: always reconciles. Default for direct EventLoop.tick callers. */
    static readonly ALWAYS = new ReconcileGate(0, 0)

    /**
     * Builds a per-worker gate that forces a reconcile at least every [forceReconcileEveryMillis].
     * The first sweep is seeded with a random offset so many workers booted together don't sweep
     * in a synchronized herd.
     */
    static create(
        forceReconcileEveryMillis: number,
        nowNanos: number = monotonicNanos(),
    ): ReconcileGate {
        const intervalNanos = forceReconcileEveryMillis * 1_000_000
        if (intervalNanos <= 0) {
            return ReconcileGate.ALWAYS
        }
        const jitterNanos = Math.floor(Math.random() * intervalNanos)
        return new ReconcileGate(intervalNanos, nowNanos + jitterNanos)
    }
}

function monotonicNanos(): number {
    return Number(process.hrtime.bigint())
}
