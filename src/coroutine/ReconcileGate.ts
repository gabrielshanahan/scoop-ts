/**
 * Decides, per worker, whether a given tick should run the reconciliation step (the EMITTED→SEEN
 * / ROLLBACK_EMITTED→ROLLING_BACK anti-joins over `message_event`).
 *
 * Reconciliation only has work to do when a new EMITTED or ROLLBACK_EMITTED row appeared on this
 * worker's topic — every such row is accompanied by a Postgres NOTIFY. This gate turns
 * reconciliation from "every tick" into "only when a notification says there might be work, plus
 * a rare safety sweep".
 *
 * ## Drain, don't reconcile-once
 * A single notification can require several reconcile passes to fully drain (sibling handlers
 * contend on the parent's SEEN row via FOR UPDATE SKIP LOCKED). Once woken, the gate keeps
 * reconciling every tick until it observes QUIET_TICKS consecutive passes that inserted nothing.
 *
 * ## Safety net
 * Independently of notifications, a reconcile is forced at least every [forceReconcileEvery] —
 * correctness never depends on notification delivery, only latency does.
 *
 * Monotonic time is measured in nanoseconds via `process.hrtime.bigint()` (converted to Number —
 * safe for relative comparisons over any realistic process lifetime).
 */
export class ReconcileGate {
    // Set by NOTIFY, consumed in shouldReconcile. Starts armed so the first tick after
    // subscribe/startup reconciles, catching messages emitted while this worker was down.
    private dirtySignal = true

    // Remaining ticks to keep reconciling before going idle; reset whenever a notification
    // arrives or a reconcile pass is productive.
    private quietTicksRemaining = QUIET_TICKS

    // Monotonic instant of the next forced safety-net reconcile.
    private nextSweepNanos: number

    private constructor(
        private readonly intervalNanos: number,
        seedNanos: number,
    ) {
        this.nextSweepNanos = seedNanos
    }

    /** Records that a notification arrived for this worker's topic. */
    markDirty(): void {
        this.dirtySignal = true
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
        this.nextSweepNanos = nowNanos + this.intervalNanos
    }

    /** Keeps the drain armed so a failed/rolled-back reconcile is retried on the next tick. */
    reconcileFailed(): void {
        if (this.intervalNanos > 0) {
            this.quietTicksRemaining = QUIET_TICKS
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

/**
 * Consecutive zero-insert reconcile passes required before a woken worker stops reconciling.
 */
const QUIET_TICKS = 3

function monotonicNanos(): number {
    return Number(process.hrtime.bigint())
}
