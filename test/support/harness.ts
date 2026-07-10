import { after, before, beforeEach } from "node:test"
import postgres, { Sql } from "postgres"
import { Scoop } from "../../src/Scoop.js"
import { JsonbHelper } from "../../src/JsonbHelper.js"
import type { DistributedCoroutine } from "../../src/coroutine/DistributedCoroutine.js"
import { PostgresMessageQueue } from "../../src/messaging/PostgresMessageQueue.js"
import type { Subscription } from "../../src/messaging/Subscription.js"
import { calibrateClockToDatabase } from "../../src/node/calibrateClock.js"
import { PostgresTopicNotifier } from "../../src/node/PostgresTopicNotifier.js"
import { sleep } from "./latch.js"

const CI_TIMEOUT_MULTIPLIER = Number(process.env.CI_TIMEOUT_MULTIPLIER ?? "1")

/** Sleep scaled by CI_TIMEOUT_MULTIPLIER — the analog of the Kotlin `ciSleep`. */
export function ciSleep(millis: number): Promise<void> {
    return sleep(millis * CI_TIMEOUT_MULTIPLIER)
}

/**
 * Waits (bounded) until an async condition holds. Used where the Kotlin tests sleep a fixed
 * duration to let handlers finish — polling the actual completion condition is the deterministic
 * equivalent (see DECISIONS.md); assertions after the wait are unchanged.
 */
export async function waitUntil(
    condition: () => Promise<boolean>,
    timeoutMillis = 15_000,
    description = "condition",
): Promise<void> {
    const deadline = Date.now() + timeoutMillis * CI_TIMEOUT_MULTIPLIER
    while (Date.now() < deadline) {
        if (await condition()) {
            return
        }
        await sleep(20)
    }
    throw new Error(`Timed out waiting for ${description}`)
}

/**
 * Waits (best-effort, bounded) until the event log is TERMINAL: for every cooperation lineage,
 * the latest "start" event (SEEN or ROLLING_BACK) is followed by a terminal event
 * (COMMITTED / ROLLED_BACK / ROLLBACK_FAILED). Comparing latest-start vs latest-terminal (not
 * mere existence) matters for rollback requests: a lineage that COMMITTED and is later rolled
 * back re-enters via a fresh ROLLING_BACK, and its earlier COMMITTED must not count as settled.
 *
 * This is the deterministic replacement for the original's fixed post-latch settle sleeps
 * (`Thread.sleep(100)` etc.) before asserting on the final event log: the latch fires inside the
 * last step body, but the saga's terminal event is only written on a LATER tick, so a fixed
 * sleep races it under load (the original README acknowledges this flakiness). On timeout this
 * returns instead of throwing, so a genuinely wrong end-state still fails through the test's own
 * assertion with its informative diff. See DECISIONS.md.
 */
export async function eventLogSettled(
    sql: Sql,
    timeoutMillis = 15_000,
): Promise<void> {
    const deadline = Date.now() + timeoutMillis * CI_TIMEOUT_MULTIPLIER
    while (Date.now() < deadline) {
        // Event ids are UUIDv7, so max(id) per group is the latest event in wall-clock order.
        const [row] = await sql`
            SELECT COUNT(*)::int AS unsettled
            FROM (
                SELECT
                    max(id::text) FILTER (WHERE type IN ('SEEN', 'ROLLING_BACK')) AS last_start,
                    max(id::text) FILTER (
                        WHERE type IN ('COMMITTED', 'ROLLED_BACK', 'ROLLBACK_FAILED')
                    ) AS last_terminal
                FROM message_event
                GROUP BY cooperation_lineage
            ) per_lineage
            WHERE last_start IS NOT NULL
              AND (last_terminal IS NULL OR last_terminal < last_start)
        `
        if (Number(row!.unsettled) === 0) {
            return
        }
        await sleep(20)
    }
}

/**
 * The per-file test fixture — the analog of the Kotlin `StructuredCooperationTest` base class
 * (DI container + @BeforeEach TRUNCATE). One postgres.js pool per test file (the container is
 * shared for the whole run; see scripts/run-tests.ts), TRUNCATE before each test with ticks
 * paused (TRUNCATE's AccessExclusiveLock would deadlock with a tick's AccessShareLock).
 */
export class ScoopHarness {
    sql!: Sql
    scoop!: Scoop
    messageQueue!: PostgresMessageQueue
    topicNotifier!: PostgresTopicNotifier
    readonly jsonbHelper = new JsonbHelper()

    readonly rootTopic = "root-topic"
    readonly childTopic = "child-topic"
    readonly grandchildTopic = "grandchild-topic"

    private readonly openSubscriptions = new Set<Subscription>()

    /**
     * Subscribe with automatic cleanup at file teardown (tests still close explicitly). Awaits
     * [Subscription.ready] so tests can publish immediately after subscribing without racing the
     * LISTEN registration (the polling safety net would still deliver, but only on its schedule
     * — far past the tests' latch timeouts; see DECISIONS.md).
     */
    async subscribe(
        topic: string,
        saga: DistributedCoroutine,
        instances = 1,
    ): Promise<Subscription> {
        const subscription = this.messageQueue.subscribe(topic, saga, instances)
        this.openSubscriptions.add(subscription)
        await subscription.ready()
        return {
            ready: () => subscription.ready(),
            close: async () => {
                this.openSubscriptions.delete(subscription)
                await subscription.close()
            },
        }
    }

    async closeAllSubscriptions(): Promise<void> {
        for (const subscription of [...this.openSubscriptions]) {
            await subscription.close()
        }
        this.openSubscriptions.clear()
    }

    async cleanupDatabase(): Promise<void> {
        // Pause the always-on internal sleep-handler subscription's periodic tick before
        // TRUNCATE; sleeping slightly longer than the tick interval lets any in-flight tick
        // drain before TRUNCATE runs.
        this.messageQueue.pauseTicks()
        try {
            await sleep(60)
            await this.sql`TRUNCATE TABLE message_event, message, return_value CASCADE`
        } finally {
            this.messageQueue.resumeTicks()
        }
    }
}

/** Registers before/beforeEach/after hooks for the current test file and returns the fixture. */
export function setupScoopTest(options: { tickIntervalMillis?: number } = {}): ScoopHarness {
    const harness = new ScoopHarness()

    before(async () => {
        const url = process.env.DATABASE_URL
        if (!url) {
            throw new Error("DATABASE_URL not set — run tests via `npm test` (scripts/run-tests.ts)")
        }
        harness.sql = postgres(url, { max: 30 })
        // The engine's authoritative clock is Postgres; align the injected client clock with it
        // (Docker VM clocks drift relative to the host — see DECISIONS.md).
        const offset = await calibrateClockToDatabase(harness.sql)
        if (Math.abs(offset) > 50) {
            console.log(`[harness] db-host clock offset: ${Math.round(offset)}ms (calibrated)`)
        }
        harness.topicNotifier = new PostgresTopicNotifier(harness.sql)
        harness.scoop = Scoop.create(harness.sql, {
            topicNotifier: harness.topicNotifier,
            tickIntervalMillis: options.tickIntervalMillis,
            // Tests allow sagas ~10s to finish. Any missed wake signal (NOTIFY raced by an
            // in-flight LISTEN registration or a listen-connection hiccup) is repaired by the
            // reconcile safety net, whose production default (30s) is deliberately lazy. The
            // system's contract is that notification delivery affects only latency, never
            // correctness — so tests run the sweep at 2s to keep worst-case recovery well
            // inside their latch windows (see DECISIONS.md).
            reconcileSafetyNetMillis: 2_000,
        })
        harness.messageQueue = harness.scoop.messageQueue
        await harness.scoop.ready()
    })

    beforeEach(async () => {
        await harness.cleanupDatabase()
    })

    after(async () => {
        await harness.closeAllSubscriptions()
        await harness.scoop.close()
        await harness.sql.end({ timeout: 5 })
    })

    return harness
}
