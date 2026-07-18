import assert from "node:assert/strict"
import { describe, test } from "node:test"
import { saga } from "../../../src/coroutine/builder/SagaBuilder.js"
import { repairSettledFlags } from "../../../src/coroutine/structuredcooperation/SettledFlag.js"
import { transactional } from "../../../src/coroutine/TransactionRunner.js"
import { eventLoopStrategy } from "../../../src/messaging/HandlerRegistry.js"
import { eventLogSettled, setupScoopTest, waitUntil } from "../../support/harness.js"
import { CountDownLatch } from "../../support/latch.js"

const h = setupScoopTest()

/**
 * The settled_at stamp (V6__seen_settled_flag, DECISIONS.md): terminal events settle the SEEN in
 * the same statement, ROLLING_BACK re-activates it, and dispatch only scans unsettled rows. The
 * unsettle path is load-bearing for the whole rollback suite — a SEEN that stayed settled would
 * never be dispatched for rollback and RollbackPathTest would hang — so these tests only pin the
 * at-rest states and the repair sweep.
 */
describe("SettledFlagTest", () => {
    async function seenFlags(): Promise<Array<{ name: string; settled: boolean }>> {
        const rows = await h.sql`
            SELECT coroutine_name AS name, settled_at IS NOT NULL AS settled
            FROM message_event WHERE type = 'SEEN' ORDER BY coroutine_name
        `
        return rows.map(r => ({ name: r.name as string, settled: Boolean(r.settled) }))
    }

    test("a committed saga's SEEN is stamped settled", async () => {
        const latch = new CountDownLatch(1)
        const subscription = await h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue, h.strategyEpoch), b => {
                b.step({ invoke: () => latch.countDown() })
            }),
        )
        try {
            await transactional(h.sql, connection =>
                h.messageQueue.launch(connection, h.rootTopic, { initial: "true" }),
            )
            assert.ok(await latch.await(10_000), "saga did not run")
            await waitUntil(
                async () => (await seenFlags()).every(f => f.settled),
                10_000,
                "SEEN to be stamped settled after COMMITTED",
            )
        } finally {
            await subscription.close()
        }
    })

    test("a failed saga's SEEN ends settled via ROLLED_BACK", async () => {
        const latch = new CountDownLatch(1)
        const subscription = await h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue, h.strategyEpoch), b => {
                b.step({
                    invoke: () => {
                        latch.countDown()
                        throw new Error("boom")
                    },
                })
            }),
        )
        try {
            await transactional(h.sql, connection =>
                h.messageQueue.launch(connection, h.rootTopic, { initial: "true" }),
            )
            assert.ok(await latch.await(10_000), "saga did not run")
            await waitUntil(
                async () => {
                    const [row] = await h.sql`
                        SELECT count(*)::int AS n FROM message_event WHERE type = 'ROLLED_BACK'
                    `
                    return Number(row!.n) >= 1
                },
                10_000,
                "saga to roll back",
            )
            await waitUntil(
                async () => (await seenFlags()).every(f => f.settled),
                10_000,
                "SEEN to be stamped settled after ROLLED_BACK",
            )
        } finally {
            await subscription.close()
        }
    })

    test("a post-commit rollback request re-activates and then re-settles the hierarchy", async () => {
        const latch = new CountDownLatch(2)
        const rollbackLatch = new CountDownLatch(1)
        const rootSubscription = await h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue, h.strategyEpoch), b => {
                b.step({
                    invoke: async (scope, _message) => {
                        await scope.launch(h.childTopic, { from: "root-handler" })
                        latch.countDown()
                    },
                    rollback: (_scope, _message, _throwable) => {
                        rollbackLatch.countDown()
                    },
                })
            }),
        )
        const childSubscription = await h.subscribe(
            h.childTopic,
            saga("child-handler", eventLoopStrategy(h.messageQueue, h.strategyEpoch), b => {
                b.step({ invoke: () => latch.countDown() })
            }),
        )
        try {
            const cooperationRoot = await transactional(h.sql, connection =>
                h.messageQueue.launch(connection, h.rootTopic, { initial: "true" }),
            )
            assert.ok(await latch.await(10_000), "hierarchy did not run")
            await waitUntil(
                async () => {
                    const flags = await seenFlags()
                    return flags.length === 2 && flags.every(f => f.settled)
                },
                10_000,
                "both SEENs settled after commit",
            )

            await transactional(h.sql, async connection => {
                await h.scoop.capabilities.rollback(
                    connection,
                    cooperationRoot.cooperationScopeIdentifier,
                    "master-system",
                    "settled-flag-test",
                )
            })
            assert.ok(await rollbackLatch.await(10_000), "rollback did not run")
            await eventLogSettled(h.sql)
            // The rollback ran at all — which proves ROLLING_BACK cleared the stamp (dispatch
            // only scans unsettled rows) — and the terminal ROLLED_BACK re-settled it.
            await waitUntil(
                async () => (await seenFlags()).every(f => f.settled),
                10_000,
                "SEENs re-settled after rollback completed",
            )
        } finally {
            await childSubscription.close()
            await rootSubscription.close()
        }
    })

    test("repairSettledFlags restores both drift directions from the event log", async () => {
        const latch = new CountDownLatch(1)
        const subscription = await h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue, h.strategyEpoch), b => {
                b.step({ invoke: () => latch.countDown() })
            }),
        )
        try {
            await transactional(h.sql, connection =>
                h.messageQueue.launch(connection, h.rootTopic, { initial: "true" }),
            )
            assert.ok(await latch.await(10_000), "saga did not run")
            await waitUntil(
                async () => (await seenFlags()).every(f => f.settled),
                10_000,
                "SEEN settled after commit",
            )

            // Corrupt in the unsettle direction (settled saga wrongly marked active)...
            await h.sql`UPDATE message_event SET settled_at = NULL WHERE type = 'SEEN'`
            let result = await transactional(h.sql, connection => repairSettledFlags(connection))
            assert.equal(result.settled, 1, "repair re-settles the finished saga")
            assert.equal(result.unsettled, 0)
            assert.ok((await seenFlags()).every(f => f.settled))

            // ...and in the settle direction (an active-looking saga wrongly marked settled):
            // fake an unfinished saga by deleting its terminal event.
            await h.sql`DELETE FROM message_event WHERE type = 'COMMITTED'`
            result = await transactional(h.sql, connection => repairSettledFlags(connection))
            assert.equal(result.unsettled, 1, "repair re-activates the unfinished saga")
            assert.equal(result.settled, 0)
            assert.ok((await seenFlags()).every(f => !f.settled))
        } finally {
            await subscription.close()
        }
    })
})
