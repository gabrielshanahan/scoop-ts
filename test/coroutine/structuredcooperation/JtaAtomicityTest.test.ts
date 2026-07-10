import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { beforeEach, describe, test } from "node:test"
import type { TransactionSql } from "postgres"
import { saga } from "../../../src/coroutine/builder/SagaBuilder.js"
import { eventLoopStrategy } from "../../../src/messaging/HandlerRegistry.js"
import { transactional } from "../../../src/coroutine/TransactionRunner.js"
import { ciSleep, setupScoopTest } from "../../support/harness.js"
import { CountDownLatch } from "../../support/latch.js"

const h = setupScoopTest()

/**
 * Proves that a business write performed inside a saga step is atomic with scoop's own
 * `message_event` writes — both are part of the one per-step transaction.
 *
 * The Kotlin original asserts this for the JTA seam (a `@Transactional` bean whose connection
 * Agroal enlists in the step's Narayana transaction). On this stack the equivalent guarantee is
 * the `sql.begin()` per-step transaction: business code runs on the step's connection
 * (`scope.connection`) and commits or rolls back with it (mapping recorded in PORT-LEDGER.md).
 */

/** Business writer using the step's transaction handle — the analog of the @Transactional bean. */
const probeWriter = {
    async write(connection: TransactionSql, id: string): Promise<void> {
        await connection`INSERT INTO scoop_jta_probe (id) VALUES (${id})`
    },
}

async function probeRowExists(id: string): Promise<boolean> {
    const rows = await h.sql`SELECT 1 FROM scoop_jta_probe WHERE id = ${id}`
    return rows.length > 0
}

async function eventTypesFor(coroutineName: string): Promise<string[]> {
    const rows = await h.sql`
        SELECT type FROM message_event WHERE coroutine_name = ${coroutineName} ORDER BY created_at
    `
    return rows.map(row => row.type as string)
}

describe("JtaAtomicityTest", () => {
    beforeEach(async () => {
        await h.sql`CREATE TABLE IF NOT EXISTS scoop_jta_probe (id text PRIMARY KEY)`
        await h.sql`TRUNCATE TABLE scoop_jta_probe`
    })

    test("business write inside a step commits atomically with scoop events", async () => {
        const probeId = `commit-${randomUUID()}`
        const latch = new CountDownLatch(1)

        const subscription = h.subscribe(
            h.rootTopic,
            saga("jta-commit-handler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: async (scope, _message) => {
                        // A child launch ensures the step's connection (used by scope.launch)
                        // is the same transaction-bound connection as the business write.
                        await scope.launch(h.childTopic, { from: "jta" })
                        await probeWriter.write(scope.connection, probeId)
                        latch.countDown()
                    },
                })
            }),
        )

        const childSubscription = h.subscribe(
            h.childTopic,
            saga("jta-commit-child", eventLoopStrategy(h.messageQueue), b => {
                b.step({ invoke: () => {} })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
            })

            assert.ok(
                await latch.await(10_000),
                "Step did not run (a transaction-enlistment failure would prevent it)",
            )
            await ciSleep(500)

            // (a) the step ran and committed
            // (b) the business row is present
            assert.ok(
                await probeRowExists(probeId),
                "Business write should have committed with the step",
            )
            // (c) scoop's SUSPENDED + COMMITTED events for this saga exist
            const events = await eventTypesFor("jta-commit-handler")
            assert.ok(events.includes("SUSPENDED"), `Expected a SUSPENDED event, got ${events}`)
            assert.ok(events.includes("COMMITTED"), `Expected a COMMITTED event, got ${events}`)
        } finally {
            await subscription.close()
            await childSubscription.close()
        }
    })

    test("business write rolls back together with the step when the step throws", async () => {
        const probeId = `rollback-${randomUUID()}`
        const latch = new CountDownLatch(1)

        const subscription = h.subscribe(
            h.rootTopic,
            saga("jta-rollback-handler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: async (scope, _message) => {
                        await probeWriter.write(scope.connection, probeId)
                        latch.countDown()
                        throw new SimulatedStepFailure("Simulated failure after business write")
                    },
                    rollback: (_scope, _message, _throwable) => {},
                })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
            })

            assert.ok(await latch.await(10_000), "Step did not run")
            await ciSleep(500)

            // The business write and scoop's step tx are one unit: since the step threw,
            // the business row must have been rolled back with it.
            assert.ok(
                !(await probeRowExists(probeId)),
                "Business write should have rolled back with the failed step",
            )
        } finally {
            await subscription.close()
        }
    })
})

/** Dedicated exception so a step can fail deterministically. */
class SimulatedStepFailure extends Error {
    constructor(message: string) {
        super(message)
        this.name = "SimulatedStepFailure"
    }
}
