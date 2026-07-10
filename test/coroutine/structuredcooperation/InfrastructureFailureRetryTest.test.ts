import assert from "node:assert/strict"
import { describe, test } from "node:test"
import { saga } from "../../../src/coroutine/builder/SagaBuilder.js"
import { ScoopInfrastructureException } from "../../../src/coroutine/ScoopInfrastructureException.js"
import { eventLoopStrategy } from "../../../src/messaging/HandlerRegistry.js"
import { transactional } from "../../../src/coroutine/TransactionRunner.js"
import { ciSleep, setupScoopTest } from "../../support/harness.js"
import { CountDownLatch } from "../../support/latch.js"
import { getEventSequence } from "../../support/util.js"

const h = setupScoopTest()

/** A named business exception so the "still rolls back" test does not throw a generic one. */
class SimulatedBusinessFailure extends Error {
    constructor(message: string) {
        super(message)
        this.name = "SimulatedBusinessFailure"
    }
}

/**
 * Covers the core property that keeps perpetual sagas alive across transient infrastructure
 * faults: a ScoopInfrastructureException raised while a step is being processed must NOT roll the
 * saga back — the run is left at its last committed step and retried on a later tick. A plain
 * business exception, by contrast, must still roll back exactly as before.
 */
describe("InfrastructureFailureRetryTest", () => {
    test("a ScoopInfrastructureException is retried (not rolled back) and the run eventually commits", async () => {
        let attempts = 0
        const committed = new CountDownLatch(1)

        const rootHandler = "root-handler"
        const rootSubscription = h.subscribe(
            h.rootTopic,
            saga(rootHandler, eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: (_scope, _message) => {
                        const attempt = ++attempts
                        // Fail the first two attempts the way a dead connection would, then
                        // succeed.
                        if (attempt < 3) {
                            throw new ScoopInfrastructureException(
                                new Error(`simulated dead connection #${attempt}`),
                            )
                        }
                        committed.countDown()
                    },
                })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
            })

            assert.ok(
                await committed.await(20_000),
                "the run should eventually succeed by retrying",
            )
            await ciSleep(500)

            // Two transient failures, then a success.
            assert.equal(attempts, 3)

            const types = (await getEventSequence(h.sql)).map(([type]) => type)
            assert.ok(
                !types.includes("ROLLING_BACK"),
                `an infrastructure failure must never enter ROLLING_BACK: ${types}`,
            )
            assert.ok(
                !types.includes("ROLLED_BACK"),
                `an infrastructure failure must never roll back: ${types}`,
            )
            assert.ok(
                types.includes("COMMITTED"),
                `the run should commit after a successful retry: ${types}`,
            )
        } finally {
            await rootSubscription.close()
        }
    })

    test("a plain business exception still rolls back and is not retried", async () => {
        let attempts = 0

        const rootHandler = "root-handler"
        const rootSubscription = h.subscribe(
            h.rootTopic,
            saga(rootHandler, eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: (_scope, _message) => {
                        attempts++
                        throw new SimulatedBusinessFailure("business failure")
                    },
                })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
            })

            const deadline = Date.now() + 15_000
            while (
                Date.now() < deadline &&
                !(await getEventSequence(h.sql)).some(([type]) => type === "ROLLED_BACK")
            ) {
                await ciSleep(200)
            }
            await ciSleep(500)

            const types = (await getEventSequence(h.sql)).map(([type]) => type)
            assert.ok(
                types.includes("ROLLING_BACK"),
                `a business failure should enter ROLLING_BACK: ${types}`,
            )
            assert.ok(
                types.includes("ROLLED_BACK"),
                `a business failure should roll back: ${types}`,
            )
            // A business failure is not retried — the step ran exactly once.
            assert.equal(attempts, 1)
        } finally {
            await rootSubscription.close()
        }
    })
})
