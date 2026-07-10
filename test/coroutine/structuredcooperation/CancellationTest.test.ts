import assert from "node:assert/strict"
import { describe, test } from "node:test"
import { saga } from "../../../src/coroutine/builder/SagaBuilder.js"
import { eventLoopStrategy } from "../../../src/messaging/HandlerRegistry.js"
import { transactional } from "../../../src/coroutine/TransactionRunner.js"
import { ciSleep, setupScoopTest } from "../../support/harness.js"
import { CountDownLatch } from "../../support/latch.js"
import {
    asSource,
    assertEquivalent,
    fetchExceptions,
    getEventSequence,
    triple,
} from "../../support/util.js"

const h = setupScoopTest()

describe("CancellationTest", () => {
    test("cancellation works", async () => {
        const executionOrder: string[] = []

        const latch = new CountDownLatch(3)
        const childIsExecuting = new CountDownLatch(1)
        const cancellation = new CountDownLatch(1)

        const rootHandlerCoroutine = saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
            b.step({
                invoke: async (scope, _message) => {
                    await scope.launch(h.childTopic, { from: "root-handler" })
                    latch.countDown()
                    executionOrder.push("root-handler-step-1")
                },
                rollback: (_scope, _message, _throwable) => {
                    latch.countDown()
                    executionOrder.push("root-handler-rollback-step-1")
                },
            })
        })
        const rootSubscription = h.subscribe(h.rootTopic, rootHandlerCoroutine)

        const childHandlerCoroutine = saga(
            "child-handler",
            eventLoopStrategy(h.messageQueue),
            b => {
                b.step({
                    invoke: async (_scope, _message) => {
                        childIsExecuting.countDown()
                        await ciSleep(100)
                        latch.countDown()
                        executionOrder.push("child-handler-step-1")
                        await cancellation.await(60_000)
                    },
                })
            },
        )
        const childSubscription = h.subscribe(h.childTopic, childHandlerCoroutine)

        try {
            const cooperationRoot = await transactional(h.sql, connection =>
                h.messageQueue.launch(connection, h.rootTopic, { initial: "true" }),
            )

            await childIsExecuting.await(60_000)
            await transactional(h.sql, async connection => {
                await h.scoop.capabilities.cancel(
                    connection,
                    cooperationRoot.cooperationScopeIdentifier,
                    "master-system",
                    "feelz",
                )
            })
            cancellation.countDown()

            assert.ok(await latch.await(10_000), `Latch count is ${latch.getCount()}`)
            await ciSleep(100)

            assert.equal(executionOrder.length, 3, "Not everything completed correctly")
            assert.deepEqual(
                executionOrder,
                [
                    "root-handler-step-1",
                    "child-handler-step-1",
                    "root-handler-rollback-step-1",
                ],
                "Execution order obeys structured cooperation rules",
            )

            assert.deepEqual(await getEventSequence(h.sql), [
                triple("EMITTED", null, null),
                triple("SEEN", null, "root-handler"),
                triple("EMITTED", "0", "root-handler"),
                triple("SUSPENDED", "0", "root-handler"),
                triple("SEEN", null, "child-handler"),
                triple("CANCELLATION_REQUESTED", null, null),
                triple("ROLLING_BACK", "0", "child-handler"),
                triple("ROLLED_BACK", "Rollback of 0[0,]", "child-handler"),
                triple("ROLLING_BACK", "0", "root-handler"),
                triple("ROLLBACK_EMITTED", "Rollback of 0[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of 0[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of 0[0,]", "root-handler"),
                triple("ROLLED_BACK", "Rollback of 0[0,]", "root-handler"),
            ])

            const cancellationExceptions = await fetchExceptions(
                h.sql,
                h.jsonbHelper,
                "CANCELLATION_REQUESTED",
                null,
            )

            assertEquivalent(
                [
                    {
                        message: "[master-system] CancellationRequestedException: feelz",
                        type: "CancellationRequestedException",
                        source: "master-system",
                    },
                ],
                cancellationExceptions,
            )

            const childHandlerRollingBackExceptions = await fetchExceptions(
                h.sql,
                h.jsonbHelper,
                "ROLLING_BACK",
                "child-handler",
            )

            assertEquivalent(
                [
                    {
                        message: `[${asSource(childHandlerCoroutine.identifier)}] GaveUpException: <no message>`,
                        type: "GaveUpException",
                        source: asSource(childHandlerCoroutine.identifier),
                        causes: [
                            {
                                message: "[master-system] CancellationRequestedException: feelz",
                                type: "CancellationRequestedException",
                                source: "master-system",
                            },
                        ],
                    },
                ],
                childHandlerRollingBackExceptions,
            )

            const rootHandlerRollingBackExceptions = await fetchExceptions(
                h.sql,
                h.jsonbHelper,
                "ROLLING_BACK",
                "root-handler",
            )

            assertEquivalent(
                [
                    {
                        message: `[${asSource(rootHandlerCoroutine.identifier)}] GaveUpException: <no message>`,
                        type: "GaveUpException",
                        source: asSource(rootHandlerCoroutine.identifier),
                        causes: [
                            {
                                message: "[master-system] CancellationRequestedException: feelz",
                                type: "CancellationRequestedException",
                                source: "master-system",
                            },
                        ],
                    },
                ],
                rootHandlerRollingBackExceptions,
            )
        } finally {
            await rootSubscription.close()
            await childSubscription.close()
        }
    })

    test("cancellation after everything has finished running has no effect", async () => {
        const executionOrder: string[] = []

        const latch = new CountDownLatch(2)

        const rootSubscription = h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: async (scope, _message) => {
                        await scope.launch(h.childTopic, { from: "root-handler" })
                        latch.countDown()
                        executionOrder.push("root-handler-step-1")
                    },
                })
            }),
        )

        const childSubscription = h.subscribe(
            h.childTopic,
            saga("child-handler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: async (_scope, _message) => {
                        await ciSleep(100)
                        latch.countDown()
                        executionOrder.push("child-handler-step-1")
                    },
                })
            }),
        )

        try {
            const cooperationRoot = await transactional(h.sql, connection =>
                h.messageQueue.launch(connection, h.rootTopic, { initial: "true" }),
            )

            assert.ok(await latch.await(10_000), "Not everything completed correctly")
            await ciSleep(100)

            await transactional(h.sql, async connection => {
                await h.scoop.capabilities.cancel(
                    connection,
                    cooperationRoot.cooperationScopeIdentifier,
                    "master-system",
                    "feelz",
                )
            })

            assert.equal(executionOrder.length, 2, "Not everything completed correctly")
            assert.deepEqual(
                executionOrder,
                ["root-handler-step-1", "child-handler-step-1"],
                "Execution order obeys structured cooperation rules",
            )

            assert.deepEqual(await getEventSequence(h.sql), [
                triple("EMITTED", null, null),
                triple("SEEN", null, "root-handler"),
                triple("EMITTED", "0", "root-handler"),
                triple("SUSPENDED", "0", "root-handler"),
                triple("SEEN", null, "child-handler"),
                triple("SUSPENDED", "0", "child-handler"),
                triple("COMMITTED", "0", "child-handler"),
                triple("COMMITTED", "0", "root-handler"),
                triple("CANCELLATION_REQUESTED", null, null),
            ])
        } finally {
            await rootSubscription.close()
            await childSubscription.close()
        }
    })
})
