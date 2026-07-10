import assert from "node:assert/strict"
import { describe, test } from "node:test"
import { saga } from "../../../src/coroutine/builder/SagaBuilder.js"
import {
    has,
    MappedElement,
    MappedKey,
} from "../../../src/coroutine/context/CooperationContext.js"
import type { CooperationScopeIdentifier } from "../../../src/coroutine/CooperationScopeIdentifier.js"
import { eventLoopStrategy } from "../../../src/messaging/HandlerRegistry.js"
import { transactional } from "../../../src/coroutine/TransactionRunner.js"
import { ciSleep, setupScoopTest, waitUntil } from "../../support/harness.js"
import { CountDownLatch } from "../../support/latch.js"
import {
    asSource,
    assertEquivalent,
    fetchExceptions,
    getEventSequence,
    keepOnlyHandlers,
    keepOnlyPrefixedBy,
    triple,
} from "../../support/util.js"

const h = setupScoopTest()

const TriedAgainKey = new MappedKey<TriedAgainValue>("TriedAgainKey", () => TriedAgainValue)

class TriedAgainValueClass extends MappedElement {
    constructor() {
        super(TriedAgainKey)
    }
}

const TriedAgainValue = new TriedAgainValueClass()

describe("RollbackPathTest", () => {
    test("a handler failing in its first step should never emit what is in the step and not call rollback() (since the transaction wasn't committed)", async () => {
        const latch = new CountDownLatch(1)
        const executionOrder: string[] = []

        const rootHandler = "root-handler"
        const rootSubscription = h.subscribe(
            h.rootTopic,
            saga(rootHandler, eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: async (scope, _message) => {
                        await scope.launch(h.childTopic, { from: rootHandler })
                        executionOrder.push("root-handler-step-1")
                        latch.countDown()
                        throw new Error("Simulated failure to test rollback")
                    },
                    rollback: (_scope, _message, _throwable) => {
                        executionOrder.push("root-handler-rollback-step-1")
                    },
                    handleChildFailures: (_scope, _message, throwable) => {
                        executionOrder.push("root-handler-handleChildFailures-step-1")
                        throw throwable
                    },
                })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
            })

            assert.ok(await latch.await(10_000))
            await ciSleep(500)

            assert.deepEqual(await getEventSequence(h.sql), [
                triple("EMITTED", null, null),
                triple("SEEN", null, "root-handler"),
                triple("ROLLING_BACK", "0", "root-handler"),
                triple("ROLLED_BACK", "Rollback of 0[0,]", "root-handler"),
            ])
            assert.deepEqual(executionOrder, ["root-handler-step-1"])
        } finally {
            await rootSubscription.close()
        }
    })

    test("a handler failing in its second step should emit ROLLBACK_EMITTEDs for messages emitted in the first step, and then roll it back", async () => {
        const latch = new CountDownLatch(3)
        const executionOrder: string[] = []

        const rootHandler = "root-handler"
        const rootSubscription = h.subscribe(
            h.rootTopic,
            saga(rootHandler, eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: async (scope, _message) => {
                        await scope.launch(h.childTopic, { from: rootHandler })
                        executionOrder.push("root-handler-step-1")
                        latch.countDown()
                    },
                    rollback: (_scope, _message, _throwable) => {
                        executionOrder.push("root-handler-rollback-step-1")
                        latch.countDown()
                    },
                    handleChildFailures: (_scope, _message, throwable) => {
                        executionOrder.push("root-handler-handleChildFailures-step-1")
                        throw throwable
                    },
                })

                b.step({
                    invoke: async (scope, _message) => {
                        await scope.launch(h.childTopic, { from: rootHandler })
                        executionOrder.push("root-handler-step-2")
                        latch.countDown()
                        throw new Error("Simulated failure to test rollback")
                    },
                    rollback: (_scope, _message, _throwable) => {
                        executionOrder.push("root-handler-rollback-step-2")
                    },
                    handleChildFailures: (_scope, _message, throwable) => {
                        executionOrder.push("root-handler-handleChildFailures-step-2")
                        throw throwable
                    },
                })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
            })

            assert.ok(await latch.await(100_000), "Not everything completed correctly")
            await ciSleep(100)

            assert.deepEqual(await getEventSequence(h.sql), [
                triple("EMITTED", null, null),
                triple("SEEN", null, "root-handler"),
                triple("EMITTED", "0", "root-handler"),
                triple("SUSPENDED", "0", "root-handler"),
                triple("ROLLING_BACK", "1", "root-handler"),
                triple("ROLLBACK_EMITTED", "Rollback of 0[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of 0[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of 0[0,]", "root-handler"),
                triple("ROLLED_BACK", "Rollback of 0[0,]", "root-handler"),
            ])

            assert.deepEqual(executionOrder, [
                "root-handler-step-1",
                "root-handler-step-2",
                "root-handler-rollback-step-1",
            ])
        } finally {
            await rootSubscription.close()
        }
    })

    test("when a child fails, rollbacks happen in reverse order", async () => {
        const latch = new CountDownLatch(3)
        const executionOrder: string[] = []

        const rootHandler = "root-handler"
        const rootSubscription = h.subscribe(
            h.rootTopic,
            saga(rootHandler, eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: async (scope, _message) => {
                        await scope.launch(h.childTopic, { from: rootHandler })
                        executionOrder.push("root-handler-step-1")
                        latch.countDown()
                    },
                    rollback: (_scope, _message, _throwable) => {
                        executionOrder.push("root-handler-rollback-step-1")
                        latch.countDown()
                    },
                    handleChildFailures: (_scope, _message, throwable) => {
                        executionOrder.push("root-handler-handleChildFailures-step-1")
                        throw throwable
                    },
                })
            }),
        )

        const childHandler = "child-handler"
        const childSubscription = h.subscribe(
            h.childTopic,
            saga(childHandler, eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("child-handler-step-1")
                        latch.countDown()
                    },
                    rollback: (_scope, _message, _throwable) => {
                        executionOrder.push("child-handler-rollback-step-1")
                        latch.countDown()
                    },
                    handleChildFailures: (_scope, _message, throwable) => {
                        executionOrder.push("child-handler-handleChildFailures-step-1")
                        throw throwable
                    },
                })

                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("child-handler-step-2")
                        latch.countDown()
                        throw new Error("Simulated failure to test rollback")
                    },
                    rollback: (_scope, _message, _throwable) => {
                        executionOrder.push("child-handler-rollback-step-2")
                    },
                    handleChildFailures: (_scope, _message, throwable) => {
                        executionOrder.push("child-handler-handleChildFailures-step-2")
                        throw throwable
                    },
                })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
            })

            assert.ok(await latch.await(10_000), "Not everything completed correctly")
            await ciSleep(700)

            assert.deepEqual(await getEventSequence(h.sql), [
                triple("EMITTED", null, null),
                triple("SEEN", null, "root-handler"),
                triple("EMITTED", "0", "root-handler"),
                triple("SUSPENDED", "0", "root-handler"),
                triple("SEEN", null, "child-handler"),
                triple("SUSPENDED", "0", "child-handler"),
                triple("ROLLING_BACK", "1", "child-handler"),
                triple("SUSPENDED", "Rollback of 0[0,] (rolling back child scopes)", "child-handler"),
                triple("SUSPENDED", "Rollback of 0[0,]", "child-handler"),
                triple("ROLLED_BACK", "Rollback of 0[0,]", "child-handler"),
                triple("ROLLING_BACK", "0", "root-handler"),
                triple("ROLLBACK_EMITTED", "Rollback of 0[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of 0[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of 0[0,]", "root-handler"),
                triple("ROLLED_BACK", "Rollback of 0[0,]", "root-handler"),
            ])

            assert.deepEqual(executionOrder, [
                "root-handler-step-1",
                "child-handler-step-1",
                "child-handler-step-2",
                "child-handler-rollback-step-1",
                "root-handler-handleChildFailures-step-1",
                "root-handler-rollback-step-1",
            ])
        } finally {
            await childSubscription.close()
            await rootSubscription.close()
        }
    })

    test("when a later step fails, previous emissions are rolled back", async () => {
        const latch = new CountDownLatch(7)
        const executionOrder: string[] = []

        const rootHandler = "root-handler"
        const rootSubscription = h.subscribe(
            h.rootTopic,
            saga(rootHandler, eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: async (scope, _message) => {
                        await scope.launch(h.childTopic, { from: rootHandler })
                        executionOrder.push("root-handler-step-1")
                        latch.countDown()
                    },
                    rollback: (_scope, _message, _throwable) => {
                        executionOrder.push("root-handler-rollback-step-1")
                        latch.countDown()
                    },
                    handleChildFailures: (_scope, _message, throwable) => {
                        executionOrder.push("root-handler-handleChildFailures-step-1")
                        throw throwable
                    },
                })

                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("root-handler-step-2")
                        latch.countDown()
                        throw new Error("Simulated failure to test rollback")
                    },
                    rollback: (_scope, _message, _throwable) => {
                        executionOrder.push("root-handler-rollback-step-2")
                    },
                    handleChildFailures: (_scope, _message, throwable) => {
                        executionOrder.push("root-handler-handleChildFailures-step-2")
                        throw throwable
                    },
                })
            }),
        )

        const childHandler = "child-handler"
        const childSubscription = h.subscribe(
            h.childTopic,
            saga(childHandler, eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("child-handler-step-1")
                        latch.countDown()
                    },
                    rollback: (_scope, _message, _throwable) => {
                        executionOrder.push("child-handler-rollback-step-1")
                        latch.countDown()
                    },
                    handleChildFailures: (_scope, _message, throwable) => {
                        executionOrder.push("child-handler-handleChildFailures-step-1")
                        throw throwable
                    },
                })

                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("child-handler-step-2")
                        latch.countDown()
                    },
                    rollback: (_scope, _message, _throwable) => {
                        executionOrder.push("child-handler-rollback-step-2")
                        latch.countDown()
                    },
                    handleChildFailures: (_scope, _message, throwable) => {
                        executionOrder.push("child-handler-handleChildFailures-step-2")
                        throw throwable
                    },
                })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
            })

            assert.ok(await latch.await(10_000), "Not everything completed correctly")
            await ciSleep(200)

            assert.deepEqual(await getEventSequence(h.sql), [
                triple("EMITTED", null, null),
                triple("SEEN", null, "root-handler"),
                triple("EMITTED", "0", "root-handler"),
                triple("SUSPENDED", "0", "root-handler"),
                triple("SEEN", null, "child-handler"),
                triple("SUSPENDED", "0", "child-handler"),
                triple("SUSPENDED", "1", "child-handler"),
                triple("COMMITTED", "1", "child-handler"),
                triple("ROLLING_BACK", "1", "root-handler"),
                triple("ROLLBACK_EMITTED", "Rollback of 0[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of 0[0,] (rolling back child scopes)", "root-handler"),
                triple("ROLLING_BACK", null, "child-handler"),
                triple("SUSPENDED", "Rollback of 1[0,] (rolling back child scopes)", "child-handler"),
                triple("SUSPENDED", "Rollback of 1[0,]", "child-handler"),
                triple("SUSPENDED", "Rollback of 0[0,] (rolling back child scopes)", "child-handler"),
                triple("SUSPENDED", "Rollback of 0[0,]", "child-handler"),
                triple("ROLLED_BACK", "Rollback of 0[0,]", "child-handler"),
                triple("SUSPENDED", "Rollback of 0[0,]", "root-handler"),
                triple("ROLLED_BACK", "Rollback of 0[0,]", "root-handler"),
            ])

            assert.deepEqual(executionOrder, [
                "root-handler-step-1",
                "child-handler-step-1",
                "child-handler-step-2",
                "root-handler-step-2",
                "child-handler-rollback-step-2",
                "child-handler-rollback-step-1",
                "root-handler-rollback-step-1",
            ])
        } finally {
            await childSubscription.close()
            await rootSubscription.close()
        }
    })

    test("rollbacks are well behaved n-deep", async () => {
        const executionOrder: string[] = []

        // (2 root steps + 3 child1 steps + 2 child2 steps + 2 grandchild steps) * 2
        // rollbacks - 1 child2_step2_rollback + 1 root handleChildFailures
        const latch = new CountDownLatch(18)

        const rootHandlerCoroutine = saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
            b.step({
                invoke: (_scope, _message) => {
                    latch.countDown()
                    executionOrder.push("root-handler-step-1")
                },
                rollback: (_scope, _message, _throwable) => {
                    executionOrder.push("root-handler-rollback-step-1")
                    latch.countDown()
                },
            })
            b.step({
                invoke: async (scope, _message) => {
                    await scope.launch(h.childTopic, { from: "root-handler" })
                    latch.countDown()
                    executionOrder.push("root-handler-step-2")
                },
                rollback: (_scope, _message, _throwable) => {
                    executionOrder.push("root-handler-rollback-step-2")
                    latch.countDown()
                },
                handleChildFailures: (_scope, _message, throwable) => {
                    executionOrder.push("root-handler-handleChildFailures-step-2")
                    latch.countDown()
                    throw throwable
                },
            })
        })
        const rootSubscription = h.subscribe(h.rootTopic, rootHandlerCoroutine)

        const childHandler1Coroutine = saga(
            "child-handler-1",
            eventLoopStrategy(h.messageQueue),
            b => {
                b.step({
                    invoke: (_scope, _message) => {
                        latch.countDown()
                        executionOrder.push("child-handler-1-step-1")
                    },
                    rollback: (_scope, _message, _throwable) => {
                        executionOrder.push("child-handler-1-rollback-step-1")
                        latch.countDown()
                    },
                })
                b.step({
                    invoke: async (scope, _message) => {
                        await scope.launch(h.grandchildTopic, { from: "child-handler-1" })
                        latch.countDown()
                        executionOrder.push("child-handler-1-step-2")
                    },
                    rollback: (_scope, _message, _throwable) => {
                        executionOrder.push("child-handler-1-rollback-step-2")
                        latch.countDown()
                    },
                })
                b.step({
                    invoke: (_scope, _message) => {
                        latch.countDown()
                        executionOrder.push("child-handler-1-step-3")
                    },
                    rollback: (_scope, _message, _throwable) => {
                        executionOrder.push("child-handler-1-rollback-step-3")
                        latch.countDown()
                    },
                })
            },
        )
        const childSubscription1 = h.subscribe(h.childTopic, childHandler1Coroutine)

        const childHandler2Coroutine = saga(
            "child-handler-2",
            eventLoopStrategy(h.messageQueue),
            b => {
                b.step({
                    invoke: (_scope, _message) => {
                        latch.countDown()
                        executionOrder.push("child-handler-2-step-1")
                    },
                    rollback: (_scope, _message, _throwable) => {
                        executionOrder.push("child-handler-2-rollback-step-1")
                        latch.countDown()
                    },
                })
                b.step({
                    invoke: (_scope, _message) => {
                        latch.countDown()
                        executionOrder.push("child-handler-2-step-2")
                        throw new Error("Simulated failure to test rollback")
                    },
                })
            },
        )
        const childSubscription2 = h.subscribe(h.childTopic, childHandler2Coroutine)

        const grandChildCoroutine = saga(
            "grandchild-handler",
            eventLoopStrategy(h.messageQueue),
            b => {
                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("grandchild-handler-step-1")
                        latch.countDown()
                    },
                    rollback: (_scope, _message, _throwable) => {
                        executionOrder.push("grandchild-handler-rollback-step-1")
                        latch.countDown()
                    },
                })
                b.step({
                    invoke: async (_scope, _message) => {
                        latch.countDown()
                        await ciSleep(200)
                        executionOrder.push("grandchild-handler-step-2")
                    },
                    rollback: (_scope, _message, _throwable) => {
                        executionOrder.push("grandchild-handler-rollback-step-2")
                        latch.countDown()
                    },
                })
            },
        )
        const grandchildSubscription = h.subscribe(h.grandchildTopic, grandChildCoroutine)

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
            })

            assert.ok(await latch.await(10_000))

            await ciSleep(750)

            assert.equal(executionOrder.length, 18)

            assert.deepEqual(
                keepOnlyPrefixedBy(
                    executionOrder,
                    "root-handler",
                    "child-handler-1",
                    "grandchild-handler",
                ),
                [
                    "root-handler-step-1",
                    "root-handler-step-2",
                    "child-handler-1-step-1",
                    "child-handler-1-step-2",
                    "grandchild-handler-step-1",
                    "grandchild-handler-step-2",
                    "child-handler-1-step-3",
                    "root-handler-handleChildFailures-step-2",
                    "child-handler-1-rollback-step-3",
                    "grandchild-handler-rollback-step-2",
                    "grandchild-handler-rollback-step-1",
                    "child-handler-1-rollback-step-2",
                    "child-handler-1-rollback-step-1",
                    "root-handler-rollback-step-2",
                    "root-handler-rollback-step-1",
                ],
                "Execution order obeys structured cooperation rules",
            )
            assert.deepEqual(
                keepOnlyPrefixedBy(executionOrder, "root-handler", "child-handler-2"),
                [
                    "root-handler-step-1",
                    "root-handler-step-2",
                    "child-handler-2-step-1",
                    "child-handler-2-step-2",
                    "child-handler-2-rollback-step-1",
                    "root-handler-handleChildFailures-step-2",
                    "root-handler-rollback-step-2",
                    "root-handler-rollback-step-1",
                ],
                "Execution order obeys structured cooperation rules",
            )

            assert.deepEqual(
                keepOnlyHandlers(
                    await getEventSequence(h.sql),
                    "root-handler",
                    "child-handler-1",
                    "grandchild-handler",
                ),
                [
                    triple("EMITTED", null, null),
                    triple("SEEN", null, "root-handler"),
                    triple("SUSPENDED", "0", "root-handler"),
                    triple("EMITTED", "1", "root-handler"),
                    triple("SUSPENDED", "1", "root-handler"),
                    triple("SEEN", null, "child-handler-1"),
                    triple("SUSPENDED", "0", "child-handler-1"),
                    triple("EMITTED", "1", "child-handler-1"),
                    triple("SUSPENDED", "1", "child-handler-1"),
                    triple("SEEN", null, "grandchild-handler"),
                    triple("SUSPENDED", "0", "grandchild-handler"),
                    triple("SUSPENDED", "1", "grandchild-handler"),
                    triple("COMMITTED", "1", "grandchild-handler"),
                    triple("SUSPENDED", "2", "child-handler-1"),
                    triple("COMMITTED", "2", "child-handler-1"),
                    triple("ROLLING_BACK", "1", "root-handler"),
                    triple("ROLLBACK_EMITTED", "Rollback of 1[0,] (rolling back child scopes)", "root-handler"),
                    triple("SUSPENDED", "Rollback of 1[0,] (rolling back child scopes)", "root-handler"),
                    triple("ROLLING_BACK", null, "child-handler-1"),
                    triple("SUSPENDED", "Rollback of 2[0,] (rolling back child scopes)", "child-handler-1"),
                    triple("SUSPENDED", "Rollback of 2[0,]", "child-handler-1"),
                    triple("ROLLBACK_EMITTED", "Rollback of 1[0,] (rolling back child scopes)", "child-handler-1"),
                    triple("SUSPENDED", "Rollback of 1[0,] (rolling back child scopes)", "child-handler-1"),
                    triple("ROLLING_BACK", null, "grandchild-handler"),
                    triple("SUSPENDED", "Rollback of 1[0,] (rolling back child scopes)", "grandchild-handler"),
                    triple("SUSPENDED", "Rollback of 1[0,]", "grandchild-handler"),
                    triple("SUSPENDED", "Rollback of 0[0,] (rolling back child scopes)", "grandchild-handler"),
                    triple("SUSPENDED", "Rollback of 0[0,]", "grandchild-handler"),
                    triple("ROLLED_BACK", "Rollback of 0[0,]", "grandchild-handler"),
                    triple("SUSPENDED", "Rollback of 1[0,]", "child-handler-1"),
                    triple("SUSPENDED", "Rollback of 0[0,] (rolling back child scopes)", "child-handler-1"),
                    triple("SUSPENDED", "Rollback of 0[0,]", "child-handler-1"),
                    triple("ROLLED_BACK", "Rollback of 0[0,]", "child-handler-1"),
                    triple("SUSPENDED", "Rollback of 1[0,]", "root-handler"),
                    triple("SUSPENDED", "Rollback of 0[0,] (rolling back child scopes)", "root-handler"),
                    triple("SUSPENDED", "Rollback of 0[0,]", "root-handler"),
                    triple("ROLLED_BACK", "Rollback of 0[0,]", "root-handler"),
                ],
            )

            assert.deepEqual(
                keepOnlyHandlers(await getEventSequence(h.sql), "root-handler", "child-handler-2"),
                [
                    triple("EMITTED", null, null),
                    triple("SEEN", null, "root-handler"),
                    triple("SUSPENDED", "0", "root-handler"),
                    triple("EMITTED", "1", "root-handler"),
                    triple("SUSPENDED", "1", "root-handler"),
                    triple("SEEN", null, "child-handler-2"),
                    triple("SUSPENDED", "0", "child-handler-2"),
                    triple("ROLLING_BACK", "1", "child-handler-2"),
                    triple("SUSPENDED", "Rollback of 0[0,] (rolling back child scopes)", "child-handler-2"),
                    triple("SUSPENDED", "Rollback of 0[0,]", "child-handler-2"),
                    triple("ROLLED_BACK", "Rollback of 0[0,]", "child-handler-2"),
                    triple("ROLLING_BACK", "1", "root-handler"),
                    triple("ROLLBACK_EMITTED", "Rollback of 1[0,] (rolling back child scopes)", "root-handler"),
                    triple("SUSPENDED", "Rollback of 1[0,] (rolling back child scopes)", "root-handler"),
                    triple("SUSPENDED", "Rollback of 1[0,]", "root-handler"),
                    triple("SUSPENDED", "Rollback of 0[0,] (rolling back child scopes)", "root-handler"),
                    triple("SUSPENDED", "Rollback of 0[0,]", "root-handler"),
                    triple("ROLLED_BACK", "Rollback of 0[0,]", "root-handler"),
                ],
            )

            const childHandler2RollingBackExceptions = await fetchExceptions(
                h.sql,
                h.jsonbHelper,
                "ROLLING_BACK",
                "child-handler-2",
            )

            assertEquivalent(
                [
                    {
                        message: `[${asSource(childHandler2Coroutine.identifier)}] Error: Simulated failure to test rollback`,
                        type: "Error",
                        source: asSource(childHandler2Coroutine.identifier),
                    },
                ],
                childHandler2RollingBackExceptions,
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
                        message: `[${asSource(rootHandlerCoroutine.identifier)}] ChildRolledBackException: Child failure occurred while suspended in step [1]`,
                        type: "ChildRolledBackException",
                        source: asSource(rootHandlerCoroutine.identifier),
                        causes: [
                            {
                                message: `[${asSource(childHandler2Coroutine.identifier)}] Error: Simulated failure to test rollback`,
                                type: "Error",
                                source: asSource(childHandler2Coroutine.identifier),
                            },
                        ],
                    },
                ],
                rootHandlerRollingBackExceptions,
            )

            const rootHandlerRollbackEmittedExceptions = await fetchExceptions(
                h.sql,
                h.jsonbHelper,
                "ROLLBACK_EMITTED",
                "root-handler",
            )

            assertEquivalent(
                [
                    {
                        message: `[${asSource(rootHandlerCoroutine.identifier)}] ParentSaidSoException: <no message>`,
                        type: "ParentSaidSoException",
                        source: asSource(rootHandlerCoroutine.identifier),
                        causes: [
                            {
                                message: `[${asSource(rootHandlerCoroutine.identifier)}] ChildRolledBackException: Child failure occurred while suspended in step [1]`,
                                type: "ChildRolledBackException",
                                source: asSource(rootHandlerCoroutine.identifier),
                                causes: [
                                    {
                                        message: `[${asSource(childHandler2Coroutine.identifier)}] Error: Simulated failure to test rollback`,
                                        type: "Error",
                                        source: asSource(childHandler2Coroutine.identifier),
                                    },
                                ],
                            },
                        ],
                    },
                ],
                rootHandlerRollbackEmittedExceptions,
            )

            const childHandler1RollingBackExceptions = await fetchExceptions(
                h.sql,
                h.jsonbHelper,
                "ROLLING_BACK",
                "child-handler-1",
            )

            assertEquivalent(
                [
                    {
                        message: `[${asSource(rootHandlerCoroutine.identifier)}] ParentSaidSoException: <no message>`,
                        type: "ParentSaidSoException",
                        source: asSource(rootHandlerCoroutine.identifier),
                        causes: [
                            {
                                message: `[${asSource(rootHandlerCoroutine.identifier)}] ChildRolledBackException: Child failure occurred while suspended in step [1]`,
                                type: "ChildRolledBackException",
                                source: asSource(rootHandlerCoroutine.identifier),
                                causes: [
                                    {
                                        message: `[${asSource(childHandler2Coroutine.identifier)}] Error: Simulated failure to test rollback`,
                                        type: "Error",
                                        source: asSource(childHandler2Coroutine.identifier),
                                    },
                                ],
                            },
                        ],
                    },
                ],
                childHandler1RollingBackExceptions,
            )

            const childHandler1RollbackEmittedExceptions = await fetchExceptions(
                h.sql,
                h.jsonbHelper,
                "ROLLBACK_EMITTED",
                "child-handler-1",
            )

            assertEquivalent(
                [
                    {
                        message: `[${asSource(childHandler1Coroutine.identifier)}] ParentSaidSoException: <no message>`,
                        type: "ParentSaidSoException",
                        source: asSource(childHandler1Coroutine.identifier),
                        causes: [
                            {
                                message: `[${asSource(rootHandlerCoroutine.identifier)}] ParentSaidSoException: <no message>`,
                                type: "ParentSaidSoException",
                                source: asSource(rootHandlerCoroutine.identifier),
                                causes: [
                                    {
                                        message: `[${asSource(rootHandlerCoroutine.identifier)}] ChildRolledBackException: Child failure occurred while suspended in step [1]`,
                                        type: "ChildRolledBackException",
                                        source: asSource(rootHandlerCoroutine.identifier),
                                        causes: [
                                            {
                                                message: `[${asSource(childHandler2Coroutine.identifier)}] Error: Simulated failure to test rollback`,
                                                type: "Error",
                                                source: asSource(childHandler2Coroutine.identifier),
                                            },
                                        ],
                                    },
                                ],
                            },
                        ],
                    },
                ],
                childHandler1RollbackEmittedExceptions,
            )

            const grandChildHandlerRollingBackExceptions = await fetchExceptions(
                h.sql,
                h.jsonbHelper,
                "ROLLING_BACK",
                "grandchild-handler",
            )

            assertEquivalent(
                [
                    {
                        message: `[${asSource(childHandler1Coroutine.identifier)}] ParentSaidSoException: <no message>`,
                        type: "ParentSaidSoException",
                        source: asSource(childHandler1Coroutine.identifier),
                        causes: [
                            {
                                message: `[${asSource(rootHandlerCoroutine.identifier)}] ParentSaidSoException: <no message>`,
                                type: "ParentSaidSoException",
                                source: asSource(rootHandlerCoroutine.identifier),
                                causes: [
                                    {
                                        message: `[${asSource(rootHandlerCoroutine.identifier)}] ChildRolledBackException: Child failure occurred while suspended in step [1]`,
                                        type: "ChildRolledBackException",
                                        source: asSource(rootHandlerCoroutine.identifier),
                                        causes: [
                                            {
                                                message: `[${asSource(childHandler2Coroutine.identifier)}] Error: Simulated failure to test rollback`,
                                                type: "Error",
                                                source: asSource(childHandler2Coroutine.identifier),
                                            },
                                        ],
                                    },
                                ],
                            },
                        ],
                    },
                ],
                grandChildHandlerRollingBackExceptions,
            )
        } finally {
            await rootSubscription.close()
            await childSubscription1.close()
            await childSubscription2.close()
            await grandchildSubscription.close()
        }
    })

    // We're only including the rollback/handlerChildFailure lambdas that actually get called
    // here, for brevity
    test("failed rollbacks are well behaved n-deep", async () => {
        const executionOrder: string[] = []

        const latch = new CountDownLatch(16)

        const rootHandlerCoroutine = saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
            b.step({
                invoke: (_scope, _message) => {
                    latch.countDown()
                    executionOrder.push("root-handler-step-1")
                },
            })
            b.step({
                invoke: async (scope, _message) => {
                    await scope.launch(h.childTopic, { from: "root-handler" })
                    latch.countDown()
                    executionOrder.push("root-handler-step-2")
                },
                handleChildFailures: (_scope, _message, throwable) => {
                    // This will be called twice - once for the "normal" exception that starts
                    // the rollback process, and then for the additional exception that get's
                    // thrown during the rollback process
                    executionOrder.push("root-handler-handleChildFailures-step-2")
                    latch.countDown()
                    throw throwable
                },
            })
        })

        const rootSubscription = h.subscribe(h.rootTopic, rootHandlerCoroutine)

        const childHandler1Coroutine = saga(
            "child-handler-1",
            eventLoopStrategy(h.messageQueue),
            b => {
                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("child-handler-1-step-1")
                        latch.countDown()
                    },
                })
                b.step({
                    invoke: async (scope, _message) => {
                        await scope.launch(h.grandchildTopic, { from: "child-handler-1" })
                        executionOrder.push("child-handler-1-step-2")
                        latch.countDown()
                    },
                    handleChildFailures: (_scope, _message, throwable) => {
                        executionOrder.push("child-handler-1-handleChildFailures-step-2")
                        latch.countDown()
                        throw throwable
                    },
                })
                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("child-handler-1-step-3")
                        latch.countDown()
                    },
                    rollback: (_scope, _message, _throwable) => {
                        executionOrder.push("child-handler-1-rollback-step-3")
                        latch.countDown()
                    },
                })
            },
        )
        const childSubscription1 = h.subscribe(h.childTopic, childHandler1Coroutine)

        const childHandler2Coroutine = saga(
            "child-handler-2",
            eventLoopStrategy(h.messageQueue),
            b => {
                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("child-handler-2-step-1")
                        latch.countDown()
                    },
                    rollback: (_scope, _message, _throwable) => {
                        executionOrder.push("child-handler-2-rollback-step-1")
                        latch.countDown()
                    },
                })
                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("child-handler-2-step-2")
                        latch.countDown()
                        throw new Error("Simulated failure to test rollback")
                    },
                })
            },
        )
        const childSubscription2 = h.subscribe(h.childTopic, childHandler2Coroutine)

        const grandChildCoroutine = saga(
            "grandchild-handler",
            eventLoopStrategy(h.messageQueue),
            b => {
                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("grandchild-handler-step-1")
                        latch.countDown()
                    },
                    rollback: (_scope, _message, _throwable) => {
                        executionOrder.push("grandchild-handler-rollback-step-1")
                        latch.countDown()
                        throw new IllegalStateError("Rollback failure")
                    },
                })
                b.step({
                    invoke: async (_scope, _message) => {
                        await ciSleep(200)
                        executionOrder.push("grandchild-handler-step-2")
                        latch.countDown()
                    },
                    rollback: (_scope, _message, _throwable) => {
                        executionOrder.push("grandchild-handler-rollback-step-2")
                        latch.countDown()
                    },
                })
            },
        )
        const grandchildSubscription = h.subscribe(h.grandchildTopic, grandChildCoroutine)

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
            })

            assert.ok(await latch.await(10_000), `Latch has count ${latch.getCount()}`)

            await ciSleep(750)

            assert.equal(executionOrder.length, 16)

            assert.deepEqual(
                keepOnlyPrefixedBy(
                    executionOrder,
                    "root-handler",
                    "child-handler-1",
                    "grandchild-handler",
                ),
                [
                    "root-handler-step-1",
                    "root-handler-step-2",
                    "child-handler-1-step-1",
                    "child-handler-1-step-2",
                    "grandchild-handler-step-1",
                    "grandchild-handler-step-2",
                    "child-handler-1-step-3",
                    "root-handler-handleChildFailures-step-2",
                    "child-handler-1-rollback-step-3",
                    "grandchild-handler-rollback-step-2",
                    "grandchild-handler-rollback-step-1",
                    "child-handler-1-handleChildFailures-step-2",
                    "root-handler-handleChildFailures-step-2",
                ],
                "Execution order obeys structured cooperation rules",
            )
            assert.deepEqual(
                keepOnlyPrefixedBy(executionOrder, "root-handler", "child-handler-2"),
                [
                    "root-handler-step-1",
                    "root-handler-step-2",
                    "child-handler-2-step-1",
                    "child-handler-2-step-2",
                    "child-handler-2-rollback-step-1",
                    "root-handler-handleChildFailures-step-2",
                    "root-handler-handleChildFailures-step-2",
                ],
                "Execution order obeys structured cooperation rules",
            )

            assert.deepEqual(
                keepOnlyHandlers(
                    await getEventSequence(h.sql),
                    "root-handler",
                    "child-handler-1",
                    "grandchild-handler",
                ),
                [
                    triple("EMITTED", null, null),
                    triple("SEEN", null, "root-handler"),
                    triple("SUSPENDED", "0", "root-handler"),
                    triple("EMITTED", "1", "root-handler"),
                    triple("SUSPENDED", "1", "root-handler"),
                    triple("SEEN", null, "child-handler-1"),
                    triple("SUSPENDED", "0", "child-handler-1"),
                    triple("EMITTED", "1", "child-handler-1"),
                    triple("SUSPENDED", "1", "child-handler-1"),
                    triple("SEEN", null, "grandchild-handler"),
                    triple("SUSPENDED", "0", "grandchild-handler"),
                    triple("SUSPENDED", "1", "grandchild-handler"),
                    triple("COMMITTED", "1", "grandchild-handler"),
                    triple("SUSPENDED", "2", "child-handler-1"),
                    triple("COMMITTED", "2", "child-handler-1"),
                    triple("ROLLING_BACK", "1", "root-handler"),
                    triple("ROLLBACK_EMITTED", "Rollback of 1[0,] (rolling back child scopes)", "root-handler"),
                    triple("SUSPENDED", "Rollback of 1[0,] (rolling back child scopes)", "root-handler"),
                    triple("ROLLING_BACK", null, "child-handler-1"),
                    triple("SUSPENDED", "Rollback of 2[0,] (rolling back child scopes)", "child-handler-1"),
                    triple("SUSPENDED", "Rollback of 2[0,]", "child-handler-1"),
                    triple("ROLLBACK_EMITTED", "Rollback of 1[0,] (rolling back child scopes)", "child-handler-1"),
                    triple("SUSPENDED", "Rollback of 1[0,] (rolling back child scopes)", "child-handler-1"),
                    triple("ROLLING_BACK", null, "grandchild-handler"),
                    triple("SUSPENDED", "Rollback of 1[0,] (rolling back child scopes)", "grandchild-handler"),
                    triple("SUSPENDED", "Rollback of 1[0,]", "grandchild-handler"),
                    triple("SUSPENDED", "Rollback of 0[0,] (rolling back child scopes)", "grandchild-handler"),
                    triple("ROLLBACK_FAILED", "Rollback of 0[0,]", "grandchild-handler"),
                    triple("ROLLBACK_FAILED", "Rollback of 1[0,] (rolling back child scopes)", "child-handler-1"),
                    triple("ROLLBACK_FAILED", "Rollback of 1[0,] (rolling back child scopes)", "root-handler"),
                ],
            )

            assert.deepEqual(
                keepOnlyHandlers(await getEventSequence(h.sql), "root-handler", "child-handler-2"),
                [
                    triple("EMITTED", null, null),
                    triple("SEEN", null, "root-handler"),
                    triple("SUSPENDED", "0", "root-handler"),
                    triple("EMITTED", "1", "root-handler"),
                    triple("SUSPENDED", "1", "root-handler"),
                    triple("SEEN", null, "child-handler-2"),
                    triple("SUSPENDED", "0", "child-handler-2"),
                    triple("ROLLING_BACK", "1", "child-handler-2"),
                    triple("SUSPENDED", "Rollback of 0[0,] (rolling back child scopes)", "child-handler-2"),
                    triple("SUSPENDED", "Rollback of 0[0,]", "child-handler-2"),
                    triple("ROLLED_BACK", "Rollback of 0[0,]", "child-handler-2"),
                    triple("ROLLING_BACK", "1", "root-handler"),
                    triple("ROLLBACK_EMITTED", "Rollback of 1[0,] (rolling back child scopes)", "root-handler"),
                    triple("SUSPENDED", "Rollback of 1[0,] (rolling back child scopes)", "root-handler"),
                    triple("ROLLBACK_FAILED", "Rollback of 1[0,] (rolling back child scopes)", "root-handler"),
                ],
            )

            const childHandler2RollingBackExceptions = await fetchExceptions(
                h.sql,
                h.jsonbHelper,
                "ROLLING_BACK",
                "child-handler-2",
            )

            assertEquivalent(
                [
                    {
                        message: `[${asSource(childHandler2Coroutine.identifier)}] Error: Simulated failure to test rollback`,
                        type: "Error",
                        source: asSource(childHandler2Coroutine.identifier),
                    },
                ],
                childHandler2RollingBackExceptions,
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
                        message: `[${asSource(rootHandlerCoroutine.identifier)}] ChildRolledBackException: Child failure occurred while suspended in step [1]`,
                        type: "ChildRolledBackException",
                        source: asSource(rootHandlerCoroutine.identifier),
                        causes: [
                            {
                                message: `[${asSource(childHandler2Coroutine.identifier)}] Error: Simulated failure to test rollback`,
                                type: "Error",
                                source: asSource(childHandler2Coroutine.identifier),
                            },
                        ],
                    },
                ],
                rootHandlerRollingBackExceptions,
            )

            const rootHandlerRollbackEmittedExceptions = await fetchExceptions(
                h.sql,
                h.jsonbHelper,
                "ROLLBACK_EMITTED",
                "root-handler",
            )

            assertEquivalent(
                [
                    {
                        message: `[${asSource(rootHandlerCoroutine.identifier)}] ParentSaidSoException: <no message>`,
                        type: "ParentSaidSoException",
                        source: asSource(rootHandlerCoroutine.identifier),
                        causes: [
                            {
                                message: `[${asSource(rootHandlerCoroutine.identifier)}] ChildRolledBackException: Child failure occurred while suspended in step [1]`,
                                type: "ChildRolledBackException",
                                source: asSource(rootHandlerCoroutine.identifier),
                                causes: [
                                    {
                                        message: `[${asSource(childHandler2Coroutine.identifier)}] Error: Simulated failure to test rollback`,
                                        type: "Error",
                                        source: asSource(childHandler2Coroutine.identifier),
                                    },
                                ],
                            },
                        ],
                    },
                ],
                rootHandlerRollbackEmittedExceptions,
            )

            const childHandler1RollingBackExceptions = await fetchExceptions(
                h.sql,
                h.jsonbHelper,
                "ROLLING_BACK",
                "child-handler-1",
            )

            assertEquivalent(
                [
                    {
                        message: `[${asSource(rootHandlerCoroutine.identifier)}] ParentSaidSoException: <no message>`,
                        type: "ParentSaidSoException",
                        source: asSource(rootHandlerCoroutine.identifier),
                        causes: [
                            {
                                message: `[${asSource(rootHandlerCoroutine.identifier)}] ChildRolledBackException: Child failure occurred while suspended in step [1]`,
                                type: "ChildRolledBackException",
                                source: asSource(rootHandlerCoroutine.identifier),
                                causes: [
                                    {
                                        message: `[${asSource(childHandler2Coroutine.identifier)}] Error: Simulated failure to test rollback`,
                                        type: "Error",
                                        source: asSource(childHandler2Coroutine.identifier),
                                    },
                                ],
                            },
                        ],
                    },
                ],
                childHandler1RollingBackExceptions,
            )

            const childHandler1RollbackEmittedExceptions = await fetchExceptions(
                h.sql,
                h.jsonbHelper,
                "ROLLBACK_EMITTED",
                "child-handler-1",
            )

            assertEquivalent(
                [
                    {
                        message: `[${asSource(childHandler1Coroutine.identifier)}] ParentSaidSoException: <no message>`,
                        type: "ParentSaidSoException",
                        source: asSource(childHandler1Coroutine.identifier),
                        causes: [
                            {
                                message: `[${asSource(rootHandlerCoroutine.identifier)}] ParentSaidSoException: <no message>`,
                                type: "ParentSaidSoException",
                                source: asSource(rootHandlerCoroutine.identifier),
                                causes: [
                                    {
                                        message: `[${asSource(rootHandlerCoroutine.identifier)}] ChildRolledBackException: Child failure occurred while suspended in step [1]`,
                                        type: "ChildRolledBackException",
                                        source: asSource(rootHandlerCoroutine.identifier),
                                        causes: [
                                            {
                                                message: `[${asSource(childHandler2Coroutine.identifier)}] Error: Simulated failure to test rollback`,
                                                type: "Error",
                                                source: asSource(childHandler2Coroutine.identifier),
                                            },
                                        ],
                                    },
                                ],
                            },
                        ],
                    },
                ],
                childHandler1RollbackEmittedExceptions,
            )

            const grandChildHandlerRollingBackExceptions = await fetchExceptions(
                h.sql,
                h.jsonbHelper,
                "ROLLING_BACK",
                "grandchild-handler",
            )

            assertEquivalent(
                [
                    {
                        message: `[${asSource(childHandler1Coroutine.identifier)}] ParentSaidSoException: <no message>`,
                        type: "ParentSaidSoException",
                        source: asSource(childHandler1Coroutine.identifier),
                        causes: [
                            {
                                message: `[${asSource(rootHandlerCoroutine.identifier)}] ParentSaidSoException: <no message>`,
                                type: "ParentSaidSoException",
                                source: asSource(rootHandlerCoroutine.identifier),
                                causes: [
                                    {
                                        message: `[${asSource(rootHandlerCoroutine.identifier)}] ChildRolledBackException: Child failure occurred while suspended in step [1]`,
                                        type: "ChildRolledBackException",
                                        source: asSource(rootHandlerCoroutine.identifier),
                                        causes: [
                                            {
                                                message: `[${asSource(childHandler2Coroutine.identifier)}] Error: Simulated failure to test rollback`,
                                                type: "Error",
                                                source: asSource(childHandler2Coroutine.identifier),
                                            },
                                        ],
                                    },
                                ],
                            },
                        ],
                    },
                ],
                grandChildHandlerRollingBackExceptions,
            )

            const grandChildHandlerRollbackFailedExceptions = await fetchExceptions(
                h.sql,
                h.jsonbHelper,
                "ROLLBACK_FAILED",
                "grandchild-handler",
            )

            assertEquivalent(
                [
                    {
                        message: `[${asSource(grandChildCoroutine.identifier)}] IllegalStateError: Rollback failure`,
                        type: "IllegalStateError",
                        source: asSource(grandChildCoroutine.identifier),
                    },
                ],
                grandChildHandlerRollbackFailedExceptions,
            )

            const childHandler1RollbackFailedExceptions = await fetchExceptions(
                h.sql,
                h.jsonbHelper,
                "ROLLBACK_FAILED",
                "child-handler-1",
            )

            assertEquivalent(
                [
                    {
                        message:
                            `[${asSource(childHandler1Coroutine.identifier)}] ` +
                            "ChildRollbackFailedException: " +
                            "Child rollback failure occurred while suspended in step [Rollback of 1[0,] (rolling back child scopes)]",
                        type: "ChildRollbackFailedException",
                        source: asSource(childHandler1Coroutine.identifier),
                        causes: [
                            {
                                message: `[${asSource(grandChildCoroutine.identifier)}] IllegalStateError: Rollback failure`,
                                type: "IllegalStateError",
                                source: asSource(grandChildCoroutine.identifier),
                            },
                            {
                                message: `[${asSource(rootHandlerCoroutine.identifier)}] ParentSaidSoException: <no message>`,
                                type: "ParentSaidSoException",
                                source: asSource(rootHandlerCoroutine.identifier),
                                causes: [
                                    {
                                        message: `[${asSource(rootHandlerCoroutine.identifier)}] ChildRolledBackException: Child failure occurred while suspended in step [1]`,
                                        type: "ChildRolledBackException",
                                        source: asSource(rootHandlerCoroutine.identifier),
                                        causes: [
                                            {
                                                message: `[${asSource(childHandler2Coroutine.identifier)}] Error: Simulated failure to test rollback`,
                                                type: "Error",
                                                source: asSource(childHandler2Coroutine.identifier),
                                            },
                                        ],
                                    },
                                ],
                            },
                        ],
                    },
                ],
                childHandler1RollbackFailedExceptions,
            )

            const rootHandlerRollbackFailedExceptions = await fetchExceptions(
                h.sql,
                h.jsonbHelper,
                "ROLLBACK_FAILED",
                "root-handler",
            )

            assertEquivalent(
                [
                    {
                        message:
                            `[${asSource(rootHandlerCoroutine.identifier)}] ` +
                            "ChildRollbackFailedException: " +
                            "Child rollback failure occurred while suspended in step [Rollback of 1[0,] (rolling back child scopes)]",
                        type: "ChildRollbackFailedException",
                        source: asSource(rootHandlerCoroutine.identifier),
                        causes: [
                            {
                                message:
                                    `[${asSource(childHandler1Coroutine.identifier)}] ` +
                                    "ChildRollbackFailedException: " +
                                    "Child rollback failure occurred while suspended in step [Rollback of 1[0,] (rolling back child scopes)]",
                                type: "ChildRollbackFailedException",
                                source: asSource(childHandler1Coroutine.identifier),
                                causes: [
                                    {
                                        message: `[${asSource(grandChildCoroutine.identifier)}] IllegalStateError: Rollback failure`,
                                        type: "IllegalStateError",
                                        source: asSource(grandChildCoroutine.identifier),
                                    },
                                    {
                                        message: `[${asSource(rootHandlerCoroutine.identifier)}] ParentSaidSoException: <no message>`,
                                        type: "ParentSaidSoException",
                                        source: asSource(rootHandlerCoroutine.identifier),
                                        causes: [
                                            {
                                                message: `[${asSource(rootHandlerCoroutine.identifier)}] ChildRolledBackException: Child failure occurred while suspended in step [1]`,
                                                type: "ChildRolledBackException",
                                                source: asSource(rootHandlerCoroutine.identifier),
                                                causes: [
                                                    {
                                                        message: `[${asSource(childHandler2Coroutine.identifier)}] Error: Simulated failure to test rollback`,
                                                        type: "Error",
                                                        source: asSource(
                                                            childHandler2Coroutine.identifier,
                                                        ),
                                                    },
                                                ],
                                            },
                                        ],
                                    },
                                ],
                            },
                        ],
                    },
                ],
                rootHandlerRollbackFailedExceptions,
            )
        } finally {
            await rootSubscription.close()
            await childSubscription1.close()
            await childSubscription2.close()
            await grandchildSubscription.close()
        }
    })

    describe("HandleChildFailures", () => {
        test("when stuff is emitted in handleChildFailures and then a rollback happens, all things that haven't already been rolled back are rolled back", async () => {
            const latch = new CountDownLatch(1)
            const executionOrder: string[] = []

            const rootHandler = "root-handler"
            const rootSubscription = h.subscribe(
                h.rootTopic,
                saga(rootHandler, eventLoopStrategy(h.messageQueue), b => {
                    b.step({
                        invoke: async (scope, _message) => {
                            await scope.launch(h.childTopic, {
                                from: rootHandler,
                                phase: "original",
                            })
                            executionOrder.push("root-handler")
                        },
                        rollback: (_scope, _message, _throwable) => {
                            executionOrder.push("root-handler-rollback")
                            latch.countDown()
                        },
                        handleChildFailures: async (scope, _message, throwable) => {
                            executionOrder.push("root-handler-handleChildFailures")
                            if (has(scope.context, TriedAgainKey)) {
                                throw throwable
                            } else {
                                scope.context = scope.context.plus(TriedAgainValue)
                                await scope.launch(h.childTopic, {
                                    from: rootHandler,
                                    phase: "retry",
                                })
                            }
                        },
                    })
                }),
            )

            const childHandler1 = "child-handler-1"
            const childSubscription1 = h.subscribe(
                h.childTopic,
                saga(childHandler1, eventLoopStrategy(h.messageQueue), b => {
                    b.step({
                        invoke: (_scope, message) => {
                            const phase = (message.payload as Record<string, string>).phase
                            executionOrder.push(`child-handler-1-${phase}`)
                            throw new Error("Simulated failure to test rollback")
                        },
                        rollback: (_scope, message, _throwable) => {
                            const phase = (message.payload as Record<string, string>).phase
                            executionOrder.push(`child-handler-1-rollback-${phase}`)
                        },
                        handleChildFailures: (_scope, message, throwable) => {
                            const phase = (message.payload as Record<string, string>).phase
                            executionOrder.push(`child-handler-1-handleChildFailures-${phase}`)
                            throw throwable
                        },
                    })
                }),
            )

            const childHandler2 = "child-handler-2"
            const childSubscription2 = h.subscribe(
                h.childTopic,
                saga(childHandler2, eventLoopStrategy(h.messageQueue), b => {
                    b.step({
                        invoke: (_scope, message) => {
                            const phase = (message.payload as Record<string, string>).phase
                            executionOrder.push(`child-handler-2-${phase}`)
                        },
                        rollback: (_scope, message, _throwable) => {
                            const phase = (message.payload as Record<string, string>).phase
                            executionOrder.push(`child-handler-2-rollback-${phase}`)
                        },
                        handleChildFailures: (_scope, message, throwable) => {
                            const phase = (message.payload as Record<string, string>).phase
                            executionOrder.push(`child-handler-2-handleChildFailures-${phase}`)
                            throw throwable
                        },
                    })
                }),
            )

            try {
                await transactional(h.sql, async connection => {
                    await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
                })

                assert.ok(await latch.await(10_000), "Not everything completed correctly")
                await ciSleep(200)

                assert.deepEqual(
                    keepOnlyHandlers(
                        await getEventSequence(h.sql),
                        "root-handler",
                        "child-handler-1",
                    ),
                    [
                        triple("EMITTED", null, null),
                        triple("SEEN", null, "root-handler"),
                        triple("EMITTED", "0", "root-handler"),
                        triple("SUSPENDED", "0", "root-handler"),
                        triple("SEEN", null, "child-handler-1"),
                        triple("ROLLING_BACK", "0", "child-handler-1"),
                        triple("ROLLED_BACK", "Rollback of 0[0,]", "child-handler-1"),
                        triple("EMITTED", "0", "root-handler"),
                        triple("SUSPENDED", "0", "root-handler"),
                        triple("SEEN", null, "child-handler-1"),
                        triple("ROLLING_BACK", "0", "child-handler-1"),
                        triple("ROLLED_BACK", "Rollback of 0[0,]", "child-handler-1"),
                        triple("ROLLING_BACK", "0", "root-handler"),
                        triple("ROLLBACK_EMITTED", "Rollback of 0[0,0] (rolling back child scopes)", "root-handler"),
                        triple("SUSPENDED", "Rollback of 0[0,0] (rolling back child scopes)", "root-handler"),
                        triple("ROLLBACK_EMITTED", "Rollback of 0[0,] (rolling back child scopes)", "root-handler"),
                        triple("SUSPENDED", "Rollback of 0[0,] (rolling back child scopes)", "root-handler"),
                        triple("SUSPENDED", "Rollback of 0[0,]", "root-handler"),
                        triple("ROLLED_BACK", "Rollback of 0[0,]", "root-handler"),
                    ],
                )

                assert.deepEqual(
                    keepOnlyHandlers(
                        await getEventSequence(h.sql),
                        "root-handler",
                        "child-handler-2",
                    ),
                    [
                        triple("EMITTED", null, null),
                        triple("SEEN", null, "root-handler"),
                        triple("EMITTED", "0", "root-handler"),
                        triple("SUSPENDED", "0", "root-handler"),
                        triple("SEEN", null, "child-handler-2"),
                        triple("SUSPENDED", "0", "child-handler-2"),
                        triple("COMMITTED", "0", "child-handler-2"),
                        triple("EMITTED", "0", "root-handler"),
                        triple("SUSPENDED", "0", "root-handler"),
                        triple("SEEN", null, "child-handler-2"),
                        triple("SUSPENDED", "0", "child-handler-2"),
                        triple("COMMITTED", "0", "child-handler-2"),
                        triple("ROLLING_BACK", "0", "root-handler"),
                        triple("ROLLBACK_EMITTED", "Rollback of 0[0,0] (rolling back child scopes)", "root-handler"),
                        triple("SUSPENDED", "Rollback of 0[0,0] (rolling back child scopes)", "root-handler"),
                        triple("ROLLING_BACK", null, "child-handler-2"),
                        triple("SUSPENDED", "Rollback of 0[0,] (rolling back child scopes)", "child-handler-2"),
                        triple("SUSPENDED", "Rollback of 0[0,]", "child-handler-2"),
                        triple("ROLLED_BACK", "Rollback of 0[0,]", "child-handler-2"),
                        triple("ROLLBACK_EMITTED", "Rollback of 0[0,] (rolling back child scopes)", "root-handler"),
                        triple("SUSPENDED", "Rollback of 0[0,] (rolling back child scopes)", "root-handler"),
                        triple("ROLLING_BACK", null, "child-handler-2"),
                        triple("SUSPENDED", "Rollback of 0[0,] (rolling back child scopes)", "child-handler-2"),
                        triple("SUSPENDED", "Rollback of 0[0,]", "child-handler-2"),
                        triple("ROLLED_BACK", "Rollback of 0[0,]", "child-handler-2"),
                        triple("SUSPENDED", "Rollback of 0[0,]", "root-handler"),
                        triple("ROLLED_BACK", "Rollback of 0[0,]", "root-handler"),
                    ],
                )

                assert.deepEqual(
                    keepOnlyPrefixedBy(executionOrder, "root-handler", "child-handler-1"),
                    [
                        "root-handler",
                        "child-handler-1-original",
                        "root-handler-handleChildFailures",
                        "child-handler-1-retry",
                        "root-handler-handleChildFailures",
                        "root-handler-rollback",
                    ],
                )

                assert.deepEqual(
                    keepOnlyPrefixedBy(executionOrder, "root-handler", "child-handler-2"),
                    [
                        "root-handler",
                        "child-handler-2-original",
                        "root-handler-handleChildFailures",
                        "child-handler-2-retry",
                        "root-handler-handleChildFailures",
                        // This ordering (first rolling back the original, and then the retry)
                        // is actually not guaranteed in the general case - here, it's only
                        // the case because we're running a single instance of the handler,
                        // so events are processed in the order they are created. However,
                        // in general, with multiple handler instances running, no guarantees
                        // can be made about the order in which the following two lines would
                        // appear.
                        "child-handler-2-rollback-retry",
                        "child-handler-2-rollback-original",
                        "root-handler-rollback",
                    ],
                )
            } finally {
                await childSubscription1.close()
                await childSubscription2.close()
                await rootSubscription.close()
            }
        })
    })

    describe("RollbackRequests", () => {
        test("rolling back the entire hierarchy works", async () => {
            const latch = new CountDownLatch(1)
            const rollbackLatch = new CountDownLatch(1)

            const rootSubscription = h.subscribe(
                h.rootTopic,
                saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                    b.step({
                        invoke: async (scope, _message) => {
                            await scope.launch(h.childTopic, { from: "root-handler" })
                        },
                        rollback: (_scope, _message, _throwable) => {
                            rollbackLatch.countDown()
                        },
                    })
                }),
            )

            const childSubscription = h.subscribe(
                h.childTopic,
                saga("child-handler", eventLoopStrategy(h.messageQueue), b => {
                    b.step({ invoke: () => latch.countDown() })
                }),
            )

            try {
                const cooperationRoot = await transactional(h.sql, connection =>
                    h.messageQueue.launch(connection, h.rootTopic, { initial: "true" }),
                )

                assert.ok(await latch.await(10_000), "Not everything completed correctly")
                // The rollback request is only honoured once nothing in the hierarchy is still
                // running; the original's fixed 100ms settle races slow commits, so wait for the
                // exact precondition (both sagas COMMITTED) instead (DECISIONS.md).
                await waitUntil(async () => {
                    const [row] = await h.sql`
                        SELECT count(*)::int AS committed FROM message_event WHERE type = 'COMMITTED'
                    `
                    return Number(row!.committed) >= 2
                }, 10_000, "both sagas to commit")

                await transactional(h.sql, async connection => {
                    await h.scoop.capabilities.rollback(
                        connection,
                        cooperationRoot.cooperationScopeIdentifier,
                        "master-system",
                        "feelz",
                    )
                })

                assert.ok(
                    await rollbackLatch.await(1_000),
                    "Not everything rolled back correctly",
                )
                await ciSleep(100)

                assert.deepEqual(await getEventSequence(h.sql), [
                    triple("EMITTED", null, null),
                    triple("SEEN", null, "root-handler"),
                    triple("EMITTED", "0", "root-handler"),
                    triple("SUSPENDED", "0", "root-handler"),
                    triple("SEEN", null, "child-handler"),
                    triple("SUSPENDED", "0", "child-handler"),
                    triple("COMMITTED", "0", "child-handler"),
                    triple("COMMITTED", "0", "root-handler"),
                    triple("ROLLBACK_EMITTED", null, null),
                    triple("ROLLING_BACK", null, "root-handler"),
                    triple("ROLLBACK_EMITTED", "Rollback of 0[0,] (rolling back child scopes)", "root-handler"),
                    triple("SUSPENDED", "Rollback of 0[0,] (rolling back child scopes)", "root-handler"),
                    triple("ROLLING_BACK", null, "child-handler"),
                    triple("SUSPENDED", "Rollback of 0[0,] (rolling back child scopes)", "child-handler"),
                    triple("SUSPENDED", "Rollback of 0[0,]", "child-handler"),
                    triple("ROLLED_BACK", "Rollback of 0[0,]", "child-handler"),
                    triple("SUSPENDED", "Rollback of 0[0,]", "root-handler"),
                    triple("ROLLED_BACK", "Rollback of 0[0,]", "root-handler"),
                ])
            } finally {
                await rootSubscription.close()
                await childSubscription.close()
            }
        })

        test("rolling back sub-hierarchy works (but should be done carefully, as you run the risk of bringing the state of the system into an inconsistent state from a business perspective)", async () => {
            const latch = new CountDownLatch(1)
            const rollbackLatch = new CountDownLatch(1)

            const rootSubscription = h.subscribe(
                h.rootTopic,
                saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                    b.step({
                        invoke: async (scope, _message) => {
                            await scope.launch(h.childTopic, { from: "root-handler" })
                        },
                    })
                }),
            )

            let cooperationScopeIdentifier!: CooperationScopeIdentifier

            const childSubscription = h.subscribe(
                h.childTopic,
                saga("child-handler", eventLoopStrategy(h.messageQueue), b => {
                    b.step({
                        invoke: (scope, _message) => {
                            cooperationScopeIdentifier = scope.scopeIdentifier
                            latch.countDown()
                        },
                        rollback: (_scope, _message, _throwable) => {
                            rollbackLatch.countDown()
                        },
                    })
                }),
            )

            try {
                await transactional(h.sql, async connection => {
                    await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
                })

                assert.ok(await latch.await(10_000), "Not everything completed correctly")
                // The rollback request is only honoured once nothing in the hierarchy is still
                // running; the original's fixed 100ms settle races slow commits, so wait for the
                // exact precondition (both sagas COMMITTED) instead (DECISIONS.md).
                await waitUntil(async () => {
                    const [row] = await h.sql`
                        SELECT count(*)::int AS committed FROM message_event WHERE type = 'COMMITTED'
                    `
                    return Number(row!.committed) >= 2
                }, 10_000, "both sagas to commit")

                await transactional(h.sql, async connection => {
                    await h.scoop.capabilities.rollback(
                        connection,
                        cooperationScopeIdentifier,
                        "master-system",
                        "feelz",
                    )
                })

                assert.ok(
                    await rollbackLatch.await(1_000),
                    "Not everything rolled back correctly",
                )
                await ciSleep(100)

                assert.deepEqual(await getEventSequence(h.sql), [
                    triple("EMITTED", null, null),
                    triple("SEEN", null, "root-handler"),
                    triple("EMITTED", "0", "root-handler"),
                    triple("SUSPENDED", "0", "root-handler"),
                    triple("SEEN", null, "child-handler"),
                    triple("SUSPENDED", "0", "child-handler"),
                    triple("COMMITTED", "0", "child-handler"),
                    triple("COMMITTED", "0", "root-handler"),
                    triple("ROLLBACK_EMITTED", null, null),
                    triple("ROLLING_BACK", null, "child-handler"),
                    triple("SUSPENDED", "Rollback of 0[0,] (rolling back child scopes)", "child-handler"),
                    triple("SUSPENDED", "Rollback of 0[0,]", "child-handler"),
                    triple("ROLLED_BACK", "Rollback of 0[0,]", "child-handler"),
                ])
            } finally {
                await rootSubscription.close()
                await childSubscription.close()
            }
        })

        test("rolling back while things are still running has no effect", async () => {
            const latch = new CountDownLatch(1)
            const secondRootStepExecuting = new CountDownLatch(1)
            const rollbackEmitted = new CountDownLatch(1)

            const rootSubscription = h.subscribe(
                h.rootTopic,
                saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                    b.step({
                        invoke: async (scope, _message) => {
                            await scope.launch(h.childTopic, { from: "root-handler" })
                        },
                    })
                    b.step({
                        invoke: async (_scope, _message) => {
                            secondRootStepExecuting.countDown()
                            await rollbackEmitted.await(60_000)
                            latch.countDown()
                        },
                    })
                }),
            )

            let cooperationScopeIdentifier!: CooperationScopeIdentifier

            const childSubscription = h.subscribe(
                h.childTopic,
                saga("child-handler", eventLoopStrategy(h.messageQueue), b => {
                    b.step({
                        invoke: (scope, _message) => {
                            cooperationScopeIdentifier = scope.scopeIdentifier
                        },
                    })
                }),
            )

            try {
                await transactional(h.sql, async connection => {
                    await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
                })

                assert.ok(
                    await secondRootStepExecuting.await(1_000),
                    "Second step didn't start executing",
                )

                await transactional(h.sql, async connection => {
                    await h.scoop.capabilities.rollback(
                        connection,
                        cooperationScopeIdentifier,
                        "master-system",
                        "feelz",
                    )
                })

                rollbackEmitted.countDown()

                assert.ok(await latch.await(10_000), "Not everything completed correctly")
                await ciSleep(100)

                assert.deepEqual(await getEventSequence(h.sql), [
                    triple("EMITTED", null, null),
                    triple("SEEN", null, "root-handler"),
                    triple("EMITTED", "0", "root-handler"),
                    triple("SUSPENDED", "0", "root-handler"),
                    triple("SEEN", null, "child-handler"),
                    triple("SUSPENDED", "0", "child-handler"),
                    triple("COMMITTED", "0", "child-handler"),
                    triple("SUSPENDED", "1", "root-handler"),
                    triple("COMMITTED", "1", "root-handler"),
                ])
            } finally {
                await rootSubscription.close()
                await childSubscription.close()
            }
        })
    })
})

class IllegalStateError extends Error {
    constructor(message: string) {
        super(message)
        this.name = "IllegalStateError"
    }
}
