import assert from "node:assert/strict"
import { describe, test } from "node:test"
import { saga } from "../../../src/coroutine/builder/SagaBuilder.js"
import { eventLoopStrategy } from "../../../src/messaging/HandlerRegistry.js"
import { transactional } from "../../../src/coroutine/TransactionRunner.js"
import { ciSleep, setupScoopTest } from "../../support/harness.js"
import { CountDownLatch } from "../../support/latch.js"
import { getEventSequence, keepOnlyHandlers, keepOnlyPrefixedBy, triple } from "../../support/util.js"

const h = setupScoopTest()

describe("HappyPathTest", () => {
    test("handler should not complete until handlers listening to emitted messages complete - depth 1", async () => {
        const executionOrder: string[] = []

        const latch = new CountDownLatch(4) // 2 root steps + 2 child steps

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
                b.step({
                    invoke: async (_scope, _message) => {
                        latch.countDown()
                        executionOrder.push("root-handler-step-2")
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
                b.step({
                    invoke: async (_scope, _message) => {
                        latch.countDown()
                        executionOrder.push("child-handler-step-2")
                    },
                })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
            })

            assert.ok(await latch.await(10_000), "Not everything completed correctly")
            await ciSleep(100)

            assert.equal(executionOrder.length, 4, "Not everything completed correctly")
            assert.deepEqual(
                executionOrder,
                [
                    "root-handler-step-1",
                    "child-handler-step-1",
                    "child-handler-step-2",
                    "root-handler-step-2",
                ],
                "Execution order obeys structured cooperation rules",
            )

            assert.deepEqual(await getEventSequence(h.sql), [
                triple("EMITTED", null, null),
                triple("SEEN", null, "root-handler"),
                triple("EMITTED", "0", "root-handler"),
                triple("SUSPENDED", "0", "root-handler"),
                triple("SEEN", null, "child-handler"),
                triple("SUSPENDED", "0", "child-handler"),
                triple("SUSPENDED", "1", "child-handler"),
                triple("COMMITTED", "1", "child-handler"),
                triple("SUSPENDED", "1", "root-handler"),
                triple("COMMITTED", "1", "root-handler"),
            ])
        } finally {
            await rootSubscription.close()
            await childSubscription.close()
        }
    })

    test("handler should not complete until handlers listening to emitted messages complete - depth 2", async () => {
        const executionOrder: string[] = []

        const latch = new CountDownLatch(7) // 2 root steps + 3 child steps + 2 grandchild steps

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
                b.step({
                    invoke: async (_scope, _message) => {
                        latch.countDown()
                        executionOrder.push("root-handler-step-2")
                    },
                })
            }),
        )

        const childSubscription = h.subscribe(
            h.childTopic,
            saga("child-handler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: async (_scope, _message) => {
                        latch.countDown()
                        executionOrder.push("child-handler-step-1")
                    },
                })
                b.step({
                    invoke: async (scope, _message) => {
                        await scope.launch(h.grandchildTopic, { from: "child-handler" })
                        latch.countDown()
                        executionOrder.push("child-handler-step-2")
                    },
                })
                b.step({
                    invoke: async (_scope, _message) => {
                        latch.countDown()
                        executionOrder.push("child-handler-step-3")
                    },
                })
            }),
        )

        const grandchildSubscription = h.subscribe(
            h.grandchildTopic,
            saga("grandchild-handler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: async (_scope, _message) => {
                        await ciSleep(200)
                        executionOrder.push("grandchild-handler-step-1")
                        latch.countDown()
                    },
                })
                b.step({
                    invoke: async (_scope, _message) => {
                        latch.countDown()
                        executionOrder.push("grandchild-handler-step-2")
                    },
                })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
            })

            assert.ok(await latch.await(10_000), "All handlers should complete")

            await ciSleep(100)

            assert.equal(executionOrder.length, 7, "Not everything completed correctly")
            assert.deepEqual(
                executionOrder,
                [
                    "root-handler-step-1",
                    "child-handler-step-1",
                    "child-handler-step-2",
                    "grandchild-handler-step-1",
                    "grandchild-handler-step-2",
                    "child-handler-step-3",
                    "root-handler-step-2",
                ],
                "Execution order obeys structured cooperation rules",
            )

            assert.deepEqual(await getEventSequence(h.sql), [
                triple("EMITTED", null, null),
                triple("SEEN", null, "root-handler"),
                triple("EMITTED", "0", "root-handler"),
                triple("SUSPENDED", "0", "root-handler"),
                triple("SEEN", null, "child-handler"),
                triple("SUSPENDED", "0", "child-handler"),
                triple("EMITTED", "1", "child-handler"),
                triple("SUSPENDED", "1", "child-handler"),
                triple("SEEN", null, "grandchild-handler"),
                triple("SUSPENDED", "0", "grandchild-handler"),
                triple("SUSPENDED", "1", "grandchild-handler"),
                triple("COMMITTED", "1", "grandchild-handler"),
                triple("SUSPENDED", "2", "child-handler"),
                triple("COMMITTED", "2", "child-handler"),
                triple("SUSPENDED", "1", "root-handler"),
                triple("COMMITTED", "1", "root-handler"),
            ])
        } finally {
            await rootSubscription.close()
            await childSubscription.close()
            await grandchildSubscription.close()
        }
    })

    test("multiple handlers at same level should all complete before parent handler completes", async () => {
        const executionOrder: string[] = []

        const latch = new CountDownLatch(6) // 2 root steps + 2 x 2 child steps

        const childTopic2 = "child-topic-2"

        const rootSubscription = h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: async (scope, _message) => {
                        executionOrder.push("root-handler-step-1")
                        await scope.launch(h.childTopic, { from: "root-handler" })
                        await scope.launch(childTopic2, { from: "root-handler" })
                        latch.countDown()
                    },
                })
                b.step({
                    invoke: async (_scope, _message) => {
                        latch.countDown()
                        executionOrder.push("root-handler-step-2")
                    },
                })
            }),
        )

        const childSubscription1 = h.subscribe(
            h.childTopic,
            saga("child-handler-1", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: async (_scope, _message) => {
                        await ciSleep(100)
                        executionOrder.push("child-handler-1-step-1")
                        latch.countDown()
                    },
                })
                b.step({
                    invoke: async (_scope, _message) => {
                        executionOrder.push("child-handler-1-step-2")
                        latch.countDown()
                    },
                })
            }),
        )

        const childSubscription2 = h.subscribe(
            childTopic2,
            saga("child-handler-2", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: async (_scope, _message) => {
                        executionOrder.push("child-handler-2-step-1")
                        latch.countDown()
                    },
                })
                b.step({
                    invoke: async (_scope, _message) => {
                        await ciSleep(300)
                        executionOrder.push("child-handler-2-step-2")
                        latch.countDown()
                    },
                })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
            })

            assert.ok(await latch.await(10_000), "All handlers should complete")

            await ciSleep(100)

            assert.equal(executionOrder.length, 6, "Not everything completed correctly")

            assert.deepEqual(
                keepOnlyPrefixedBy(executionOrder, "root-handler", "child-handler-1"),
                [
                    "root-handler-step-1",
                    "child-handler-1-step-1",
                    "child-handler-1-step-2",
                    "root-handler-step-2",
                ],
                "Execution order obeys structured cooperation rules",
            )

            assert.deepEqual(
                keepOnlyPrefixedBy(executionOrder, "root-handler", "child-handler-2"),
                [
                    "root-handler-step-1",
                    "child-handler-2-step-1",
                    "child-handler-2-step-2",
                    "root-handler-step-2",
                ],
                "Execution order obeys structured cooperation rules",
            )

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
                    triple("EMITTED", "0", "root-handler"),
                    triple("SUSPENDED", "0", "root-handler"),
                    triple("SEEN", null, "child-handler-1"),
                    triple("SUSPENDED", "0", "child-handler-1"),
                    triple("SUSPENDED", "1", "child-handler-1"),
                    triple("COMMITTED", "1", "child-handler-1"),
                    triple("SUSPENDED", "1", "root-handler"),
                    triple("COMMITTED", "1", "root-handler"),
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
                    triple("EMITTED", "0", "root-handler"),
                    triple("SUSPENDED", "0", "root-handler"),
                    triple("SEEN", null, "child-handler-2"),
                    triple("SUSPENDED", "0", "child-handler-2"),
                    triple("SUSPENDED", "1", "child-handler-2"),
                    triple("COMMITTED", "1", "child-handler-2"),
                    triple("SUSPENDED", "1", "root-handler"),
                    triple("COMMITTED", "1", "root-handler"),
                ],
            )
        } finally {
            await rootSubscription.close()
            await childSubscription1.close()
            await childSubscription2.close()
        }
    })

    test("parent should wait for multiple handlers listening to the same topic", async () => {
        const executionOrder: string[] = []

        const latch = new CountDownLatch(6) // 2 root + 2 x 2 children listening to same topic

        const rootSubscription = h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: async (scope, _message) => {
                        executionOrder.push("root-handler-step-1")
                        await scope.launch(h.childTopic, { from: "root-handler" })
                        latch.countDown()
                    },
                })
                b.step({
                    invoke: async (_scope, _message) => {
                        latch.countDown()
                        executionOrder.push("root-handler-step-2")
                    },
                })
            }),
        )

        const childSubscription1 = h.subscribe(
            h.childTopic,
            saga("child-handler-1", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: async (_scope, _message) => {
                        await ciSleep(100)
                        executionOrder.push("child-handler-1-step-1")
                        latch.countDown()
                    },
                })
                b.step({
                    invoke: async (_scope, _message) => {
                        latch.countDown()
                        executionOrder.push("child-handler-1-step-2")
                    },
                })
            }),
        )

        const childSubscription2 = h.subscribe(
            h.childTopic,
            saga("child-handler-2", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: async (_scope, _message) => {
                        await ciSleep(300)
                        executionOrder.push("child-handler-2-step-1")
                        latch.countDown()
                    },
                })
                b.step({
                    invoke: async (_scope, _message) => {
                        latch.countDown()
                        executionOrder.push("child-handler-2-step-2")
                    },
                })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
            })

            assert.ok(await latch.await(10_000), "All handlers should complete")

            await ciSleep(100)

            assert.equal(executionOrder.length, 6, "Not everything completed correctly")

            assert.deepEqual(
                keepOnlyPrefixedBy(executionOrder, "root-handler", "child-handler-1"),
                [
                    "root-handler-step-1",
                    "child-handler-1-step-1",
                    "child-handler-1-step-2",
                    "root-handler-step-2",
                ],
                "Execution order obeys structured cooperation rules",
            )

            assert.deepEqual(
                keepOnlyPrefixedBy(executionOrder, "root-handler", "child-handler-2"),
                [
                    "root-handler-step-1",
                    "child-handler-2-step-1",
                    "child-handler-2-step-2",
                    "root-handler-step-2",
                ],
                "Execution order obeys structured cooperation rules",
            )

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
                    triple("SUSPENDED", "0", "child-handler-1"),
                    triple("SUSPENDED", "1", "child-handler-1"),
                    triple("COMMITTED", "1", "child-handler-1"),
                    triple("SUSPENDED", "1", "root-handler"),
                    triple("COMMITTED", "1", "root-handler"),
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
                    triple("SUSPENDED", "1", "child-handler-2"),
                    triple("COMMITTED", "1", "child-handler-2"),
                    triple("SUSPENDED", "1", "root-handler"),
                    triple("COMMITTED", "1", "root-handler"),
                ],
            )
        } finally {
            await rootSubscription.close()
            await childSubscription1.close()
            await childSubscription2.close()
        }
    })
})
