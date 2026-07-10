import assert from "node:assert/strict"
import { describe, test } from "node:test"
import { saga } from "../../../src/coroutine/builder/SagaBuilder.js"
import { tryFinallyStep } from "../../../src/coroutine/builder/TryFinally.js"
import { eventLoopStrategy } from "../../../src/messaging/HandlerRegistry.js"
import { transactional } from "../../../src/coroutine/TransactionRunner.js"
import { ciSleep, setupScoopTest } from "../../support/harness.js"
import { CountDownLatch } from "../../support/latch.js"

const h = setupScoopTest()

describe("TryFinallyTest", () => {
    test("finally is executed on success", async () => {
        const executionOrder: string[] = []

        const finallyTopic = "finally-topic"
        const latch = new CountDownLatch(1)

        const rootSubscription = h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                tryFinallyStep(
                    b,
                    async (scope, _message) => {
                        executionOrder.push("root-try")
                        await scope.launch(h.childTopic, { from: "root-handler" })
                    },
                    async (scope, _message) => {
                        executionOrder.push("root-finally")
                        await scope.launch(finallyTopic, { from: "root-handler" })
                    },
                )
                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("root-end")
                        latch.countDown()
                    },
                })
            }),
        )

        const childSubscription = h.subscribe(
            h.childTopic,
            saga("child-handler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("child-handler")
                    },
                })
            }),
        )

        const finallySubscription = h.subscribe(
            finallyTopic,
            saga("finally-handler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("finally-handler")
                    },
                })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
            })

            assert.ok(await latch.await(10_000), "All handlers should complete")
            await ciSleep(200)

            assert.deepEqual(
                executionOrder,
                ["root-try", "child-handler", "root-finally", "finally-handler", "root-end"],
                "Execution order obeys structured cooperation rules",
            )
        } finally {
            await rootSubscription.close()
            await childSubscription.close()
            await finallySubscription.close()
        }
    })

    test("finally is executed on root failure but messages are not emitted, because neither were those in the 'try' step", async () => {
        const executionOrder: string[] = []

        const finallyTopic = "finally-topic"
        const latch = new CountDownLatch(1)

        const rootSubscription = h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                tryFinallyStep(
                    b,
                    (_scope, _message) => {
                        executionOrder.push("root-try")
                        throw new Error("Simulated failure to test rollback")
                    },
                    async (scope, _message) => {
                        executionOrder.push("root-finally")
                        await scope.launch(finallyTopic, { from: "root-handler" })
                        latch.countDown()
                    },
                )
            }),
        )

        const finallySubscription = h.subscribe(
            finallyTopic,
            saga("finally-handler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("finally-handler")
                    },
                })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
            })

            assert.ok(await latch.await(10_000), "All handlers should complete")
            await ciSleep(200)

            assert.deepEqual(
                executionOrder,
                ["root-try", "root-finally"],
                "Execution order obeys structured cooperation rules",
            )
        } finally {
            await rootSubscription.close()
            await finallySubscription.close()
        }
    })

    test("finally is executed on child failure", async () => {
        const executionOrder: string[] = []

        const finallyTopic = "finally-topic"
        const latch = new CountDownLatch(1)

        const rootSubscription = h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                tryFinallyStep(
                    b,
                    async (scope, _message) => {
                        executionOrder.push("root-try")
                        await scope.launch(h.childTopic, { from: "root-handler" })
                    },
                    async (scope, _message) => {
                        executionOrder.push("root-finally")
                        await scope.launch(finallyTopic, { from: "root-handler" })
                    },
                )
                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("root-end")
                    },
                })
            }),
        )

        const childSubscription = h.subscribe(
            h.childTopic,
            saga("child-handler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("child-handler")
                        throw new Error("Simulated failure to test rollback")
                    },
                })
            }),
        )

        const finallySubscription = h.subscribe(
            finallyTopic,
            saga("finally-handler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("finally-handler")
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
            await ciSleep(200)

            assert.deepEqual(
                executionOrder,
                ["root-try", "child-handler", "root-finally", "finally-handler"],
                "Execution order obeys structured cooperation rules",
            )
        } finally {
            await rootSubscription.close()
            await childSubscription.close()
            await finallySubscription.close()
        }
    })

    test("finally is executed, once, on subsequent step failure", async () => {
        const executionOrder: string[] = []

        const finallyTopic = "finally-topic"
        const latch = new CountDownLatch(1)

        const rootSubscription = h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("root-start")
                    },
                    rollback: (_scope, _message, _throwable) => {
                        executionOrder.push("root-rollback")
                        latch.countDown()
                    },
                })
                tryFinallyStep(
                    b,
                    (_scope, _message) => {
                        executionOrder.push("root-try")
                    },
                    async (scope, _message) => {
                        executionOrder.push("root-finally")
                        await scope.launch(finallyTopic, { from: "root-handler" })
                    },
                )
                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("root-failure")
                        throw new Error("Simulated failure to test rollback")
                    },
                })
            }),
        )

        const finallySubscription = h.subscribe(
            finallyTopic,
            saga("finally-handler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("finally-handler")
                    },
                })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
            })

            assert.ok(await latch.await(10_000), "All handlers should complete")
            await ciSleep(200)

            assert.deepEqual(
                executionOrder,
                [
                    "root-start",
                    "root-try",
                    "root-finally",
                    "finally-handler",
                    "root-failure",
                    "root-rollback",
                ],
                "Execution order obeys structured cooperation rules",
            )
        } finally {
            await rootSubscription.close()
            await finallySubscription.close()
        }
    })

    test("finally is only executed once when its child causes a rollback", async () => {
        const executionOrder: string[] = []

        const finallyTopic = "finally-topic"
        const latch = new CountDownLatch(1)

        const rootSubscription = h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("root-start")
                    },
                    rollback: (_scope, _message, _throwable) => {
                        executionOrder.push("root-rollback")
                        latch.countDown()
                    },
                })
                tryFinallyStep(
                    b,
                    (_scope, _message) => {
                        executionOrder.push("root-try")
                    },
                    async (scope, _message) => {
                        executionOrder.push("root-finally")
                        await scope.launch(finallyTopic, { from: "root-handler" })
                    },
                )
            }),
        )

        const finallySubscription = h.subscribe(
            finallyTopic,
            saga("finally-handler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("finally-handler")
                        throw new Error("Simulated failure to test rollback")
                    },
                })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
            })

            assert.ok(await latch.await(10_000), "All handlers should complete")
            await ciSleep(200)

            assert.deepEqual(
                executionOrder,
                ["root-start", "root-try", "root-finally", "finally-handler", "root-rollback"],
                "Execution order obeys structured cooperation rules",
            )
        } finally {
            await rootSubscription.close()
            await finallySubscription.close()
        }
    })
})
