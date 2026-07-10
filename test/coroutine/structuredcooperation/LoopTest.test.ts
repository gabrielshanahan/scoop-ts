import assert from "node:assert/strict"
import { describe, test } from "node:test"
import { saga } from "../../../src/coroutine/builder/SagaBuilder.js"
import { Continue, Repeat } from "../../../src/coroutine/DistributedCoroutine.js"
import { eventLoopStrategy } from "../../../src/messaging/HandlerRegistry.js"
import { transactional } from "../../../src/coroutine/TransactionRunner.js"
import { ciSleep, eventLogSettled, setupScoopTest } from "../../support/harness.js"
import { CountDownLatch } from "../../support/latch.js"
import { getEventSequence, triple } from "../../support/util.js"

const h = setupScoopTest()

describe("LoopTest", () => {
    test("step returning Repeat re-executes with incremented iteration", async () => {
        const executionOrder: string[] = []
        let loopCounter = 0

        const latch = new CountDownLatch(4) // 3 loop iterations + 1 next step

        const rootSubscription = h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                b.controlledStep({
                    name: "loop-step",
                    invoke: (_scope, _message, iteration) => {
                        executionOrder.push(`loop-step-iter-${iteration}`)
                        latch.countDown()
                        return ++loopCounter < 3 ? Repeat : Continue
                    },
                })
                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("after-loop-step")
                        latch.countDown()
                    },
                })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
            })

            assert.ok(await latch.await(10_000), "Not everything completed correctly")
            await eventLogSettled(h.sql)

            assert.deepEqual(
                executionOrder,
                ["loop-step-iter-0", "loop-step-iter-1", "loop-step-iter-2", "after-loop-step"],
                "Loop step should execute 3 times then advance",
            )

            assert.deepEqual(await getEventSequence(h.sql), [
                triple("EMITTED", null, null),
                triple("SEEN", null, "root-handler"),
                triple("SUSPENDED", "loop-step", "root-handler"),
                triple("SUSPENDED", "loop-step", "root-handler"),
                triple("SUSPENDED", "loop-step", "root-handler"),
                triple("SUSPENDED", "1", "root-handler"),
                triple("COMMITTED", "1", "root-handler"),
            ])
        } finally {
            await rootSubscription.close()
        }
    })

    test("zero-iteration loop behaves like normal step", async () => {
        const executionOrder: string[] = []

        const latch = new CountDownLatch(2)

        const rootSubscription = h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                b.controlledStep({
                    name: "immediate-continue",
                    invoke: (_scope, _message, iteration) => {
                        executionOrder.push(`step-1-iter-${iteration}`)
                        latch.countDown()
                        return Continue
                    },
                })
                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("step-2")
                        latch.countDown()
                    },
                })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
            })

            assert.ok(await latch.await(10_000), "Not everything completed correctly")
            await eventLogSettled(h.sql)

            assert.deepEqual(
                executionOrder,
                ["step-1-iter-0", "step-2"],
                "Immediate Continue behaves like a normal step",
            )

            assert.deepEqual(await getEventSequence(h.sql), [
                triple("EMITTED", null, null),
                triple("SEEN", null, "root-handler"),
                triple("SUSPENDED", "immediate-continue", "root-handler"),
                triple("SUSPENDED", "1", "root-handler"),
                triple("COMMITTED", "1", "root-handler"),
            ])
        } finally {
            await rootSubscription.close()
        }
    })

    test("loop with child launches waits for each batch", async () => {
        const executionOrder: string[] = []
        let loopCounter = 0

        const latch = new CountDownLatch(5) // 2 loop iterations + 2 child steps + 1 next step

        const rootSubscription = h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                b.controlledStep({
                    name: "loop-step",
                    invoke: async (scope, _message, iteration) => {
                        await scope.launch(h.childTopic, { from: `loop-iter-${iteration}` })
                        executionOrder.push(`loop-step-iter-${iteration}`)
                        latch.countDown()
                        return ++loopCounter < 2 ? Repeat : Continue
                    },
                })
                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("after-loop-step")
                        latch.countDown()
                    },
                })
            }),
        )

        const childSubscription = h.subscribe(
            h.childTopic,
            saga("child-handler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: async (_scope, _message) => {
                        await ciSleep(50)
                        executionOrder.push("child-handler-step")
                        latch.countDown()
                    },
                })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
            })

            assert.ok(await latch.await(10_000), "Not everything completed correctly")
            await eventLogSettled(h.sql)

            // Each loop iteration should wait for its children before the next iteration
            assert.deepEqual(
                executionOrder,
                [
                    "loop-step-iter-0",
                    "child-handler-step",
                    "loop-step-iter-1",
                    "child-handler-step",
                    "after-loop-step",
                ],
                "Each loop iteration waits for child handlers before next iteration",
            )

            assert.deepEqual(await getEventSequence(h.sql), [
                triple("EMITTED", null, null),
                triple("SEEN", null, "root-handler"),
                triple("EMITTED", "loop-step", "root-handler"),
                triple("SUSPENDED", "loop-step", "root-handler"),
                triple("SEEN", null, "child-handler"),
                triple("SUSPENDED", "0", "child-handler"),
                triple("COMMITTED", "0", "child-handler"),
                triple("EMITTED", "loop-step", "root-handler"),
                triple("SUSPENDED", "loop-step", "root-handler"),
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

    test("handleChildFailures receives correct childFailureHandlerIteration", async () => {
        const executionOrder: string[] = []

        const latch = new CountDownLatch(1) // root rollback

        const rootSubscription = h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                b.controlledStep({
                    name: "step-with-children",
                    invoke: async (scope, _message, stepIteration) => {
                        await scope.launch(h.childTopic, { from: "root" })
                        executionOrder.push(`invoke-iter-${stepIteration}`)
                        return Continue
                    },
                    rollback: (_scope, _message, _throwable, _stepIteration, _cfhi) => {
                        executionOrder.push("rollback")
                        latch.countDown()
                    },
                    handleChildFailures: async (
                        scope,
                        _message,
                        throwable,
                        _stepIteration,
                        childFailureHandlerIteration,
                        nextStep,
                    ) => {
                        executionOrder.push(
                            `handleChildFailures-childFailureHandlerIteration-${childFailureHandlerIteration}`,
                        )
                        if (childFailureHandlerIteration < 1) {
                            // First failure handling: retry by launching another child
                            await scope.launch(h.childTopic, { from: "retry" })
                            return nextStep
                        }
                        // Second failure handling: give up
                        throw throwable
                    },
                })
            }),
        )

        const childSubscription = h.subscribe(
            h.childTopic,
            saga("child-handler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("child-step")
                        throw new Error("Simulated child failure")
                    },
                })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
            })

            assert.ok(await latch.await(10_000), "Not everything completed correctly")
            await eventLogSettled(h.sql)

            assert.deepEqual(
                executionOrder,
                [
                    "invoke-iter-0",
                    "child-step",
                    "handleChildFailures-childFailureHandlerIteration-0",
                    "child-step",
                    "handleChildFailures-childFailureHandlerIteration-1",
                    "rollback",
                ],
                "handleChildFailures should receive incrementing childFailureHandlerIteration values",
            )

            assert.deepEqual(await getEventSequence(h.sql), [
                triple("EMITTED", null, null),
                triple("SEEN", null, "root-handler"),
                triple("EMITTED", "step-with-children", "root-handler"),
                triple("SUSPENDED", "step-with-children", "root-handler"),
                triple("SEEN", null, "child-handler"),
                triple("ROLLING_BACK", "0", "child-handler"),
                triple("ROLLED_BACK", "Rollback of 0[0,]", "child-handler"),
                triple("EMITTED", "step-with-children", "root-handler"),
                triple("SUSPENDED", "step-with-children", "root-handler"),
                triple("SEEN", null, "child-handler"),
                triple("ROLLING_BACK", "0", "child-handler"),
                triple("ROLLED_BACK", "Rollback of 0[0,]", "child-handler"),
                triple("ROLLING_BACK", "step-with-children", "root-handler"),
                triple("ROLLBACK_EMITTED", "Rollback of step-with-children[0,0] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of step-with-children[0,0] (rolling back child scopes)", "root-handler"),
                triple("ROLLBACK_EMITTED", "Rollback of step-with-children[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of step-with-children[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of step-with-children[0,]", "root-handler"),
                triple("ROLLED_BACK", "Rollback of step-with-children[0,]", "root-handler"),
            ])
        } finally {
            await rootSubscription.close()
            await childSubscription.close()
        }
    })

    test("mid-loop failure triggers rollback for each iteration in reverse order", async () => {
        const executionOrder: string[] = []
        let loopCounter = 0

        const latch = new CountDownLatch(1) // wait for final rollback

        const rootSubscription = h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                b.controlledStep({
                    name: "loop-step",
                    invoke: (_scope, _message, stepIteration) => {
                        executionOrder.push(`invoke-iter-${stepIteration}`)
                        if (++loopCounter < 3) {
                            return Repeat
                        }
                        // Fail on iteration 2
                        throw new Error("Fail on iteration 2")
                    },
                    rollback: (_scope, _message, _throwable, stepIteration, _cfhi) => {
                        executionOrder.push(`rollback-iter-${stepIteration}`)
                        if (stepIteration === 0) {
                            latch.countDown()
                        }
                    },
                })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
            })

            assert.ok(await latch.await(10_000), "Not everything completed correctly")
            await eventLogSettled(h.sql)

            // Iteration 2 threw during invoke, so its transaction rolled back and no
            // persistent state was committed. Rollback only covers committed iterations 0 and 1.
            // stepIteration in rollback is always 0 (hardcoded).
            assert.deepEqual(
                executionOrder,
                [
                    "invoke-iter-0",
                    "invoke-iter-1",
                    "invoke-iter-2",
                    "rollback-iter-0",
                    "rollback-iter-0",
                ],
                "Rollback should traverse committed iterations in reverse order",
            )

            assert.deepEqual(await getEventSequence(h.sql), [
                triple("EMITTED", null, null),
                triple("SEEN", null, "root-handler"),
                triple("SUSPENDED", "loop-step", "root-handler"),
                triple("SUSPENDED", "loop-step", "root-handler"),
                triple("ROLLING_BACK", "loop-step", "root-handler"),
                triple("SUSPENDED", "Rollback of loop-step[1,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of loop-step[1,]", "root-handler"),
                triple("SUSPENDED", "Rollback of loop-step[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of loop-step[0,]", "root-handler"),
                triple("ROLLED_BACK", "Rollback of loop-step[0,]", "root-handler"),
            ])
        } finally {
            await rootSubscription.close()
        }
    })

    test("single iteration loop with rollback behaves like normal step rollback", async () => {
        const executionOrder: string[] = []

        const latch = new CountDownLatch(1)

        const rootSubscription = h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                b.controlledStep({
                    name: "loop-step",
                    invoke: (_scope, _message, stepIteration) => {
                        executionOrder.push(`invoke-loop-${stepIteration}`)
                        return Continue
                    },
                    rollback: (_scope, _message, _throwable, _stepIteration, _cfhi) => {
                        executionOrder.push("rollback-loop")
                        latch.countDown()
                    },
                })
                b.step({
                    name: "failing-step",
                    invoke: (_scope, _message) => {
                        executionOrder.push("invoke-failing")
                        throw new Error("Failure")
                    },
                })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
            })

            assert.ok(await latch.await(10_000), "Not everything completed correctly")
            await eventLogSettled(h.sql)

            assert.deepEqual(
                executionOrder,
                ["invoke-loop-0", "invoke-failing", "rollback-loop"],
                "Single-iteration loop rollback behaves like normal step rollback",
            )

            assert.deepEqual(await getEventSequence(h.sql), [
                triple("EMITTED", null, null),
                triple("SEEN", null, "root-handler"),
                triple("SUSPENDED", "loop-step", "root-handler"),
                triple("ROLLING_BACK", "failing-step", "root-handler"),
                triple("SUSPENDED", "Rollback of loop-step[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of loop-step[0,]", "root-handler"),
                triple("ROLLED_BACK", "Rollback of loop-step[0,]", "root-handler"),
            ])
        } finally {
            await rootSubscription.close()
        }
    })

    test("completed loop followed by later failure rolls back all iterations", async () => {
        const executionOrder: string[] = []
        let loopCounter = 0

        const latch = new CountDownLatch(1) // wait for last rollback

        const rootSubscription = h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                b.controlledStep({
                    name: "loop-step",
                    invoke: (_scope, _message, stepIteration) => {
                        executionOrder.push(`invoke-loop-${stepIteration}`)
                        return ++loopCounter < 3 ? Repeat : Continue
                    },
                    rollback: (_scope, _message, _throwable, _stepIteration, _cfhi) => {
                        executionOrder.push("rollback-loop")
                        if (executionOrder.filter(entry => entry === "rollback-loop").length >= 3) {
                            latch.countDown()
                        }
                    },
                })
                b.step({
                    name: "middle-step",
                    invoke: (_scope, _message) => {
                        executionOrder.push("invoke-middle")
                    },
                    rollback: (_scope, _message, _throwable) => {
                        executionOrder.push("rollback-middle")
                    },
                })
                b.step({
                    name: "failing-step",
                    invoke: (_scope, _message) => {
                        executionOrder.push("invoke-failing")
                        throw new Error("Step after loop fails")
                    },
                })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
            })

            assert.ok(await latch.await(10_000), "Not everything completed correctly")
            await eventLogSettled(h.sql)

            // Flow: loop(0)→loop(1)→loop(2,Continue)→middle→failing(throws)
            // Rollback covers committed SUSPENDED steps in reverse chronological order:
            //   middle, loop(iter-2), loop(iter-1), loop(iter-0)
            // failing-step threw during invoke, so no SUSPENDED was committed for it.
            assert.deepEqual(
                executionOrder,
                [
                    "invoke-loop-0",
                    "invoke-loop-1",
                    "invoke-loop-2",
                    "invoke-middle",
                    "invoke-failing",
                    "rollback-middle",
                    "rollback-loop",
                    "rollback-loop",
                    "rollback-loop",
                ],
                "Rollback should cover middle-step and all 3 loop iterations in reverse",
            )

            assert.deepEqual(await getEventSequence(h.sql), [
                triple("EMITTED", null, null),
                triple("SEEN", null, "root-handler"),
                triple("SUSPENDED", "loop-step", "root-handler"),
                triple("SUSPENDED", "loop-step", "root-handler"),
                triple("SUSPENDED", "loop-step", "root-handler"),
                triple("SUSPENDED", "middle-step", "root-handler"),
                triple("ROLLING_BACK", "failing-step", "root-handler"),
                triple("SUSPENDED", "Rollback of middle-step[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of middle-step[0,]", "root-handler"),
                triple("SUSPENDED", "Rollback of loop-step[2,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of loop-step[2,]", "root-handler"),
                triple("SUSPENDED", "Rollback of loop-step[1,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of loop-step[1,]", "root-handler"),
                triple("SUSPENDED", "Rollback of loop-step[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of loop-step[0,]", "root-handler"),
                triple("ROLLED_BACK", "Rollback of loop-step[0,]", "root-handler"),
            ])
        } finally {
            await rootSubscription.close()
        }
    })

    test("multiple loop steps followed by failure roll back all iterations of both", async () => {
        const executionOrder: string[] = []
        let loopACounter = 0
        let loopBCounter = 0

        const latch = new CountDownLatch(1) // wait for last rollback

        const rootSubscription = h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                b.controlledStep({
                    name: "loop-A",
                    invoke: (_scope, _message, stepIteration) => {
                        executionOrder.push(`invoke-A-${stepIteration}`)
                        return ++loopACounter < 2 ? Repeat : Continue
                    },
                    rollback: (_scope, _message, _throwable, _stepIteration, _cfhi) => {
                        executionOrder.push("rollback-A")
                        if (executionOrder.filter(entry => entry === "rollback-A").length >= 2) {
                            latch.countDown()
                        }
                    },
                })
                b.controlledStep({
                    name: "loop-B",
                    invoke: (_scope, _message, stepIteration) => {
                        executionOrder.push(`invoke-B-${stepIteration}`)
                        return ++loopBCounter < 2 ? Repeat : Continue
                    },
                    rollback: (_scope, _message, _throwable, _stepIteration, _cfhi) => {
                        executionOrder.push("rollback-B")
                    },
                })
                b.step({
                    name: "failing-step",
                    invoke: (_scope, _message) => {
                        executionOrder.push("invoke-failing")
                        throw new Error("Final step fails")
                    },
                })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
            })

            assert.ok(await latch.await(10_000), "Not everything completed correctly")
            await eventLogSettled(h.sql)

            // Flow: A(0)→A(1,Continue)→B(0)→B(1,Continue)→failing(throws)
            // Rollback in reverse chronological: B(1), B(0), A(1), A(0)
            assert.deepEqual(
                executionOrder,
                [
                    "invoke-A-0",
                    "invoke-A-1",
                    "invoke-B-0",
                    "invoke-B-1",
                    "invoke-failing",
                    "rollback-B",
                    "rollback-B",
                    "rollback-A",
                    "rollback-A",
                ],
                "Rollback should cover all iterations of both loop steps in reverse order",
            )

            assert.deepEqual(await getEventSequence(h.sql), [
                triple("EMITTED", null, null),
                triple("SEEN", null, "root-handler"),
                triple("SUSPENDED", "loop-A", "root-handler"),
                triple("SUSPENDED", "loop-A", "root-handler"),
                triple("SUSPENDED", "loop-B", "root-handler"),
                triple("SUSPENDED", "loop-B", "root-handler"),
                triple("ROLLING_BACK", "failing-step", "root-handler"),
                triple("SUSPENDED", "Rollback of loop-B[1,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of loop-B[1,]", "root-handler"),
                triple("SUSPENDED", "Rollback of loop-B[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of loop-B[0,]", "root-handler"),
                triple("SUSPENDED", "Rollback of loop-A[1,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of loop-A[1,]", "root-handler"),
                triple("SUSPENDED", "Rollback of loop-A[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of loop-A[0,]", "root-handler"),
                triple("ROLLED_BACK", "Rollback of loop-A[0,]", "root-handler"),
            ])
        } finally {
            await rootSubscription.close()
        }
    })

    test("childFailureHandlerIteration is tracked correctly during rollback", async () => {
        const executionOrder: string[] = []
        const latch = new CountDownLatch(1)

        const rootSubscription = h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                b.controlledStep({
                    name: "step-with-child",
                    invoke: async (scope, _message, _stepIteration) => {
                        await scope.launch(h.childTopic, { from: "root" })
                        executionOrder.push("invoke-step-0")
                        return Continue
                    },
                    handleChildFailures: async (
                        scope,
                        _message,
                        throwable,
                        _stepIteration,
                        childFailureHandlerIteration,
                        nextStep,
                    ) => {
                        executionOrder.push(
                            `handleChildFailures-rollback-cfhi-${childFailureHandlerIteration}`,
                        )
                        if (childFailureHandlerIteration < 1) {
                            // First failure: retry by launching a child that will also fail
                            await scope.launch(h.grandchildTopic, { from: "retry" })
                            return nextStep
                        }
                        // Second failure: give up
                        latch.countDown()
                        throw throwable
                    },
                })
                b.step({
                    name: "failing-step",
                    invoke: (_scope, _message) => {
                        executionOrder.push("invoke-step-1-throws")
                        throw new Error("Trigger rollback")
                    },
                })
            }),
        )

        const childSubscription = h.subscribe(
            h.childTopic,
            saga("child-handler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("child-step")
                    },
                    rollback: (_scope, _message, _throwable) => {
                        executionOrder.push("child-rollback-throws")
                        throw new Error("Child rollback fails")
                    },
                })
            }),
        )

        const grandchildSubscription = h.subscribe(
            h.grandchildTopic,
            saga("grandchild-handler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("grandchild-step-throws")
                        throw new Error("Grandchild fails")
                    },
                })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
            })

            assert.ok(await latch.await(10_000), "Not everything completed correctly")
            await eventLogSettled(h.sql)

            assert.deepEqual(
                executionOrder,
                [
                    "invoke-step-0",
                    "child-step",
                    "invoke-step-1-throws",
                    "child-rollback-throws",
                    "handleChildFailures-rollback-cfhi-0",
                    "grandchild-step-throws",
                    "handleChildFailures-rollback-cfhi-1",
                ],
                "handleChildFailures should receive incrementing childFailureHandlerIteration " +
                    "during rollback",
            )

            assert.deepEqual(await getEventSequence(h.sql), [
                triple("EMITTED", null, null),
                triple("SEEN", null, "root-handler"),
                triple("EMITTED", "step-with-child", "root-handler"),
                triple("SUSPENDED", "step-with-child", "root-handler"),
                triple("SEEN", null, "child-handler"),
                triple("SUSPENDED", "0", "child-handler"),
                triple("COMMITTED", "0", "child-handler"),
                triple("ROLLING_BACK", "failing-step", "root-handler"),
                triple("ROLLBACK_EMITTED", "Rollback of step-with-child[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of step-with-child[0,] (rolling back child scopes)", "root-handler"),
                triple("ROLLING_BACK", null, "child-handler"),
                triple("SUSPENDED", "Rollback of 0[0,] (rolling back child scopes)", "child-handler"),
                triple("ROLLBACK_FAILED", "Rollback of 0[0,]", "child-handler"),
                triple("EMITTED", "Rollback of step-with-child[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of step-with-child[0,] (rolling back child scopes)", "root-handler"),
                triple("SEEN", null, "grandchild-handler"),
                triple("ROLLING_BACK", "0", "grandchild-handler"),
                triple("ROLLED_BACK", "Rollback of 0[0,]", "grandchild-handler"),
                triple("ROLLBACK_FAILED", "Rollback of step-with-child[0,] (rolling back child scopes)", "root-handler"),
            ])
        } finally {
            await rootSubscription.close()
            await childSubscription.close()
            await grandchildSubscription.close()
        }
    })
})
