import assert from "node:assert/strict"
import { describe, test } from "node:test"
import { saga } from "../../../src/coroutine/builder/SagaBuilder.js"
import { Continue, GoTo, NextStep, Repeat } from "../../../src/coroutine/DistributedCoroutine.js"
import { eventLoopStrategy } from "../../../src/messaging/HandlerRegistry.js"
import { transactional } from "../../../src/coroutine/TransactionRunner.js"
import { ciSleep, eventLogSettled, setupScoopTest } from "../../support/harness.js"
import { CountDownLatch } from "../../support/latch.js"
import { getEventSequence, triple } from "../../support/util.js"

const h = setupScoopTest()

/** Renders a NextStep exactly like the Kotlin data classes' toString (used in assertions). */
function renderNextStep(nextStep: NextStep): string {
    switch (nextStep.kind) {
        case "continue":
            return "Continue"
        case "repeat":
            return "Repeat"
        case "goTo":
            return `GoTo(stepIndex=${nextStep.stepIndex})`
    }
}

describe("GoToTest", () => {
    test("GoTo forward skips intermediate steps", async () => {
        const executionOrder: string[] = []

        const latch = new CountDownLatch(2) // step-0 + step-2

        const rootSubscription = await h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                b.controlledStep({
                    name: "step-0",
                    invoke: (_scope, _message, _stepIteration) => {
                        executionOrder.push("step-0")
                        latch.countDown()
                        return GoTo(2)
                    },
                })
                b.step({
                    name: "step-1",
                    invoke: (_scope, _message) => {
                        executionOrder.push("step-1-SHOULD-NOT-EXECUTE")
                        latch.countDown()
                    },
                })
                b.step({
                    name: "step-2",
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
                ["step-0", "step-2"],
                "GoTo(2) should skip step-1 entirely",
            )

            assert.deepEqual(await getEventSequence(h.sql), [
                triple("EMITTED", null, null),
                triple("SEEN", null, "root-handler"),
                triple("SUSPENDED", "step-0", "root-handler"),
                triple("SUSPENDED", "step-2", "root-handler"),
                triple("COMMITTED", "step-2", "root-handler"),
            ])
        } finally {
            await rootSubscription.close()
        }
    })

    test("GoTo backward re-executes from target step", async () => {
        const executionOrder: string[] = []
        let cycleCount = 0

        // A→B→C→GoTo(0)→A→B→C = 6 step executions
        const latch = new CountDownLatch(6)

        const rootSubscription = await h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                b.controlledStep({
                    name: "step-A",
                    invoke: (_scope, _message, stepIteration) => {
                        executionOrder.push(`A-crc-${stepIteration}`)
                        latch.countDown()
                        return Continue
                    },
                })
                b.step({
                    name: "step-B",
                    invoke: (_scope, _message) => {
                        executionOrder.push("B")
                        latch.countDown()
                    },
                })
                b.controlledStep({
                    name: "step-C",
                    invoke: (_scope, _message, _stepIteration) => {
                        executionOrder.push("C")
                        latch.countDown()
                        return ++cycleCount < 2 ? GoTo(0) : Continue
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

            // GoTo backward works: step A re-executes after GoTo(0) from step C.
            // stepIteration for second A is 0 because B and C ran in between,
            // breaking the consecutive chain.
            assert.deepEqual(
                executionOrder,
                ["A-crc-0", "B", "C", "A-crc-0", "B", "C"],
                "GoTo(0) should re-execute from step A with crc reset to 0",
            )

            assert.deepEqual(await getEventSequence(h.sql), [
                triple("EMITTED", null, null),
                triple("SEEN", null, "root-handler"),
                triple("SUSPENDED", "step-A", "root-handler"),
                triple("SUSPENDED", "step-B", "root-handler"),
                triple("SUSPENDED", "step-C", "root-handler"),
                triple("SUSPENDED", "step-A", "root-handler"),
                triple("SUSPENDED", "step-B", "root-handler"),
                triple("SUSPENDED", "step-C", "root-handler"),
                triple("COMMITTED", "step-C", "root-handler"),
            ])
        } finally {
            await rootSubscription.close()
        }
    })

    test("GoTo self behaves like Repeat", async () => {
        const executionOrder: string[] = []
        let loopCounter = 0

        const latch = new CountDownLatch(4) // 3 self-goto iterations + 1 next step

        const rootSubscription = await h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                b.controlledStep({
                    name: "self-goto-step",
                    invoke: (_scope, _message, stepIteration) => {
                        executionOrder.push(`self-goto-iter-${stepIteration}`)
                        latch.countDown()
                        return ++loopCounter < 3 ? GoTo(0) : Continue // GoTo own index
                    },
                })
                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("after-self-goto")
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
                ["self-goto-iter-0", "self-goto-iter-1", "self-goto-iter-2", "after-self-goto"],
                "GoTo(ownIndex) should behave identically to Repeat",
            )

            assert.deepEqual(await getEventSequence(h.sql), [
                triple("EMITTED", null, null),
                triple("SEEN", null, "root-handler"),
                triple("SUSPENDED", "self-goto-step", "root-handler"),
                triple("SUSPENDED", "self-goto-step", "root-handler"),
                triple("SUSPENDED", "self-goto-step", "root-handler"),
                triple("SUSPENDED", "1", "root-handler"),
                triple("COMMITTED", "1", "root-handler"),
            ])
        } finally {
            await rootSubscription.close()
        }
    })

    test("GoTo plus rollback rolls back in reverse chronological order", async () => {
        const executionOrder: string[] = []
        let stepACount = 0

        const latch = new CountDownLatch(1) // wait for last rollback

        const rootSubscription = await h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                b.controlledStep({
                    name: "step-A",
                    invoke: (_scope, _message, _stepIteration) => {
                        executionOrder.push("invoke-A")
                        // Second time: skip B, go directly to C
                        return ++stepACount < 2 ? Continue : GoTo(2)
                    },
                    rollback: (_scope, _message, _throwable, _stepIteration, _cfhi) => {
                        executionOrder.push("rollback-A")
                        if (executionOrder.filter(entry => entry === "rollback-A").length >= 2) {
                            latch.countDown()
                        }
                    },
                })
                b.step({
                    name: "step-B",
                    invoke: (_scope, _message) => {
                        executionOrder.push("invoke-B")
                    },
                    rollback: (_scope, _message, _throwable) => {
                        executionOrder.push("rollback-B")
                    },
                })
                b.controlledStep({
                    name: "step-C",
                    invoke: (_scope, _message, _stepIteration) => {
                        const cCount = executionOrder.filter(
                            entry => entry === "invoke-C",
                        ).length
                        executionOrder.push("invoke-C")
                        if (cCount > 0) {
                            // Second time C runs: fail
                            throw new Error("Fail on second C")
                        }
                        return GoTo(0)
                    },
                    rollback: (_scope, _message, _throwable, _stepIteration, _cfhi) => {
                        executionOrder.push("rollback-C")
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

            // Flow: A→B→C(GoTo(0))→A(GoTo(2))→C(fail)
            // Rollback covers committed SUSPENDED steps in reverse chronological order:
            //   A(2nd), C(1st), B, A(1st)
            // stepIteration in rollback is always 0 (hardcoded in RollbackPathContinuation)
            assert.deepEqual(
                executionOrder,
                [
                    "invoke-A",
                    "invoke-B",
                    "invoke-C",
                    "invoke-A",
                    "invoke-C",
                    "rollback-A",
                    "rollback-C",
                    "rollback-B",
                    "rollback-A",
                ],
                "Rollback should traverse committed steps in reverse chronological order",
            )

            assert.deepEqual(await getEventSequence(h.sql), [
                triple("EMITTED", null, null),
                triple("SEEN", null, "root-handler"),
                triple("SUSPENDED", "step-A", "root-handler"),
                triple("SUSPENDED", "step-B", "root-handler"),
                triple("SUSPENDED", "step-C", "root-handler"),
                triple("SUSPENDED", "step-A", "root-handler"),
                triple("ROLLING_BACK", "step-C", "root-handler"),
                triple("SUSPENDED", "Rollback of step-A[1,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of step-A[1,]", "root-handler"),
                triple("SUSPENDED", "Rollback of step-C[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of step-C[0,]", "root-handler"),
                triple("SUSPENDED", "Rollback of step-B[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of step-B[0,]", "root-handler"),
                triple("SUSPENDED", "Rollback of step-A[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of step-A[0,]", "root-handler"),
                triple("ROLLED_BACK", "Rollback of step-A[0,]", "root-handler"),
            ])
        } finally {
            await rootSubscription.close()
        }
    })

    test("loop with children rolls back each iteration's children in reverse", async () => {
        const executionOrder: string[] = []
        let loopCounter = 0

        const latch = new CountDownLatch(1) // wait for root rollback

        const rootSubscription = await h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                b.controlledStep({
                    name: "loop-step",
                    invoke: async (scope, _message, stepIteration) => {
                        await scope.launch(h.childTopic, { iter: `${stepIteration}` })
                        executionOrder.push(`invoke-loop-${stepIteration}`)
                        return ++loopCounter < 2 ? Repeat : Continue
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
                        throw new Error("Step after loop fails")
                    },
                    rollback: (_scope, _message, _throwable) => {
                        executionOrder.push("rollback-failing")
                    },
                })
            }),
        )

        const childSubscription = await h.subscribe(
            h.childTopic,
            saga("child-handler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: async (_scope, _message) => {
                        await ciSleep(50)
                        executionOrder.push("child-step")
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

            // Flow: loop-0(child)→loop-1(child)→failing(fail)
            // Rollback: failing-step has no committed SUSPENDED (it failed),
            // then loop-step iteration 1, then loop-step iteration 0
            // Each loop iteration's child rollback emits rollback for that iteration's children
            assert.deepEqual(
                executionOrder,
                [
                    "invoke-loop-0",
                    "child-step",
                    "invoke-loop-1",
                    "child-step",
                    "invoke-failing",
                    "rollback-loop",
                    "rollback-loop",
                ],
                "Rollback should cover each loop iteration in reverse order",
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
                triple("ROLLING_BACK", "failing-step", "root-handler"),
                triple("ROLLBACK_EMITTED", "Rollback of loop-step[1,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of loop-step[1,] (rolling back child scopes)", "root-handler"),
                triple("ROLLING_BACK", null, "child-handler"),
                triple("SUSPENDED", "Rollback of 0[0,] (rolling back child scopes)", "child-handler"),
                triple("SUSPENDED", "Rollback of 0[0,]", "child-handler"),
                triple("ROLLED_BACK", "Rollback of 0[0,]", "child-handler"),
                triple("SUSPENDED", "Rollback of loop-step[1,]", "root-handler"),
                triple("ROLLBACK_EMITTED", "Rollback of loop-step[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of loop-step[0,] (rolling back child scopes)", "root-handler"),
                triple("ROLLING_BACK", null, "child-handler"),
                triple("SUSPENDED", "Rollback of 0[0,] (rolling back child scopes)", "child-handler"),
                triple("SUSPENDED", "Rollback of 0[0,]", "child-handler"),
                triple("ROLLED_BACK", "Rollback of 0[0,]", "child-handler"),
                triple("SUSPENDED", "Rollback of loop-step[0,]", "root-handler"),
                triple("ROLLED_BACK", "Rollback of loop-step[0,]", "root-handler"),
            ])
        } finally {
            await rootSubscription.close()
            await childSubscription.close()
        }
    })

    test("handleChildFailures can override NextStep and childFailureHandlerIteration increments correctly", async () => {
        const executionOrder: string[] = []

        const latch = new CountDownLatch(1) // wait for rollback

        const rootSubscription = await h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                b.controlledStep({
                    name: "step-0",
                    invoke: async (scope, _message, _stepIteration) => {
                        await scope.launch(h.childTopic, { from: "step-0" })
                        executionOrder.push("invoke-0")
                        // invoke returns Continue, but hcf will override to GoTo(0)
                        return Continue
                    },
                    rollback: (_scope, _message, _throwable, _stepIteration, _cfhi) => {
                        executionOrder.push("rollback-0")
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
                            `hcf-childFailureHandlerIteration-${childFailureHandlerIteration}-invoke-${renderNextStep(nextStep)}`,
                        )
                        if (childFailureHandlerIteration < 1) {
                            // Override: retry with a new child, change NextStep to GoTo(0)
                            await scope.launch(h.childTopic, { from: "hcf-retry" })
                            return GoTo(0) // Override Continue → GoTo(0)
                        }
                        // Give up on second failure
                        throw throwable
                    },
                })
                b.step({
                    name: "step-1",
                    invoke: (_scope, _message) => {
                        executionOrder.push("invoke-1-SHOULD-NOT-EXECUTE")
                    },
                })
            }),
        )

        const childSubscription = await h.subscribe(
            h.childTopic,
            saga("child-handler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("child-step")
                        throw new Error("Child always fails")
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

            // Flow: invoke-0 (Continue) → child fails → hcf childFailureHandlerIteration=0
            // overrides to GoTo(0), launches retry child → retry child fails → hcf
            // childFailureHandlerIteration=1 rethrows → rollback
            // Note: hcf's GoTo(0) becomes the next SUSPENDED's next_step=0, which is
            // reconstructed as Repeat (since step index is also 0)
            assert.deepEqual(
                executionOrder,
                [
                    "invoke-0",
                    "child-step",
                    "hcf-childFailureHandlerIteration-0-invoke-Continue",
                    "child-step",
                    "hcf-childFailureHandlerIteration-1-invoke-Repeat", // Reconstructed from
                    // next_step=0 on step 0
                    "rollback-0",
                ],
                "hcf should receive incrementing childFailureHandlerIteration and be able to override NextStep",
            )

            assert.deepEqual(await getEventSequence(h.sql), [
                triple("EMITTED", null, null),
                triple("SEEN", null, "root-handler"),
                triple("EMITTED", "step-0", "root-handler"),
                triple("SUSPENDED", "step-0", "root-handler"),
                triple("SEEN", null, "child-handler"),
                triple("ROLLING_BACK", "0", "child-handler"),
                triple("ROLLED_BACK", "Rollback of 0[0,]", "child-handler"),
                triple("EMITTED", "step-0", "root-handler"),
                triple("SUSPENDED", "step-0", "root-handler"),
                triple("SEEN", null, "child-handler"),
                triple("ROLLING_BACK", "0", "child-handler"),
                triple("ROLLED_BACK", "Rollback of 0[0,]", "child-handler"),
                triple("ROLLING_BACK", "step-0", "root-handler"),
                triple("ROLLBACK_EMITTED", "Rollback of step-0[0,0] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of step-0[0,0] (rolling back child scopes)", "root-handler"),
                triple("ROLLBACK_EMITTED", "Rollback of step-0[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of step-0[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of step-0[0,]", "root-handler"),
                triple("ROLLED_BACK", "Rollback of step-0[0,]", "root-handler"),
            ])
        } finally {
            await rootSubscription.close()
            await childSubscription.close()
        }
    })

    test("handleChildFailures receives correct nextStep for GoTo", async () => {
        const executionOrder: string[] = []

        const latch = new CountDownLatch(1) // wait for rollback

        const rootSubscription = await h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                b.controlledStep({
                    name: "step-0",
                    invoke: async (scope, _message, _stepIteration) => {
                        await scope.launch(h.childTopic, { from: "step-0" })
                        executionOrder.push("invoke-0")
                        return GoTo(2) // invoke returns GoTo(2)
                    },
                    rollback: (_scope, _message, _throwable, _stepIteration, _cfhi) => {
                        executionOrder.push("rollback-0")
                        latch.countDown()
                    },
                    handleChildFailures: (
                        _scope,
                        _message,
                        throwable,
                        _stepIteration,
                        childFailureHandlerIteration,
                        nextStep,
                    ) => {
                        // Verify nextStep is GoTo(2) from the original invoke
                        executionOrder.push(
                            `hcf-childFailureHandlerIteration-${childFailureHandlerIteration}-nextStep-${renderNextStep(nextStep)}`,
                        )
                        // The only way to exit hcf is to rethrow
                        throw throwable
                    },
                })
                b.step({
                    name: "step-1",
                    invoke: (_scope, _message) => {
                        executionOrder.push("invoke-1")
                    },
                })
                b.step({
                    name: "step-2",
                    invoke: (_scope, _message) => {
                        executionOrder.push("invoke-2")
                    },
                })
            }),
        )

        const childSubscription = await h.subscribe(
            h.childTopic,
            saga("child-handler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("child-failing")
                        throw new Error("Child always fails")
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

            // invoke returns GoTo(2), child fails, hcf receives nextStep=GoTo(2)
            // and rethrows → rollback. The nextStep proves GoTo is correctly
            // reconstructed from the SUSPENDED event's next_step column.
            assert.deepEqual(
                executionOrder,
                [
                    "invoke-0",
                    "child-failing",
                    "hcf-childFailureHandlerIteration-0-nextStep-GoTo(stepIndex=2)",
                    "rollback-0",
                ],
                "hcf should receive the original GoTo nextStep",
            )

            assert.deepEqual(await getEventSequence(h.sql), [
                triple("EMITTED", null, null),
                triple("SEEN", null, "root-handler"),
                triple("EMITTED", "step-0", "root-handler"),
                triple("SUSPENDED", "step-0", "root-handler"),
                triple("SEEN", null, "child-handler"),
                triple("ROLLING_BACK", "0", "child-handler"),
                triple("ROLLED_BACK", "Rollback of 0[0,]", "child-handler"),
                triple("ROLLING_BACK", "step-0", "root-handler"),
                triple("ROLLBACK_EMITTED", "Rollback of step-0[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of step-0[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of step-0[0,]", "root-handler"),
                triple("ROLLED_BACK", "Rollback of step-0[0,]", "root-handler"),
            ])
        } finally {
            await rootSubscription.close()
            await childSubscription.close()
        }
    })

    test("GoTo forward skip does not include skipped step in rollback", async () => {
        const executionOrder: string[] = []

        const latch = new CountDownLatch(1)

        const rootSubscription = await h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                b.controlledStep({
                    name: "step-A",
                    invoke: (_scope, _message, _stepIteration) => {
                        executionOrder.push("invoke-A")
                        return GoTo(2)
                    },
                    rollback: (_scope, _message, _throwable, _stepIteration, _cfhi) => {
                        executionOrder.push("rollback-A")
                        latch.countDown()
                    },
                })
                b.step({
                    name: "step-B",
                    invoke: (_scope, _message) => {
                        executionOrder.push("invoke-B-SHOULD-NOT-EXECUTE")
                    },
                    rollback: (_scope, _message, _throwable) => {
                        executionOrder.push("rollback-B-SHOULD-NOT-EXECUTE")
                    },
                })
                b.step({
                    name: "step-C",
                    invoke: (_scope, _message) => {
                        executionOrder.push("invoke-C")
                    },
                    rollback: (_scope, _message, _throwable) => {
                        executionOrder.push("rollback-C")
                    },
                })
                b.step({
                    name: "step-D",
                    invoke: (_scope, _message) => {
                        executionOrder.push("invoke-D")
                        throw new Error("Step D fails")
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

            // Flow: A(GoTo 2)→C→D(fail)
            // Rollback: C, A (step-B was never executed, so not in rollback)
            assert.deepEqual(
                executionOrder,
                ["invoke-A", "invoke-C", "invoke-D", "rollback-C", "rollback-A"],
                "Skipped step-B should not appear in execution or rollback",
            )

            assert.deepEqual(await getEventSequence(h.sql), [
                triple("EMITTED", null, null),
                triple("SEEN", null, "root-handler"),
                triple("SUSPENDED", "step-A", "root-handler"),
                triple("SUSPENDED", "step-C", "root-handler"),
                triple("ROLLING_BACK", "step-D", "root-handler"),
                triple("SUSPENDED", "Rollback of step-C[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of step-C[0,]", "root-handler"),
                triple("SUSPENDED", "Rollback of step-A[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of step-A[0,]", "root-handler"),
                triple("ROLLED_BACK", "Rollback of step-A[0,]", "root-handler"),
            ])
        } finally {
            await rootSubscription.close()
        }
    })

    test("GoTo forward then GoTo backward with failure rolls back all visited instances", async () => {
        const executionOrder: string[] = []
        let stepCCount = 0

        const latch = new CountDownLatch(1)

        const rootSubscription = await h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                b.controlledStep({
                    name: "step-A",
                    invoke: (_scope, _message, _stepIteration) => {
                        executionOrder.push("invoke-A")
                        return GoTo(2) // Skip B, go to C
                    },
                    rollback: (_scope, _message, _throwable, _stepIteration, _cfhi) => {
                        executionOrder.push("rollback-A")
                        latch.countDown()
                    },
                })
                b.step({
                    name: "step-B",
                    invoke: (_scope, _message) => {
                        executionOrder.push("invoke-B")
                    },
                    rollback: (_scope, _message, _throwable) => {
                        executionOrder.push("rollback-B")
                    },
                })
                b.controlledStep({
                    name: "step-C",
                    invoke: (_scope, _message, _stepIteration) => {
                        executionOrder.push("invoke-C")
                        if (++stepCCount < 2) {
                            return GoTo(1) // Go back to B
                        }
                        throw new Error("Step C fails on second visit")
                    },
                    rollback: (_scope, _message, _throwable, _stepIteration, _cfhi) => {
                        executionOrder.push("rollback-C")
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

            // Flow: A(GoTo 2)→C(GoTo 1)→B→C(fail)
            // Committed: A, C(1st), B. C's 2nd invoke threw, not committed.
            // Rollback in reverse chronological: B, C(1st), A
            assert.deepEqual(
                executionOrder,
                [
                    "invoke-A",
                    "invoke-C",
                    "invoke-B",
                    "invoke-C",
                    "rollback-B",
                    "rollback-C",
                    "rollback-A",
                ],
                "Rollback covers all visited steps in reverse chronological order",
            )

            assert.deepEqual(await getEventSequence(h.sql), [
                triple("EMITTED", null, null),
                triple("SEEN", null, "root-handler"),
                triple("SUSPENDED", "step-A", "root-handler"),
                triple("SUSPENDED", "step-C", "root-handler"),
                triple("SUSPENDED", "step-B", "root-handler"),
                triple("ROLLING_BACK", "step-C", "root-handler"),
                triple("SUSPENDED", "Rollback of step-B[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of step-B[0,]", "root-handler"),
                triple("SUSPENDED", "Rollback of step-C[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of step-C[0,]", "root-handler"),
                triple("SUSPENDED", "Rollback of step-A[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of step-A[0,]", "root-handler"),
                triple("ROLLED_BACK", "Rollback of step-A[0,]", "root-handler"),
            ])
        } finally {
            await rootSubscription.close()
        }
    })

    test("GoTo to repeating step then failure rolls back all instances", async () => {
        const executionOrder: string[] = []
        let repeatCount = 0

        const latch = new CountDownLatch(1)

        const rootSubscription = await h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                b.controlledStep({
                    name: "step-A",
                    invoke: (_scope, _message, _stepIteration) => {
                        executionOrder.push("invoke-A")
                        return GoTo(2)
                    },
                    rollback: (_scope, _message, _throwable, _stepIteration, _cfhi) => {
                        executionOrder.push("rollback-A")
                        latch.countDown()
                    },
                })
                b.step({
                    name: "step-B",
                    invoke: (_scope, _message) => {
                        executionOrder.push("invoke-B-SHOULD-NOT-EXECUTE")
                    },
                    rollback: (_scope, _message, _throwable) => {
                        executionOrder.push("rollback-B-SHOULD-NOT-EXECUTE")
                    },
                })
                b.controlledStep({
                    name: "step-C",
                    invoke: (_scope, _message, stepIteration) => {
                        executionOrder.push(`invoke-C-${stepIteration}`)
                        if (++repeatCount < 2) {
                            return Repeat
                        }
                        throw new Error("Step C fails after repeating")
                    },
                    rollback: (_scope, _message, _throwable, _stepIteration, _cfhi) => {
                        executionOrder.push("rollback-C")
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

            // Flow: A(GoTo 2)→C(iter 0, Repeat)→C(iter 1, fail)
            // Committed: A, C(iter 0). C iter 1 threw, not committed.
            // Rollback: C(iter 0), A — step B never executed, not in rollback
            assert.deepEqual(
                executionOrder,
                ["invoke-A", "invoke-C-0", "invoke-C-1", "rollback-C", "rollback-A"],
                "GoTo to repeating step: rollback covers committed instances, skipped step excluded",
            )

            assert.deepEqual(await getEventSequence(h.sql), [
                triple("EMITTED", null, null),
                triple("SEEN", null, "root-handler"),
                triple("SUSPENDED", "step-A", "root-handler"),
                triple("SUSPENDED", "step-C", "root-handler"),
                triple("ROLLING_BACK", "step-C", "root-handler"),
                triple("SUSPENDED", "Rollback of step-C[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of step-C[0,]", "root-handler"),
                triple("SUSPENDED", "Rollback of step-A[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of step-A[0,]", "root-handler"),
                triple("ROLLED_BACK", "Rollback of step-A[0,]", "root-handler"),
            ])
        } finally {
            await rootSubscription.close()
        }
    })
})
