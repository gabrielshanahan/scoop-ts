import assert from "node:assert/strict"
import { describe, test } from "node:test"
import { saga } from "../../../src/coroutine/builder/SagaBuilder.js"
import { DistributedCoroutine } from "../../../src/coroutine/DistributedCoroutine.js"
import { Handler } from "../../../src/coroutine/Handler.js"
import { Topic } from "../../../src/coroutine/Topic.js"
import { VariableName } from "../../../src/coroutine/VariableName.js"
import type { JsonValue } from "../../../src/JsonbHelper.js"
import { eventLoopStrategy } from "../../../src/messaging/HandlerRegistry.js"
import { transactional } from "../../../src/coroutine/TransactionRunner.js"
import { ciSleep, eventLogSettled, setupScoopTest } from "../../support/harness.js"
import { CountDownLatch } from "../../support/latch.js"

const h = setupScoopTest()

const TestResult = new VariableName("TestResult")
const AnotherResult = new VariableName("AnotherResult")

// Test topics
const ChildTestTopic = new Topic<unknown>("ChildTestTopic")
const ChildTestTopic2 = new Topic<unknown>("ChildTestTopic2")

// Test handlers - used as type-safe keys for return value retrieval.
// Their handlerName must match the saga name used in subscribe().
class TestHandler extends Handler<unknown> {
    implementation(): DistributedCoroutine {
        throw new Error("Test handler - implementation provided inline")
    }
}

const ChildHandler = new TestHandler("ChildHandler", ChildTestTopic)
const ChildHandler1 = new TestHandler("ChildHandler1", ChildTestTopic)
const ChildHandler2 = new TestHandler("ChildHandler2", ChildTestTopic2)

/** A handler that doesn't exist in any test - used to verify null return for missing handlers. */
const NonexistentHandler = new TestHandler("NonexistentHandler", new Topic<unknown>("anonymous"))

/** Maps handler name strings (from DB) to Handler objects for test assertions. */
const testHandlerRegistry = (name: string): Handler<unknown> => {
    switch (name) {
        case "ChildHandler":
            return ChildHandler
        case "ChildHandler1":
            return ChildHandler1
        case "ChildHandler2":
            return ChildHandler2
        default:
            throw new Error(`Unknown handler: ${name}`)
    }
}

describe("ReturnValueTest", () => {
    test("child handler can store a return value that parent retrieves", async () => {
        let retrievedValues: Map<Handler<unknown>, JsonValue> | null = null
        const latch = new CountDownLatch(2)

        const rootSubscription = h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: async (scope, _message) => {
                        await scope.launch(h.childTopic, { task: "compute" })
                        latch.countDown()
                    },
                })
                b.step({
                    invoke: async (scope, _message) => {
                        retrievedValues = await scope.getReturnValues(
                            TestResult,
                            testHandlerRegistry,
                        )
                        latch.countDown()
                    },
                })
            }),
        )

        const childSubscription = h.subscribe(
            h.childTopic,
            saga("ChildHandler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: async (scope, _message) => {
                        await scope.storeReturnValue(TestResult, { answer: 42 })
                    },
                })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { start: true })
            })

            assert.ok(await latch.await(10_000), "All handlers should complete")
            await eventLogSettled(h.sql)

            const values = retrievedValues!
            assert.ok(values, "Return values should be retrieved")
            assert.equal(values.size, 1, "Should have one return value from ChildHandler")
            assert.ok(values.has(ChildHandler), "Key should be the ChildHandler object")

            const returnedJson = JSON.stringify(values.get(ChildHandler))
            assert.ok(returnedJson.includes("42"), "Return value should contain the stored data")
        } finally {
            await rootSubscription.close()
            await childSubscription.close()
        }
    })

    test("multiple child handlers can each store return values", async () => {
        let retrievedValues: Map<Handler<unknown>, JsonValue> | null = null
        const latch = new CountDownLatch(2)

        const childTopic2 = "child-topic-2"

        const rootSubscription = h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: async (scope, _message) => {
                        await scope.launch(h.childTopic, { task: "a" })
                        await scope.launch(childTopic2, { task: "b" })
                        latch.countDown()
                    },
                })
                b.step({
                    invoke: async (scope, _message) => {
                        retrievedValues = await scope.getReturnValues(
                            TestResult,
                            testHandlerRegistry,
                        )
                        latch.countDown()
                    },
                })
            }),
        )

        const childSubscription1 = h.subscribe(
            h.childTopic,
            saga("ChildHandler1", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: async (scope, _message) => {
                        await scope.storeReturnValue(TestResult, { result: "from-child-1" })
                    },
                })
            }),
        )

        const childSubscription2 = h.subscribe(
            childTopic2,
            saga("ChildHandler2", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: async (scope, _message) => {
                        await scope.storeReturnValue(TestResult, { result: "from-child-2" })
                    },
                })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { start: true })
            })

            assert.ok(await latch.await(10_000), "All handlers should complete")
            await eventLogSettled(h.sql)

            const values = retrievedValues!
            assert.ok(values, "Return values should be retrieved")
            assert.equal(values.size, 2, "Should have return values from both children")
            assert.ok(values.has(ChildHandler1))
            assert.ok(values.has(ChildHandler2))

            assert.ok(JSON.stringify(values.get(ChildHandler1)).includes("from-child-1"))
            assert.ok(JSON.stringify(values.get(ChildHandler2)).includes("from-child-2"))
        } finally {
            await rootSubscription.close()
            await childSubscription1.close()
            await childSubscription2.close()
        }
    })

    test("getReturnValue retrieves a specific child's return value by handler", async () => {
        let specificValue: JsonValue | null = null
        let missingValue: JsonValue | null = {} // sentinel to detect null
        const latch = new CountDownLatch(2)

        const rootSubscription = h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: async (scope, _message) => {
                        await scope.launch(h.childTopic, { task: "compute" })
                        latch.countDown()
                    },
                })
                b.step({
                    invoke: async (scope, _message) => {
                        specificValue = await scope.getReturnValue(TestResult, ChildHandler)
                        missingValue = await scope.getReturnValue(TestResult, NonexistentHandler)
                        latch.countDown()
                    },
                })
            }),
        )

        const childSubscription = h.subscribe(
            h.childTopic,
            saga("ChildHandler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: async (scope, _message) => {
                        await scope.storeReturnValue(TestResult, { value: "found" })
                    },
                })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { start: true })
            })

            assert.ok(await latch.await(10_000), "All handlers should complete")
            await eventLogSettled(h.sql)

            assert.ok(specificValue, "Should find ChildHandler's return value")
            assert.ok(JSON.stringify(specificValue).includes("found"))
            assert.equal(missingValue, null, "Should return null for nonexistent handler")
        } finally {
            await rootSubscription.close()
            await childSubscription.close()
        }
    })

    test("different variable names are independent", async () => {
        let testResults: Map<Handler<unknown>, JsonValue> | null = null
        let anotherResults: Map<Handler<unknown>, JsonValue> | null = null
        const latch = new CountDownLatch(2)

        const rootSubscription = h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: async (scope, _message) => {
                        await scope.launch(h.childTopic, { task: "multi" })
                        latch.countDown()
                    },
                })
                b.step({
                    invoke: async (scope, _message) => {
                        testResults = await scope.getReturnValues(TestResult, testHandlerRegistry)
                        anotherResults = await scope.getReturnValues(
                            AnotherResult,
                            testHandlerRegistry,
                        )
                        latch.countDown()
                    },
                })
            }),
        )

        const childSubscription = h.subscribe(
            h.childTopic,
            saga("ChildHandler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: async (scope, _message) => {
                        await scope.storeReturnValue(TestResult, { type: "test" })
                        await scope.storeReturnValue(AnotherResult, { type: "another" })
                    },
                })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { start: true })
            })

            assert.ok(await latch.await(10_000), "All handlers should complete")
            await eventLogSettled(h.sql)

            const testMap = testResults!
            const anotherMap = anotherResults!

            assert.equal(testMap.size, 1)
            assert.equal(anotherMap.size, 1)
            assert.ok(JSON.stringify(testMap.get(ChildHandler)).includes("test"))
            assert.ok(JSON.stringify(anotherMap.get(ChildHandler)).includes("another"))
        } finally {
            await rootSubscription.close()
            await childSubscription.close()
        }
    })
})
