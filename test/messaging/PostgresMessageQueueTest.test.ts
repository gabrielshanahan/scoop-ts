import assert from "node:assert/strict"
import { describe, test } from "node:test"
import { saga } from "../../src/coroutine/builder/SagaBuilder.js"
import { eventLoopStrategy } from "../../src/messaging/HandlerRegistry.js"
import { transactional } from "../../src/coroutine/TransactionRunner.js"
import { ciSleep, eventLogSettled, setupScoopTest } from "../support/harness.js"
import { CountDownLatch } from "../support/latch.js"

const h = setupScoopTest()

const testTopic = "test-topic"
const testHandler = "test-handler"

describe("PostgresMessageQueueTest", () => {
    test("should publish a message", async () => {
        const testPayload = { text: "Hello, World!", priority: "HIGH" }
        const root = await transactional(h.sql, connection =>
            h.messageQueue.launch(connection, testTopic, testPayload),
        )
        const message = root.message

        const persistedMessage = (await transactional(h.sql, connection =>
            h.messageQueue.fetch(connection, message.id),
        ))!

        const persistedMap = persistedMessage.payload as Record<string, string>
        assert.ok(persistedMessage.id)
        assert.equal(persistedMessage.topic, testTopic)
        assert.equal(persistedMap.text, testPayload.text)
        assert.equal(persistedMap.priority, testPayload.priority)
        assert.ok(persistedMessage.createdAt)
        await ciSleep(200)
    })

    test("should subscribe to messages", async () => {
        const messageCount = 5
        let receivedCount = 0
        const latch = new CountDownLatch(messageCount)

        const subscription = h.subscribe(
            testTopic,
            saga(testHandler, eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: (_scope, _message) => {
                        receivedCount++
                        latch.countDown()
                    },
                })
            }),
        )
        try {
            for (let i = 1; i <= messageCount; i++) {
                await transactional(h.sql, async connection => {
                    await h.messageQueue.launch(connection, testTopic, { text: `Message ${i}` })
                })
            }

            const received = await latch.await(10_000)
            assert.ok(received)
            assert.equal(receivedCount, messageCount)
        } finally {
            await subscription.close()
        }
        await ciSleep(200)
    })

    test("subscribe should isolate transactions between messages and correctly roll back failures", async () => {
        const latch = new CountDownLatch(2)
        let failedMessageIndex = -1
        let successMessageIndex = -1

        const otherTopic = "otherTopic"
        const otherPayload = { otherIndex: 1 }

        const subscription = h.subscribe(
            testTopic,
            saga(testHandler, eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: async (scope, message) => {
                        await h.messageQueue.launch(scope.connection, otherTopic, otherPayload)

                        const index = (message.payload as Record<string, number>).index

                        if (index === 2) {
                            successMessageIndex = index
                            latch.countDown()
                        } else if (index === 1) {
                            failedMessageIndex = index
                            latch.countDown()
                            // Throwing an exception to simulate a failure
                            throw new Error(`Simulated failure for message ${index}`)
                        }
                    },
                })
            }),
        )
        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, testTopic, { index: 1 })
                await h.messageQueue.launch(connection, testTopic, { index: 2 })
            })

            assert.ok(await latch.await(10_000))

            await eventLogSettled(h.sql)

            assert.equal(successMessageIndex, 2)
            assert.equal(failedMessageIndex, 1)

            const [row] = await h.sql`SELECT count(*)::int AS count FROM message WHERE topic = ${otherTopic}`
            assert.equal(
                Number(row!.count),
                1,
                "Only one message should have been published to otherTopic",
            )
        } finally {
            await subscription.close()
        }
        await ciSleep(200)
    })

    test("subscribe with multiple instances fans work out across distinct instance UUIDs", async () => {
        const instanceCount = 3
        // One message per worker. Each worker is serialized (scheduled and NOTIFY ticks funnel
        // through the same gate) and suspends in `await` inside its step, holding the row lock
        // via SKIP LOCKED. That forces every message to be picked up by a *different* worker, so
        // exactly `instanceCount` distinct instance UUIDs must participate.
        const latch = new CountDownLatch(instanceCount)
        const seenInstances = new Set<string>()
        const seenNames = new Set<string>()

        const subscription = h.subscribe(
            testTopic,
            saga(testHandler, eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: async (scope, _message) => {
                        const identifier =
                            scope.continuation.continuationIdentifier
                                .distributedCoroutineIdentifier
                        seenInstances.add(identifier.instance)
                        seenNames.add(identifier.name)
                        latch.countDown()
                        const done = await latch.await(10_000)
                        if (!done) {
                            throw new Error(
                                `Timed out waiting for all ${instanceCount} workers to enter the step`,
                            )
                        }
                    },
                })
            }),
            instanceCount,
        )
        try {
            for (let i = 1; i <= instanceCount; i++) {
                await transactional(h.sql, async connection => {
                    await h.messageQueue.launch(connection, testTopic, { text: `Message ${i}` })
                })
            }

            assert.ok(
                await latch.await(10_000),
                `All ${instanceCount} messages should be processed concurrently`,
            )

            assert.equal(
                seenInstances.size,
                instanceCount,
                `Expected exactly ${instanceCount} distinct instance UUIDs, saw: ${[...seenInstances]}`,
            )
            assert.deepEqual(
                seenNames,
                new Set([testHandler]),
                "All instances should share the saga name",
            )
        } finally {
            await subscription.close()
        }
        await ciSleep(200)
    })

    test("subscribe rejects instances less than one", () => {
        const testSaga = saga(testHandler, eventLoopStrategy(h.messageQueue), b => {
            b.step({ invoke: () => {} })
        })

        assert.throws(
            () => h.messageQueue.subscribe(testTopic, testSaga, 0),
            (error: Error) => {
                assert.ok(
                    error.message.includes("instances must be >= 1"),
                    `Unexpected message: ${error.message}`,
                )
                return true
            },
        )
    })

    test("requiredConnectionCount reflects registered worker instances", async () => {
        const baseline = h.messageQueue.requiredConnectionCount

        const subscription = h.subscribe(
            testTopic,
            saga(testHandler, eventLoopStrategy(h.messageQueue), b => {
                b.step({ invoke: () => {} })
            }),
            3,
        )
        try {
            assert.equal(
                h.messageQueue.requiredConnectionCount,
                baseline + 3,
                "Subscribing with instances = 3 should add 3 workers to the connection budget",
            )
        } finally {
            await subscription.close()
        }

        assert.equal(
            h.messageQueue.requiredConnectionCount,
            baseline,
            "Closing the subscription should release all three workers from the connection budget",
        )
    })
})
