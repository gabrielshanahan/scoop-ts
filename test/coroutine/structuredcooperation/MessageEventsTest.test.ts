import assert from "node:assert/strict"
import { describe, test } from "node:test"
import { saga } from "../../../src/coroutine/builder/SagaBuilder.js"
import { eventLoopStrategy } from "../../../src/messaging/HandlerRegistry.js"
import { transactional } from "../../../src/coroutine/TransactionRunner.js"
import { ciSleep, eventLogSettled, setupScoopTest } from "../../support/harness.js"
import { CountDownLatch } from "../../support/latch.js"

const h = setupScoopTest()

const testTopic = "test-events-topic"

async function countMessageEvents(messageId: string, type: string): Promise<number> {
    const [row] = await h.sql`
        SELECT COUNT(*)::int AS count FROM message_event
        WHERE message_id = ${messageId} AND type = ${type}::message_event_type
    `
    return Number(row!.count)
}

async function countHandlerMessageEvents(
    handlerName: string,
    messageId: string,
    type: string,
): Promise<number> {
    const [row] = await h.sql`
        SELECT COUNT(*)::int AS count FROM message_event
        WHERE coroutine_name = ${handlerName} AND message_id = ${messageId} AND type = ${type}::message_event_type
    `
    return Number(row!.count)
}

describe("MessageEventsTest", () => {
    test("should write EMITTED event when message is published", async () => {
        const payload = { text: "Testing EMITTED event" }

        const message = (
            await transactional(h.sql, connection =>
                h.messageQueue.launch(connection, testTopic, payload),
            )
        ).message

        const emittedCount = await countMessageEvents(message.id, "EMITTED")
        assert.equal(emittedCount, 1, "There should be exactly one EMITTED event message")

        const rows = await h.sql`
            SELECT type FROM message_event WHERE message_id = ${message.id}
        `
        assert.equal(rows.length, 1)
        assert.equal(rows[0]!.type, "EMITTED")
    })

    test("should write one SEEN event per handler", async () => {
        const payload = { text: "Testing SEEN event" }
        const handlerName1 = "test-handler-1"
        const handlerName2 = "test-handler-2"
        const latch = new CountDownLatch(2)

        const subscription1 = await h.subscribe(
            testTopic,
            saga(handlerName1, eventLoopStrategy(h.messageQueue, h.strategyEpoch), b => {
                b.step({ invoke: () => latch.countDown() })
            }),
        )

        const subscription2 = await h.subscribe(
            testTopic,
            saga(handlerName2, eventLoopStrategy(h.messageQueue, h.strategyEpoch), b => {
                b.step({ invoke: () => latch.countDown() })
            }),
        )

        const message = (
            await transactional(h.sql, connection =>
                h.messageQueue.launch(connection, testTopic, payload),
            )
        ).message

        assert.ok(await latch.await(10_000), "Handlers should process the message")

        const seenCount1 = await countHandlerMessageEvents(handlerName1, message.id, "SEEN")
        const seenCount2 = await countHandlerMessageEvents(handlerName2, message.id, "SEEN")

        assert.equal(seenCount1, 1, "Handler 1 should have exactly one SEEN event")
        assert.equal(seenCount2, 1, "Handler 2 should have exactly one SEEN event")

        await subscription1.close()
        await subscription2.close()
    })

    test("should synchronize multiple instances of the same handler using message event records", async () => {
        const payload = { text: "Testing handler synchronization" }
        const handlerName = "sync-test-handler"
        let processedCount = 0
        const latch = new CountDownLatch(1)

        const subscription1 = await h.subscribe(
            testTopic,
            saga(handlerName, eventLoopStrategy(h.messageQueue, h.strategyEpoch), b => {
                b.step({
                    invoke: () => {
                        processedCount++
                        latch.countDown()
                    },
                })
            }),
        )

        const subscription2 = await h.subscribe(
            testTopic,
            saga(handlerName, eventLoopStrategy(h.messageQueue, h.strategyEpoch), b => {
                b.step({
                    invoke: () => {
                        processedCount++
                        latch.countDown()
                    },
                })
            }),
        )

        const message = (
            await transactional(h.sql, connection =>
                h.messageQueue.launch(connection, testTopic, payload),
            )
        ).message

        assert.ok(await latch.await(10_000), "Message should be processed")

        assert.equal(processedCount, 1, "Only one handler instance should process the message")

        const seenCount = await countHandlerMessageEvents(handlerName, message.id, "SEEN")
        assert.equal(seenCount, 1, "There should be exactly one SEEN event entry for this handler")

        await subscription1.close()
        await subscription2.close()
    })

    test("should write COMMITTED event on successful transaction", async () => {
        const payload = { text: "Testing COMMITTED event" }
        const handlerName = "commit-test-handler"
        const latch = new CountDownLatch(1)

        const subscription = await h.subscribe(
            testTopic,
            saga(handlerName, eventLoopStrategy(h.messageQueue, h.strategyEpoch), b => {
                b.step({ invoke: () => latch.countDown() })
            }),
        )

        const message = (
            await transactional(h.sql, connection =>
                h.messageQueue.launch(connection, testTopic, payload),
            )
        ).message

        assert.ok(await latch.await(10_000), "Message should be processed")

        await eventLogSettled(h.sql)

        const committedCount = await countHandlerMessageEvents(handlerName, message.id, "COMMITTED")
        assert.equal(committedCount, 1, "There should be exactly one COMMITTED event")
        await subscription.close()
    })

    test("should write ROLLED_BACK event when exception is thrown", async () => {
        const payload = { text: "Testing ROLLED_BACK event" }
        const handlerName = "rollback-test-handler"
        const latch = new CountDownLatch(1)

        const subscription = await h.subscribe(
            testTopic,
            saga(handlerName, eventLoopStrategy(h.messageQueue, h.strategyEpoch), b => {
                b.step({
                    invoke: () => {
                        latch.countDown()
                        throw new Error("Simulated failure to test rollback")
                    },
                })
            }),
        )

        const message = (
            await transactional(h.sql, connection =>
                h.messageQueue.launch(connection, testTopic, payload),
            )
        ).message

        assert.ok(await latch.await(10_000), "Message should be processed")

        await eventLogSettled(h.sql)

        const rolledBackCount = await countHandlerMessageEvents(
            handlerName,
            message.id,
            "ROLLED_BACK",
        )
        assert.equal(rolledBackCount, 1, "There should be exactly one ROLLED_BACK event")
        await subscription.close()
    })

    test("should follow complete message event writing sequence on successful processing", async () => {
        const payload = { text: "Testing full event sequence" }
        const handlerName = "sequence-test-handler"
        const latch = new CountDownLatch(1)

        const subscription = await h.subscribe(
            testTopic,
            saga(handlerName, eventLoopStrategy(h.messageQueue, h.strategyEpoch), b => {
                b.step({ invoke: () => latch.countDown() })
            }),
        )

        const message = (
            await transactional(h.sql, connection =>
                h.messageQueue.launch(connection, testTopic, payload),
            )
        ).message

        assert.ok(await latch.await(10_000), "Message should be processed")

        await eventLogSettled(h.sql)

        const emittedCount = await countMessageEvents(message.id, "EMITTED")
        const seenCount = await countHandlerMessageEvents(handlerName, message.id, "SEEN")
        const committedCount = await countHandlerMessageEvents(handlerName, message.id, "COMMITTED")
        const rolledBackCount = await countHandlerMessageEvents(
            handlerName,
            message.id,
            "ROLLED_BACK",
        )

        assert.equal(emittedCount, 1, "There should be exactly one EMITTED event")
        assert.equal(seenCount, 1, "There should be exactly one SEEN event")
        assert.equal(committedCount, 1, "There should be exactly one COMMITTED event")
        assert.equal(rolledBackCount, 0, "There should be no ROLLED_BACK event")

        const rows = await h.sql`
            SELECT type
            FROM message_event
            WHERE message_id = ${message.id} AND (coroutine_name = ${handlerName} OR type = 'EMITTED')
            ORDER BY created_at ASC
        `
        const events = rows.map(row => row.type as string)

        assert.equal(events.length, 4, "There should be three events in total")
        assert.equal(events[0], "EMITTED", "First event should be EMITTED")
        assert.equal(events[1], "SEEN", "Second event should be SEEN")
        assert.equal(events[2], "SUSPENDED", "Third event should be SUSPENDED")
        assert.equal(events[3], "COMMITTED", "Fourth event should be COMMITTED")
        await subscription.close()
    })

    test("should follow complete message event writing sequence on failed processing", async () => {
        const payload = { text: "Testing failed event sequence" }
        const handlerName = "failed-sequence-handler"
        const latch = new CountDownLatch(1)

        const subscription = await h.subscribe(
            testTopic,
            saga(handlerName, eventLoopStrategy(h.messageQueue, h.strategyEpoch), b => {
                b.step({
                    invoke: () => {
                        latch.countDown()
                        throw new Error("Simulated failure for event sequence test")
                    },
                })
            }),
        )

        const message = (
            await transactional(h.sql, connection =>
                h.messageQueue.launch(connection, testTopic, payload),
            )
        ).message

        assert.ok(await latch.await(10_000), "Message should be processed (and fail)")

        await eventLogSettled(h.sql)

        const emittedCount = await countMessageEvents(message.id, "EMITTED")
        const seenCount = await countHandlerMessageEvents(handlerName, message.id, "SEEN")
        const committedCount = await countHandlerMessageEvents(handlerName, message.id, "COMMITTED")
        const rollingBackCount = await countHandlerMessageEvents(
            handlerName,
            message.id,
            "ROLLING_BACK",
        )
        const rolledBackCount = await countHandlerMessageEvents(
            handlerName,
            message.id,
            "ROLLED_BACK",
        )

        assert.equal(emittedCount, 1, "There should be exactly one EMITTED event entry")
        assert.equal(seenCount, 1, "There should be exactly one SEEN event entry")
        assert.equal(committedCount, 0, "There should be no COMMITTED event entries")
        assert.equal(rollingBackCount, 1, "There should be exactly one ROLLING_BACK event entry")
        assert.equal(rolledBackCount, 1, "There should be exactly one ROLLED_BACK event entry")

        const rows = await h.sql`
            SELECT type
            FROM message_event
            WHERE message_id = ${message.id} AND (coroutine_name = ${handlerName} OR type = 'EMITTED')
            ORDER BY created_at ASC
        `
        const events = rows.map(row => row.type as string)

        assert.equal(events.length, 4, "There should be three event entries in total")
        assert.equal(events[0], "EMITTED", "First entry should be EMITTED")
        assert.equal(events[1], "SEEN", "Second entry should be SEEN")
        assert.equal(events[2], "ROLLING_BACK", "Third entry should be ROLLING_BACK")
        assert.equal(events[3], "ROLLED_BACK", "Fourth entry should be ROLLED_BACK")
        await subscription.close()
    })

    test("multiple handler instances should coordinate using message events for multiple messages", async () => {
        const messageCount = 5
        const handlerName = "concurrent-handler"
        const processedMessages = new Set<string>()
        const countDownLatch = new CountDownLatch(messageCount)

        const subscription1 = await h.subscribe(
            testTopic,
            saga(handlerName, eventLoopStrategy(h.messageQueue, h.strategyEpoch), b => {
                b.step({
                    invoke: (_scope, message) => {
                        processedMessages.add(message.id)
                        countDownLatch.countDown()
                    },
                })
            }),
        )

        const subscription2 = await h.subscribe(
            testTopic,
            saga(handlerName, eventLoopStrategy(h.messageQueue, h.strategyEpoch), b => {
                b.step({
                    invoke: (_scope, message) => {
                        processedMessages.add(message.id)
                        countDownLatch.countDown()
                    },
                })
            }),
        )

        const messages: string[] = []
        for (let i = 1; i <= messageCount; i++) {
            const message = (
                await transactional(h.sql, connection =>
                    h.messageQueue.launch(connection, testTopic, {
                        text: `Concurrent message ${i}`,
                    }),
                )
            ).message
            messages.push(message.id)
        }

        assert.ok(await countDownLatch.await(1_000), "All messages should be processed")
        await eventLogSettled(h.sql)

        assert.equal(
            processedMessages.size,
            messageCount,
            "All messages should be handled exactly once",
        )
        assert.ok(
            messages.every(messageId => processedMessages.has(messageId)),
            "All published messages should be processed",
        )

        for (const messageId of messages) {
            const emittedCount = await countMessageEvents(messageId, "EMITTED")
            const seenCount = await countHandlerMessageEvents(handlerName, messageId, "SEEN")
            const committedCount = await countHandlerMessageEvents(
                handlerName,
                messageId,
                "COMMITTED",
            )

            assert.equal(emittedCount, 1, "Each message should have exactly one EMITTED event")
            assert.equal(
                seenCount,
                1,
                "Each message should have exactly one SEEN event for this handler",
            )
            assert.equal(
                committedCount,
                1,
                "Each message should have exactly one COMMITTED event for this handler",
            )
        }

        await subscription1.close()
        await subscription2.close()
    })
})
