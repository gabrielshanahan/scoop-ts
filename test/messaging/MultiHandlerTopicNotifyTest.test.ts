import assert from "node:assert/strict"
import { describe, test } from "node:test"
import { saga } from "../../src/coroutine/builder/SagaBuilder.js"
import { eventLoopStrategy } from "../../src/messaging/HandlerRegistry.js"
import { transactional } from "../../src/coroutine/TransactionRunner.js"
import { setupScoopTest } from "../support/harness.js"
import { CountDownLatch } from "../support/latch.js"

const h = setupScoopTest()

/**
 * Two *distinct* sagas subscribed to the *same* topic must both be woken by a single message's
 * NOTIFY — and promptly, well inside the reconcile safety-net interval. Guards the per-topic
 * fan-out in PostgresTopicNotifier (one LISTEN per topic dispatching to every registered
 * callback).
 */
describe("MultiHandlerTopicNotifyTest", () => {
    test("both sagas on one topic are notified promptly by a single message", async () => {
        const latchA = new CountDownLatch(1)
        const latchB = new CountDownLatch(1)

        const subA = await h.subscribe(
            h.rootTopic,
            saga("handler-a", eventLoopStrategy(h.messageQueue, h.strategyEpoch), b => {
                b.step({ invoke: () => latchA.countDown() })
            }),
        )
        const subB = await h.subscribe(
            h.rootTopic,
            saga("handler-b", eventLoopStrategy(h.messageQueue, h.strategyEpoch), b => {
                b.step({ invoke: () => latchB.countDown() })
            }),
        )

        try {
            // Make sure the LISTENs are active before the message is launched (the Kotlin
            // PgSubscriber connects at boot; postgres.js registers asynchronously).
            await h.topicNotifier.ready()

            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { k: "v" })
            })

            // 5s is far below the 30s safety net but ample for a NOTIFY-driven reconcile +
            // resume, so a pass here means both handlers were woken by the notification, not by
            // the safety net.
            assert.ok(await latchA.await(5_000), "handler-a should run within one NOTIFY hop")
            assert.ok(await latchB.await(5_000), "handler-b should run within one NOTIFY hop")
        } finally {
            await subA.close()
            await subB.close()
        }
    })
})
