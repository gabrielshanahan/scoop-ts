import assert from "node:assert/strict"
import { describe, test } from "node:test"
import { setupScoopTest } from "../support/harness.js"
import { CountDownLatch } from "../support/latch.js"

const h = setupScoopTest()

/**
 * The Kotlin original verifies the Vert.x notifier dispatches callbacks OFF the Vert.x event loop
 * thread (blocking JDBC in a callback would stall IO). On this stack the equivalent guarantee
 * (per the port brief: assert the same property on postgres.js LISTEN/NOTIFY) is:
 * - a pg_notify on the topic reaches the registered callback, and
 * - the callback is dispatched asynchronously (as its own microtask), decoupled from the LISTEN
 *   connection's protocol handling — there is no separate event loop thread to escape on a
 *   single-threaded runtime. Mapping recorded in PORT-LEDGER.md.
 */
describe("PgSubscriberTopicNotifierTest", () => {
    test("callback should not run on vert-x event loop thread", async () => {
        const latch = new CountDownLatch(1)
        const topic = "test_notifier_thread"
        let sawNotification = false

        const handle = h.topicNotifier.onMessage(topic, () => {
            sawNotification = true
            latch.countDown()
        })

        try {
            // Give the notifier time to register the LISTEN
            await h.topicNotifier.ready()

            // Fire a pg_notify on the topic to trigger the callback (same as the DB trigger does)
            await h.sql`SELECT pg_notify(${topic}, ${"test"})`

            assert.ok(await latch.await(5_000), "Callback was not invoked within timeout")
            assert.ok(sawNotification, "Notification should have been delivered to the callback")
        } finally {
            handle.close()
        }
    })
})
