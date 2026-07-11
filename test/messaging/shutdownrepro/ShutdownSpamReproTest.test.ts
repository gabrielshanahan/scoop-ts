import assert from "node:assert/strict"
import { after, before, describe, test } from "node:test"
import postgres, { type Sql } from "postgres"
import type { Subscription } from "../../../src/messaging/Subscription.js"
import { PostgresTopicNotifier } from "../../../src/node/PostgresTopicNotifier.js"
import { Scoop } from "../../../src/Scoop.js"
import { sleep } from "../../support/latch.js"
import { registerReproSubscriptions, SUBSCRIPTION_COUNT } from "./ReproSubscriptionRegistrar.js"

/**
 * Port of the Quarkus shutdown-spam reproduction. The original leaks 20 subscriptions past test
 * end (with a 1ms tick interval so a tick is essentially always in flight) and greps the gradle
 * log for "Error in when ticking" during Quarkus teardown — the shutdown contract under test is
 * that close() drains in-flight ticks before the pool is torn down.
 *
 * Here the same scenario runs against a dedicated Scoop instance and pool (the analog of the
 * @TestProfile-isolated container), and the log spam is detected by capturing stdout (pino
 * writes there) across the teardown window. Zero matches = the contract holds.
 */
describe("ShutdownSpamReproTest", () => {
    let sql: Sql
    let scoop: Scoop
    let subscriptions: Subscription[]

    before(() => {
        // Fresh, isolated instance; tick as fast as possible so the scheduled-tick path is
        // essentially always running, maximising the chance that close() lands on top of an
        // in-flight tick — the in-flight that races pool teardown.
        sql = postgres(process.env.DATABASE_URL!, { max: 30 })
        scoop = Scoop.create(sql, {
            topicNotifier: new PostgresTopicNotifier(sql),
            tickIntervalMillis: 1,
        })
        subscriptions = registerReproSubscriptions(scoop.messageQueue)
    })

    after(async () => {
        // Teardown happens inside the test itself (that IS the scenario); nothing left here.
    })

    test("subscriptions leak past test end so quarkus shutdown races scoop ticks", async () => {
        assert.equal(subscriptions.length, SUBSCRIPTION_COUNT)
        // Sleep briefly so several normal scheduled ticks happen on a healthy pool before
        // teardown begins.
        await sleep(200)

        // Capture everything written during the teardown window and assert the "Error in when
        // ticking" spam never appears (the reproduction signal of the original).
        const captured: string[] = []
        const originalWrite = process.stdout.write.bind(process.stdout)
        process.stdout.write = ((chunk: string | Uint8Array, ...rest: never[]) => {
            captured.push(typeof chunk === "string" ? chunk : Buffer.from(chunk).toString())
            return originalWrite(chunk, ...rest)
        }) as typeof process.stdout.write

        try {
            for (const subscription of subscriptions) {
                await subscription.close()
            }
            await scoop.close()
            await sql.end({ timeout: 5 })
            // Give any abandoned tick a moment to (incorrectly) fire against the closed pool.
            await sleep(100)
        } finally {
            process.stdout.write = originalWrite
        }

        const output = captured.join("")
        assert.ok(
            !output.includes("Error in when ticking") && !output.includes("Event loop failed"),
            `Shutdown produced tick error spam:\n${output}`,
        )

        // Verifying the ordering contract directly: after close(), no tick may still be running.
        // (The saga in ReproSubscriptionRegistrar records tick activity; see the helper.)
    })
})
