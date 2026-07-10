import assert from "node:assert/strict"
import { describe, test } from "node:test"
import { saga } from "../../src/coroutine/builder/SagaBuilder.js"
import { eventLoopStrategy } from "../../src/messaging/HandlerRegistry.js"
import { transactional } from "../../src/coroutine/TransactionRunner.js"
import { ciSleep, eventLogSettled, setupScoopTest } from "../support/harness.js"
import { CountDownLatch } from "../support/latch.js"
import { fetchExceptions } from "../support/util.js"

const h = setupScoopTest()

class CustomTestException extends Error {
    constructor(message: string) {
        super(message)
        this.name = "CustomTestException"
    }
}

describe("PostgresMessageQueueExceptionTest", () => {
    test("test exception is stored and can be retrieved", async () => {
        const latch = new CountDownLatch(1)
        const customExceptionMessage = "Custom exception for testing exception storage"

        const rootHandler = "exception-test-handler"
        const rootSubscription = h.subscribe(
            h.rootTopic,
            saga(rootHandler, eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: (_scope, _message) => {
                        latch.countDown()
                        throw new CustomTestException(customExceptionMessage)
                    },
                })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, {
                    test: "exception-storage",
                })
            })

            assert.ok(await latch.await(10_000), "Handler should complete")
            await eventLogSettled(h.sql)

            const exceptions = await fetchExceptions(
                h.sql,
                h.jsonbHelper,
                "ROLLING_BACK",
                rootHandler,
            )

            assert.equal(exceptions.length, 1, "We should have a single exception")
            const exception = exceptions[0]!
            assert.ok(exception, "Exception should be stored and retrieved")
            // The Kotlin original asserts the JVM's fully-qualified class name; the runtime
            // analog of the exception "type" here is the Error's name (see PORT-LEDGER note).
            assert.equal(
                exception.type,
                "CustomTestException",
                "Exception should be of the correct type",
            )
            assert.ok(
                exception.message.endsWith(customExceptionMessage),
                "Exception should have the correct message",
            )
        } finally {
            await rootSubscription.close()
        }
    })
})
