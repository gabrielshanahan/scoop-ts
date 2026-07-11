import assert from "node:assert/strict"
import { describe, test } from "node:test"
import { saga } from "../../../src/coroutine/builder/SagaBuilder.js"
import { periodic, scheduledStep, sleepForStep } from "../../../src/coroutine/builder/Sleep.js"
import { eventLoopStrategy } from "../../../src/messaging/HandlerRegistry.js"
import { transactional } from "../../../src/coroutine/TransactionRunner.js"
import { isoFromNowMillis, nowMillis } from "../../../src/util/Clock.js"
import { ciSleep, eventLogSettled, setupScoopTest } from "../../support/harness.js"
import { CountDownLatch } from "../../support/latch.js"
import { getEventSequence, triple } from "../../support/util.js"

const h = setupScoopTest()

describe("SleepTest", () => {
    test("sleep works", async () => {
        const latch = new CountDownLatch(1)
        let step1Time = 0
        let step3Time = 0

        const rootHandlerCoroutine = saga("root-handler", eventLoopStrategy(h.messageQueue, h.strategyEpoch), b => {
            b.step({
                invoke: (_scope, _message) => {
                    step1Time = nowMillis()
                },
            })
            sleepForStep(b, 500)
            b.step({
                invoke: (_scope, _message) => {
                    step3Time = nowMillis()
                    latch.countDown()
                },
            })
        })
        const rootSubscription = await h.subscribe(h.rootTopic, rootHandlerCoroutine)

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
            })

            assert.ok(await latch.await(10_000), "All handlers should complete")
            await eventLogSettled(h.sql)

            assert.ok(step3Time - step1Time > 500, "Sleep doesn't work")
            assert.deepEqual(await getEventSequence(h.sql), [
                triple("EMITTED", null, null),
                triple("SEEN", null, "root-handler"),
                triple("SUSPENDED", "0", "root-handler"),
                triple("EMITTED", "1", "root-handler"),
                triple("SUSPENDED", "1", "root-handler"),
                triple("SEEN", null, "sleep-handler"),
                triple("SUSPENDED", "sleep", "sleep-handler"),
                triple("COMMITTED", "sleep", "sleep-handler"),
                triple("SUSPENDED", "2", "root-handler"),
                triple("COMMITTED", "2", "root-handler"),
            ])
        } finally {
            await rootSubscription.close()
        }
    })

    test("scheduling works", async () => {
        const latch = new CountDownLatch(1)
        let scheduledStepTime = ""
        const startAfter = isoFromNowMillis(500)

        const rootHandlerCoroutine = saga("root-handler", eventLoopStrategy(h.messageQueue, h.strategyEpoch), b => {
            scheduledStep(b, String(b.steps.length), startAfter, (_scope, _message) => {
                scheduledStepTime = isoFromNowMillis(0)
                latch.countDown()
            })
        })
        const rootSubscription = await h.subscribe(h.rootTopic, rootHandlerCoroutine)

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
            })

            assert.ok(await latch.await(10_000), "All handlers should complete")
            await eventLogSettled(h.sql)

            assert.ok(
                Date.parse(scheduledStepTime) > Date.parse(startAfter),
                "Scheduling doesn't work",
            )
            assert.deepEqual(await getEventSequence(h.sql), [
                triple("EMITTED", null, null),
                triple("SEEN", null, "root-handler"),
                triple("EMITTED", "0 (waiting for scheduled time)", "root-handler"),
                triple("SUSPENDED", "0 (waiting for scheduled time)", "root-handler"),
                triple("SEEN", null, "sleep-handler"),
                triple("SUSPENDED", "sleep", "sleep-handler"),
                triple("COMMITTED", "sleep", "sleep-handler"),
                triple("SUSPENDED", "0", "root-handler"),
                triple("COMMITTED", "0", "root-handler"),
            ])
        } finally {
            await rootSubscription.close()
        }
    })

    test("periodic scheduling works", async () => {
        const latch = new CountDownLatch(3)
        const runEveryMillis = 400
        const times: number[] = []

        const rootHandlerCoroutine = saga("root-handler", eventLoopStrategy(h.messageQueue, h.strategyEpoch), b => {
            periodic(b, runEveryMillis, 3)
            b.step({
                invoke: (_scope, _message) => {
                    times.push(nowMillis())
                    latch.countDown()
                },
            })
        })
        const rootSubscription = await h.subscribe(h.rootTopic, rootHandlerCoroutine)

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
            })

            assert.ok(await latch.await(10_000), "All handlers should complete")
            await eventLogSettled(h.sql)
            assert.ok(
                times.length === 3 &&
                    times[1]! - times[0]! > runEveryMillis &&
                    times[2]! - times[1]! > runEveryMillis,
                `Periodic scheduling doesn't work: ${times}`,
            )
        } finally {
            await rootSubscription.close()
        }
    })
})
