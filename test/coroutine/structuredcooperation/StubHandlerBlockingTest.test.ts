import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { describe, test } from "node:test"
import { saga } from "../../../src/coroutine/builder/SagaBuilder.js"
import { Repeat } from "../../../src/coroutine/DistributedCoroutine.js"
import { StandardEventLoopStrategy } from "../../../src/coroutine/eventloop/strategy/StandardEventLoopStrategy.js"
import { eventLoopStrategy } from "../../../src/messaging/HandlerRegistry.js"
import { transactional } from "../../../src/coroutine/TransactionRunner.js"
import { nowIso } from "../../../src/util/Clock.js"
import { ciSleep, setupScoopTest } from "../../support/harness.js"
import { CountDownLatch } from "../../support/latch.js"

const h = setupScoopTest()

const stubTopic = "stub-topic"
const stubHandlerName = "stub-handler"

/**
 * Tests that a saga step properly blocks when a handler is registered in the topology but NOT
 * subscribed to the event loop (a "stub handler") — modelling e.g. a human-driven step whose SEEN
 * + COMMITTED events are written out-of-band.
 */
describe("StubHandlerBlockingTest", () => {
    function strategyWithStubHandler(): StandardEventLoopStrategy {
        return new StandardEventLoopStrategy(nowIso(), () => {
            const topology = new Map(h.messageQueue.listenersByTopic())
            topology.set(stubTopic, [...(topology.get(stubTopic) ?? []), stubHandlerName])
            return topology
        })
    }

    async function externallyCompleteStubHandler(): Promise<void> {
        await transactional(h.sql, async connection => {
            const [row] = await connection`
                SELECT m.id as message_id, me.cooperation_lineage
                FROM message m
                JOIN message_event me ON me.message_id = m.id AND me.type = 'EMITTED'
                WHERE m.topic = ${stubTopic}
            `
            const messageId = row!.message_id as string
            const parentLineage = row!.cooperation_lineage as string[]
            const childLineage = [...parentLineage, randomUUID()]

            for (const type of ["SEEN", "COMMITTED"]) {
                await connection`
                    INSERT INTO message_event (message_id, type, cooperation_lineage, coroutine_name, context)
                    VALUES (
                        ${messageId}, ${type}::message_event_type,
                        ${`{${childLineage.join(",")}}`}::uuid[],
                        ${stubHandlerName}, '{}'::jsonb
                    )
                `
            }
        })
    }

    test("repeating step blocks when stub handler has not started", async () => {
        const executionOrder: string[] = []
        let loopCounter = 0
        const latch = new CountDownLatch(1)

        const rootSubscription = await h.subscribe(
            h.rootTopic,
            saga("root-handler", strategyWithStubHandler(), b => {
                b.controlledStep({
                    name: "loop-step",
                    invoke: async (scope, _message, iteration) => {
                        const count = ++loopCounter
                        executionOrder.push(`loop-iter-${iteration}`)

                        if (count === 1) {
                            await scope.launch(h.childTopic, { from: "root" })
                            await scope.launch(stubTopic, { task: "wait-for-human" })
                            latch.countDown()
                        }
                        return Repeat
                    },
                })
            }),
        )

        const childSubscription = await h.subscribe(
            h.childTopic,
            saga("child-handler", eventLoopStrategy(h.messageQueue, h.strategyEpoch), b => {
                b.step({
                    invoke: (_scope, _message) => {
                        executionOrder.push("child-handler-done")
                    },
                })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
            })

            assert.ok(await latch.await(10_000), "First iteration did not complete")
            await ciSleep(2000)

            assert.deepEqual(
                executionOrder.filter(
                    entry => entry !== "child-handler-done" || executionOrder.includes(entry),
                ),
                ["loop-iter-0", "child-handler-done"],
            )

            const loopIterations = executionOrder.filter(entry => entry.startsWith("loop-iter-"))
            assert.equal(
                loopIterations.length,
                1,
                "Parent saga should execute only once when stub handler hasn't started. " +
                    `Got: ${executionOrder}`,
            )
        } finally {
            await rootSubscription.close()
            await childSubscription.close()
        }
    })

    test("repeating step resumes after externally writing SEEN and COMMITTED for stub handler", async () => {
        const executionOrder: string[] = []
        let loopCounter = 0
        const firstIterLatch = new CountDownLatch(1)
        const secondIterLatch = new CountDownLatch(1)

        const rootSubscription = await h.subscribe(
            h.rootTopic,
            saga("root-handler", strategyWithStubHandler(), b => {
                b.controlledStep({
                    name: "loop-step",
                    invoke: async (scope, _message, iteration) => {
                        const count = ++loopCounter
                        executionOrder.push(`loop-iter-${iteration}`)

                        if (count === 1) {
                            await scope.launch(stubTopic, { task: "wait-for-human" })
                            firstIterLatch.countDown()
                        } else if (count === 2) {
                            secondIterLatch.countDown()
                        }
                        return Repeat
                    },
                })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(connection, h.rootTopic, { initial: "true" })
            })

            assert.ok(await firstIterLatch.await(10_000), "First iteration did not complete")
            await ciSleep(500)
            assert.equal(loopCounter, 1, "Should be blocked after first iteration")

            await externallyCompleteStubHandler()

            assert.ok(
                await secondIterLatch.await(10_000),
                "Second iteration did not complete after externally writing SEEN+COMMITTED. " +
                    `Events: ${executionOrder}`,
            )
            assert.ok(
                loopCounter >= 2,
                "Parent should have executed at least twice after stub handler completed",
            )
        } finally {
            await rootSubscription.close()
        }
    })
})
