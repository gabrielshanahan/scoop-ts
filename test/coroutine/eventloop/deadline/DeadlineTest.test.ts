import assert from "node:assert/strict"
import { describe, test } from "node:test"
import { saga } from "../../../../src/coroutine/builder/SagaBuilder.js"
import {
    happyPathTimeout,
    HappyPathDeadline,
    noHappyPathTimeout,
} from "../../../../src/coroutine/eventloop/deadline/HappyPathDeadline.js"
import { eventLoopStrategy } from "../../../../src/messaging/HandlerRegistry.js"
import { transactional } from "../../../../src/coroutine/TransactionRunner.js"
import { ciSleep, eventLogSettled, setupScoopTest } from "../../../support/harness.js"
import { CountDownLatch } from "../../../support/latch.js"
import {
    asSource,
    assertEquivalent,
    fetchExceptions,
    getEventSequence,
    triple,
} from "../../../support/util.js"

const h = setupScoopTest()

describe("DeadlineTest", () => {
    test("happy path deadlines work", async () => {
        const latch = new CountDownLatch(1)
        let childStarted = false
        let deadline: HappyPathDeadline | null = null

        const rootHandlerCoroutine = saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
            b.step({
                invoke: async (scope, _message) => {
                    deadline = happyPathTimeout(0, "root handler")
                    await scope.launch(h.childTopic, { from: "root-handler" }, deadline)
                },
                rollback: (_scope, _message, _throwable) => {
                    latch.countDown()
                },
            })
        })
        const rootSubscription = await h.subscribe(h.rootTopic, rootHandlerCoroutine)

        const childHandlerCoroutine = saga(
            "child-handler",
            eventLoopStrategy(h.messageQueue),
            b => {
                b.step({
                    invoke: async (_scope, _message) => {
                        childStarted = true
                        await new Promise(() => {}) // sleep forever (never reached)
                    },
                })
            },
        )
        const childSubscription = await h.subscribe(h.childTopic, childHandlerCoroutine)

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(
                    connection,
                    h.rootTopic,
                    { initial: "true" },
                    noHappyPathTimeout("root system"),
                )
            })

            assert.ok(await latch.await(10_000), "All handlers should complete")
            await eventLogSettled(h.sql)

            assert.ok(
                !childStarted,
                "Child shouldn't even start, since shouldGiveUp() is called before (and after) the handler actually runs",
            )
            assert.deepEqual(await getEventSequence(h.sql), [
                triple("EMITTED", null, null),
                triple("SEEN", null, "root-handler"),
                triple("EMITTED", "0", "root-handler"),
                triple("SUSPENDED", "0", "root-handler"),
                triple("SEEN", null, "child-handler"),
                triple("ROLLING_BACK", "0", "child-handler"),
                triple("ROLLED_BACK", "Rollback of 0[0,]", "child-handler"),
                triple("ROLLING_BACK", "0", "root-handler"),
                triple("ROLLBACK_EMITTED", "Rollback of 0[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of 0[0,] (rolling back child scopes)", "root-handler"),
                triple("SUSPENDED", "Rollback of 0[0,]", "root-handler"),
                triple("ROLLED_BACK", "Rollback of 0[0,]", "root-handler"),
            ])

            const expectedDeadlineMessage =
                `Missed happy path deadline of root handler at ${deadline!.deadline}. ` +
                `Deadline trace: [{"HappyPathDeadlineKey": {"trace": [], "source": "root system", "deadline": "9999-12-31T23:59:59.999999Z"}}]`

            const childHandlerRollingBackExceptions = await fetchExceptions(
                h.sql,
                h.jsonbHelper,
                "ROLLING_BACK",
                "child-handler",
            )

            assertEquivalent(
                [
                    {
                        message: `[${asSource(childHandlerCoroutine.identifier)}] GaveUpException: <no message>`,
                        type: "GaveUpException",
                        source: asSource(childHandlerCoroutine.identifier),
                        causes: [
                            {
                                message: `[root handler] MissedHappyPathDeadline: ${expectedDeadlineMessage}`,
                                type: "MissedHappyPathDeadline",
                                source: "root handler",
                            },
                        ],
                    },
                ],
                childHandlerRollingBackExceptions,
            )

            const rootHandlerRollingBackExceptions = await fetchExceptions(
                h.sql,
                h.jsonbHelper,
                "ROLLING_BACK",
                "root-handler",
            )

            assertEquivalent(
                [
                    {
                        message:
                            `[${asSource(rootHandlerCoroutine.identifier)}] ` +
                            "ChildRolledBackException: " +
                            "Child failure occurred while suspended in step [0]",
                        type: "ChildRolledBackException",
                        source: asSource(rootHandlerCoroutine.identifier),
                        causes: [
                            {
                                message: `[${asSource(childHandlerCoroutine.identifier)}] GaveUpException: <no message>`,
                                type: "GaveUpException",
                                source: asSource(childHandlerCoroutine.identifier),
                                causes: [
                                    {
                                        message: `[root handler] MissedHappyPathDeadline: ${expectedDeadlineMessage}`,
                                        type: "MissedHappyPathDeadline",
                                        source: "root handler",
                                    },
                                ],
                            },
                        ],
                    },
                ],
                rootHandlerRollingBackExceptions,
            )
        } finally {
            await rootSubscription.close()
            await childSubscription.close()
        }
    })
})
