import assert from "node:assert/strict"
import { describe, test } from "node:test"
import { saga } from "../../../src/coroutine/builder/SagaBuilder.js"
import { MappedElement, MappedKey } from "../../../src/coroutine/context/CooperationContext.js"
import { writeContextJson } from "../../../src/coroutine/context/CooperationContextModule.js"
import { eventLoopStrategy } from "../../../src/messaging/HandlerRegistry.js"
import { transactional } from "../../../src/coroutine/TransactionRunner.js"
import { ciSleep, eventLogSettled, setupScoopTest } from "../../support/harness.js"
import { CountDownLatch } from "../../support/latch.js"

const h = setupScoopTest()

const ParentContextKey = new MappedKey<ParentContext>(
    "ParentContextKey",
    json => new ParentContext(json.value),
)

class ParentContext extends MappedElement {
    constructor(readonly value: number) {
        super(ParentContextKey)
    }
}

const ChildContextKey = new MappedKey<ChildContext>(
    "ChildContextKey",
    json => new ChildContext(json.value),
)

class ChildContext extends MappedElement {
    constructor(readonly value: number) {
        super(ChildContextKey)
    }
}

describe("ContextPropagationTest", () => {
    test("context is propagated correctly", async () => {
        const contextValues: string[] = []

        const latch = new CountDownLatch(1)

        const rootSubscription = h.subscribe(
            h.rootTopic,
            saga("root-handler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: async (scope, _message) => {
                        contextValues.push(writeContextJson(scope.context))
                        scope.context = scope.context.plus(
                            new ParentContext(scope.context.get(ParentContextKey)!.value + 1),
                        )
                        contextValues.push(writeContextJson(scope.context))
                        await scope.launch(
                            h.childTopic,
                            { from: "root-handler" },
                            new ChildContext(0),
                        )
                    },
                    rollback: (scope, _message, _throwable) => {
                        scope.context = scope.context.plus(
                            new ParentContext(scope.context.get(ParentContextKey)!.value + 1),
                        )
                        contextValues.push(writeContextJson(scope.context))
                        latch.countDown()
                    },
                })

                b.step({
                    invoke: (scope, _message) => {
                        scope.context = scope.context.plus(
                            new ParentContext(scope.context.get(ParentContextKey)!.value + 1),
                        )
                        contextValues.push(writeContextJson(scope.context))
                        throw new Error("Failure")
                    },
                })
            }),
        )

        const childSubscription = h.subscribe(
            h.childTopic,
            saga("child-handler", eventLoopStrategy(h.messageQueue), b => {
                b.step({
                    invoke: (scope, _message) => {
                        scope.context = scope.context.plus(
                            new ChildContext(scope.context.get(ChildContextKey)!.value + 1),
                        )
                        contextValues.push(writeContextJson(scope.context))
                    },
                    rollback: (scope, _message, _throwable) => {
                        scope.context = scope.context.plus(
                            new ChildContext(scope.context.get(ChildContextKey)!.value + 1),
                        )
                        contextValues.push(writeContextJson(scope.context))
                    },
                })
                b.step({
                    invoke: (scope, _message) => {
                        scope.context = scope.context.plus(
                            new ChildContext(scope.context.get(ChildContextKey)!.value + 1),
                        )
                        contextValues.push(writeContextJson(scope.context))
                    },
                    rollback: (scope, _message, _throwable) => {
                        scope.context = scope.context.plus(
                            new ChildContext(scope.context.get(ChildContextKey)!.value + 1),
                        )
                        contextValues.push(writeContextJson(scope.context))
                    },
                })
            }),
        )

        try {
            await transactional(h.sql, async connection => {
                await h.messageQueue.launch(
                    connection,
                    h.rootTopic,
                    { initial: "true" },
                    new ParentContext(0),
                )
            })

            assert.ok(await latch.await(10_000), "Not everything completed correctly")

            await eventLogSettled(h.sql)

            assert.deepEqual(
                contextValues,
                [
                    '{"ParentContextKey":{"value":0}}',
                    '{"ParentContextKey":{"value":1}}',
                    '{"ChildContextKey":{"value":1},"ParentContextKey":{"value":1}}',
                    '{"ChildContextKey":{"value":2},"ParentContextKey":{"value":1}}',
                    '{"ParentContextKey":{"value":2}}',
                    '{"ChildContextKey":{"value":3},"ParentContextKey":{"value":2}}',
                    '{"ChildContextKey":{"value":4},"ParentContextKey":{"value":2}}',
                    '{"ParentContextKey":{"value":3}}',
                ],
                "Context values should match",
            )
        } finally {
            await rootSubscription.close()
            await childSubscription.close()
        }
    })
})
