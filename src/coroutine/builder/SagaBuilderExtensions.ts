import type { DistributedCoroutine } from "../DistributedCoroutine.js"
import type { EventLoopStrategy } from "../eventloop/strategy/EventLoopStrategy.js"
import type { Handler } from "../Handler.js"
import { type SagaBuilder, saga } from "./SagaBuilder.js"

/**
 * Builds a saga for a handler using the handler's name as the saga name — the analog of the
 * Kotlin `Handler<*>.saga(eventLoopStrategy) { … }` extension.
 */
export function handlerSaga(
    handler: Handler<unknown>,
    eventLoopStrategy: EventLoopStrategy,
    block: (builder: SagaBuilder) => void,
): DistributedCoroutine {
    return saga(handler.handlerName, eventLoopStrategy, block)
}
