import type { Handler } from "../coroutine/Handler.js"
import type { PostgresMessageQueue } from "./PostgresMessageQueue.js"
import type { Subscription } from "./Subscription.js"

/**
 * Subscribes the given handler to its topic — extracts the topic and implementation from the
 * handler and delegates to `PostgresMessageQueue.subscribe`.
 */
export function subscribeHandler(
    messageQueue: PostgresMessageQueue,
    handler: Handler<never> | Handler<unknown>,
): Subscription {
    return messageQueue.subscribe(
        handler.topic.serializedValue,
        (handler as Handler<unknown>).implementation(),
    )
}
