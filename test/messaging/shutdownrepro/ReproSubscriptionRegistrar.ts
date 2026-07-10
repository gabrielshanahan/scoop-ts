import { saga } from "../../../src/coroutine/builder/SagaBuilder.js"
import { eventLoopStrategy } from "../../../src/messaging/HandlerRegistry.js"
import type { PostgresMessageQueue } from "../../../src/messaging/PostgresMessageQueue.js"
import type { Subscription } from "../../../src/messaging/Subscription.js"

export const SUBSCRIPTION_COUNT = 20

/**
 * Port of the Kotlin ReproSubscriptionRegistrar (a @Startup CDI bean): models the typical
 * application-side pattern of registering many subscriptions on startup and closing them at
 * shutdown. The shutdown contract under test is that close() drains in-flight ticks before
 * returning, so no tick keeps running against a half-closed pool.
 */
export function registerReproSubscriptions(messageQueue: PostgresMessageQueue): Subscription[] {
    const subscriptions: Subscription[] = []
    for (let i = 0; i < SUBSCRIPTION_COUNT; i++) {
        const topic = `scoop-shutdown-repro-${i}`
        subscriptions.push(
            messageQueue.subscribe(
                topic,
                saga(topic, eventLoopStrategy(messageQueue), b => {
                    b.step({ invoke: () => {} })
                }),
            ),
        )
    }
    return subscriptions
}
