/**
 * Abstraction for receiving notifications when messages are published to a topic.
 *
 * Implementations typically use PostgreSQL LISTEN/NOTIFY to provide real-time message arrival
 * notifications. Notifications are an optimization supplementing polling — the system works
 * correctly with just polling via the event loop.
 */
export interface TopicNotifier {
    /**
     * Register callback for topic notifications. Returns a handle to unsubscribe, plus [ready] —
     * resolved once the underlying notification channel for the topic is actually active (e.g.
     * LISTEN acknowledged by the server). Until then, notifications for freshly published
     * messages can be missed and delivery falls back to the polling safety net; callers that
     * publish immediately after subscribing should await [ready] to avoid that latency window.
     */
    onMessage(topic: string, callback: () => void): { close(): void; ready: Promise<void> }
}

/** No-op implementation that relies solely on polling. */
export const NoOpTopicNotifier: TopicNotifier = {
    onMessage: () => ({ close: () => {}, ready: Promise.resolve() }),
}
