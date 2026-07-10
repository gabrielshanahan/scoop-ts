/**
 * Abstraction for receiving notifications when messages are published to a topic.
 *
 * Implementations typically use PostgreSQL LISTEN/NOTIFY to provide real-time message arrival
 * notifications. Notifications are an optimization supplementing polling — the system works
 * correctly with just polling via the event loop.
 */
export interface TopicNotifier {
    /** Register callback for topic notifications. Returns a handle to unsubscribe. */
    onMessage(topic: string, callback: () => void): { close(): void }
}

/** No-op implementation that relies solely on polling. */
export const NoOpTopicNotifier: TopicNotifier = {
    onMessage: () => ({ close: () => {} }),
}
