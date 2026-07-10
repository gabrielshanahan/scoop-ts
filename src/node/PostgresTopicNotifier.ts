import type { ListenMeta, Sql } from "postgres"
import { logger } from "../logging.js"
import type { TopicNotifier } from "../messaging/TopicNotifier.js"

const log = logger("PostgresTopicNotifier")

/**
 * Implements [TopicNotifier] using postgres.js LISTEN/NOTIFY — the analog of the Quarkus module's
 * Vert.x `PgSubscriberTopicNotifier` (see DECISIONS.md). postgres.js owns the dedicated listen
 * connection, reconnection, and channel demux.
 *
 * ## Fan-out
 * Exactly one LISTEN per topic is registered; each notification is dispatched asynchronously to
 * every callback currently registered for that topic (the analog of the original's one-Vert.x-
 * handler-per-topic fan-out, with `queueMicrotask` playing the role of the per-callback virtual
 * thread). Removing a callback stops it firing; the per-topic LISTEN is left in place — an empty
 * list dispatches to nobody, and it lives only as long as the pool.
 */
export class PostgresTopicNotifier implements TopicNotifier {
    private readonly callbacksByTopic = new Map<string, Set<() => void>>()
    private readonly listens: Promise<ListenMeta>[] = []

    constructor(private readonly sql: Sql) {}

    onMessage(topic: string, callback: () => void): { close(): void } {
        let callbacks = this.callbacksByTopic.get(topic)
        if (!callbacks) {
            const set = new Set<() => void>()
            this.callbacksByTopic.set(topic, set)
            callbacks = set
            this.listens.push(
                this.sql
                    .listen(topic, () => {
                        for (const cb of set) {
                            queueMicrotask(cb)
                        }
                    })
                    .catch(e => {
                        log.error({ err: e, topic }, "Failed to LISTEN on topic")
                        throw e
                    }),
            )
        }
        callbacks.add(callback)

        return {
            close: () => {
                callbacks.delete(callback)
            },
        }
    }

    /** Waits until every requested LISTEN is active (useful in tests to avoid startup races). */
    async ready(): Promise<void> {
        await Promise.all(this.listens)
    }
}
