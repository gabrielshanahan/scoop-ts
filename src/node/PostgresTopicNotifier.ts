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
 * handler-per-topic fan-out onto per-callback virtual threads). Each dispatch gets a small
 * random delay: virtual-thread scheduling desynchronizes sibling workers on the JVM, whereas a
 * same-microtask dispatch on Node's single event loop leaves same-topic workers PHASE-LOCKED —
 * their reconcile passes then collide on the parent SEEN row's FOR UPDATE SKIP LOCKED every
 * tick, and the loser can stay starved until the reconcile safety net. The jitter restores the
 * concurrency texture the original's gate sizing assumes (see DECISIONS.md).
 *
 * Removing a callback stops it firing; the per-topic LISTEN is left in place — an empty list
 * dispatches to nobody, and it lives only as long as the pool.
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
                            setTimeout(cb, Math.random() * 10)
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
