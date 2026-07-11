import type { Sql } from "postgres"
import { EventLoop } from "./coroutine/EventLoop.js"
import {
    Capabilities,
    StructuredCooperationCapabilities,
} from "./coroutine/structuredcooperation/Capabilities.js"
import { MessageEventRepository } from "./coroutine/structuredcooperation/MessageEventRepository.js"
import { ReturnValueRepository } from "./coroutine/structuredcooperation/ReturnValueRepository.js"
import { PostgresTransactionRunner, TransactionRunner } from "./coroutine/TransactionRunner.js"
import { JsonbHelper } from "./JsonbHelper.js"
import { logger } from "./logging.js"
import { MessageRepository } from "./messaging/MessageRepository.js"
import {
    DEFAULT_TICK_INTERVAL_MILLIS,
    PostgresMessageQueue,
} from "./messaging/PostgresMessageQueue.js"
import { NoOpTopicNotifier, TopicNotifier } from "./messaging/TopicNotifier.js"
import { DEFAULT_RECONCILE_SAFETY_NET_MILLIS } from "./coroutine/EventLoop.js"

const log = logger("Scoop")

export interface ScoopOptions {
    /** Notification mechanism for topic messages (default: polling only). */
    topicNotifier?: TopicNotifier
    /** How often the event loop polls for ready sagas (default: 50ms). */
    tickIntervalMillis?: number
    /**
     * Cutoff for the internal sleep handler's "ignore hierarchies older than" (default: client
     * clock at creation). Pass a database-clock timestamp to avoid clock-skew sensitivity.
     */
    ignoreMessagesOlderThan?: string
    /** Safety-net reconcile sweep interval (default: 30s). */
    reconcileSafetyNetMillis?: number
    /** Per-step transaction runner (default: postgres.js sql.begin). */
    transactionRunner?: TransactionRunner
    /** Exponential backoff base/cap after infrastructure failures (default: retry every tick). */
    retryBackoffBaseMillis?: number
    retryBackoffMaxMillis?: number
}

/**
 * Main entry point for creating and using Scoop — wires together all components from a
 * postgres.js pool. This is the composition root that, in the Kotlin original, exists twice
 * (Scoop.create for plain JVM and the Quarkus CDI producers) — see DECISIONS.md.
 *
 * ```ts
 * const sql = postgres(databaseUrl)
 * const scoop = Scoop.create(sql)
 * // scoop.messageQueue.subscribe(...) / launch(...)
 * await scoop.close()
 * ```
 */
export class Scoop {
    private constructor(
        readonly messageQueue: PostgresMessageQueue,
        readonly capabilities: StructuredCooperationCapabilities,
        readonly jsonbHelper: JsonbHelper,
    ) {}

    static create(sql: Sql, options: ScoopOptions = {}): Scoop {
        const jsonbHelper = new JsonbHelper()
        const messageRepository = new MessageRepository(jsonbHelper)
        const messageEventRepository = new MessageEventRepository(jsonbHelper)
        const returnValueRepository = new ReturnValueRepository(jsonbHelper)
        const capabilities = new Capabilities(
            messageRepository,
            messageEventRepository,
            returnValueRepository,
        )
        const transactionRunner = options.transactionRunner ?? new PostgresTransactionRunner(sql)
        const eventLoop = new EventLoop(
            sql,
            messageEventRepository,
            capabilities,
            jsonbHelper,
            transactionRunner,
            options.retryBackoffBaseMillis ?? 0,
            options.retryBackoffMaxMillis ?? 0,
        )
        const messageQueue = new PostgresMessageQueue(
            options.topicNotifier ?? NoOpTopicNotifier,
            capabilities,
            messageRepository,
            eventLoop,
            options.tickIntervalMillis ?? DEFAULT_TICK_INTERVAL_MILLIS,
            options.reconcileSafetyNetMillis ?? DEFAULT_RECONCILE_SAFETY_NET_MILLIS,
            options.ignoreMessagesOlderThan,
        )

        log.info("Scoop framework initialized")
        return new Scoop(messageQueue, capabilities, jsonbHelper)
    }

    /** Stops Scoop's internal subscriptions. Pool lifecycle stays with the caller. */
    /** Resolves once the internal notification channels are active — see [Subscription.ready]. */
    ready(): Promise<void> {
        return this.messageQueue.ready()
    }

    close(): Promise<void> {
        return this.messageQueue.close()
    }
}
