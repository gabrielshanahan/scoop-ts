import type { JsonbHelper, JsonValue } from "../JsonbHelper.js"
import { logger } from "../logging.js"
import { DbConnection, queryNamed } from "../sql.js"
import { asScoopInfrastructure } from "../coroutine/ScoopInfrastructureException.js"
import type { Message } from "./Message.js"

const log = logger("MessageRepository")

/**
 * Message persistence and retrieval — the append-only `message` table underlying the queue.
 * All operations run on the caller-supplied connection/transaction, keeping message persistence
 * atomic with message-event bookkeeping.
 */
export class MessageRepository {
    constructor(private readonly jsonbHelper: JsonbHelper) {}

    /** Retrieves a message by id, or null when not found. */
    async fetchMessage(connection: DbConnection, messageId: string): Promise<Message | null> {
        return asScoopInfrastructure(async () => {
            const rows = await queryNamed(
                connection,
                "SELECT id, topic, payload, created_at FROM message WHERE id = :messageId",
                { messageId },
            )
            const row = rows[0]
            if (row === undefined) {
                log.debug({ messageId }, "Message not found")
                return null
            }
            return {
                id: row.id as string,
                topic: row.topic as string,
                payload: this.jsonbHelper.fromJsonb(row.payload),
                createdAt: row.created_at as Date,
            }
        })
    }

    /** Persists a new message and returns it with its generated id and timestamp. */
    async insertMessage(
        connection: DbConnection,
        topic: string,
        payload: JsonValue,
    ): Promise<Message> {
        return asScoopInfrastructure(async () => {
            const rows = await queryNamed(
                connection,
                "INSERT INTO message (topic, payload) VALUES (:topic, :payload::jsonb) RETURNING id, created_at",
                { topic, payload: this.jsonbHelper.toJsonbParam(payload) },
            )
            const row = rows[0]!
            const message: Message = {
                id: row.id as string,
                topic,
                payload,
                createdAt: row.created_at as Date,
            }
            log.debug({ id: message.id, topic }, "Inserted message")
            return message
        })
    }
}
