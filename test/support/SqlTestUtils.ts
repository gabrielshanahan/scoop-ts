import type { Sql } from "postgres"
import type { JsonbHelper, JsonValue } from "../../src/JsonbHelper.js"
import type { ContinuationIdentifier } from "../../src/coroutine/continuation/ContinuationIdentifier.js"
import type { DistributedCoroutineIdentifier } from "../../src/coroutine/DistributedCoroutineIdentifier.js"
import { cooperationFailureFromThrowable } from "../../src/coroutine/structuredcooperation/CooperationFailure.js"
import type { Message } from "../../src/messaging/Message.js"

/**
 * Port of the Kotlin `SqlTestUtils` — helpers for manually crafting message/message_event rows in
 * tests that exercise the readiness SQL directly.
 */
export class SqlTestUtils {
    constructor(
        private readonly sql: Sql,
        private readonly jsonbHelper: JsonbHelper,
    ) {}

    async createMessage(topic: string, payload: JsonValue): Promise<Message> {
        const [row] = await this.sql`
            INSERT INTO message (topic, payload)
            VALUES (${topic}, ${this.jsonbHelper.toJsonbParam(payload) as never}::jsonb)
            RETURNING id, created_at
        `
        return {
            id: row!.id as string,
            topic,
            payload,
            createdAt: row!.created_at as Date,
        }
    }

    createSimpleMessage(topic: string, key = "key", value = "value"): Promise<Message> {
        return this.createMessage(topic, { [key]: value })
    }

    async emitted(
        messageId: string,
        continuationIdentifier: ContinuationIdentifier | null = null,
        cooperationLineage: string[] = [],
    ): Promise<string> {
        if (continuationIdentifier !== null) {
            const [row] = await this.sql`
                INSERT INTO message_event (message_id, type, coroutine_name, coroutine_identifier, step, cooperation_lineage)
                VALUES (
                    ${messageId}, 'EMITTED',
                    ${continuationIdentifier.distributedCoroutineIdentifier.name},
                    ${continuationIdentifier.distributedCoroutineIdentifier.instance},
                    ${continuationIdentifier.stepName},
                    ${`{${cooperationLineage.join(",")}}`}::uuid[]
                )
                RETURNING id
            `
            return row!.id as string
        }
        const [row] = await this.sql`
            INSERT INTO message_event (message_id, type) VALUES (${messageId}, 'EMITTED') RETURNING id
        `
        return row!.id as string
    }

    async coroutineEvent(
        messageId: string,
        distributedCoroutineIdentifier: DistributedCoroutineIdentifier,
        cooperationLineage: string[],
        eventType: string,
        throwable: Error | null,
    ): Promise<string> {
        const exception = throwable
            ? (this.jsonbHelper.toJsonbParam(
                  cooperationFailureFromThrowable(
                      throwable,
                      `${distributedCoroutineIdentifier.name}[${distributedCoroutineIdentifier.instance}]`,
                  ),
              ) as never)
            : null
        const [row] = await this.sql`
            INSERT INTO message_event (message_id, type, coroutine_name, coroutine_identifier, cooperation_lineage, exception)
            VALUES (
                ${messageId}, ${eventType}::message_event_type,
                ${distributedCoroutineIdentifier.name},
                ${distributedCoroutineIdentifier.instance},
                ${`{${cooperationLineage.join(",")}}`}::uuid[],
                ${exception}::jsonb
            )
            RETURNING id
        `
        return row!.id as string
    }

    seen(
        messageId: string,
        distributedCoroutineIdentifier: DistributedCoroutineIdentifier,
        cooperationLineage: string[],
    ): Promise<string> {
        return this.coroutineEvent(
            messageId,
            distributedCoroutineIdentifier,
            cooperationLineage,
            "SEEN",
            null,
        )
    }

    async continuationEvent(
        messageId: string,
        continuationIdentifier: ContinuationIdentifier,
        cooperationLineage: string[],
        eventType: string,
        throwable: Error | null,
    ): Promise<string> {
        const identifier = continuationIdentifier.distributedCoroutineIdentifier
        const exception = throwable
            ? (this.jsonbHelper.toJsonbParam(
                  cooperationFailureFromThrowable(
                      throwable,
                      `${identifier.name}[${identifier.instance}]`,
                  ),
              ) as never)
            : null
        const [row] = await this.sql`
            INSERT INTO message_event (message_id, type, coroutine_name, coroutine_identifier, step, cooperation_lineage, exception)
            VALUES (
                ${messageId}, ${eventType}::message_event_type,
                ${identifier.name},
                ${identifier.instance},
                ${continuationIdentifier.stepName},
                ${`{${cooperationLineage.join(",")}}`}::uuid[],
                ${exception}::jsonb
            )
            RETURNING id
        `
        return row!.id as string
    }

    suspended(
        messageId: string,
        continuationIdentifier: ContinuationIdentifier,
        cooperationLineage: string[],
    ): Promise<string> {
        return this.continuationEvent(
            messageId,
            continuationIdentifier,
            cooperationLineage,
            "SUSPENDED",
            null,
        )
    }

    committed(
        messageId: string,
        continuationIdentifier: ContinuationIdentifier,
        cooperationLineage: string[],
    ): Promise<string> {
        return this.continuationEvent(
            messageId,
            continuationIdentifier,
            cooperationLineage,
            "COMMITTED",
            null,
        )
    }

    rollingBackCoroutine(
        messageId: string,
        distributedCoroutineIdentifier: DistributedCoroutineIdentifier,
        cooperationLineage: string[],
        throwable: Error,
    ): Promise<string> {
        return this.coroutineEvent(
            messageId,
            distributedCoroutineIdentifier,
            cooperationLineage,
            "ROLLING_BACK",
            throwable,
        )
    }

    rollingBack(
        messageId: string,
        continuationIdentifier: ContinuationIdentifier,
        cooperationLineage: string[],
        throwable: Error,
    ): Promise<string> {
        return this.continuationEvent(
            messageId,
            continuationIdentifier,
            cooperationLineage,
            "ROLLING_BACK",
            throwable,
        )
    }

    rollbackEmitted(
        messageId: string,
        continuationIdentifier: ContinuationIdentifier,
        cooperationLineage: string[],
        throwable: Error,
    ): Promise<string> {
        return this.continuationEvent(
            messageId,
            continuationIdentifier,
            cooperationLineage,
            "ROLLBACK_EMITTED",
            throwable,
        )
    }

    rolledBack(
        messageId: string,
        continuationIdentifier: ContinuationIdentifier,
        cooperationLineage: string[],
    ): Promise<string> {
        return this.continuationEvent(
            messageId,
            continuationIdentifier,
            cooperationLineage,
            "ROLLED_BACK",
            null,
        )
    }

    rollbackFailed(
        messageId: string,
        continuationIdentifier: ContinuationIdentifier,
        cooperationLineage: string[],
        throwable: Error,
    ): Promise<string> {
        return this.continuationEvent(
            messageId,
            continuationIdentifier,
            cooperationLineage,
            "ROLLBACK_FAILED",
            throwable,
        )
    }
}
