import type { JsonValue } from "../../JsonbHelper.js"
import { logger } from "../../logging.js"
import type { DbConnection } from "../../sql.js"
import type { Message } from "../../messaging/Message.js"
import type { MessageRepository } from "../../messaging/MessageRepository.js"
import { uuidV7 } from "../../util/UuidV7.js"
import { CooperationContext, emptyContext } from "../context/CooperationContext.js"
import type { CooperationScope } from "../CooperationScope.js"
import {
    CooperationScopeIdentifier,
    RootScopeIdentifier,
} from "../CooperationScopeIdentifier.js"
import { renderAsString } from "../DistributedCoroutineIdentifier.js"
import type { Handler } from "../Handler.js"
import type { VariableName } from "../VariableName.js"
import { cooperationFailureFromThrowable } from "./CooperationFailure.js"
import {
    CancellationRequestedException,
    GaveUpException,
    ParentSaidSoException,
    RollbackRequestedException,
} from "./exceptions.js"
import type { MessageEventRepository } from "./MessageEventRepository.js"
import type { ReturnValueRepository } from "./ReturnValueRepository.js"

const log = logger("Capabilities")

/** The root of an independent cooperation hierarchy, returned by launchOnGlobalScope. */
export interface CooperationRoot {
    cooperationScopeIdentifier: RootScopeIdentifier
    message: Message
}

/**
 * Capabilities available to sagas during execution — operations performed from *within* a running
 * saga (via its [CooperationScope]).
 */
export interface ScopeCapabilities {
    launch(
        scope: CooperationScope,
        topic: string,
        payload: JsonValue,
        additionalContext: CooperationContext | null,
    ): Promise<Message>

    launchOnGlobalScope(
        scope: CooperationScope,
        topic: string,
        payload: JsonValue,
        context: CooperationContext | null,
    ): Promise<CooperationRoot>

    /**
     * Triggers rollback for all messages emitted from a specific step execution instance,
     * identified by the timestamp of the SUSPENDED event that closed its tick.
     */
    emitRollbacksForEmissions(
        scope: CooperationScope,
        suspendedAt: string,
        throwable: Error,
    ): Promise<void>

    /** Checks give-up conditions via the strategy's SQL and throws [GaveUpException] if met. */
    giveUpIfNecessary(
        scope: CooperationScope,
        giveUpSqlProvider: (seenAlias: string) => string,
    ): Promise<void>

    storeReturnValue(
        scope: CooperationScope,
        variableName: VariableName,
        value: JsonValue,
    ): Promise<void>

    getReturnValues(
        scope: CooperationScope,
        variableName: VariableName,
        handlerRegistry: (name: string) => Handler<unknown>,
    ): Promise<Map<Handler<unknown>, JsonValue>>

    getReturnValue(
        scope: CooperationScope,
        variableName: VariableName,
        handler: Handler<unknown>,
    ): Promise<JsonValue | null>
}

/**
 * External management capabilities for sagas — operations performed from the outside on arbitrary
 * cooperation lineages.
 */
export interface StructuredCooperationCapabilities {
    /** Starts a new independent cooperation hierarchy from outside any saga context. */
    launchOnGlobalScope(
        connection: DbConnection,
        topic: string,
        payload: JsonValue,
        context?: CooperationContext | null,
    ): Promise<CooperationRoot>

    /** Requests *user-initiated* cancellation of a saga from outside its execution context. */
    cancel(
        connection: DbConnection,
        cooperationScopeIdentifier: CooperationScopeIdentifier,
        source: string,
        reason: string,
    ): Promise<void>

    /** Requests rollback of an already-completed saga ("undo") from outside. */
    rollback(
        connection: DbConnection,
        cooperationScopeIdentifier: CooperationScopeIdentifier,
        source: string,
        reason: string,
    ): Promise<void>
}

/** Default implementation of both capability interfaces. */
export class Capabilities implements ScopeCapabilities, StructuredCooperationCapabilities {
    constructor(
        private readonly messageRepository: MessageRepository,
        private readonly messageEventRepository: MessageEventRepository,
        private readonly returnValueRepository: ReturnValueRepository,
    ) {}

    async launchOnGlobalScope(
        connectionOrScope: DbConnection | CooperationScope,
        topic: string,
        payload: JsonValue,
        context?: CooperationContext | null,
    ): Promise<CooperationRoot> {
        const connection = isScope(connectionOrScope)
            ? connectionOrScope.connection
            : connectionOrScope
        log.debug({ topic }, "Launching message on global scope")
        const message = await this.messageRepository.insertMessage(connection, topic, payload)
        const cooperationId = uuidV7()
        const cooperationLineage = [cooperationId]
        await this.messageEventRepository.insertGlobalEmittedEvent(
            connection,
            message.id,
            cooperationLineage,
            context ?? null,
        )
        return {
            cooperationScopeIdentifier: new RootScopeIdentifier(cooperationId),
            message,
        }
    }

    async launch(
        scope: CooperationScope,
        topic: string,
        payload: JsonValue,
        additionalContext: CooperationContext | null,
    ): Promise<Message> {
        const identifier = scope.continuation.continuationIdentifier
        log.debug(
            {
                topic,
                coroutine: renderAsString(identifier.distributedCoroutineIdentifier),
                step: identifier.stepName,
            },
            "Launching scoped message",
        )
        const message = await this.messageRepository.insertMessage(
            scope.connection,
            topic,
            payload,
        )

        await this.messageEventRepository.insertScopedEmittedEvent(
            scope.connection,
            message.id,
            identifier.distributedCoroutineIdentifier.name,
            identifier.distributedCoroutineIdentifier.instance,
            identifier.stepName,
            scope.scopeIdentifier.cooperationLineage,
            scope.context.plus(additionalContext ?? emptyContext()),
        )

        scope.emitted(message)

        return message
    }

    async cancel(
        connection: DbConnection,
        cooperationScopeIdentifier: CooperationScopeIdentifier,
        source: string,
        reason: string,
    ): Promise<void> {
        log.debug({ source, reason }, "Requesting cancellation")
        const exception = new CancellationRequestedException(reason)
        const cooperationFailure = cooperationFailureFromThrowable(exception, source)
        await this.messageEventRepository.insertCancellationRequestedEvent(
            connection,
            cooperationScopeIdentifier.cooperationLineage,
            cooperationFailure,
        )
    }

    async rollback(
        connection: DbConnection,
        cooperationScopeIdentifier: CooperationScopeIdentifier,
        source: string,
        reason: string,
    ): Promise<void> {
        log.debug({ source, reason }, "Requesting rollback")
        const exception = new RollbackRequestedException(reason)
        const cooperationFailure = cooperationFailureFromThrowable(exception, source)
        await this.messageEventRepository.insertRollbackEmittedEvent(
            connection,
            cooperationScopeIdentifier.cooperationLineage,
            cooperationFailure,
        )
    }

    async emitRollbacksForEmissions(
        scope: CooperationScope,
        suspendedAt: string,
        throwable: Error,
    ): Promise<void> {
        const identifier = scope.continuation.continuationIdentifier
        log.debug(
            {
                coroutine: renderAsString(identifier.distributedCoroutineIdentifier),
                step: identifier.stepName,
            },
            "Emitting rollbacks for child emissions",
        )
        const cooperationFailure = cooperationFailureFromThrowable(
            new ParentSaidSoException(throwable),
            renderAsString(identifier.distributedCoroutineIdentifier),
        )

        await this.messageEventRepository.insertRollbackEmittedEventsForStep(
            scope.connection,
            scope.scopeIdentifier.cooperationLineage,
            identifier.distributedCoroutineIdentifier.name,
            identifier.distributedCoroutineIdentifier.instance,
            identifier.stepName,
            cooperationFailure,
            scope.context,
            suspendedAt,
        )
    }

    async giveUpIfNecessary(
        scope: CooperationScope,
        giveUpSqlProvider: (seenAlias: string) => string,
    ): Promise<void> {
        const exceptions = await this.messageEventRepository.fetchGiveUpExceptions(
            scope.connection,
            giveUpSqlProvider,
            scope.scopeIdentifier.cooperationLineage,
        )

        if (exceptions.length > 0) {
            log.warn(
                {
                    count: exceptions.length,
                    coroutine: renderAsString(
                        scope.continuation.continuationIdentifier.distributedCoroutineIdentifier,
                    ),
                },
                "Saga giving up due to exception(s)",
            )
            throw new GaveUpException(exceptions)
        }
    }

    async storeReturnValue(
        scope: CooperationScope,
        variableName: VariableName,
        value: JsonValue,
    ): Promise<void> {
        log.debug({ variableName: variableName.serializedValue }, "Storing return value")
        const handlerName =
            scope.continuation.continuationIdentifier.distributedCoroutineIdentifier.name
        await this.returnValueRepository.storeReturnValue(
            scope.connection,
            scope.scopeIdentifier.cooperationLineage,
            handlerName,
            variableName,
            value,
        )
    }

    getReturnValues(
        scope: CooperationScope,
        variableName: VariableName,
        handlerRegistry: (name: string) => Handler<unknown>,
    ): Promise<Map<Handler<unknown>, JsonValue>> {
        log.debug({ variableName: variableName.serializedValue }, "Retrieving return values")
        return this.returnValueRepository.getReturnValues(
            scope.connection,
            scope.scopeIdentifier.cooperationLineage,
            variableName,
            handlerRegistry,
        )
    }

    getReturnValue(
        scope: CooperationScope,
        variableName: VariableName,
        handler: Handler<unknown>,
    ): Promise<JsonValue | null> {
        return this.returnValueRepository.getReturnValue(
            scope.connection,
            scope.scopeIdentifier.cooperationLineage,
            variableName,
            handler,
        )
    }
}

function isScope(value: DbConnection | CooperationScope): value is CooperationScope {
    return typeof (value as CooperationScope).launchOnGlobalScope === "function"
}
