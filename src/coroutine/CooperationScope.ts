import type { TransactionSql } from "postgres"
import type { JsonValue } from "../JsonbHelper.js"
import type { Message } from "../messaging/Message.js"
import type { CooperationContext } from "./context/CooperationContext.js"
import type { ChildScopeIdentifier } from "./CooperationScopeIdentifier.js"
import type { Continuation } from "./continuation/Continuation.js"
import type { CooperationRoot } from "./structuredcooperation/Capabilities.js"
import type { Handler } from "./Handler.js"
import type { VariableName } from "./VariableName.js"

/**
 * An object bound to a single run of a saga. Created anew for each step (each step can run in a
 * different service instance), but conceptually always representing the same logical scope,
 * identified by its cooperation lineage.
 *
 * When a saga emits a message using [launch], handlers of that message become "children" of the
 * emitting saga; the parent suspends after its current step and waits for all children (and their
 * descendants) to finish before proceeding — governed by the EventLoopStrategy.
 */
export interface CooperationScope {
    readonly scopeIdentifier: ChildScopeIdentifier

    /** Shared context data that flows through the cooperation lineage. */
    context: CooperationContext

    /** The continuation this scope belongs to (the same object — see CooperationContinuation). */
    readonly continuation: Continuation

    /** The transaction handle for this saga's current step transaction. */
    readonly connection: TransactionSql

    /** Messages emitted during the current step execution. */
    readonly emittedMessages: readonly Message[]

    /** Records that a message has been emitted during this step. */
    emitted(message: Message): void

    /**
     * Emits a message within the current cooperation scope, creating a child scope. The saga
     * suspends after the current step until all handlers of the message complete.
     */
    launch(
        topic: string,
        payload: JsonValue,
        additionalContext?: CooperationContext | null,
    ): Promise<Message>

    /**
     * Emits a message on the global scope, breaking cooperation lineage — handlers run
     * independently and this saga does NOT wait for them.
     */
    launchOnGlobalScope(
        topic: string,
        payload: JsonValue,
        context?: CooperationContext | null,
    ): Promise<CooperationRoot>

    /**
     * Checks cancellation requests/deadlines and throws if the saga should be cancelled. Called
     * automatically just before and after each step; call manually in long-running steps for
     * cooperative cancellation.
     */
    giveUpIfNecessary(): Promise<void>

    /** Stores a return value that the parent saga can retrieve after this saga completes. */
    storeReturnValue(variableName: VariableName, value: JsonValue): Promise<void>

    /** Retrieves all return values from direct children for the given variable name. */
    getReturnValues(
        variableName: VariableName,
        handlerRegistry: (name: string) => Handler<unknown>,
    ): Promise<Map<Handler<unknown>, JsonValue>>

    /** Retrieves a specific return value from a direct child by handler. */
    getReturnValue(variableName: VariableName, handler: Handler<unknown>): Promise<JsonValue | null>
}
