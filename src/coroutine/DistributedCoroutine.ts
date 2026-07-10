import type { Message } from "../messaging/Message.js"
import type { CooperationScope } from "./CooperationScope.js"
import type { DistributedCoroutineIdentifier } from "./DistributedCoroutineIdentifier.js"
import type { ChildFailureHandlerIteration } from "./eventloop/SuspensionState.js"
import type { EventLoopStrategy } from "./eventloop/strategy/EventLoopStrategy.js"

/**
 * Result returned by [TransactionalStep.invoke] to control loop execution.
 * - Continue: the step completed normally; the saga advances to the next step.
 * - Repeat: the step should be re-executed (equivalent to GoTo(ownIndex)).
 * - GoTo: jump to a specific step by index.
 */
export type NextStep =
    | { readonly kind: "continue" }
    | { readonly kind: "repeat" }
    | { readonly kind: "goTo"; readonly stepIndex: number }

export const Continue: NextStep = { kind: "continue" }
export const Repeat: NextStep = { kind: "repeat" }
export function GoTo(stepIndex: number): NextStep {
    return { kind: "goTo", stepIndex }
}

/**
 * Represents a single step in a distributed saga. Each step corresponds to a single database
 * transaction. A step can perform business logic ([invoke]), handle failures from child handlers
 * ([handleChildFailures]), and compensate for its actions during rollback ([rollback]).
 *
 * When a step emits messages via `scope.launch()`, the handlers of those messages become "child
 * handlers" of this saga; the saga suspends after the step and won't proceed until ALL child
 * handlers have completed — the core rule of structured cooperation.
 */
export interface TransactionalStep {
    readonly name: string

    invoke(scope: CooperationScope, message: Message, stepIteration: number): Promise<NextStep>

    /**
     * The compensating action for this step during rollback. Runs in its own database
     * transaction. Default implementation does nothing.
     */
    rollback(
        scope: CooperationScope,
        message: Message,
        throwable: Error,
        stepIteration: number,
        childFailureHandlerIteration: ChildFailureHandlerIteration,
    ): Promise<void>

    /**
     * Handles failures from child message handlers spawned by this step. Conceptually part of the
     * same step as [invoke], even though it runs in a separate transaction — messages emitted
     * during its execution are treated as if emitted from [invoke]. The default implementation
     * re-throws, causing the saga to enter rollback mode. The returned [NextStep] overrides
     * whatever [invoke] returned.
     */
    handleChildFailures(
        scope: CooperationScope,
        message: Message,
        throwable: Error,
        stepIteration: number,
        childFailureHandlerIteration: number,
        nextStep: NextStep,
    ): Promise<NextStep>
}

/** Default no-op rollback and rethrowing child-failure handler, as in the Kotlin interface. */
export const transactionalStepDefaults = {
    async rollback(): Promise<void> {},
    async handleChildFailures(
        _scope: CooperationScope,
        _message: Message,
        throwable: Error,
    ): Promise<NextStep> {
        throw throwable
    },
}

/**
 * Represents a distributed saga — a sequence of [TransactionalStep]s that can be suspended and
 * resumed across database transactions and service boundaries. State is preserved in the database
 * (the `message_event` log) between suspension and resumption.
 */
export class DistributedCoroutine {
    constructor(
        readonly identifier: DistributedCoroutineIdentifier,
        readonly steps: TransactionalStep[],
        readonly eventLoopStrategy: EventLoopStrategy,
    ) {
        if (steps.length === 0) {
            throw new Error("Steps cannot be empty")
        }
        if (new Set(steps.map(step => step.name)).size !== steps.length) {
            throw new Error("Steps must have unique names")
        }
    }
}
