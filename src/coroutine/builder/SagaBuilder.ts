import type { Message } from "../../messaging/Message.js"
import type { CooperationScope } from "../CooperationScope.js"
import {
    Continue,
    DistributedCoroutine,
    type NextStep,
    type TransactionalStep,
} from "../DistributedCoroutine.js"
import { DistributedCoroutineIdentifier } from "../DistributedCoroutineIdentifier.js"
import type { ChildFailureHandlerIteration } from "../eventloop/SuspensionState.js"
import type { EventLoopStrategy } from "../eventloop/strategy/EventLoopStrategy.js"

type MaybePromise<T> = T | Promise<T>

/** Simple step callbacks (the Kotlin builder's short-lambda overloads). */
export interface SimpleStepSpec {
    name?: string
    invoke: (scope: CooperationScope, message: Message) => MaybePromise<void>
    rollback?: (scope: CooperationScope, message: Message, throwable: Error) => MaybePromise<void>
    handleChildFailures?: (
        scope: CooperationScope,
        message: Message,
        throwable: Error,
    ) => MaybePromise<void>
}

/** stepIteration-aware step callbacks (the Kotlin builder's NextStep-returning overloads). */
export interface ControlledStepSpec {
    name?: string
    invoke: (
        scope: CooperationScope,
        message: Message,
        stepIteration: number,
    ) => MaybePromise<NextStep>
    rollback?: (
        scope: CooperationScope,
        message: Message,
        throwable: Error,
        stepIteration: number,
        childFailureHandlerIteration: ChildFailureHandlerIteration,
    ) => MaybePromise<void>
    handleChildFailures?: (
        scope: CooperationScope,
        message: Message,
        throwable: Error,
        stepIteration: number,
        childFailureHandlerIteration: number,
        nextStep: NextStep,
    ) => MaybePromise<NextStep>
}

/**
 * Fluent builder for creating distributed sagas — how Scoop implements the saga pattern as
 * sequences of [TransactionalStep]s. The saga name becomes the `coroutine_name` in the
 * `message_event` table.
 *
 * The Kotlin original distinguishes the simple and stepIteration-aware forms by lambda overloads;
 * here they are the [step] and [controlledStep] methods (see DECISIONS.md).
 */
export class SagaBuilder {
    readonly steps: TransactionalStep[] = []

    constructor(
        readonly name: string,
        readonly eventLoopStrategy: EventLoopStrategy,
    ) {}

    /**
     * Adds a step with simplified callbacks: invoke always advances with Continue, and a provided
     * handleChildFailures keeps the original navigation decision (returns the incoming nextStep).
     */
    step(spec: SimpleStepSpec): void {
        const name = spec.name ?? String(this.steps.length)
        this.steps.push({
            name,
            async invoke(scope, message, _stepIteration): Promise<NextStep> {
                await spec.invoke(scope, message)
                return Continue
            },
            async rollback(scope, message, throwable, _stepIteration, _cfhi): Promise<void> {
                if (spec.rollback) {
                    await spec.rollback(scope, message, throwable)
                }
            },
            async handleChildFailures(
                scope,
                message,
                throwable,
                _stepIteration,
                _childFailureHandlerIteration,
                nextStep,
            ): Promise<NextStep> {
                if (spec.handleChildFailures) {
                    await spec.handleChildFailures(scope, message, throwable)
                    return nextStep
                }
                throw throwable
            },
        })
    }

    /**
     * Adds a stepIteration-aware step that can control loop execution via the [NextStep] returned
     * from invoke (Continue / Repeat / GoTo) and handleChildFailures.
     */
    controlledStep(spec: ControlledStepSpec): void {
        const name = spec.name ?? String(this.steps.length)
        this.steps.push({
            name,
            invoke(scope, message, stepIteration): Promise<NextStep> {
                return Promise.resolve(spec.invoke(scope, message, stepIteration))
            },
            async rollback(
                scope,
                message,
                throwable,
                stepIteration,
                childFailureHandlerIteration,
            ): Promise<void> {
                if (spec.rollback) {
                    await spec.rollback(
                        scope,
                        message,
                        throwable,
                        stepIteration,
                        childFailureHandlerIteration,
                    )
                }
            },
            handleChildFailures(
                scope,
                message,
                throwable,
                stepIteration,
                childFailureHandlerIteration,
                nextStep,
            ): Promise<NextStep> {
                if (spec.handleChildFailures) {
                    return Promise.resolve(
                        spec.handleChildFailures(
                            scope,
                            message,
                            throwable,
                            stepIteration,
                            childFailureHandlerIteration,
                            nextStep,
                        ),
                    )
                }
                throw throwable
            },
        })
    }

    /** Adds a fully custom [TransactionalStep] (the analog of steps.add(object : … )). */
    addStep(step: TransactionalStep): void {
        this.steps.push(step)
    }

    build(): DistributedCoroutine {
        return new DistributedCoroutine(
            new DistributedCoroutineIdentifier(this.name),
            this.steps,
            this.eventLoopStrategy,
        )
    }
}

/**
 * Creates a distributed saga using the builder DSL — the main entry point for defining sagas.
 *
 * ```ts
 * const mySaga = saga("handler-name", strategy, b => {
 *     b.step({ invoke: async (scope, message) => { await scope.launch("child-topic", …) } })
 * })
 * ```
 */
export function saga(
    name: string,
    eventLoopStrategy: EventLoopStrategy,
    block: (builder: SagaBuilder) => void,
): DistributedCoroutine {
    const builder = new SagaBuilder(name, eventLoopStrategy)
    block(builder)
    return builder.build()
}
