import type { TransactionSql } from "postgres"
import type { JsonValue } from "../../JsonbHelper.js"
import { logger } from "../../logging.js"
import type { Message } from "../../messaging/Message.js"
import type { CooperationScope } from "../CooperationScope.js"
import type { ChildScopeIdentifier } from "../CooperationScopeIdentifier.js"
import type { CooperationContext } from "../context/CooperationContext.js"
import {
    Continue,
    type DistributedCoroutine,
    type NextStep,
    type TransactionalStep,
} from "../DistributedCoroutine.js"
import type { ChildFailureHandlerIteration } from "../eventloop/SuspensionState.js"
import type { Handler } from "../Handler.js"
import { ScoopInfrastructureException } from "../ScoopInfrastructureException.js"
import type { CooperationRoot, ScopeCapabilities } from "../structuredcooperation/Capabilities.js"
import type { VariableName } from "../VariableName.js"
import {
    type ChildFailed,
    type Continuation,
    type ContinuationResult,
    Failure,
    type LastStepResult,
    SUCCESS,
    Suspend,
} from "./Continuation.js"
import type { ContinuationIdentifier } from "./ContinuationIdentifier.js"

const log = logger("CooperationContinuation")

/**
 * Since the lifetime of a [CooperationScope] is exactly the same as the lifetime of the
 * (delimited) [Continuation], the continuation itself IS the scope — same design as the
 * original (and, incidentally, Kotlin coroutines' AbstractCoroutine).
 */
export interface CooperationContinuation extends Continuation, CooperationScope {}

/** Where the saga execution is suspended. */
export type SuspensionPoint = BeforeFirstStep | BetweenSteps | AfterLastStep

/** About to run the first step (fresh saga, or a rollback that hasn't rolled anything back yet). */
export class BeforeFirstStep {
    readonly kind = "beforeFirstStep" as const

    constructor(readonly firstStep: TransactionalStep) {}
}

/** Between two steps — previous step finished, next step hasn't started. */
export class BetweenSteps {
    readonly kind = "betweenSteps" as const

    constructor(
        readonly previousStep: TransactionalStep,
        readonly nextStep: TransactionalStep,
    ) {}
}

/** All steps completed — about to finish. */
export class AfterLastStep {
    readonly kind = "afterLastStep" as const

    constructor(readonly lastStep: TransactionalStep) {}
}

/**
 * Base implementation providing the shared continuation logic for both execution modes.
 * Subclasses ([HappyPathContinuation], [RollbackPathContinuation]) specialize only
 * [giveUpStrategy].
 */
export abstract class BaseCooperationContinuation implements CooperationContinuation {
    /**
     * The current step being executed within this continuation. Set by [resumeWith]: on a child
     * failure it is the PREVIOUS step (the one that emitted the failing messages, whose
     * handleChildFailures runs and to which emissions are attributed); on success it is the NEXT
     * step.
     */
    private currentStep!: TransactionalStep

    private readonly emittedMessages_: Message[] = []

    constructor(
        readonly connection: TransactionSql,
        public context: CooperationContext,
        readonly scopeIdentifier: ChildScopeIdentifier,
        private readonly suspensionPoint: SuspensionPoint,
        protected readonly distributedCoroutine: DistributedCoroutine,
        private readonly scopeCapabilities: ScopeCapabilities,
        private stepIteration: number,
        private childFailureHandlerIteration: ChildFailureHandlerIteration,
    ) {}

    get continuation(): Continuation {
        return this
    }

    get emittedMessages(): readonly Message[] {
        return this.emittedMessages_
    }

    get continuationIdentifier(): ContinuationIdentifier {
        return {
            stepName: this.currentStep.name,
            stepIteration: this.stepIteration,
            childFailureHandlerIteration: this.childFailureHandlerIteration,
            distributedCoroutineIdentifier: this.distributedCoroutine.identifier,
        }
    }

    /**
     * SQL that returns exception records when this continuation should give up and fail
     * (timeouts, cancellation requests). [seen] is the alias of the CTE containing the scope's
     * SEEN event.
     */
    abstract giveUpStrategy(seen: string): string

    emitted(message: Message): void {
        this.emittedMessages_.push(message)
    }

    launch(
        topic: string,
        payload: JsonValue,
        additionalContext: CooperationContext | null = null,
    ): Promise<Message> {
        return this.scopeCapabilities.launch(this, topic, payload, additionalContext)
    }

    launchOnGlobalScope(
        topic: string,
        payload: JsonValue,
        context: CooperationContext | null = null,
    ): Promise<CooperationRoot> {
        return this.scopeCapabilities.launchOnGlobalScope(this, topic, payload, context)
    }

    giveUpIfNecessary(): Promise<void> {
        return this.scopeCapabilities.giveUpIfNecessary(this, seen => this.giveUpStrategy(seen))
    }

    storeReturnValue(variableName: VariableName, value: JsonValue): Promise<void> {
        return this.scopeCapabilities.storeReturnValue(this, variableName, value)
    }

    getReturnValues(
        variableName: VariableName,
        handlerRegistry: (name: string) => Handler<unknown>,
    ): Promise<Map<Handler<unknown>, JsonValue>> {
        return this.scopeCapabilities.getReturnValues(this, variableName, handlerRegistry)
    }

    getReturnValue(
        variableName: VariableName,
        handler: Handler<unknown>,
    ): Promise<JsonValue | null> {
        return this.scopeCapabilities.getReturnValue(this, variableName, handler)
    }

    /**
     * Resumes execution from the suspension point, picking the step to run based on where the
     * saga was suspended and what happened in the previous execution:
     * - BeforeFirstStep: always execute the first step
     * - BetweenSteps + child failure: run the PREVIOUS step's failure handler
     * - BetweenSteps + success: run the NEXT step
     * - AfterLastStep: final cleanup on the last step (usually completes the saga)
     */
    async resumeWith(lastStepResult: LastStepResult): Promise<ContinuationResult> {
        log.debug(
            {
                suspensionPoint: this.suspensionPoint.kind,
                coroutine: this.distributedCoroutine.identifier.toString(),
            },
            "Resuming continuation",
        )
        switch (this.suspensionPoint.kind) {
            case "beforeFirstStep":
                this.currentStep = this.suspensionPoint.firstStep
                return this.resumeCoroutine(lastStepResult)
            case "betweenSteps":
                if (lastStepResult.kind === "childFailed") {
                    // Child handlers failed - let the step that emitted them handle the failure
                    this.currentStep = this.suspensionPoint.previousStep
                } else {
                    // Previous step completed successfully - advance to the next step
                    this.currentStep = this.suspensionPoint.nextStep
                }
                return this.resumeCoroutine(lastStepResult)
            case "afterLastStep":
                this.currentStep = this.suspensionPoint.lastStep
                return this.resumeCoroutine(lastStepResult)
        }
    }

    /**
     * Executes the continuation logic with give-up checks before and after the step, converting
     * step exceptions into a [Failure] result — except [ScoopInfrastructureException], which is
     * rethrown so the tick's transaction rolls back and the saga is retried from its last
     * committed step rather than rolled back.
     */
    async resumeCoroutine(lastStepResult: LastStepResult): Promise<ContinuationResult> {
        try {
            // Check if we should abandon execution before doing any work
            await this.giveUpIfNecessary()

            log.debug(
                {
                    coroutine: this.distributedCoroutine.identifier.toString(),
                    step: this.currentStep.name,
                },
                "Executing step",
            )

            // Execute the step logic and check for give-up conditions that arose during execution
            const result = await this.handleFailuresOrResume(lastStepResult)
            await this.giveUpIfNecessary()
            return result
        } catch (e) {
            if (e instanceof ScoopInfrastructureException) {
                throw e
            }
            return new Failure(e as Error)
        }
    }

    /**
     * Dispatches to the appropriate step method:
     * - ChildFailed → handleChildFailures on the current step (always suspends if it returns)
     * - SuccessfullyInvoked → invoke
     * - SuccessfullyRolledBack → rollback (always continues linearly)
     */
    private async handleFailuresOrResume(
        lastStepResult: LastStepResult,
    ): Promise<ContinuationResult> {
        switch (lastStepResult.kind) {
            case "childFailed": {
                log.debug(
                    {
                        step: this.currentStep.name,
                        iteration: this.childFailureHandlerIteration,
                    },
                    "Handling child failures",
                )
                // Increment child failure handler iteration for each failure handling invocation
                const incrementedIteration = this.childFailureHandlerIteration.incremented()
                this.childFailureHandlerIteration = incrementedIteration
                const nextStep = await this.currentStep.handleChildFailures(
                    this,
                    lastStepResult.message,
                    lastStepResult.throwable,
                    this.stepIteration,
                    incrementedIteration.iteration,
                    (lastStepResult as ChildFailed).nextStep,
                )
                // Failure handling always suspends (may have emitted retry messages)
                return new Suspend(this.emittedMessages_, this.validateNextStep(nextStep))
            }
            case "successfullyInvoked":
                // Previous execution succeeded - continue with normal forward execution
                return this.resume(() =>
                    this.currentStep.invoke(this, lastStepResult.message, this.stepIteration),
                )
            case "successfullyRolledBack":
                // Previous rollback succeeded - continue with compensating action execution
                return this.resume(async () => {
                    await this.currentStep.rollback(
                        this,
                        lastStepResult.message,
                        lastStepResult.throwable,
                        this.stepIteration,
                        this.childFailureHandlerIteration,
                    )
                    return Continue
                })
        }
    }

    /** Validates that a GoTo index is within the bounds of the saga's steps. */
    private validateNextStep(nextStep: NextStep): NextStep {
        if (nextStep.kind === "goTo") {
            if (
                nextStep.stepIndex < 0 ||
                nextStep.stepIndex >= this.distributedCoroutine.steps.length
            ) {
                throw new Error(
                    `GoTo step index ${nextStep.stepIndex} is out of bounds ` +
                        `(saga has ${this.distributedCoroutine.steps.length} steps)`,
                )
            }
        }
        return nextStep
    }

    /** Determines suspension vs. completion for normal step execution. */
    private async resume(resumeStep: () => Promise<NextStep>): Promise<ContinuationResult> {
        if (this.suspensionPoint.kind === "afterLastStep") {
            // All steps completed - saga is done, don't execute the step
            log.debug(
                { coroutine: this.distributedCoroutine.identifier.toString() },
                "Saga completed all steps",
            )
            return SUCCESS
        }
        // More steps remain - execute this step and suspend to wait for children
        const nextStep = this.validateNextStep(await resumeStep())
        return new Suspend(this.emittedMessages_, nextStep)
    }
}
