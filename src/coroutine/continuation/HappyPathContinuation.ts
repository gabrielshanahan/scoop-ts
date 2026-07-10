import type { TransactionSql } from "postgres"
import type { CooperationContext } from "../context/CooperationContext.js"
import type { ChildScopeIdentifier } from "../CooperationScopeIdentifier.js"
import type { DistributedCoroutine } from "../DistributedCoroutine.js"
import type { CoroutineState } from "../EventLoop.js"
import {
    ChildFailureHandlerIteration,
    NO_CHILD_FAILURE,
} from "../eventloop/SuspensionState.js"
import type { ScopeCapabilities } from "../structuredcooperation/Capabilities.js"
import {
    AfterLastStep,
    BaseCooperationContinuation,
    BeforeFirstStep,
    BetweenSteps,
    SuspensionPoint,
} from "./CooperationContinuation.js"

/**
 * A continuation for normal (forward) execution of saga steps. All the logic lives in
 * [BaseCooperationContinuation]; only the give-up strategy is specialized.
 */
export class HappyPathContinuation extends BaseCooperationContinuation {
    constructor(
        connection: TransactionSql,
        context: CooperationContext,
        scopeIdentifier: ChildScopeIdentifier,
        suspensionPoint: SuspensionPoint,
        distributedCoroutine: DistributedCoroutine,
        scopeCapabilities: ScopeCapabilities,
        stepIteration: number,
        childFailureHandlerIteration: ChildFailureHandlerIteration,
    ) {
        super(
            connection,
            context,
            scopeIdentifier,
            suspensionPoint,
            distributedCoroutine,
            scopeCapabilities,
            stepIteration,
            childFailureHandlerIteration,
        )
    }

    giveUpStrategy(seen: string): string {
        return this.distributedCoroutine.eventLoopStrategy.giveUpOnHappyPath(seen)
    }
}

/**
 * Builds a [HappyPathContinuation] from the current saga state, determining the correct
 * [SuspensionPoint] from what step was last executed and what step executes next.
 */
export function buildHappyPathContinuation(
    distributedCoroutine: DistributedCoroutine,
    connection: TransactionSql,
    coroutineState: CoroutineState,
    scopeCapabilities: ScopeCapabilities,
): HappyPathContinuation {
    const steps = distributedCoroutine.steps
    const suspensionState = coroutineState.suspensionState
    switch (suspensionState.kind) {
        case "notSuspendedYet":
            // Case 1: Not suspended yet - Create a continuation to execute the first step
            return new HappyPathContinuation(
                connection,
                coroutineState.cooperationContext,
                coroutineState.scopeIdentifier,
                new BeforeFirstStep(steps[0]!),
                distributedCoroutine,
                scopeCapabilities,
                0,
                NO_CHILD_FAILURE,
            )

        case "lastStepFinished":
            // Case 2: Last step finished - Create completion continuation
            return new HappyPathContinuation(
                connection,
                coroutineState.cooperationContext,
                coroutineState.scopeIdentifier,
                new AfterLastStep(steps[steps.length - 1]!),
                distributedCoroutine,
                scopeCapabilities,
                coroutineState.stepIteration,
                suspensionState.childFailureHandlerIteration,
            )

        case "suspendedBetweenSteps": {
            const completedStepIdx = steps.findIndex(
                step => step.name === suspensionState.completedStep,
            )
            const nextStepIdx = steps.findIndex(step => step.name === suspensionState.nextStep)
            if (completedStepIdx === -1) {
                throw new Error(`Step ${suspensionState.completedStep} was not found`)
            }
            if (nextStepIdx === -1) {
                throw new Error(`Next step ${suspensionState.nextStep} was not found`)
            }

            // Case 3: Normal forward execution - Create continuation for next step
            return new HappyPathContinuation(
                connection,
                coroutineState.cooperationContext,
                coroutineState.scopeIdentifier,
                new BetweenSteps(steps[completedStepIdx]!, steps[nextStepIdx]!),
                distributedCoroutine,
                scopeCapabilities,
                coroutineState.stepIteration,
                suspensionState.childFailureHandlerIteration,
            )
        }

        case "suspendedAfterStepRollback":
            throw new Error(
                "buildHappyPathContinuation called with SuspendedAfterStepRollback state",
            )
    }
}
