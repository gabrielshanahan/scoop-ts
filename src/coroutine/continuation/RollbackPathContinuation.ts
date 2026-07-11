import type { TransactionSql } from "postgres"
import { logger } from "../../logging.js"
import type { Message } from "../../messaging/Message.js"
import type { CooperationScope } from "../CooperationScope.js"
import type { DistributedCoroutine, NextStep, TransactionalStep } from "../DistributedCoroutine.js"
import type { CoroutineState, StepInstance } from "../EventLoop.js"
import {
    type ChildFailureHandlerIteration,
    NO_CHILD_FAILURE,
} from "../eventloop/SuspensionState.js"
import type { ScopeCapabilities } from "../structuredcooperation/Capabilities.js"
import {
    AfterLastStep,
    BaseCooperationContinuation,
    BeforeFirstStep,
    BetweenSteps,
} from "./CooperationContinuation.js"

/**
 * A continuation for rollback (compensating) execution of saga steps.
 *
 * Each executed step instance is split into two synthetic sub-steps during rollback:
 * 1. Child rollback step ("<prefix><step>[iter,cfh]<suffix>") — emits rollback messages to all
 *    child handlers spawned by that instance, then waits for them
 * 2. Self rollback step ("<prefix><step>[iter,cfh]") — executes the step's own rollback
 *
 * handleChildFailures instances generate only the child rollback step (their own invoke never
 * ran). Rollback operates on executed step INSTANCES (not step definitions), which is what makes
 * loops (Repeat/GoTo) roll back correctly — each execution rolled back individually, most recent
 * first.
 */
export const ROLLING_BACK_PREFIX = "Rollback of "
export const ROLLING_BACK_CHILD_SCOPES_STEP_SUFFIX = " (rolling back child scopes)"

const log = logger("RollbackPathContinuation")

export class RollbackPathContinuation extends BaseCooperationContinuation {
    giveUpStrategy(seen: string): string {
        return this.distributedCoroutine.eventLoopStrategy.giveUpOnRollbackPath(seen)
    }
}

/**
 * Builds a [RollbackPathContinuation] positioned to resume at the correct rollback sub-step,
 * from the saga's executed step instances.
 */
export function buildRollbackPathContinuation(
    distributedCoroutine: DistributedCoroutine,
    connection: TransactionSql,
    coroutineState: CoroutineState,
    scopeCapabilities: ScopeCapabilities,
): RollbackPathContinuation {
    const steps = distributedCoroutine.steps

    // Phase 1: Generate unique prefixes/suffixes to avoid conflicts with user-defined step names.
    let rollingBackPrefix = ROLLING_BACK_PREFIX
    while (steps.some(step => step.name.startsWith(rollingBackPrefix))) {
        rollingBackPrefix += "$"
    }
    let rollingBackSuffix = ROLLING_BACK_CHILD_SCOPES_STEP_SUFFIX
    while (steps.some(step => step.name.endsWith(rollingBackSuffix))) {
        rollingBackSuffix += "$"
    }

    // Phase 2: Filter to only original (non-rollback) step instances.
    const originalInstances = coroutineState.executedStepInstances.filter(
        instance => !instance.step.startsWith(rollingBackPrefix),
    )

    // Phase 3: Transform original instances into rollback sub-steps.
    const rollbackSteps = buildRollbackSteps(
        distributedCoroutine,
        rollingBackPrefix,
        rollingBackSuffix,
        scopeCapabilities,
        originalInstances,
    )

    log.debug(
        {
            coroutine: distributedCoroutine.identifier.toString(),
            rollbackSteps: rollbackSteps.length,
        },
        "Built rollback plan",
    )

    // Sentinel for edge cases where there is nothing to actually roll back but the continuation
    // needs a valid step reference.
    function createSentinelRollbackStep(stepName: string): TransactionalStep {
        const syntheticRollbackSteps = buildRollbackSteps(
            distributedCoroutine,
            rollingBackPrefix,
            rollingBackSuffix,
            scopeCapabilities,
            [
                {
                    step: stepName,
                    childFailureHandlerIteration: NO_CHILD_FAILURE,
                    suspendedAt: "1970-01-01T00:00:00Z",
                },
            ],
        )
        return syntheticRollbackSteps.length > 0
            ? syntheticRollbackSteps[syntheticRollbackSteps.length - 1]!
            : steps[0]!
    }

    function buildRollbackInitiatedContinuation(completedStep: string): RollbackPathContinuation {
        if (rollbackSteps.length === 0) {
            // Case 2: No rollback steps to execute. Create a sentinel and complete immediately.
            const sentinel = createSentinelRollbackStep(completedStep)
            return new RollbackPathContinuation(
                connection,
                coroutineState.cooperationContext,
                coroutineState.scopeIdentifier,
                new AfterLastStep(sentinel),
                distributedCoroutine,
                scopeCapabilities,
                0,
                NO_CHILD_FAILURE,
            )
        }
        // Case 3: Rollback just initiated — start before the first rollback step.
        return new RollbackPathContinuation(
            connection,
            coroutineState.cooperationContext,
            coroutineState.scopeIdentifier,
            new BeforeFirstStep(rollbackSteps[0]!),
            distributedCoroutine,
            scopeCapabilities,
            0,
            NO_CHILD_FAILURE,
        )
    }

    const state = coroutineState.suspensionState
    switch (state.kind) {
        case "notSuspendedYet": {
            // Case 1: Rollback before any step committed — nothing was emitted, nothing to roll
            // back. Sentinel positioned after the last step so the continuation completes.
            const sentinel = createSentinelRollbackStep(steps[0]!.name)
            return new RollbackPathContinuation(
                connection,
                coroutineState.cooperationContext,
                coroutineState.scopeIdentifier,
                new AfterLastStep(sentinel),
                distributedCoroutine,
                scopeCapabilities,
                0,
                NO_CHILD_FAILURE,
            )
        }

        case "suspendedBetweenSteps":
            // Rollback just initiated — the SuspensionState type tells us this is a happy-path
            // suspension, not a rollback suspension.
            return buildRollbackInitiatedContinuation(state.completedStep)

        case "lastStepFinished":
            return buildRollbackInitiatedContinuation(state.completedStep)

        case "suspendedAfterStepRollback": {
            // Case 4: Rollback already in progress — find our position in the sequence.
            const lastStepName = state.completedRollbackStep
            const nextStepIdx = rollbackSteps.findIndex(step => step.name === lastStepName)

            if (nextStepIdx === -1) {
                throw new Error(`Rollback step ${lastStepName} was not found`)
            }

            if (nextStepIdx === rollbackSteps.length - 1) {
                // At the last rollback step — rollback is completing.
                return new RollbackPathContinuation(
                    connection,
                    coroutineState.cooperationContext,
                    coroutineState.scopeIdentifier,
                    new AfterLastStep(rollbackSteps[rollbackSteps.length - 1]!),
                    distributedCoroutine,
                    scopeCapabilities,
                    0,
                    state.childFailureHandlerIteration,
                )
            }
            // Rollback continues — move to the next rollback sub-step.
            return new RollbackPathContinuation(
                connection,
                coroutineState.cooperationContext,
                coroutineState.scopeIdentifier,
                new BetweenSteps(rollbackSteps[nextStepIdx]!, rollbackSteps[nextStepIdx + 1]!),
                distributedCoroutine,
                scopeCapabilities,
                0,
                state.childFailureHandlerIteration,
            )
        }
    }
}

/**
 * Transforms executed step instances (most recent first) into rollback sub-steps executing in
 * that same order. Iteration indices are computed chronologically per step name;
 * handleChildFailures instances share the iteration index of the most recent normal instance of
 * the same step.
 */
function buildRollbackSteps(
    distributedCoroutine: DistributedCoroutine,
    rollingBackPrefix: string,
    rollingBackSuffix: string,
    scopeCapabilities: ScopeCapabilities,
    executedInstances: StepInstance[],
): TransactionalStep[] {
    const chronological = [...executedInstances].reverse()
    const stepCounters = new Map<string, number>()
    const instancesWithIterIdx: Array<[StepInstance, number]> = chronological
        .map((instance): [StepInstance, number] => {
            let iterIdx: number
            if (instance.childFailureHandlerIteration.kind === "noChildFailure") {
                iterIdx = stepCounters.get(instance.step) ?? 0
                stepCounters.set(instance.step, iterIdx + 1)
            } else {
                // childFailureHandler instance shares iteration with the preceding normal instance
                iterIdx = (stepCounters.get(instance.step) ?? 1) - 1
            }
            return [instance, iterIdx]
        })
        .reverse()

    const result: TransactionalStep[] = []
    for (const [instance, iterationIndex] of instancesWithIterIdx) {
        const step = distributedCoroutine.steps.find(s => s.name === instance.step)!
        const childFailureHandlerSuffix =
            instance.childFailureHandlerIteration.kind === "noChildFailure"
                ? ""
                : String(instance.childFailureHandlerIteration.iteration)
        const instanceSuffix = `[${iterationIndex},${childFailureHandlerSuffix}]`

        // Child rollback step: emits rollback messages to child handlers spawned by this instance.
        const childRollbackStep: TransactionalStep = {
            name: rollingBackPrefix + step.name + instanceSuffix + rollingBackSuffix,
            invoke(): Promise<NextStep> {
                throw new Error("Should never be invoked")
            },
            rollback(scope: CooperationScope, _message: Message, throwable: Error): Promise<void> {
                return scopeCapabilities.emitRollbacksForEmissions(
                    scope,
                    instance.suspendedAt,
                    throwable,
                )
            },
            handleChildFailures(
                scope: CooperationScope,
                message: Message,
                throwable: Error,
                stepIteration: number,
                childFailureHandlerIteration: number,
                nextStep: NextStep,
            ): Promise<NextStep> {
                return step.handleChildFailures(
                    scope,
                    message,
                    throwable,
                    stepIteration,
                    childFailureHandlerIteration,
                    nextStep,
                )
            },
        }

        if (instance.childFailureHandlerIteration.kind === "handlerIteration") {
            // Child failure handler instance: only the child rollback step is needed (no
            // self-rollback, since invoke was not called during failure handling).
            result.push(childRollbackStep)
        } else {
            // Normal instance: both child rollback and self rollback steps.
            const selfRollbackStep: TransactionalStep = {
                name: rollingBackPrefix + step.name + instanceSuffix,
                invoke(
                    scope: CooperationScope,
                    message: Message,
                    stepIteration: number,
                ): Promise<NextStep> {
                    return step.invoke(scope, message, stepIteration)
                },
                rollback(
                    scope: CooperationScope,
                    message: Message,
                    throwable: Error,
                    _stepIteration: number,
                    childFailureHandlerIteration: ChildFailureHandlerIteration,
                ): Promise<void> {
                    return step.rollback(scope, message, throwable, 0, childFailureHandlerIteration)
                },
                handleChildFailures(
                    scope: CooperationScope,
                    message: Message,
                    throwable: Error,
                    stepIteration: number,
                    childFailureHandlerIteration: number,
                    nextStep: NextStep,
                ): Promise<NextStep> {
                    return step.handleChildFailures(
                        scope,
                        message,
                        throwable,
                        stepIteration,
                        childFailureHandlerIteration,
                        nextStep,
                    )
                },
            }
            result.push(childRollbackStep, selfRollbackStep)
        }
    }
    return result
}
