import type { Sql, TransactionSql } from "postgres"
import type { JsonbHelper } from "../JsonbHelper.js"
import { logger } from "../logging.js"
import type { Message } from "../messaging/Message.js"
import { whileISaySo } from "../utils.js"
import type { CooperationScope } from "./CooperationScope.js"
import { ChildScopeIdentifier } from "./CooperationScopeIdentifier.js"
import { type CooperationContext, emptyContext } from "./context/CooperationContext.js"
import type { ContinuationResult, LastStepResult } from "./continuation/Continuation.js"
import {
    ChildFailed,
    SuccessfullyInvoked,
    SuccessfullyRolledBack,
} from "./continuation/Continuation.js"
import { buildHappyPathContinuation } from "./continuation/HappyPathContinuation.js"
import { buildRollbackPathContinuation } from "./continuation/RollbackPathContinuation.js"
import {
    Continue,
    type DistributedCoroutine,
    GoTo,
    type NextStep,
    Repeat,
} from "./DistributedCoroutine.js"
import { renderAsString } from "./DistributedCoroutineIdentifier.js"
import {
    ChildrenFailedAndFailedToRollBack,
    ChildrenFailedAndSuccessfullyRolledBack,
    ChildrenFailedWhileRollingBackLastStep,
    GUCCI,
    type RollbackState,
    SuccessfullyRolledBackLastStep,
} from "./eventloop/RollbackState.js"
import {
    type ChildFailureHandlerIteration,
    HandlerIteration,
    LastStepFinished,
    NO_CHILD_FAILURE,
    NOT_SUSPENDED_YET,
    SuspendedAfterStepRollback,
    SuspendedBetweenSteps,
    type SuspensionState,
} from "./eventloop/SuspensionState.js"
import type { PeriodicTick } from "./PeriodicTick.js"
import { ReconcileGate } from "./ReconcileGate.js"
import { ScoopInfrastructureException } from "./ScoopInfrastructureException.js"
import type { ScopeCapabilities } from "./structuredcooperation/Capabilities.js"
import {
    type CooperationException,
    type CooperationFailure,
    cooperationFailureFromThrowable,
    toCooperationException,
} from "./structuredcooperation/CooperationFailure.js"
import type { MessageEventRepository } from "./structuredcooperation/MessageEventRepository.js"
import { type TransactionRunner, transactional } from "./TransactionRunner.js"

const log = logger("EventLoop")

/** Max time close() waits for an in-flight tick before abandoning it with a warning. */
const SHUTDOWN_TIMEOUT_MILLIS = 10_000

/**
 * Default upper bound on how stale a worker's reconciliation may get when no NOTIFY arrives — the
 * safety-net sweep interval of the [ReconcileGate].
 */
export const DEFAULT_RECONCILE_SAFETY_NET_MILLIS = 30_000

/** Raw executed-step-instance rows from the finalSelect JSON aggregation. */
interface RawStepInstance {
    step: string
    child_failure_handler_iteration: number | null
    suspended_at: string
}

/**
 * A specific execution instance of a step, identified by name, child-failure-handler state, and
 * the (textual) timestamp of the SUSPENDED event.
 */
export interface StepInstance {
    step: string
    childFailureHandlerIteration: ChildFailureHandlerIteration
    suspendedAt: string
}

/** The execution state of a saga at a specific point in time. */
export interface CoroutineState {
    message: Message
    suspensionState: SuspensionState
    scopeIdentifier: ChildScopeIdentifier
    cooperationContext: CooperationContext
    rollbackState: RollbackState
    executedStepInstances: StepInstance[]
    stepIteration: number
}

/**
 * The core execution engine that drives structured cooperation by resuming suspended sagas.
 * A [tick] does two things: start continuations (EMITTED→SEEN / ROLLBACK_EMITTED→ROLLING_BACK
 * reconciliation, gated by [ReconcileGate]), and resume every ready saga run of the given type.
 */
export class EventLoop {
    // Exponential backoff for retries after a ScoopInfrastructureException. Default 0 base =
    // retry on every tick. State is in-memory, per-run (keyed by root message id); attempts reset
    // whenever the run makes progress.
    private readonly backoffAttempts = new Map<string, number>()
    private readonly backoffUntil = new Map<string, number>()

    constructor(
        private readonly sql: Sql,
        private readonly messageEventRepository: MessageEventRepository,
        private readonly scopeCapabilities: ScopeCapabilities,
        private readonly jsonbHelper: JsonbHelper,
        private readonly transactionRunner: TransactionRunner,
        private readonly retryBackoffBaseMillis: number = 0,
        private readonly retryBackoffMaxMillis: number = 0,
    ) {}

    private isBackedOff(messageId: string): boolean {
        const until = this.backoffUntil.get(messageId)
        return until !== undefined && Date.now() < until
    }

    private recordInfraBackoff(messageId: string): void {
        if (this.retryBackoffBaseMillis === 0) {
            return // retry-every-tick: keep no state
        }
        const attempt = (this.backoffAttempts.get(messageId) ?? 0) + 1
        this.backoffAttempts.set(messageId, attempt)
        const rawMillis = this.retryBackoffBaseMillis * 2 ** (attempt - 1)
        const cappedMillis = Math.min(rawMillis, this.retryBackoffMaxMillis)
        this.backoffUntil.set(messageId, Date.now() + cappedMillis)
        log.debug(
            { messageId, attempt, cappedMillis },
            "Infrastructure failure for run: backing off",
        )
    }

    private clearBackoff(messageId: string): void {
        this.backoffAttempts.delete(messageId)
        this.backoffUntil.delete(messageId)
    }

    /**
     * Executes a single tick of the event loop for the given saga type: reconcile (if the gate
     * says so), then drain all ready saga runs — one fresh transaction per resumed run.
     */
    async tick(
        topic: string,
        distributedCoroutine: DistributedCoroutine,
        reconcileGate: ReconcileGate = ReconcileGate.ALWAYS,
        isShuttingDown: () => boolean = () => false,
    ): Promise<void> {
        try {
            if (reconcileGate.shouldReconcile()) {
                try {
                    const insertedRows = await transactional(this.sql, connection =>
                        this.messageEventRepository
                            .startContinuationsForCoroutine(
                                connection,
                                distributedCoroutine.identifier.name,
                                distributedCoroutine.identifier.instance,
                                topic,
                                distributedCoroutine.eventLoopStrategy,
                            )
                            .catch(e => {
                                log.error(
                                    {
                                        err: e,
                                        coroutine: distributedCoroutine.identifier.toString(),
                                    },
                                    "Error when starting continuations for coroutine",
                                )
                                throw e
                            }),
                    )
                    reconcileGate.reconcileSucceeded(insertedRows)
                } catch (e) {
                    // Re-arm so a failed/rolled-back reconcile is retried on the next tick, then
                    // propagate (aborting this tick before the drain).
                    reconcileGate.reconcileFailed()
                    throw e
                }
            }

            await whileISaySo(async (repeatCount, saySo) => {
                await this.transactionRunner.inStepTransaction(async connection => {
                    try {
                        log.debug(
                            {
                                repeatCount,
                                topic,
                                coroutine: distributedCoroutine.identifier.toString(),
                            },
                            "Tick drain iteration",
                        )
                        const coroutineState = await this.fetchSomePendingCoroutineState(
                            connection,
                            distributedCoroutine,
                        )
                        if (coroutineState === null) {
                            return
                        }
                        const messageId = coroutineState.message.id
                        // Honour an in-effect infrastructure-failure backoff: leave the run
                        // pending and end this tick so it is retried on a later tick.
                        if (this.isBackedOff(messageId)) {
                            return
                        }
                        saySo()
                        let continuationResult: ContinuationResult
                        try {
                            continuationResult = await this.resumeCoroutine(
                                connection,
                                distributedCoroutine,
                                coroutineState,
                            )
                        } catch (e) {
                            if (e instanceof ScoopInfrastructureException) {
                                // Scoop's OWN bookkeeping failed — NOT saga logic. Don't roll the
                                // saga back: record a backoff and rethrow so this tick's
                                // transaction rolls back and the run is retried later.
                                this.recordInfraBackoff(messageId)
                            }
                            throw e
                        }

                        // The run made progress — clear any infrastructure-failure backoff.
                        this.clearBackoff(messageId)

                        if (continuationResult.kind === "failure") {
                            throw continuationResult.exception
                        }
                    } catch (e) {
                        log.error(
                            { err: e, coroutine: distributedCoroutine.identifier.toString() },
                            "Error when running coroutine",
                        )
                        throw e
                    }
                })
            })
        } catch (e) {
            // Ticks racing application shutdown throw at connection acquisition time — expected,
            // not actionable.
            if (isShuttingDown()) {
                log.debug(
                    { err: e, coroutine: distributedCoroutine.identifier.toString() },
                    "Tick failure during shutdown",
                )
            } else {
                log.error({ err: e }, "Error in when ticking")
            }
        }
    }

    /**
     * Starts a periodic event loop ticking approximately every [runApproximatelyEveryMillis]
     * (scheduleWithFixedDelay semantics: the next tick is scheduled after the previous finishes).
     * The returned handle's [trigger][PeriodicTick.trigger] queues an immediate tick, coalesced
     * with any in-flight tick — all ticks for this worker are strictly serialized.
     */
    tickPeriodically(
        topic: string,
        distributedCoroutine: DistributedCoroutine,
        runApproximatelyEveryMillis: number,
        reconcileSafetyNetMillis: number = DEFAULT_RECONCILE_SAFETY_NET_MILLIS,
        isShuttingDown: () => boolean = () => false,
    ): PeriodicTick {
        const intervalMillis = runApproximatelyEveryMillis
        const jitterMillis = Math.floor(intervalMillis * 0.02)

        // Per-worker reconciliation gate: set dirty by trigger() on every NOTIFY, consumed by
        // tick().
        const reconcileGate = ReconcileGate.create(reconcileSafetyNetMillis)

        // One permit: "there is at most one tick accounted for — running or queued". JS is
        // single-threaded, so a boolean check-and-set is atomic.
        let gateBusy = false
        let closed = false
        let timer: NodeJS.Timeout | null = null
        let inFlight: Promise<void> | null = null

        const runTick = async (): Promise<void> => {
            try {
                log.debug(
                    { topic, coroutine: distributedCoroutine.identifier.toString() },
                    "Starting tick",
                )
                await this.tick(topic, distributedCoroutine, reconcileGate, isShuttingDown)
            } catch (e) {
                log.error(
                    { err: e, coroutine: distributedCoroutine.identifier.toString() },
                    "Event loop failed",
                )
            } finally {
                gateBusy = false
            }
        }

        const scheduledLoop = async (): Promise<void> => {
            // Skip new ticks once shutdown has been signalled; an in-flight tick may finish.
            if (!closed && !isShuttingDown() && !gateBusy) {
                gateBusy = true
                inFlight = runTick()
                await inFlight
                inFlight = null
            }
            if (!closed) {
                timer = setTimeout(() => void scheduledLoop(), intervalMillis + jitterMillis)
            }
        }

        timer = setTimeout(() => void scheduledLoop(), 0)

        return {
            trigger: () => {
                // Mark dirty unconditionally, BEFORE the acquire below: a coalesced trigger must
                // still record that there may be new work — the in-flight tick may already have
                // consumed its dirty bit. Whichever tick runs next then reconciles.
                reconcileGate.markDirty()
                if (closed || gateBusy) {
                    return
                }
                gateBusy = true
                inFlight = runTick().finally(() => {
                    inFlight = null
                })
            },
            close: async () => {
                closed = true
                if (timer !== null) {
                    clearTimeout(timer)
                    timer = null
                }
                if (inFlight !== null) {
                    // Wait for the in-flight tick, bounded so a stuck tick can't deadlock
                    // shutdown (there is no thread interrupt to fall back to on this runtime).
                    const timedOut = await Promise.race([
                        inFlight.then(() => false),
                        new Promise<boolean>(resolve =>
                            setTimeout(() => resolve(true), SHUTDOWN_TIMEOUT_MILLIS),
                        ),
                    ])
                    if (timedOut) {
                        log.warn(
                            { coroutine: distributedCoroutine.identifier.toString() },
                            `Tick did not terminate within ${SHUTDOWN_TIMEOUT_MILLIS}ms; abandoning`,
                        )
                    }
                }
            },
        }
    }

    /**
     * Fetches the execution state for a saga that's ready to be resumed, combining the suspension
     * state (execution progress) and rollback state dimensions.
     */
    private async fetchSomePendingCoroutineState(
        connection: TransactionSql,
        distributedCoroutine: DistributedCoroutine,
    ): Promise<CoroutineState | null> {
        const result = await this.messageEventRepository.fetchPendingCoroutineRun(
            connection,
            distributedCoroutine.identifier.name,
            distributedCoroutine.eventLoopStrategy,
        )

        if (result === null) {
            log.debug(
                { coroutine: distributedCoroutine.identifier.toString() },
                "No messages for coroutine",
            )
            return null
        }

        log.debug(
            {
                coroutine: distributedCoroutine.identifier.toString(),
                id: result.messageId,
            },
            "Processing message for coroutine",
        )

        const message: Message = {
            id: result.messageId,
            topic: result.topic,
            payload: result.payload,
            createdAt: result.createdAt,
        }

        // Exceptions from child sagas that successfully rolled back, if any
        const childRolledBackExceptions = this.jsonbHelper
            .fromJsonb<CooperationFailure[]>(result.childRolledBackExceptions)
            .map(toCooperationException)

        // Exceptions from child rollbacks that failed, if any
        const childRollbackFailedExceptions = this.jsonbHelper
            .fromJsonb<CooperationFailure[]>(result.childRollbackFailedExceptions)
            .map(toCooperationException)

        // The exception that caused THIS saga run to enter rollback mode, if any
        const rollingBackException: CooperationException | null =
            result.rollingBackException !== null
                ? toCooperationException(
                      this.jsonbHelper.fromJsonb<CooperationFailure>(result.rollingBackException),
                  )
                : null

        // Two contexts are tracked separately for the case where a previously-successful saga is
        // being rolled back from the outside: 1) the context at the end of our successful run
        // (only populated during the first rollback step of a previously finished run), and
        // 2) the context of whatever triggered the rollback.
        const latestScopeContext =
            result.latestScopeContext !== null
                ? this.jsonbHelper.contextFromJsonb(result.latestScopeContext)
                : emptyContext()

        const latestContext =
            result.latestContext !== null
                ? this.jsonbHelper.contextFromJsonb(result.latestContext)
                : emptyContext()

        // Determine rollback state from the combination of exceptions
        let rollbackState: RollbackState
        if (childRollbackFailedExceptions.length > 0) {
            rollbackState =
                rollingBackException === null
                    ? new ChildrenFailedAndFailedToRollBack(
                          result.step!,
                          childRollbackFailedExceptions,
                          childRolledBackExceptions,
                      )
                    : new ChildrenFailedWhileRollingBackLastStep(
                          result.step!,
                          childRollbackFailedExceptions,
                          rollingBackException,
                      )
        } else if (childRolledBackExceptions.length > 0) {
            rollbackState =
                rollingBackException === null
                    ? new ChildrenFailedAndSuccessfullyRolledBack(
                          result.step!,
                          childRolledBackExceptions,
                      )
                    : new SuccessfullyRolledBackLastStep(rollingBackException)
        } else {
            rollbackState =
                rollingBackException === null
                    ? GUCCI
                    : new SuccessfullyRolledBackLastStep(rollingBackException)
        }

        const rawInstances = this.jsonbHelper.fromJsonb<RawStepInstance[]>(
            result.executedStepInstances,
        )

        // For rollback, we need the actual execution history to know which step executions to
        // roll back; only parsed when actually rolling back.
        const executedStepInstances: StepInstance[] = rollbackState.meRollingBack
            ? rawInstances.map(raw => ({
                  step: raw.step,
                  childFailureHandlerIteration:
                      raw.child_failure_handler_iteration !== null
                          ? new HandlerIteration(raw.child_failure_handler_iteration)
                          : NO_CHILD_FAILURE,
                  suspendedAt: raw.suspended_at,
              }))
            : []

        // Compute stepIteration for the NEXT step to be executed: count consecutive SUSPENDED
        // events (from most recent, with no child failure handler iteration) matching the next
        // step name.
        let stepIteration = 0
        if (result.nextStep !== null) {
            if (result.nextStep < 0 || result.nextStep >= distributedCoroutine.steps.length) {
                throw new Error(
                    `next_step ${result.nextStep} is out of bounds for coroutine ` +
                        `${distributedCoroutine.identifier.name} with ` +
                        `${distributedCoroutine.steps.length} steps`,
                )
            }
            const nextStepName = distributedCoroutine.steps[result.nextStep]!.name
            for (const raw of rawInstances.filter(
                instance => instance.child_failure_handler_iteration === null,
            )) {
                if (raw.step === nextStepName) {
                    stepIteration++
                } else {
                    break
                }
            }
        }

        // From latest_suspended.step: null means a brand new saga
        let suspensionState: SuspensionState
        if (result.step === null) {
            suspensionState = NOT_SUSPENDED_YET
        } else {
            const childFailureHandlerIteration: ChildFailureHandlerIteration =
                result.childFailureHandlerIteration !== null
                    ? new HandlerIteration(result.childFailureHandlerIteration)
                    : NO_CHILD_FAILURE

            const isSyntheticRollbackStep =
                rollbackState.meRollingBack &&
                !distributedCoroutine.steps.some(step => step.name === result.step)

            if (isSyntheticRollbackStep) {
                // Rollback path: step name is synthetic; next step comes from the rollback step
                // sequence, not from next_step.
                suspensionState = new SuspendedAfterStepRollback(
                    result.step,
                    result.suspendedAt!,
                    childFailureHandlerIteration,
                )
            } else if (result.nextStep !== null) {
                // Happy path: between two steps
                suspensionState = new SuspendedBetweenSteps(
                    result.step,
                    distributedCoroutine.steps[result.nextStep]!.name,
                    result.suspendedAt!,
                    childFailureHandlerIteration,
                )
            } else {
                // Happy path: last step finished, saga is done
                suspensionState = new LastStepFinished(
                    result.step,
                    result.suspendedAt!,
                    childFailureHandlerIteration,
                )
            }
        }

        return {
            message,
            suspensionState,
            // The cooperation lineage from the SEEN event for this saga
            scopeIdentifier: new ChildScopeIdentifier(result.cooperationLineage),
            // On key conflicts, the rollback trigger's context (latestContext) takes precedence —
            // it comes chronologically later
            cooperationContext: latestScopeContext.plus(latestContext),
            rollbackState,
            executedStepInstances,
            stepIteration,
        }
    }

    /**
     * Resumes execution of a saga from its current state: builds the appropriate continuation,
     * executes the next step within the current transaction, and persists the resulting state —
     * with rollback state changes recorded in separate transactions (the current one is about to
     * be rolled back on failure).
     */
    private async resumeCoroutine(
        connection: TransactionSql,
        distributedCoroutine: DistributedCoroutine,
        coroutineState: CoroutineState,
    ): Promise<ContinuationResult> {
        const cooperativeContinuation = coroutineState.rollbackState.meRollingBack
            ? buildRollbackPathContinuation(
                  distributedCoroutine,
                  connection,
                  coroutineState,
                  this.scopeCapabilities,
              )
            : buildHappyPathContinuation(
                  distributedCoroutine,
                  connection,
                  coroutineState,
                  this.scopeCapabilities,
              )

        let input: LastStepResult
        const rollbackState = coroutineState.rollbackState
        switch (rollbackState.kind) {
            case "gucci":
                input = new SuccessfullyInvoked(coroutineState.message)
                break
            case "successfullyRolledBackLastStep":
                input = new SuccessfullyRolledBack(coroutineState.message, rollbackState.throwable)
                break
            default: {
                // Children.Rollbacks: reconstruct invoke's NextStep from the suspension state
                let nextStep: NextStep
                const lastStep = coroutineState.suspensionState
                if (lastStep.kind === "suspendedBetweenSteps") {
                    const completedStepIdx = distributedCoroutine.steps.findIndex(
                        step => step.name === lastStep.completedStep,
                    )
                    const nextStepIdx = distributedCoroutine.steps.findIndex(
                        step => step.name === lastStep.nextStep,
                    )
                    if (nextStepIdx === completedStepIdx) {
                        nextStep = Repeat
                    } else if (nextStepIdx === completedStepIdx + 1) {
                        nextStep = Continue
                    } else {
                        nextStep = GoTo(nextStepIdx)
                    }
                } else {
                    nextStep = Continue
                }
                input = new ChildFailed(coroutineState.message, rollbackState.throwable, nextStep)
                break
            }
        }

        const continuationResult = await cooperativeContinuation.resumeWith(input)

        switch (continuationResult.kind) {
            case "success":
                if (!coroutineState.rollbackState.meRollingBack) {
                    await this.markCommitted(cooperativeContinuation, coroutineState.message.id)
                } else {
                    await this.markRolledBack(cooperativeContinuation, coroutineState.message.id)
                }
                break
            case "failure":
                if (!coroutineState.rollbackState.meRollingBack) {
                    await this.markRollingBackInSeparateTransaction(
                        cooperativeContinuation,
                        coroutineState.message.id,
                        continuationResult.exception,
                    )
                } else {
                    await this.markRollbackFailedInSeparateTransaction(
                        cooperativeContinuation,
                        coroutineState.message.id,
                        continuationResult.exception,
                    )
                }
                break
            case "suspend":
                await this.markSuspended(
                    cooperativeContinuation,
                    coroutineState.message.id,
                    continuationResult.nextStep,
                    distributedCoroutine,
                )
                break
        }

        log.debug(
            {
                continuation: cooperativeContinuation.continuationIdentifier,
                id: coroutineState.message.id,
                result: continuationResult.kind,
            },
            "Finished processing message for continuation",
        )

        return continuationResult
    }

    private markCommitted(scope: CooperationScope, messageId: string): Promise<void> {
        return this.mark(scope, scope.connection, messageId, "COMMITTED")
    }

    private markRolledBack(scope: CooperationScope, messageId: string): Promise<void> {
        return this.mark(scope, scope.connection, messageId, "ROLLED_BACK")
    }

    private markSuspended(
        scope: CooperationScope,
        messageId: string,
        nextStep: NextStep,
        distributedCoroutine: DistributedCoroutine,
    ): Promise<void> {
        const currentStepIdx = distributedCoroutine.steps.findIndex(
            step => step.name === scope.continuation.continuationIdentifier.stepName,
        )
        // For rollback steps (synthetic names not in distributedCoroutine.steps), currentStepIdx
        // is -1. next_step is not used on the rollback path, so we write null.
        let nextStepIndex: number | null
        if (currentStepIdx === -1) {
            nextStepIndex = null
        } else {
            switch (nextStep.kind) {
                case "continue": {
                    const next = currentStepIdx + 1
                    nextStepIndex = next >= distributedCoroutine.steps.length ? null : next
                    break
                }
                case "repeat":
                    nextStepIndex = currentStepIdx
                    break
                case "goTo":
                    nextStepIndex = nextStep.stepIndex
                    break
            }
        }
        return this.mark(scope, scope.connection, messageId, "SUSPENDED", null, nextStepIndex)
    }

    private markRollingBackInSeparateTransaction(
        scope: CooperationScope,
        messageId: string,
        exception: Error | null = null,
    ): Promise<void> {
        return transactional(this.sql, connection =>
            this.mark(scope, connection, messageId, "ROLLING_BACK", exception),
        )
    }

    private markRollbackFailedInSeparateTransaction(
        scope: CooperationScope,
        messageId: string,
        exception: Error | null = null,
    ): Promise<void> {
        return transactional(this.sql, connection =>
            this.mark(scope, connection, messageId, "ROLLBACK_FAILED", exception),
        )
    }

    private mark(
        scope: CooperationScope,
        connection: TransactionSql,
        messageId: string,
        messageEventType: string,
        exception: Error | null = null,
        nextStep: number | null = null,
    ): Promise<void> {
        const cooperationFailure = exception
            ? cooperationFailureFromThrowable(
                  exception,
                  renderAsString(
                      scope.continuation.continuationIdentifier.distributedCoroutineIdentifier,
                  ),
              )
            : null

        const iterationState =
            scope.continuation.continuationIdentifier.childFailureHandlerIteration
        const childFailureHandlerIteration =
            iterationState.kind === "handlerIteration" ? iterationState.iteration : null

        return this.messageEventRepository.insertMessageEvent(
            connection,
            messageId,
            messageEventType,
            scope.continuation.continuationIdentifier.distributedCoroutineIdentifier.name,
            scope.continuation.continuationIdentifier.distributedCoroutineIdentifier.instance,
            scope.continuation.continuationIdentifier.stepName,
            scope.scopeIdentifier.cooperationLineage,
            cooperationFailure,
            scope.context,
            childFailureHandlerIteration,
            nextStep,
        )
    }
}
