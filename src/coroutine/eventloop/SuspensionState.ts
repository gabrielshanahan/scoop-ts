/**
 * Represents whether the last event loop tick was a child failure handler invocation. Each
 * `handleChildFailures` invocation is assigned an incrementing iteration number so its events are
 * distinguishable from events produced by the step's normal `invoke` execution.
 */
export type ChildFailureHandlerIteration = NoChildFailure | HandlerIteration

export class NoChildFailure {
    readonly kind = "noChildFailure" as const

    incremented(): HandlerIteration {
        return new HandlerIteration(0)
    }
}

export class HandlerIteration {
    readonly kind = "handlerIteration" as const

    constructor(readonly iteration: number) {}

    incremented(): HandlerIteration {
        return new HandlerIteration(this.iteration + 1)
    }
}

export const NO_CHILD_FAILURE = new NoChildFailure()

/**
 * Tracks the execution progress of a saga by indicating the current suspension state. Constructed
 * in EventLoop from data retrieved by `MessageEventRepository.fetchPendingCoroutineRun`.
 *
 * Timestamps (`suspendedAt`) are carried as the exact text Postgres produced, so they can be
 * passed back into SQL comparisons without losing microsecond precision.
 */
export type SuspensionState =
    | NotSuspendedYet
    | SuspendedBetweenSteps
    | LastStepFinished
    | SuspendedAfterStepRollback

/** A brand new saga that hasn't executed any steps yet. */
export class NotSuspendedYet {
    readonly kind = "notSuspendedYet" as const
}

export const NOT_SUSPENDED_YET = new NotSuspendedYet()

/** Saga completed [completedStep] and will next execute [nextStep] (which may be the same step). */
export class SuspendedBetweenSteps {
    readonly kind = "suspendedBetweenSteps" as const

    constructor(
        readonly completedStep: string,
        readonly nextStep: string,
        readonly suspendedAt: string,
        readonly childFailureHandlerIteration: ChildFailureHandlerIteration,
    ) {}
}

/** Saga completed its final step and is ready to be committed. */
export class LastStepFinished {
    readonly kind = "lastStepFinished" as const

    constructor(
        readonly completedStep: string,
        readonly suspendedAt: string,
        readonly childFailureHandlerIteration: ChildFailureHandlerIteration,
    ) {}
}

/**
 * Saga is executing rollback and completed a rollback sub-step (synthetic step name, e.g.
 * "Rollback of Step1[0,]"). The next rollback step is determined from the rollback step sequence.
 */
export class SuspendedAfterStepRollback {
    readonly kind = "suspendedAfterStepRollback" as const

    constructor(
        readonly completedRollbackStep: string,
        readonly suspendedAt: string,
        readonly childFailureHandlerIteration: ChildFailureHandlerIteration,
    ) {}
}
