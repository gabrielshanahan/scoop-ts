import type { Message } from "../../messaging/Message.js"
import type { NextStep } from "../DistributedCoroutine.js"
import type { ContinuationIdentifier } from "./ContinuationIdentifier.js"

/**
 * Represents the execution of a single step within a saga — a delimited continuation: it spans
 * from "just before the next step" (after all child handlers from the last step have finished)
 * to "after that step finishes executing, just before the transaction commits". Handles any child
 * failures from the previous step first, then executes its predetermined step, then returns
 * control to the EventLoop.
 */
export interface Continuation {
    readonly continuationIdentifier: ContinuationIdentifier

    /** Resumes execution of the saga with the result from the previous step. */
    resumeWith(lastStepResult: LastStepResult): Promise<ContinuationResult>
}

/** The outcome of the previous step execution, passed into `resumeWith`. */
export type LastStepResult = SuccessfullyInvoked | SuccessfullyRolledBack | ChildFailed

/** The previous step (and all its child handlers) completed successfully. */
export class SuccessfullyInvoked {
    readonly kind = "successfullyInvoked" as const

    constructor(readonly message: Message) {}
}

/**
 * The saga is ready to proceed with rollback execution — either a rollback step completed, or
 * rollback just started. [throwable] is the original exception that caused the rollback.
 */
export class SuccessfullyRolledBack {
    readonly kind = "successfullyRolledBack" as const

    constructor(
        readonly message: Message,
        readonly throwable: Error,
    ) {}
}

/**
 * Child handlers from the previous step failed (the step itself committed fine). [nextStep] is
 * what the step's invoke returned, so handleChildFailures can override the navigation decision.
 */
export class ChildFailed {
    readonly kind = "childFailed" as const

    constructor(
        readonly message: Message,
        readonly throwable: Error,
        readonly nextStep: NextStep,
    ) {}
}

/** The outcome of resuming the continuation. */
export type ContinuationResult = Suspend | Success | Failure

/** A step executed successfully; the saga suspends waiting for child handlers / the strategy. */
export class Suspend {
    readonly kind = "suspend" as const

    constructor(
        readonly emittedMessages: readonly Message[],
        readonly nextStep: NextStep,
    ) {}
}

/** The saga completed successfully (all steps finished). */
export class Success {
    readonly kind = "success" as const
}

export const SUCCESS = new Success()

/** The step failed with an unhandled exception; rollback processing (or failure) follows. */
export class Failure {
    readonly kind = "failure" as const

    constructor(readonly exception: Error) {}
}
