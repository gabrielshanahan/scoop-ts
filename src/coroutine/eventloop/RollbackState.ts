import { CooperationException } from "../structuredcooperation/CooperationFailure.js"
import { ChildRollbackFailedException, ChildRolledBackException } from "./exceptions.js"

/**
 * Represents the rollback status of a saga, tracking both self ("Me") and child rollback states.
 * Direct port of the Kotlin sealed hierarchy; the marker interfaces become boolean flags:
 * - `meRollingBack` ⟺ Kotlin `Me.RollingBack`
 * - `throwable` present ⟺ Kotlin `ThrowableExists`
 *
 * Exception precedence: unlike Java's suppressed exceptions, rollback failures take precedence
 * over original exceptions — the rollback failure becomes the primary exception with the original
 * as a nested cause.
 */
export type RollbackState =
    | Gucci
    | SuccessfullyRolledBackLastStep
    | ChildrenFailedWhileRollingBackLastStep
    | ChildrenFailedAndSuccessfullyRolledBack
    | ChildrenFailedAndFailedToRollBack

/** Everything is fine — this saga is not rolling back and no children failed. */
export class Gucci {
    readonly kind = "gucci" as const
    readonly meRollingBack = false as const
}

export const GUCCI = new Gucci()

/** This saga is actively rolling back; the rollback of the last step completed successfully. */
export class SuccessfullyRolledBackLastStep {
    readonly kind = "successfullyRolledBackLastStep" as const
    readonly meRollingBack = true as const

    constructor(readonly throwable: CooperationException) {}
}

/** Children failed to roll back while this saga was also rolling back ("double failure"). */
export class ChildrenFailedWhileRollingBackLastStep {
    readonly kind = "childrenFailedWhileRollingBackLastStep" as const
    readonly meRollingBack = true as const
    readonly throwable: ChildRollbackFailedException

    constructor(
        step: string,
        rollbackFailures: CooperationException[],
        originalRollbackCause: CooperationException,
    ) {
        this.throwable = new ChildRollbackFailedException(
            [
                ...rollbackFailures,
                // Prevent exceptions pointlessly multiplying ad absurdum
                ...(containsRecursively(rollbackFailures, originalRollbackCause)
                    ? []
                    : [originalRollbackCause]),
            ],
            step,
        )
    }
}

/** Children failed but were successfully rolled back; this saga is not rolling back yet. */
export class ChildrenFailedAndSuccessfullyRolledBack {
    readonly kind = "childrenFailedAndSuccessfullyRolledBack" as const
    readonly meRollingBack = false as const
    readonly throwable: ChildRolledBackException

    constructor(step: string, childrenFailures: CooperationException[]) {
        this.throwable = new ChildRolledBackException(childrenFailures, step)
    }
}

/** Children failed and also failed to roll back; this saga is not rolling back yet. */
export class ChildrenFailedAndFailedToRollBack {
    readonly kind = "childrenFailedAndFailedToRollBack" as const
    readonly meRollingBack = false as const
    readonly throwable: ChildRollbackFailedException

    constructor(
        step: string,
        rollbackFailures: CooperationException[],
        originalRollbackCauses: CooperationException[],
    ) {
        this.throwable = new ChildRollbackFailedException(
            // This order ensures the first rollback failure is used as the cause
            [...rollbackFailures, new ChildRolledBackException(originalRollbackCauses)],
            step,
        )
    }
}

/** Kotlin `Children.Rollbacks` — a state carrying child failure exceptions. */
export type ChildrenRollbacks =
    | ChildrenFailedWhileRollingBackLastStep
    | ChildrenFailedAndSuccessfullyRolledBack
    | ChildrenFailedAndFailedToRollBack

export function isChildrenRollbacks(state: RollbackState): state is ChildrenRollbacks {
    return (
        state.kind === "childrenFailedWhileRollingBackLastStep" ||
        state.kind === "childrenFailedAndSuccessfullyRolledBack" ||
        state.kind === "childrenFailedAndFailedToRollBack"
    )
}

function containsRecursively(
    exceptions: CooperationException[],
    exception: CooperationException,
): boolean {
    return exceptions.some(
        e => e.equalsStructurally(exception) || containsRecursively(e.causes, exception),
    )
}
