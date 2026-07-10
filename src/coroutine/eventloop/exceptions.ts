import { ScoopException } from "../ScoopException.js"
import { CooperationException } from "../structuredcooperation/CooperationFailure.js"

/**
 * Used when one or more child handlers fail during structured cooperation. The first failure
 * becomes the primary cause; subsequent failures are added as suppressed exceptions.
 */
export class ChildRolledBackException extends ScoopException {
    constructor(causes: CooperationException[], step: string | null = null) {
        super(
            step === null ? null : `Child failure occurred while suspended in step [${step}]`,
            causes[0]!,
            false,
        )
        for (const cause of causes.slice(1)) {
            this.addSuppressed(cause)
        }
    }
}

/**
 * Used when one or more child rollback handlers fail during distributed rollback. In Scoop,
 * rollback failures take precedence over original exceptions.
 */
export class ChildRollbackFailedException extends ScoopException {
    constructor(causes: Error[], step: string | null = null) {
        super(
            step === null
                ? null
                : `Child rollback failure occurred while suspended in step [${step}]`,
            causes[0]!,
            false,
        )
        for (const cause of causes.slice(1)) {
            this.addSuppressed(cause)
        }
    }
}
