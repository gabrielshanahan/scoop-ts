import { ScoopException } from "../ScoopException.js"

/**
 * When a failure downstream causes a rollback of a particular saga run, the exception passed to
 * this saga run is the one that originally caused the rollback, wrapped in this class.
 */
export class ParentSaidSoException extends ScoopException {
    constructor(cause: Error) {
        super(null, cause, false)
    }
}

/** Thrown inside a saga when it is canceled via user request. */
export class CancellationRequestedException extends ScoopException {
    constructor(reason: string) {
        super(reason, null, true)
    }
}

/** Used when a completed saga needs to be rolled back via user request. */
export class RollbackRequestedException extends ScoopException {
    constructor(reason: string) {
        super(reason, null, true)
    }
}

/**
 * Thrown when the EventLoopStrategy determines a saga run should be abandoned (e.g. deadline
 * exceeded, cancellation requested). The first cause becomes the primary exception, with others
 * as suppressed.
 */
export class GaveUpException extends ScoopException {
    constructor(causes: Error[]) {
        super(null, causes[0]!, false)
        for (const cause of causes.slice(1)) {
            this.addSuppressed(cause)
        }
    }
}
