/**
 * Base class for exceptions that are specific to Scoop's implementation of structured cooperation
 * (as opposed to protocol-level failures, represented by `CooperationFailure` and translated to
 * `CooperationException`). Mostly wrappers giving human-readable names to certain situations.
 */
export abstract class ScoopException extends Error {
    override readonly cause?: Error
    readonly suppressed: Error[] = []

    constructor(message: string | null, cause: Error | null, stackTrace: boolean) {
        super(message ?? undefined)
        this.name = new.target.name
        if (cause) {
            this.cause = cause
        }
        if (!stackTrace) {
            this.stack = `${this.name}: ${this.message}`
        }
    }

    addSuppressed(error: Error): void {
        this.suppressed.push(error)
    }
}
