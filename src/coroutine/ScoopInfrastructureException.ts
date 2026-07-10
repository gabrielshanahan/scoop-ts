import { ScoopException } from "./ScoopException.js"
import { ReturnValueAlreadyExistsException } from "./structuredcooperation/ReturnValueAlreadyExistsException.js"

/**
 * Thrown when one of Scoop's OWN persistence operations fails — a query Scoop runs for its own
 * bookkeeping — as opposed to anything the saga's own step code does. The defining property is
 * that the failure originates outside the code that defines the saga, so it carries no
 * information about whether the saga's business action should be compensated — and must therefore
 * NEVER drive a rollback. The event loop treats it as a transient tick failure: the current
 * tick's transaction is rolled back and the saga is re-resumed (retried) on a later tick.
 */
export class ScoopInfrastructureException extends ScoopException {
    constructor(cause: Error) {
        super(
            "A Scoop bookkeeping operation failed (most often a dead database connection). This " +
                "is not a saga-logic failure and must not trigger rollback.",
            cause,
            false,
        )
    }
}

/**
 * Runs [block] — a Scoop-internal persistence operation — re-classifying any failure that is NOT
 * a deliberate logical signal as a [ScoopInfrastructureException], so the event loop retries the
 * tick instead of rolling the saga back.
 *
 * Pass-throughs (NOT wrapped): any [ScoopException] (deliberate control signals), and
 * [ReturnValueAlreadyExistsException] (a logical uniqueness outcome callers branch on).
 */
export async function asScoopInfrastructure<T>(block: () => Promise<T>): Promise<T> {
    try {
        return await block()
    } catch (e) {
        if (e instanceof ScoopException || e instanceof ReturnValueAlreadyExistsException) {
            throw e
        }
        throw new ScoopInfrastructureException(e as Error)
    }
}
