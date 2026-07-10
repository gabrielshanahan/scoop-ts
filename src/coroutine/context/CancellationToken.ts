import { CooperationContext, has, MappedElement, MappedKey } from "./CooperationContext.js"

/**
 * Base class for context elements that can cause saga cancellation (deadlines being the common
 * case). Combining two tokens of the same type merges them via [and] ("most restrictive wins");
 * different token types follow normal [CooperationContext] combining rules.
 *
 * This represents *automatic* cancellation based on system-defined criteria — completely separate
 * from user-initiated `CANCELLATION_REQUESTED` message events.
 */
export abstract class CancellationToken<
    SELF extends CancellationToken<SELF>,
> extends MappedElement {
    constructor(key: MappedKey<any>) {
        super(key)
    }

    override plus(context: CooperationContext): CooperationContext {
        if (context instanceof CancellationToken && has(context, this.key)) {
            return this.and(context as SELF)
        }
        return super.plus(context)
    }

    abstract and(other: SELF): CancellationToken<SELF>
}
