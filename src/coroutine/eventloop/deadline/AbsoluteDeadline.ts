import { isoFromNowMillis, postgresMaxTime } from "../../../util/Clock.js"
import { CancellationToken } from "../../context/CancellationToken.js"
import { MappedKey } from "../../context/CooperationContext.js"
import { combineDeadlines, type DeadlineData } from "./Deadline.js"

/**
 * Deadline that applies to the entire saga lifecycle regardless of execution phase — normal
 * execution and rollback alike.
 */
export class AbsoluteDeadline extends CancellationToken<AbsoluteDeadline> implements DeadlineData {
    constructor(
        readonly deadline: string,
        readonly source: string,
        readonly trace: DeadlineData[] = [],
    ) {
        super(AbsoluteDeadlineKey)
    }

    and(other: AbsoluteDeadline): CancellationToken<AbsoluteDeadline> {
        if (this.key !== other.key) {
            throw new Error(`Trying to mix together ${this.key.name} and ${other.key.name}`)
        }
        return combineDeadlines(
            this,
            other,
            (deadline, source, trace) => new AbsoluteDeadline(deadline, source, trace),
        )
    }
}

/** Context key for absolute deadline tokens. */
export const AbsoluteDeadlineKey = new MappedKey<AbsoluteDeadline>(
    "AbsoluteDeadlineKey",
    json => new AbsoluteDeadline(json.deadline, json.source, json.trace ?? []),
)

/** Creates an absolute deadline [timeoutMillis] from now. */
export function absoluteTimeout(timeoutMillis: number, source: string): AbsoluteDeadline {
    return new AbsoluteDeadline(isoFromNowMillis(timeoutMillis), source)
}

/** Creates an absolute deadline that never expires. */
export function noAbsoluteTimeout(source: string): AbsoluteDeadline {
    return new AbsoluteDeadline(postgresMaxTime, source)
}
