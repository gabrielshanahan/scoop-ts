import { isoFromNowMillis, postgresMaxTime } from "../../../util/Clock.js"
import { CancellationToken } from "../../context/CancellationToken.js"
import { MappedKey } from "../../context/CooperationContext.js"
import { combineDeadlines, type DeadlineData } from "./Deadline.js"

/**
 * Deadline that applies only to normal (happy path) saga execution — it does NOT apply during
 * rollback, allowing cleanup to continue even if the original operation timed out.
 */
export class HappyPathDeadline
    extends CancellationToken<HappyPathDeadline>
    implements DeadlineData
{
    constructor(
        readonly deadline: string,
        readonly source: string,
        readonly trace: DeadlineData[] = [],
    ) {
        super(HappyPathDeadlineKey)
    }

    and(other: HappyPathDeadline): CancellationToken<HappyPathDeadline> {
        if (this.key !== other.key) {
            throw new Error(`Trying to mix together ${this.key.name} and ${other.key.name}`)
        }
        return combineDeadlines(
            this,
            other,
            (deadline, source, trace) => new HappyPathDeadline(deadline, source, trace),
        )
    }
}

/** Context key for happy path deadline tokens. */
export const HappyPathDeadlineKey = new MappedKey<HappyPathDeadline>(
    "HappyPathDeadlineKey",
    json => new HappyPathDeadline(json.deadline, json.source, json.trace ?? []),
)

/** Creates a happy path deadline [timeoutMillis] from now. */
export function happyPathTimeout(timeoutMillis: number, source: string): HappyPathDeadline {
    return new HappyPathDeadline(isoFromNowMillis(timeoutMillis), source)
}

/** Creates a happy path deadline that never expires. */
export function noHappyPathTimeout(source: string): HappyPathDeadline {
    return new HappyPathDeadline(postgresMaxTime, source)
}
