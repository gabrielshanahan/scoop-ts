import { isoFromNowMillis, postgresMaxTime } from "../../../util/Clock.js"
import { CancellationToken } from "../../context/CancellationToken.js"
import { MappedKey } from "../../context/CooperationContext.js"
import { combineDeadlines, type DeadlineData } from "./Deadline.js"

/**
 * Deadline that applies only to rollback (compensating action) execution — it does NOT apply
 * during normal execution.
 */
export class RollbackPathDeadline
    extends CancellationToken<RollbackPathDeadline>
    implements DeadlineData
{
    constructor(
        readonly deadline: string,
        readonly source: string,
        readonly trace: DeadlineData[] = [],
    ) {
        super(RollbackDeadlineKey)
    }

    and(other: RollbackPathDeadline): CancellationToken<RollbackPathDeadline> {
        if (this.key !== other.key) {
            throw new Error(`Trying to mix together ${this.key.name} and ${other.key.name}`)
        }
        return combineDeadlines(
            this,
            other,
            (deadline, source, trace) => new RollbackPathDeadline(deadline, source, trace),
        )
    }
}

/**
 * Context key for rollback path deadline tokens. Serializes as "RollbackDeadlineKey" — the name
 * the give-up SQL (strategyBuilders' `deadlineMissed(…, "rollback", …)`) and the V2 partial
 * index check. The Kotlin original serialized under the class simple name
 * `RollbackPathDeadlineKey`, which the SQL never matched, so rollback-path deadlines could
 * never fire; both repos now use the SQL's name (see DECISIONS.md).
 */
export const RollbackDeadlineKey = new MappedKey<RollbackPathDeadline>(
    "RollbackDeadlineKey",
    json => new RollbackPathDeadline(json.deadline, json.source, json.trace ?? []),
)

/** Creates a rollback path deadline [timeoutMillis] from now. */
export function rollbackPathTimeout(timeoutMillis: number, source: string): RollbackPathDeadline {
    return new RollbackPathDeadline(isoFromNowMillis(timeoutMillis), source)
}

/** Creates a rollback deadline that never expires. */
export function noRollbackPathTimeout(source: string): RollbackPathDeadline {
    return new RollbackPathDeadline(postgresMaxTime, source)
}
