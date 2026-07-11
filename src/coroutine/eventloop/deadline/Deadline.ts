import { compareTimestamps } from "../../../util/Clock.js"

/**
 * Common shape of deadline cancellation tokens. Timestamps are ISO-8601 strings (they serialize
 * directly into the context JSON, where the SQL casts them to timestamptz).
 *
 * When multiple deadlines of the same type are combined, the earliest deadline wins; the `trace`
 * maintains the complete history of all deadlines that were combined (with Set semantics —
 * structural dedup), revealing which component originally set the winning deadline.
 */
export interface DeadlineData {
    readonly deadline: string
    readonly source: string
    readonly trace: DeadlineData[]
}

type Create<T extends DeadlineData> = (deadline: string, source: string, trace: DeadlineData[]) => T

/**
 * Trace entries are REAL deadline instances (constructed via [create]) so they serialize in the
 * context-wrapped form, exactly like the Kotlin `withoutTrace()` which instantiates the concrete
 * deadline class.
 */
function withoutTrace<T extends DeadlineData>(deadline: DeadlineData, create: Create<T>): T {
    return create(deadline.deadline, deadline.source, [])
}

function asTrace<T extends DeadlineData>(
    deadline: DeadlineData,
    create: Create<T>,
): DeadlineData[] {
    return dedupe([withoutTrace(deadline, create), ...deadline.trace])
}

function structurallyEqual(a: DeadlineData, b: DeadlineData): boolean {
    return (
        a.deadline === b.deadline &&
        a.source === b.source &&
        a.trace.length === b.trace.length &&
        a.trace.every((t, i) => structurallyEqual(t, b.trace[i]!))
    )
}

function dedupe(items: DeadlineData[]): DeadlineData[] {
    const result: DeadlineData[] = []
    for (const item of items) {
        if (!result.some(existing => structurallyEqual(existing, item))) {
            result.push(item)
        }
    }
    return result
}

/**
 * Combines two deadlines by choosing the earlier (more restrictive) one, preserving the source of
 * the earlier deadline and a complete trace of all contributing deadlines.
 */
export function combineDeadlines<T extends DeadlineData>(self: T, other: T, create: Create<T>): T {
    const selfEarlier = compareTimestamps(self.deadline, other.deadline) <= 0
    const earlier = selfEarlier ? self : other
    const later = selfEarlier ? other : self
    return create(
        earlier.deadline,
        earlier.source,
        dedupe([...earlier.trace, ...asTrace(later, create)]),
    )
}
