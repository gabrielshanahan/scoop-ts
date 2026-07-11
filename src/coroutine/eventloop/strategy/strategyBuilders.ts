/**
 * SQL builder functions implementing common patterns for EventLoopStrategy conditions. These
 * generate raw SQL fragments injected into the readiness queries.
 */

export function ignoreHierarchiesOlderThan(ignoreOlderThan: string): string {
    return `EXISTS (
    SELECT 1
    FROM message_event root_emission
    WHERE root_emission.type = 'EMITTED'
        AND cardinality(root_emission.cooperation_lineage) = 1
        AND root_emission.cooperation_lineage[1] = emitted.cooperation_lineage[1]
        AND root_emission.created_at >= '${ignoreOlderThan}'
)`
}

/**
 * SQL detecting whether cancellation has been requested for a saga (including in parent runs);
 * returns the cancellation exception details when so.
 */
export function cancellationRequested(seen: string): string {
    return `SELECT cancellation_request.exception::jsonb
    FROM message_event cancellation_request
    JOIN ${seen} ON cancellation_request.cooperation_lineage <@ ${seen}.cooperation_lineage
    AND cancellation_request.type = 'CANCELLATION_REQUESTED'`
}

/** Happy path deadlines are checked against the most recent SUSPENDED or SEEN event. */
export function happyPathDeadlineMissed(seen: string): string {
    return deadlineMissed(seen, "happy path", ["SUSPENDED", "SEEN"])
}

/** Rollback deadlines are checked against the most recent SUSPENDED or ROLLING_BACK event. */
export function rollbackDeadlineMissed(seen: string): string {
    return deadlineMissed(seen, "rollback", ["SUSPENDED", "ROLLING_BACK"])
}

/** Absolute deadlines apply to the whole lifecycle: SEEN, SUSPENDED, or ROLLING_BACK events. */
export function absoluteDeadlineMissed(seen: string): string {
    return deadlineMissed(seen, "absolute", ["SEEN", "SUSPENDED", "ROLLING_BACK"])
}

/**
 * Core deadline checking SQL: compares the deadline stored in the saga's CooperationContext
 * against the current time, returning a constructed CooperationFailure when missed.
 *
 * The deadline/key names are derived exactly as in the original ("rollback" yields
 * 'RollbackDeadlineKey', which is the name the rollback deadline element serializes under —
 * see RollbackPathDeadline.ts).
 */
export function deadlineMissed(seen: string, deadlineType: string, eventTypes: string[]): string {
    const deadline = `${capitalizeWords(deadlineType).split(" ").join("")}Deadline`
    const deadlineKey = `${deadline}Key`
    const eventTypesList = `(${eventTypes.map(t => `'${t}'`).join(", ")})`

    return `SELECT jsonb_build_object(
        'message', 'Missed ${deadlineType} deadline of ' ||
            (deadline_record.context->'${deadlineKey}'->>'source') ||
            ' at ' ||
            (deadline_record.context->'${deadlineKey}'->>'deadline') ||
            '. Deadline trace: ' ||
            (COALESCE(to_jsonb(deadline_record.context->'${deadlineKey}'->'trace')::text, '[]')),
        'type', 'Missed${deadline}',
        'source', (deadline_record.context->'${deadlineKey}'->>'source'),
        'stackTrace', '[]'::jsonb,
        'causes', '[]'::jsonb
    ) as exception
    FROM (
        SELECT last_event.context
        FROM message_event last_event
        JOIN ${seen} ON last_event.cooperation_lineage = ${seen}.cooperation_lineage
        WHERE jsonb_exists_any_indexed(last_event.context, '${deadlineKey}')
        AND last_event.type IN ${eventTypesList}
        AND (last_event.context->'${deadlineKey}'->>'deadline')::timestamptz < CLOCK_TIMESTAMP()
        LIMIT 1
    ) AS deadline_record
    WHERE deadline_record.context IS NOT NULL`
}

function capitalizeWords(input: string): string {
    return input
        .split(" ")
        .map(word => (word === "" ? word : word[0]!.toUpperCase() + word.slice(1)))
        .join(" ")
}

/**
 * SQL verifying all expected handlers have started processing emissions — the heart of the dummy
 * "who is listening" solution built on HandlerRegistry. Implemented as "no missing SEENs exist"
 * over a VALUES table of all known (topic, handler) pairs.
 */
export function allEmissionsHaveCorrespondingContinuationStarts(
    topicsToHandlerNames: Map<string, string[]>,
    candidateSeen: string,
    emissionInLatestStep: string,
    emissionContinuationStart: string,
): string {
    const topicToHandlerPairs: Array<[string, string]> = []
    for (const [topic, handlers] of topicsToHandlerNames) {
        for (const handler of handlers) {
            topicToHandlerPairs.push([topic, handler])
        }
    }

    // If no handlers are registered, all emissions are trivially handled (avoids an empty VALUES)
    if (topicToHandlerPairs.length === 0) {
        return "TRUE"
    }

    const valuesClause = topicToHandlerPairs
        .map(([topic, handler]) => `('${topic}', '${handler}')`)
        .join(", ")

    return `NOT EXISTS (
    SELECT 1
        FROM ${emissionInLatestStep}
        LEFT JOIN ${emissionContinuationStart} ON ${emissionContinuationStart}.parent_cooperation_lineage = ${emissionInLatestStep}.cooperation_lineage
    WHERE
        ${emissionInLatestStep}.cooperation_lineage = ${candidateSeen}.cooperation_lineage
        AND
        EXISTS (
            SELECT 1
            FROM (VALUES ${valuesClause}) AS topic_handler(topic, handler)
            JOIN message ON topic_handler.topic = message.topic
            JOIN ${emissionInLatestStep} ON ${emissionInLatestStep}.message_id = message.id
            LEFT JOIN
                ${emissionContinuationStart} ON ${emissionContinuationStart}.message_id = ${emissionInLatestStep}.message_id
                AND ${emissionContinuationStart}.coroutine_name = topic_handler.handler
            WHERE ${emissionContinuationStart}.id is NULL
        )
)`
}
