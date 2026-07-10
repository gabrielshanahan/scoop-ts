import type { EventLoopStrategy } from "../eventloop/strategy/EventLoopStrategy.js"

/**
 * HIC SVNT DRACONES
 *
 * The SQL that implements structured cooperation's core logic for determining saga readiness —
 * the heart of Scoop. Ported verbatim from the Kotlin original (named parameters preserved; they
 * are converted to positional parameters at execution time — see sql.ts). The only textual
 * deviation is a `::text` cast on `suspended_at` so microsecond precision survives the round-trip
 * through the driver (see DECISIONS.md).
 */

/** A SQL query with optional CTE dependencies, assembled by [buildSql]. */
export interface SQL {
    cte: SQL | null
    name: string | null
    sql: string
}

export function appendAs(cte: SQL, name: string | null, sql: string): SQL {
    return { cte, name, sql }
}

/**
 * Finds all SEEN events for sagas that are not yet completed: happy-path execution, rolling back,
 * or rollback requested.
 */
export const candidateSeens: SQL = {
    cte: null,
    name: "candidate_seens",
    sql: `
        SELECT DISTINCT ON (seen.id)
            seen.id,
            seen.message_id,
            seen.cooperation_lineage,
            seen.context,
            emitted.created_at as emitted_at,
            rollback_emitted.created_at as rollback_emitted_at
        FROM message_event seen
        JOIN message_event AS emitted
            ON seen.message_id = emitted.message_id
                AND emitted.type = 'EMITTED'
        LEFT JOIN message_event AS rolling_back
            ON seen.message_id = rolling_back.message_id
                AND rolling_back.type = 'ROLLING_BACK'
                AND rolling_back.cooperation_lineage = seen.cooperation_lineage
        LEFT JOIN message_event AS rollback_emitted
            ON seen.message_id = rollback_emitted.message_id
                AND rollback_emitted.type = 'ROLLBACK_EMITTED'
        WHERE seen.coroutine_name = :coroutine_name
          AND seen.type = 'SEEN'
          AND (
          ((rollback_emitted.id IS NULL AND rolling_back.id IS NULL) AND
            NOT EXISTS (
                SELECT 1
                FROM message_event
                WHERE cooperation_lineage = seen.cooperation_lineage
                  AND type = 'COMMITTED'
            ))
            OR
            (
              (rollback_emitted.id IS NULL AND rolling_back.id IS NOT NULL)
              AND NOT EXISTS (
                  SELECT 1
                  FROM message_event
                  WHERE cooperation_lineage = seen.cooperation_lineage
                    AND type IN ('ROLLED_BACK', 'ROLLBACK_FAILED')
              )
            )
            OR
            (
              (rollback_emitted.id IS NOT NULL AND rolling_back.id IS NOT NULL)
              AND NOT EXISTS (
                  SELECT 1
                  FROM message_event
                  WHERE cooperation_lineage = seen.cooperation_lineage
                    AND type IN ('ROLLED_BACK', 'ROLLBACK_FAILED')
              )
            )
          )`,
}

/** The latest SUSPENDED event for each candidate saga. */
export const latestSuspended: SQL = appendAs(
    candidateSeens,
    "latest_suspended",
    `
        SELECT DISTINCT ON (message_event.message_id) message_event.cooperation_lineage, message_event.step, message_event.child_failure_handler_iteration, message_event.next_step, message_event.context, message_event.created_at
        FROM message_event
        JOIN candidate_seens ON message_event.cooperation_lineage = candidate_seens.cooperation_lineage
        WHERE message_event.type = 'SUSPENDED'
        ORDER BY message_event.message_id, message_event.created_at DESC`,
)

/** All messages emitted during the latest suspended step. */
export const childEmissionsInLatestStep: SQL = appendAs(
    latestSuspended,
    "child_emissions_in_latest_step",
    `
        SELECT emissions.*
        FROM message_event AS emissions
        JOIN latest_suspended
            ON emissions.cooperation_lineage = latest_suspended.cooperation_lineage
        WHERE emissions.type = 'EMITTED'
            AND emissions.created_at < latest_suspended.created_at
            AND NOT EXISTS (
                SELECT 1 FROM message_event mid
                WHERE mid.cooperation_lineage = emissions.cooperation_lineage
                  AND mid.type = 'SUSPENDED'
                  AND mid.created_at > emissions.created_at
                  AND mid.created_at < latest_suspended.created_at
            )`,
)

/** Child handler SEEN events for emissions from the latest step (direct children only). */
export const childSeens: SQL = appendAs(
    childEmissionsInLatestStep,
    "child_seens",
    `
        SELECT seen.*, child_emissions_in_latest_step.cooperation_lineage as parent_cooperation_lineage
        FROM message_event seen
        JOIN child_emissions_in_latest_step ON
            seen.message_id = child_emissions_in_latest_step.message_id
        WHERE seen.type = 'SEEN'
            AND seen.cooperation_lineage <> child_emissions_in_latest_step.cooperation_lineage
            AND child_emissions_in_latest_step.cooperation_lineage <@ seen.cooperation_lineage
            AND cardinality(seen.cooperation_lineage) = cardinality(child_emissions_in_latest_step.cooperation_lineage) + 1`,
)

/** Child handlers that have completed execution (reached a terminal state). */
export const terminatedChildSeens: SQL = appendAs(
    childSeens,
    "terminated_child_seens",
    `
        SELECT child_seens.*
        FROM message_event seen_terminations
        JOIN child_seens ON
            seen_terminations.message_id = child_seens.message_id
                AND seen_terminations.cooperation_lineage = child_seens.cooperation_lineage
        WHERE
            seen_terminations.type in ('COMMITTED', 'ROLLED_BACK', 'ROLLBACK_FAILED')`,
)

/** Rollback messages emitted during the latest suspended step. */
export const childRollbackEmissionsInLatestStep: SQL = appendAs(
    terminatedChildSeens,
    "child_rollback_emissions_in_latest_step",
    `
        SELECT rollback_emissions.*
        FROM message_event AS rollback_emissions
        JOIN latest_suspended
            ON rollback_emissions.cooperation_lineage = latest_suspended.cooperation_lineage
        WHERE rollback_emissions.type = 'ROLLBACK_EMITTED'
            AND rollback_emissions.step IS NOT NULL
            AND rollback_emissions.created_at < latest_suspended.created_at
            AND NOT EXISTS (
                SELECT 1 FROM message_event mid
                WHERE mid.cooperation_lineage = rollback_emissions.cooperation_lineage
                  AND mid.type = 'SUSPENDED'
                  AND mid.created_at > rollback_emissions.created_at
                  AND mid.created_at < latest_suspended.created_at
            )`,
)

/** Child handler ROLLING_BACK events for direct child operations. */
export const childRollingBacks: SQL = appendAs(
    childRollbackEmissionsInLatestStep,
    "child_rolling_backs",
    `
        SELECT
            rolling_backs.*,
            parent_seen.cooperation_lineage AS parent_cooperation_lineage
        FROM message_event rolling_backs
        JOIN candidate_seens AS parent_seen
            ON rolling_backs.cooperation_lineage <> parent_seen.cooperation_lineage
                AND parent_seen.cooperation_lineage <@ rolling_backs.cooperation_lineage
                AND cardinality(rolling_backs.cooperation_lineage) = cardinality(parent_seen.cooperation_lineage) + 1
        WHERE rolling_backs.type = 'ROLLING_BACK'
        ORDER BY rolling_backs.created_at`,
)

/** Child rollback handlers that have completed (ROLLED_BACK or ROLLBACK_FAILED). */
export const terminatedChildRollingBacks: SQL = appendAs(
    childRollingBacks,
    "terminated_child_rolling_backs",
    `
        SELECT child_rolling_backs.*
        FROM message_event rolling_back_terminations
        JOIN child_rolling_backs ON
            rolling_back_terminations.message_id = child_rolling_backs.message_id
                AND rolling_back_terminations.cooperation_lineage = child_rolling_backs.cooperation_lineage
        WHERE
            rolling_back_terminations.type in ('ROLLED_BACK', 'ROLLBACK_FAILED')`,
)

/**
 * Applies structured cooperation rules and [EventLoopStrategy] constraints to determine saga
 * readiness — the core logic implementing "all children finished before the parent resumes", on
 * both the happy path and the rollback path.
 */
export function candidateSeensWaitingToBeProcessed(eventLoopStrategy: EventLoopStrategy): SQL {
    return appendAs(
        terminatedChildRollingBacks,
        "candidate_seens_waiting_to_be_processed",
        `
                SELECT candidate_seens.*
                FROM candidate_seens
                WHERE
                    -- no rollback emissions present
                    (
                        NOT EXISTS ( -- no rollback emissions
                            SELECT 1
                            FROM child_rollback_emissions_in_latest_step
                            WHERE child_rollback_emissions_in_latest_step.cooperation_lineage = candidate_seens.cooperation_lineage
                        )
                        AND
                        (
                            (
                                -- strategy says resume
                                ${eventLoopStrategy.resumeHappyPath("candidate_seens", "child_emissions_in_latest_step", "child_seens")}
                                OR
                                -- strategy says give up
                                EXISTS(
                                    ${eventLoopStrategy.giveUpOnHappyPath("candidate_seens")}
                                )
                            )
                            AND
                                NOT EXISTS ( -- every SEEN has a counterpart in terminated_child_seens
                                    SELECT 1
                                    FROM child_seens
                                    LEFT JOIN terminated_child_seens ON child_seens.cooperation_lineage = terminated_child_seens.cooperation_lineage
                                    WHERE
                                        child_seens.parent_cooperation_lineage = candidate_seens.cooperation_lineage
                                            AND terminated_child_seens.cooperation_lineage IS NULL
                                )
                        )
                    )
                    OR
                    -- rollback emissions present
                    (
                        EXISTS ( -- rollback emissions present
                            SELECT 1
                            FROM child_rollback_emissions_in_latest_step
                            WHERE child_rollback_emissions_in_latest_step.cooperation_lineage = candidate_seens.cooperation_lineage
                        )
                        AND
                        (
                            (
                                -- strategy says resume
                                ${eventLoopStrategy.resumeRollbackPath("candidate_seens", "child_rollback_emissions_in_latest_step", "child_rolling_backs")}
                                OR
                                -- strategy says give up
                                EXISTS(
                                    ${eventLoopStrategy.giveUpOnRollbackPath("candidate_seens")}
                                )
                            )
                            AND
                                NOT EXISTS ( -- every ROLLING_BACK has a counterpart in terminated_child_rolling_backs
                                    SELECT
                                        1
                                    FROM child_rolling_backs
                                    LEFT JOIN terminated_child_rolling_backs ON child_rolling_backs.cooperation_lineage = terminated_child_rolling_backs.cooperation_lineage
                                    WHERE
                                        child_rolling_backs.parent_cooperation_lineage = candidate_seens.cooperation_lineage
                                            AND terminated_child_rolling_backs.cooperation_lineage IS NULL
                                )
                        )
                    )`,
    )
}

/**
 * Locks and selects one ready saga for processing (rollbacks ordered after normal emissions).
 * [secondRunAfterLock] belongs to the double-checked-locking pattern in
 * `MessageEventRepository.fetchPendingCoroutineRun`.
 */
export function seenForProcessing(
    eventLoopStrategy: EventLoopStrategy,
    secondRunAfterLock = false,
): SQL {
    if (!secondRunAfterLock) {
        return appendAs(
            candidateSeensWaitingToBeProcessed(eventLoopStrategy),
            "seen_for_processing",
            `
                SELECT candidate_seens_waiting_to_be_processed.cooperation_lineage, candidate_seens_waiting_to_be_processed.message_id, candidate_seens_waiting_to_be_processed.context
                FROM message_event
                JOIN candidate_seens_waiting_to_be_processed ON message_event.id = candidate_seens_waiting_to_be_processed.id
                -- We want to process things in the order they were emitted, and rollbacks always happen after emissions
                ORDER BY COALESCE(candidate_seens_waiting_to_be_processed.rollback_emitted_at, candidate_seens_waiting_to_be_processed.emitted_at), candidate_seens_waiting_to_be_processed.emitted_at
                FOR UPDATE SKIP LOCKED
                LIMIT 1`,
        )
    }
    // We need to leave out the FOR UPDATE SKIP LOCKED, since once a record is locked, it's
    // locked even for the transaction that locked it
    return appendAs(
        candidateSeensWaitingToBeProcessed(eventLoopStrategy),
        "seen_for_processing",
        `
                SELECT candidate_seens_waiting_to_be_processed.cooperation_lineage, candidate_seens_waiting_to_be_processed.message_id, candidate_seens_waiting_to_be_processed.context
                FROM candidate_seens_waiting_to_be_processed
                WHERE candidate_seens_waiting_to_be_processed.message_id = :message_id`,
    )
}

/**
 * The last two events, needed for correct cooperation context reconstruction when an externally
 * triggered rollback is just starting (last event ROLLING_BACK with a null step; the
 * second-to-last is then necessarily COMMITTED and carries the scope's own final context).
 */
export function lastTwoEvents(
    eventLoopStrategy: EventLoopStrategy,
    secondRunAfterLock = false,
): SQL {
    return appendAs(
        seenForProcessing(eventLoopStrategy, secondRunAfterLock),
        "last_two_events",
        `
            SELECT
                last_two_events.context,
                last_two_events.type,
                last_two_events.step
            FROM message_event last_two_events
            JOIN seen_for_processing ON seen_for_processing.cooperation_lineage = last_two_events.cooperation_lineage
            -- See the explanation above to understand why we select these particular types
            WHERE last_two_events.type IN ('SEEN', 'SUSPENDED', 'COMMITTED', 'ROLLING_BACK')
            ORDER BY last_two_events.created_at DESC
            LIMIT 2`,
    )
}

/** Gathers all data needed to build a continuation for the selected saga. */
export function finalSelect(eventLoopStrategy: EventLoopStrategy, secondRunAfterLock = false): SQL {
    return appendAs(
        lastTwoEvents(eventLoopStrategy, secondRunAfterLock),
        null,
        `
            SELECT
                message.*,
                seen_for_processing.cooperation_lineage,
                latest_suspended.step,
                latest_suspended.next_step,
                latest_suspended.child_failure_handler_iteration,
                latest_suspended.created_at::text as suspended_at,
                last_event.context as latest_context,
                CASE
                    -- See the explanation above to understand the logic behind this weird-looking selection
                    WHEN last_event.type = 'ROLLING_BACK' AND last_event.step IS NULL THEN second_to_last_event.context
                END AS latest_scope_context,
                (
                    SELECT
                        COALESCE(JSON_AGG(exception), '[]'::json)
                    FROM child_rolling_backs
                    JOIN seen_for_processing ON child_rolling_backs.parent_cooperation_lineage = seen_for_processing.cooperation_lineage
                ) AS child_rolled_back_exceptions,
                (
                    SELECT
                        COALESCE(JSON_AGG(termination_event.exception), '[]'::json)
                    FROM child_rolling_backs
                    JOIN message_event termination_event ON child_rolling_backs.cooperation_lineage = termination_event.cooperation_lineage
                    WHERE termination_event.type = 'ROLLBACK_FAILED'
                ) AS child_rollback_failed_exceptions,
                (
                    SELECT
                        exception
                    FROM message_event
                    JOIN seen_for_processing ON
                            message_event.cooperation_lineage = seen_for_processing.cooperation_lineage
                    WHERE message_event.type = 'ROLLING_BACK'
                      AND exception IS NOT NULL
                    LIMIT 1
                ) AS rolling_back_exception,
                (
                    SELECT COALESCE(
                        JSON_AGG(
                            JSON_BUILD_OBJECT(
                                'step', me.step,
                                'child_failure_handler_iteration', me.child_failure_handler_iteration,
                                'suspended_at', me.created_at
                            ) ORDER BY me.created_at DESC
                        ),
                        '[]'::json
                    )
                    FROM message_event me
                    JOIN seen_for_processing ON me.cooperation_lineage = seen_for_processing.cooperation_lineage
                    WHERE me.type = 'SUSPENDED'
                      AND me.step IS NOT NULL
                ) AS executed_step_instances
            FROM seen_for_processing
            LEFT JOIN latest_suspended ON seen_for_processing.cooperation_lineage = latest_suspended.cooperation_lineage
            JOIN (SELECT * FROM last_two_events LIMIT 1) last_event ON TRUE
            LEFT JOIN (SELECT * FROM last_two_events OFFSET 1 LIMIT 1) second_to_last_event ON TRUE
            JOIN message ON seen_for_processing.message_id = message.id`,
    )
}

/** Utility to add a comma to non-empty strings for CTE chaining. */
export function commatize(str: string): string {
    return str.trim() !== "" ? `${str},` : str
}

/** Utility to add the WITH keyword to non-empty CTE strings. */
export function withWITH(str: string): string {
    return str.trim() !== "" ? `WITH ${str}` : str
}

/** Recursively builds a CTE chain from nested SQL objects. */
export function asCTE(sql: SQL | null): string {
    if (sql === null) {
        return ""
    }
    return `${commatize(asCTE(sql.cte))}
${sql.name} AS (
    ${sql.sql}
)`
}

/** Builds the final SQL query from a chain of CTEs. */
export function buildSql(sql: SQL): string {
    return `${withWITH(asCTE(sql.cte))}
${sql.sql};`
}
