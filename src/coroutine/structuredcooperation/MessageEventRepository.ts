import type { JsonbHelper, JsonValue } from "../../JsonbHelper.js"
import { logger } from "../../logging.js"
import { DbConnection, queryNamed, uuidArrayLiteral } from "../../sql.js"
import { CooperationContext, isNotEmpty } from "../context/CooperationContext.js"
import { asScoopInfrastructure } from "../ScoopInfrastructureException.js"
import type { EventLoopStrategy } from "../eventloop/strategy/EventLoopStrategy.js"
import {
    CooperationException,
    CooperationFailure,
    toCooperationException,
} from "./CooperationFailure.js"
import { buildSql, finalSelect } from "./PendingCoroutineRunSql.js"

const log = logger("MessageEventRepository")

/**
 * Repository for the `message_event` log — the append-only table that makes structured
 * cooperation possible. Ported 1:1 from the Kotlin original; SQL preserved verbatim (named
 * parameters are converted to positional at execution time), with one addition: every INSERT
 * that writes a row carrying a cooperation lineage sets `created_at` to
 * GREATEST(CLOCK_TIMESTAMP(), max(created_at of that lineage) + 1 microsecond) instead of
 * relying on the column default. The step-window logic in PendingCoroutineRunSql compares
 * same-lineage events with strict `<` on `created_at`; two inserts in one tick transaction
 * (an emission followed by its SUSPENDED mark) can land in the same microsecond, which makes
 * the emission vanish from the "emitted in latest step" CTEs and lets the parent resume
 * without waiting for its children — a structured-cooperation invariant violation (present in
 * the Kotlin original; reproduced and verified mechanically, see DECISIONS.md). Making
 * `created_at` strictly increasing per lineage removes the tie at the source while keeping
 * the readiness SQL verbatim.
 */
export class MessageEventRepository {
    constructor(private readonly jsonbHelper: JsonbHelper) {}

    private contextParam(context: CooperationContext | null | undefined): unknown {
        if (!context || !isNotEmpty(context)) {
            return null
        }
        return this.jsonbHelper.toJsonbParam(context)
    }

    /** Records that a message was emitted on the global scope (a new cooperation root). */
    async insertGlobalEmittedEvent(
        connection: DbConnection,
        messageId: string,
        cooperationLineage: string[],
        context: CooperationContext | null,
    ): Promise<void> {
        await asScoopInfrastructure(async () => {
            log.debug({ messageId }, "Inserting global EMITTED event")
            await queryNamed(
                connection,
                `--${messageId} || ${cooperationLineage.join(",")}
                INSERT INTO message_event (message_id, type, cooperation_lineage, context, created_at)
                VALUES (:messageId, 'EMITTED', :cooperationLineage::uuid[], :context::jsonb, GREATEST(CLOCK_TIMESTAMP(), COALESCE((SELECT max(created_at) FROM message_event WHERE cooperation_lineage = :cooperationLineage::uuid[]) + INTERVAL '1 microsecond', CLOCK_TIMESTAMP())))`,
                {
                    messageId,
                    cooperationLineage: uuidArrayLiteral(cooperationLineage),
                    context: this.contextParam(context),
                },
            )
        })
    }

    /** Records that a message was emitted within a cooperation scope (extending the lineage). */
    async insertScopedEmittedEvent(
        connection: DbConnection,
        messageId: string,
        coroutineName: string,
        coroutineIdentifier: string,
        stepName: string | null,
        cooperationLineage: string[],
        context: CooperationContext | null,
    ): Promise<void> {
        await asScoopInfrastructure(async () => {
            log.debug(
                { messageId, coroutineName, stepName },
                "Inserting scoped EMITTED event",
            )
            await queryNamed(
                connection,
                `INSERT INTO message_event (message_id, type, coroutine_name, coroutine_identifier, step, cooperation_lineage, context, created_at)
                VALUES (:messageId, 'EMITTED', :coroutineName, :coroutineIdentifier, :stepName, :cooperationLineage::uuid[], :context::jsonb, GREATEST(CLOCK_TIMESTAMP(), COALESCE((SELECT max(created_at) FROM message_event WHERE cooperation_lineage = :cooperationLineage::uuid[]) + INTERVAL '1 microsecond', CLOCK_TIMESTAMP())))`,
                {
                    messageId,
                    coroutineName,
                    coroutineIdentifier,
                    stepName,
                    cooperationLineage: uuidArrayLiteral(cooperationLineage),
                    context: this.contextParam(context),
                },
            )
        })
    }

    /**
     * Records that a rollback request has been emitted for a cooperation lineage — but only when
     * no part of the cooperation tree (ancestors or descendants) is still actively running.
     */
    async insertRollbackEmittedEvent(
        connection: DbConnection,
        cooperationLineage: string[],
        exception: CooperationFailure,
    ): Promise<void> {
        await asScoopInfrastructure(async () => {
            log.debug("Inserting ROLLBACK_EMITTED event for lineage")
            await queryNamed(
                connection,
                `WITH message_id_lookup AS (
                    -- Find the message_id associated with this cooperation lineage
                    -- Any event from the same lineage will have the same message_id
                    SELECT DISTINCT message_id
                    FROM message_event
                    WHERE cooperation_lineage = :cooperationLineage::uuid[]
                    LIMIT 1
                )
                INSERT INTO message_event (
                    message_id,
                    type,
                    cooperation_lineage,
                    exception,
                    created_at
                )
                SELECT
                    message_id_lookup.message_id, 'ROLLBACK_EMITTED', :cooperationLineage::uuid[], :exception::jsonb, GREATEST(CLOCK_TIMESTAMP(), COALESCE((SELECT max(created_at) FROM message_event WHERE cooperation_lineage = :cooperationLineage::uuid[]) + INTERVAL '1 microsecond', CLOCK_TIMESTAMP()))
                FROM message_id_lookup
                WHERE NOT EXISTS (
                    -- Safety check: Only emit rollback if no part of the cooperation tree is actively running
                    -- This includes both ancestors (parents) and descendants (children) of the scope being rolled back.
                    -- Checking for COMMITTED and ROLLBACK_FAILED is enough because if something
                    -- rolled back, then everything must've rolled back, and there's no point in
                    -- emitting anything here
                    SELECT 1
                    FROM message_event seen
                    LEFT JOIN message_event terminated
                      ON terminated.cooperation_lineage = seen.cooperation_lineage
                         AND terminated.type in ('COMMITTED', 'ROLLBACK_FAILED')
                    WHERE seen.type = 'SEEN'                                    -- Handler started
                      AND (
                        -- Descendant check: our lineage is a prefix of their lineage (exclude self)
                        (:cooperationLineage::uuid[] <@ seen.cooperation_lineage AND cardinality(seen.cooperation_lineage) > cardinality(:cooperationLineage::uuid[]))
                        OR
                        -- Ancestor check: their lineage is a prefix of our lineage (exclude self)
                        (seen.cooperation_lineage <@ :cooperationLineage::uuid[] AND cardinality(seen.cooperation_lineage) < cardinality(:cooperationLineage::uuid[]))
                      )
                      AND terminated.cooperation_lineage IS NULL                -- But hasn't successfully completed yet
                )
                -- Prevent duplicate rollback events for the same message
                ON CONFLICT (message_id, type) WHERE type = 'ROLLBACK_EMITTED' DO NOTHING`,
                {
                    cooperationLineage: uuidArrayLiteral(cooperationLineage),
                    exception: this.jsonbHelper.toJsonbParam(exception),
                },
            )
        })
    }

    /** Records a user-initiated cancellation request for a cooperation lineage. */
    async insertCancellationRequestedEvent(
        connection: DbConnection,
        cooperationLineage: string[],
        exception: CooperationFailure,
    ): Promise<void> {
        await asScoopInfrastructure(async () => {
            log.debug("Inserting CANCELLATION_REQUESTED event")
            await queryNamed(
                connection,
                `WITH message_id_lookup AS (
                    SELECT DISTINCT message_id
                    FROM message_event
                    WHERE cooperation_lineage = :cooperationLineage::uuid[]
                    LIMIT 1
                )
                INSERT INTO message_event (
                    message_id,
                    type,
                    coroutine_name, coroutine_identifier, step,
                    cooperation_lineage,
                    exception,
                    created_at
                )
                SELECT
                    message_id_lookup.message_id,
                    'CANCELLATION_REQUESTED',
                    null, null, null,
                    :cooperationLineage::uuid[],
                    :exception::jsonb,
                    GREATEST(CLOCK_TIMESTAMP(), COALESCE((SELECT max(created_at) FROM message_event WHERE cooperation_lineage = :cooperationLineage::uuid[]) + INTERVAL '1 microsecond', CLOCK_TIMESTAMP()))
                FROM message_id_lookup
                ON CONFLICT (cooperation_lineage, type) WHERE type = 'CANCELLATION_REQUESTED' DO NOTHING`,
                {
                    cooperationLineage: uuidArrayLiteral(cooperationLineage),
                    exception: this.jsonbHelper.toJsonbParam(exception),
                },
            )
        })
    }

    async insertMessageEvent(
        connection: DbConnection,
        messageId: string,
        messageEventType: string,
        coroutineName: string,
        coroutineIdentifier: string,
        stepName: string | null,
        cooperationLineage: string[],
        exception: CooperationFailure | null,
        context: CooperationContext | null,
        childFailureHandlerIteration: number | null,
        nextStep: number | null,
    ): Promise<void> {
        await asScoopInfrastructure(async () => {
            await queryNamed(
                connection,
                `INSERT INTO message_event (message_id, type, coroutine_name, coroutine_identifier, step, cooperation_lineage, exception, context, child_failure_handler_iteration, next_step, created_at)
                VALUES (:messageId, :messageEventType::message_event_type, :coroutineName, :coroutineIdentifier, :stepName, :cooperationLineage::uuid[], :exception::jsonb, :context::jsonb, :childFailureHandlerIteration, :nextStep, GREATEST(CLOCK_TIMESTAMP(), COALESCE((SELECT max(created_at) FROM message_event WHERE cooperation_lineage = :cooperationLineage::uuid[]) + INTERVAL '1 microsecond', CLOCK_TIMESTAMP())))`,
                {
                    messageId,
                    messageEventType,
                    coroutineName,
                    coroutineIdentifier,
                    stepName,
                    cooperationLineage: uuidArrayLiteral(cooperationLineage),
                    exception: exception ? this.jsonbHelper.toJsonbParam(exception) : null,
                    context: this.contextParam(context),
                    childFailureHandlerIteration,
                    nextStep,
                },
            )
        })
    }

    /**
     * Records rollback requests for all messages emitted in a specific tick, identified by the
     * SUSPENDED event's timestamp (passed back verbatim as text to preserve precision).
     */
    async insertRollbackEmittedEventsForStep(
        connection: DbConnection,
        cooperationLineage: string[],
        coroutineName: string,
        coroutineIdentifier: string,
        scopeStepName: string | null,
        exception: CooperationFailure,
        context: CooperationContext | null,
        suspendedAt: string,
    ): Promise<void> {
        await asScoopInfrastructure(async () => {
            await queryNamed(
                connection,
                `WITH emitted_record AS (
                    SELECT cooperation_lineage, message_id
                    FROM message_event
                    WHERE type = 'EMITTED'
                        AND cooperation_lineage = :cooperationLineage::uuid[]
                        AND created_at < :suspendedAt::timestamptz
                        AND NOT EXISTS (
                            SELECT 1 FROM message_event mid
                            WHERE mid.cooperation_lineage = :cooperationLineage::uuid[]
                              AND mid.type = 'SUSPENDED'
                              AND mid.created_at > message_event.created_at
                              AND mid.created_at < :suspendedAt::timestamptz
                        )
                )
                INSERT INTO message_event (
                    message_id, type,
                    cooperation_lineage, coroutine_name, coroutine_identifier, step,
                    exception, context, created_at
                )
                SELECT
                    message_id, 'ROLLBACK_EMITTED',
                    :cooperationLineage::uuid[], :coroutineName, :coroutineIdentifier, :scopeStepName, :exception::jsonb, :context::jsonb, GREATEST(CLOCK_TIMESTAMP(), COALESCE((SELECT max(created_at) FROM message_event WHERE cooperation_lineage = :cooperationLineage::uuid[]) + INTERVAL '1 microsecond', CLOCK_TIMESTAMP()))
                FROM emitted_record`,
                {
                    cooperationLineage: uuidArrayLiteral(cooperationLineage),
                    coroutineName,
                    coroutineIdentifier,
                    scopeStepName,
                    exception: this.jsonbHelper.toJsonbParam(exception),
                    context: this.contextParam(context),
                    suspendedAt,
                },
            )
        })
    }

    /**
     * Fetches the exceptions produced by the strategy's give-up SQL for the given scope, if any.
     * The [giveUpSqlProvider] receives the alias of the CTE containing the scope's SEEN event.
     */
    async fetchGiveUpExceptions(
        connection: DbConnection,
        giveUpSqlProvider: (seenAlias: string) => string,
        cooperationLineage: string[],
    ): Promise<CooperationException[]> {
        return asScoopInfrastructure(async () => {
            const rows = await queryNamed(
                connection,
                `WITH seen AS (
                    SELECT * FROM message_event WHERE cooperation_lineage = :cooperationLineage::uuid[] AND type = 'SEEN'
                )
                ${giveUpSqlProvider("seen")}`,
                { cooperationLineage: uuidArrayLiteral(cooperationLineage) },
            )
            return rows
                .map(row => row.exception)
                .filter(exception => exception !== null && exception !== undefined)
                .map(exception =>
                    toCooperationException(this.jsonbHelper.fromJsonb<CooperationFailure>(exception)),
                )
        })
    }

    /**
     * Creates SEEN and ROLLING_BACK events for a given coroutine, effectively starting new
     * continuations. Uses FOR UPDATE SKIP LOCKED on the parent SEEN plus a last-parent-event
     * SUSPENDED check to prevent the child-starts-while-parent-rolls-back race. Returns how many
     * rows were inserted (consumed by the ReconcileGate's drain logic).
     */
    async startContinuationsForCoroutine(
        connection: DbConnection,
        coroutineName: string,
        coroutineIdentifier: string,
        topic: string,
        eventLoopStrategy: EventLoopStrategy,
    ): Promise<number> {
        log.debug({ coroutineName, topic }, "Starting continuations")
        const sql = `
                WITH
                -- Find EMITTED records missing a SEEN
                emitted_missing_seen AS (
                    SELECT emitted.message_id, emitted.cooperation_lineage, emitted.context
                    FROM message_event emitted
                    LEFT JOIN message_event AS coroutine_seen
                        ON coroutine_seen.message_id = emitted.message_id AND coroutine_seen.type = 'SEEN' AND coroutine_seen.coroutine_name = :coroutine_name
                    JOIN message
                        ON message.id = emitted.message_id AND message.topic = :topic
                    LEFT JOIN LATERAL (
                        -- First, check if a parent SEEN record exists at all
                        SELECT id, cooperation_lineage
                        FROM message_event seen
                        WHERE seen.type = 'SEEN' AND seen.cooperation_lineage = emitted.cooperation_lineage
                    ) parent_seen_exists ON parent_seen_exists.cooperation_lineage = emitted.cooperation_lineage
                    LEFT JOIN LATERAL (
                        -- Then try to lock it if it exists
                        SELECT 1 as locked
                        FROM message_event seen_lock
                        WHERE seen_lock.id = parent_seen_exists.id
                        FOR UPDATE SKIP LOCKED
                    ) parent_seen_lock_attempt ON parent_seen_exists.id IS NOT NULL
                    LEFT JOIN LATERAL (
                        -- Get the last event in parent sequence - validate it's a SUSPENDED
                        -- and that this EMITTED belongs to its tick (no intervening SUSPENDED)
                        SELECT type, created_at
                        FROM message_event last_event
                        WHERE last_event.cooperation_lineage = parent_seen_exists.cooperation_lineage
                        ORDER BY last_event.created_at DESC
                        LIMIT 1
                    ) last_parent_event ON parent_seen_exists.id IS NOT NULL
                    WHERE emitted.type = 'EMITTED'
                        AND coroutine_seen.id IS NULL
                        AND (
                            -- Either no SEEN record exists (i.e., this is a toplevel emission, and there's nothing to lock)
                            parent_seen_exists.id IS NULL
                            -- OR the SEEN record exists AND we successfully locked it AND the parent sequence's last event is SUSPENDED
                            -- and this EMITTED belongs to the latest tick (no SUSPENDED between this EMITTED and the latest SUSPENDED)
                            OR (
                                parent_seen_exists.id IS NOT NULL
                                AND parent_seen_lock_attempt.locked IS NOT NULL
                                AND last_parent_event.type = 'SUSPENDED'
                                AND NOT EXISTS (
                                    SELECT 1 FROM message_event mid
                                    WHERE mid.cooperation_lineage = parent_seen_exists.cooperation_lineage
                                      AND mid.type = 'SUSPENDED'
                                      AND mid.created_at > emitted.created_at
                                      AND mid.created_at < last_parent_event.created_at
                                )
                            )
                        )
                        -- EventLoopStrategy says we're good to go
                        AND (${eventLoopStrategy.start("emitted")})
                ),
                -- Find ROLLBACK_EMITTED records missing a ROLLING_BACK
                rollback_emitted_missing_rolling_back AS (
                    SELECT rollback_emitted.message_id, coroutine_seen.cooperation_lineage, rollback_emitted.exception, rollback_emitted.context, rollback_emitted.step
                    FROM message_event rollback_emitted
                    LEFT JOIN message_event AS rolling_back_check
                        ON rolling_back_check.message_id = rollback_emitted.message_id AND rolling_back_check.type = 'ROLLING_BACK' AND rolling_back_check.coroutine_name = :coroutine_name
                    JOIN message
                        ON message.id = rollback_emitted.message_id AND message.topic = :topic
                    JOIN message_event AS coroutine_seen
                         ON coroutine_seen.message_id = rollback_emitted.message_id AND coroutine_seen.type = 'SEEN' AND coroutine_seen.coroutine_name = :coroutine_name
                    LEFT JOIN LATERAL (
                        -- First, check if a parent SEEN record exists at all
                        SELECT id, cooperation_lineage
                        FROM message_event seen
                        WHERE seen.type = 'SEEN' AND seen.cooperation_lineage = rollback_emitted.cooperation_lineage
                    ) parent_seen_exists ON parent_seen_exists.cooperation_lineage = rollback_emitted.cooperation_lineage
                    LEFT JOIN LATERAL (
                        -- Then try to lock it if it exists
                        SELECT 1 as locked
                        FROM message_event seen_lock
                        WHERE seen_lock.id = parent_seen_exists.id
                        FOR UPDATE SKIP LOCKED
                    ) parent_seen_lock_attempt ON parent_seen_exists.id IS NOT NULL
                    WHERE rollback_emitted.type = 'ROLLBACK_EMITTED'
                        AND rolling_back_check.id IS NULL
                        AND (
                                -- Either no SEEN record exists (i.e., this is a toplevel emission, and there's nothing to lock)
                                parent_seen_exists.id IS NULL
                                -- OR the SEEN record exists AND we successfully locked it
                                -- we don't do require that last_parent_event be a SUSPENDED, like we do when dealing with SEEN above,
                                -- because we want to allow partial rollbacks of subtrees (even though it's dangerous)
                                OR (
                                    parent_seen_exists.id IS NOT NULL
                                    AND parent_seen_lock_attempt.locked IS NOT NULL
                                )
                            )
                ),
                -- Insert SEEN if EMITTED exists without SEEN
                seen_insert AS (
                    INSERT INTO message_event (
                        message_id, type,
                        coroutine_name, coroutine_identifier,
                        cooperation_lineage,
                        context
                    )
                    SELECT
                        emitted_missing_seen.message_id,
                        'SEEN',
                        :coroutine_name,
                        :coroutine_identifier,
                        emitted_missing_seen.cooperation_lineage || gen_uuid_v7(), -- append additional cooperation id
                        emitted_missing_seen.context
                    FROM emitted_missing_seen
                    ON CONFLICT (coroutine_name, message_id, type) WHERE type = 'SEEN' DO NOTHING
                    RETURNING id
                ),
                -- Insert ROLLING_BACK if ROLLBACK_EMITTED exists without ROLLING_BACK
                rolling_back_insert AS (
                    INSERT INTO message_event (
                        message_id, type,
                        coroutine_name, coroutine_identifier,
                        cooperation_lineage,
                        exception,
                        context
                    )
                    SELECT
                        rollback_emitted_missing_rolling_back.message_id,
                        'ROLLING_BACK',
                        :coroutine_name,
                        :coroutine_identifier,
                        rollback_emitted_missing_rolling_back.cooperation_lineage,
                        rollback_emitted_missing_rolling_back.exception,
                        rollback_emitted_missing_rolling_back.context
                    FROM rollback_emitted_missing_rolling_back
                    ON CONFLICT (coroutine_name, message_id, type) WHERE type = 'ROLLING_BACK' DO NOTHING
                    RETURNING id
                )
                -- Report how many rows were actually inserted. The caller's reconciliation gate
                -- uses this to keep reconciling across ticks until a pass drains (inserts nothing).
                SELECT
                    (SELECT count(*) FROM seen_insert)
                    + (SELECT count(*) FROM rolling_back_insert) AS inserted;`

        const rows = await queryNamed(connection, sql, {
            coroutine_name: coroutineName,
            coroutine_identifier: coroutineIdentifier,
            topic,
        })
        return Number(rows[0]!.inserted)
    }

    /**
     * Finds and locks a saga execution that is ready to be resumed, using double-checked locking:
     * FOR UPDATE SKIP LOCKED evaluates lock state at execution time (not transaction start), so a
     * concurrently-committing handler can release its lock between our readiness evaluation and
     * our lock acquisition — leaving us with stale data. Rerunning the selection after acquiring
     * the lock closes that window.
     */
    async fetchPendingCoroutineRun(
        connection: DbConnection,
        coroutineName: string,
        eventLoopStrategy: EventLoopStrategy,
    ): Promise<PendingCoroutineRun | null> {
        const firstRows = await queryNamed(
            connection,
            `--${coroutineName}\n${buildSql(finalSelect(eventLoopStrategy))}`,
            { coroutine_name: coroutineName },
        )
        const firstResult = firstRows[0]?.id as string | undefined
        if (firstResult === undefined) {
            // Nothing to do -> we're done
            return null
        }
        const rows = await queryNamed(
            connection,
            buildSql(finalSelect(eventLoopStrategy, true)),
            { coroutine_name: coroutineName, message_id: firstResult },
        )
        const row = rows[0]
        if (row === undefined) {
            // After the second fetch, the record is no longer ready for processing (i.e., the
            // race condition described above happened)
            return null
        }
        return {
            messageId: row.id as string,
            topic: row.topic as string,
            payload: this.jsonbHelper.fromJsonb(row.payload),
            createdAt: row.created_at as Date,
            cooperationLineage: row.cooperation_lineage as string[],
            step: (row.step as string | null) ?? null,
            nextStep: (row.next_step as number | null) ?? null,
            suspendedAt: (row.suspended_at as string | null) ?? null,
            childFailureHandlerIteration:
                (row.child_failure_handler_iteration as number | null) ?? null,
            latestScopeContext: row.latest_scope_context ?? null,
            latestContext: row.latest_context ?? null,
            childRolledBackExceptions: row.child_rolled_back_exceptions,
            childRollbackFailedExceptions: row.child_rollback_failed_exceptions,
            rollingBackException: row.rolling_back_exception ?? null,
            executedStepInstances: row.executed_step_instances,
        }
    }
}

/**
 * A saga execution that is ready to be resumed — everything needed to build a Continuation.
 * JSONB values arrive as parsed JSON; `suspendedAt` is the exact timestamp text Postgres
 * produced (so it can be passed back into SQL comparisons losslessly).
 */
export interface PendingCoroutineRun {
    messageId: string
    topic: string
    cooperationLineage: string[]
    payload: JsonValue
    createdAt: Date
    step: string | null
    nextStep: number | null
    suspendedAt: string | null
    childFailureHandlerIteration: number | null
    latestScopeContext: JsonValue | null
    latestContext: JsonValue | null
    childRolledBackExceptions: JsonValue
    childRollbackFailedExceptions: JsonValue
    rollingBackException: JsonValue | null
    executedStepInstances: JsonValue
}
