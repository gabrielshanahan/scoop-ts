import type { DbConnection } from "../../sql.js"
import { asScoopInfrastructure } from "../ScoopInfrastructureException.js"

/**
 * Restores the SEEN settled_at stamp (V6__seen_settled_flag) from the event log — both drift
 * directions.
 *
 * The stamp is maintained atomically (the statement that writes a terminal or ROLLING_BACK event
 * also updates it), so drift should not happen; this exists as the safety net that turns a
 * hypothetical bookkeeping bug from "saga silently never resumes" (a wrongly-settled row is
 * invisible to dispatch) or "permanent scan-set member" (a wrongly-unsettled row) into a bounded
 * delay. Run it on a maintenance cadence — it costs one pass over the SEEN rows plus lineage
 * probes, so it belongs next to retention pruning, not in a hot loop.
 *
 * Returns how many rows were corrected in each direction; anything non-zero is worth logging
 * loudly by the caller, since it means the atomic bookkeeping missed a transition.
 */
export async function repairSettledFlags(
    connection: DbConnection,
): Promise<{ settled: number; unsettled: number }> {
    return asScoopInfrastructure(async () => {
        // The event-log truth of "this SEEN's saga is finished", mirroring the live transitions:
        // a rollback terminal always settles; a COMMITTED settles unless a ROLLING_BACK
        // re-activated the lineage and its own terminal hasn't landed yet.
        const settledTruth = (seenAlias: string): string => `(
            EXISTS (SELECT 1 FROM message_event t
                    WHERE t.cooperation_lineage = ${seenAlias}.cooperation_lineage
                      AND t.type IN ('ROLLED_BACK', 'ROLLBACK_FAILED'))
            OR (
              EXISTS (SELECT 1 FROM message_event c
                      WHERE c.cooperation_lineage = ${seenAlias}.cooperation_lineage
                        AND c.type = 'COMMITTED')
              AND NOT EXISTS (SELECT 1 FROM message_event rb
                              WHERE rb.cooperation_lineage = ${seenAlias}.cooperation_lineage
                                AND rb.type = 'ROLLING_BACK')
            )
        )`
        const settledRows = await connection.unsafe(
            `UPDATE message_event seen SET settled_at = CLOCK_TIMESTAMP()
             WHERE seen.type = 'SEEN' AND seen.settled_at IS NULL AND ${settledTruth("seen")}
             RETURNING seen.id`,
        )
        const unsettledRows = await connection.unsafe(
            `UPDATE message_event seen SET settled_at = NULL
             WHERE seen.type = 'SEEN' AND seen.settled_at IS NOT NULL AND NOT ${settledTruth("seen")}
             RETURNING seen.id`,
        )
        return { settled: settledRows.length, unsettled: unsettledRows.length }
    })
}
