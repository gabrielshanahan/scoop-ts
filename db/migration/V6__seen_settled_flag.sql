-- Settledness lives ON the SEEN row.
--
-- The dispatch query (candidate_seens) used to decide "is this saga finished?" by anti-joining
-- every retained SEEN of a coroutine against the terminal events of its lineage — cost
-- proportional to the retained history, paid on every poll, around the clock (measured on a
-- live deployment 2026-07-18: these polls were ~97% of all database time). The fix stores the
-- answer on the row the moment it changes:
--
--   * writing COMMITTED / ROLLED_BACK / ROLLBACK_FAILED for a handler stamps its SEEN's
--     settled_at (same statement, so the flag can never lag the event),
--   * writing ROLLING_BACK clears it (an external rollback re-activates a committed saga),
--
-- and candidate_seens adds `AND seen.settled_at IS NULL` — its scan becomes the handful of
-- genuinely active sagas, independent of how much history is retained. The original readiness
-- branches remain in the query as the correctness authority for the rows that pass; a stale
-- "unsettled" flag costs a few wasted probes, and repairSettledFlags (SettledFlag.ts) restores
-- both drift directions on the consumer's maintenance cadence.

ALTER TABLE message_event ADD COLUMN IF NOT EXISTS settled_at TIMESTAMPTZ;

-- Backfill: stamp every SEEN whose saga is already terminal. Mirrors the live transitions:
-- a rollback terminal always settles; a COMMITTED settles unless a ROLLING_BACK re-activated
-- the lineage and its own terminal hasn't landed yet.
UPDATE message_event seen
SET settled_at = CLOCK_TIMESTAMP()
WHERE seen.type = 'SEEN'
  AND seen.settled_at IS NULL
  AND (
    EXISTS (SELECT 1 FROM message_event t
            WHERE t.cooperation_lineage = seen.cooperation_lineage
              AND t.type IN ('ROLLED_BACK', 'ROLLBACK_FAILED'))
    OR (
      EXISTS (SELECT 1 FROM message_event c
              WHERE c.cooperation_lineage = seen.cooperation_lineage
                AND c.type = 'COMMITTED')
      AND NOT EXISTS (SELECT 1 FROM message_event rb
                      WHERE rb.cooperation_lineage = seen.cooperation_lineage
                        AND rb.type = 'ROLLING_BACK')
    )
  );

-- What dispatch now scans: the unsettled SEENs of one coroutine.
CREATE INDEX IF NOT EXISTS idx_message_event_seen_unsettled
    ON message_event (coroutine_name) WHERE type = 'SEEN' AND settled_at IS NULL;
