/**
 * Replays the failing insertRollbackEmittedEventsForStep window from the post-mortem dump
 * (scratch; not shipped). Reconstructs child1's lineage rows with the exact dumped
 * timestamps, then runs the emitted_record window with the suspendedAt value each candidate
 * code path would pass, printing which values match the EMITTED row.
 */
import { PostgreSqlContainer } from "@testcontainers/postgresql"
import postgres from "postgres"
import { fileURLToPath } from "node:url"
import { dirname, join } from "node:path"
import { randomUUID } from "node:crypto"
import { applyMigrations } from "../src/node/migrations.js"

const root = join(dirname(fileURLToPath(import.meta.url)), "..")
const container = await new PostgreSqlContainer("postgres:15").start()
const sql = postgres(container.getConnectionUri(), { max: 2 })
await applyMigrations(sql, join(root, "db", "migration"))

const lineage = [randomUUID(), randomUUID(), randomUUID()]
const lin = `{${lineage.join(",")}}`
const childMsg = randomUUID()
const grandMsg = randomUUID()
await sql`INSERT INTO message (id, topic, payload) VALUES (${childMsg}, 'child-topic', '{}'), (${grandMsg}, 'grandchild-topic', '{}')`

// child1's lineage rows, timestamps verbatim from the dump
const rows: Array<[string, string, string | null, string]> = [
    [childMsg, "SEEN", null, "2026-07-11 03:50:52.803008+00"],
    [childMsg, "SUSPENDED", "0", "2026-07-11 03:50:52.812037+00"],
    [grandMsg, "EMITTED", "1", "2026-07-11 03:50:52.820026+00"],
    [childMsg, "SUSPENDED", "1", "2026-07-11 03:50:52.820963+00"],
    [childMsg, "SUSPENDED", "2", "2026-07-11 03:50:53.075418+00"],
    [childMsg, "COMMITTED", "2", "2026-07-11 03:50:53.084481+00"],
    [childMsg, "ROLLING_BACK", null, "2026-07-11 03:50:53.212294+00"],
    [childMsg, "SUSPENDED", "Rollback of 2[0,] (rolling back child scopes)", "2026-07-11 03:50:53.221788+00"],
    [childMsg, "SUSPENDED", "Rollback of 2[0,]", "2026-07-11 03:50:53.229452+00"],
]
for (const [messageId, type, step, createdAt] of rows) {
    await sql`
        INSERT INTO message_event (message_id, type, coroutine_name, coroutine_identifier, step, cooperation_lineage, created_at)
        VALUES (${messageId}, ${type}::message_event_type, 'child-handler-1', ${randomUUID()}, ${step}, ${lin}::uuid[], ${createdAt}::text::timestamptz)
    `
}

// What the executed_step_instances aggregation actually produces for this state:
const [agg] = await sql`
    SELECT JSON_AGG(
        JSON_BUILD_OBJECT('step', me.step, 'suspended_at', me.created_at)
        ORDER BY me.created_at DESC
    ) AS instances
    FROM message_event me
    WHERE me.cooperation_lineage = ${lin}::uuid[] AND me.type = 'SUSPENDED' AND me.step IS NOT NULL
`
console.log("aggregated instances:")
for (const inst of agg!.instances) {
    console.log("  ", JSON.stringify(inst))
}

async function windowMatches(suspendedAt: string, viaText = false): Promise<number> {
    if (viaText) {
        const rows = await sql`
            SELECT message_id
            FROM message_event
            WHERE type = 'EMITTED'
                AND cooperation_lineage = ${lin}::uuid[]
                AND created_at < ${suspendedAt}::text::timestamptz
                AND NOT EXISTS (
                    SELECT 1 FROM message_event mid
                    WHERE mid.cooperation_lineage = ${lin}::uuid[]
                      AND mid.type = 'SUSPENDED'
                      AND mid.created_at > message_event.created_at
                      AND mid.created_at < ${suspendedAt}::text::timestamptz
                )
        `
        return rows.length
    }
    const rows = await sql`
        SELECT message_id
        FROM message_event
        WHERE type = 'EMITTED'
            AND cooperation_lineage = ${lin}::uuid[]
            AND created_at < ${suspendedAt}::timestamptz
            AND NOT EXISTS (
                SELECT 1 FROM message_event mid
                WHERE mid.cooperation_lineage = ${lin}::uuid[]
                  AND mid.type = 'SUSPENDED'
                  AND mid.created_at > message_event.created_at
                  AND mid.created_at < ${suspendedAt}::timestamptz
            )
    `
    return rows.length
}

for (const inst of agg!.instances) {
    if (!String(inst.step).startsWith("Rollback")) {
        console.log(
            `window with suspended_at of step ${JSON.stringify(inst.step)} (${inst.suspended_at}):`,
            await windowMatches(inst.suspended_at as string),
            "match(es)",
        )
    }
}
// The stored values as text (proves whether the inserts kept microseconds):
const stored = await sql`SELECT step, created_at::text FROM message_event WHERE cooperation_lineage = ${lin}::uuid[] AND type = 'SUSPENDED' ORDER BY created_at`
for (const row of stored) console.log("stored:", row.step, row.created_at)
// The fix: bind the full-precision string via ::text::timestamptz
console.log(
    "FIXED window, step 1 full-precision string via ::text::timestamptz:",
    await windowMatches("2026-07-11T03:50:52.820963+00:00", true),
    "match(es) — expect 1",
)
console.log(
    "BROKEN window, same string via ::timestamptz bind:",
    await windowMatches("2026-07-11T03:50:52.820963+00:00"),
    "match(es) — expect 0 (client-side Date truncation)",
)

await sql.end()
await container.stop()
