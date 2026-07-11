/**
 * Whole-file flakiness soak: runs one test FILE in a loop — a fresh process per iteration, all
 * of the file's tests in declaration order — until a time budget elapses or a failure is caught.
 * Complements soak-tests.ts (per-test isolation): some flakes only reproduce with the in-file
 * context of the preceding tests (zombie ticks from a prior test's teardown, listen-connection
 * state, pool history). On failure the full output (including SCOOP_LOG_LEVEL debug, if set) is
 * written to file-flake-<n>.log and the loop stops (FILE_SOAK_STOP_ON_FLAKE=0 to keep going).
 *
 * Usage: FILE_SOAK_SECONDS=1800 npx tsx scripts/soak-file.ts test/coroutine/structuredcooperation/RollbackPathTest.test.ts
 */
import { PostgreSqlContainer, StartedPostgreSqlContainer } from "@testcontainers/postgresql"
import { spawnSync } from "node:child_process"
import { writeFileSync } from "node:fs"
import { fileURLToPath } from "node:url"
import { dirname, join } from "node:path"
import postgres from "postgres"
import { applyMigrations } from "../src/node/migrations.js"

const root = join(dirname(fileURLToPath(import.meta.url)), "..")
const tsFile = process.argv[2]
if (!tsFile) {
    console.error("usage: soak-file.ts <test file>")
    process.exit(2)
}
const budgetSeconds = Number(process.env.FILE_SOAK_SECONDS ?? "1800")
// Optional: run only tests whose name matches (anchored) — one test per process, which also
// preserves the DB state of a failing iteration for the post-mortem dump below.
const testFilter = process.env.FILE_SOAK_TEST
const stopOnFlake = process.env.FILE_SOAK_STOP_ON_FLAKE !== "0"
const MAX_REPROVISIONS = 10

let container: StartedPostgreSqlContainer = await new PostgreSqlContainer("postgres:15").start()

async function prepareDatabase(): Promise<void> {
    const sql = postgres(container.getConnectionUri(), { max: 1 })
    try {
        await applyMigrations(sql, join(root, "db", "migration"))
    } finally {
        await sql.end()
    }
}

async function containerAlive(): Promise<boolean> {
    const sql = postgres(container.getConnectionUri(), {
        max: 1,
        connect_timeout: 5,
    })
    try {
        await sql`SELECT 1`
        return true
    } catch {
        return false
    } finally {
        await sql.end({ timeout: 1 }).catch(() => {})
    }
}

await prepareDatabase()
console.log(`file-soaking ${tsFile} for ${budgetSeconds}s per loop budget`)

const deadline = Date.now() + budgetSeconds * 1000
let iterations = 0
let flakes = 0
let reprovisions = 0

while (Date.now() < deadline) {
    const result = spawnSync(
        process.execPath,
        [
            "--import",
            "tsx",
            "--test",
            ...(testFilter ? [`--test-name-pattern=^${testFilter}$`] : []),
            tsFile,
        ],
        {
            cwd: root,
            env: { ...process.env, DATABASE_URL: container.getConnectionUri() },
            encoding: "utf-8",
            maxBuffer: 256 * 1024 * 1024,
        },
    )
    const output = `${result.stdout}\n${result.stderr}`
    const fail = Number(/fail (\d+)/.exec(output)?.[1] ?? "1")
    const ok = result.status === 0 && fail === 0

    if (!ok) {
        if (!(await containerAlive())) {
            reprovisions++
            console.log(`container died (re-provision ${reprovisions}/${MAX_REPROVISIONS})`)
            if (reprovisions > MAX_REPROVISIONS) {
                console.error("too many container deaths — aborting")
                process.exit(2)
            }
            await container.stop().catch(() => {})
            container = await new PostgreSqlContainer("postgres:15").start()
            await prepareDatabase()
            continue
        }
        const logPath = join(root, `file-flake-${flakes}.log`)
        // Post-mortem: capture the message_event log as the failing process left it (only
        // meaningful with FILE_SOAK_TEST — otherwise later tests' TRUNCATEs already wiped it).
        let dump = ""
        try {
            const sql = postgres(container.getConnectionUri(), { max: 1 })
            const events = await sql`
                SELECT id, message_id, type, coroutine_name, step, cooperation_lineage::text,
                       created_at::text, child_failure_handler_iteration, next_step
                FROM message_event ORDER BY created_at, id
            `
            const messages = await sql`SELECT id, topic, created_at::text FROM message ORDER BY created_at`
            await sql.end()
            dump =
                "\n=== POST-MORTEM message table ===\n" +
                messages.map(row => JSON.stringify(row)).join("\n") +
                "\n=== POST-MORTEM message_event table ===\n" +
                events.map(row => JSON.stringify(row)).join("\n") +
                "\n"
        } catch (e) {
            dump = `\n=== POST-MORTEM dump failed: ${e} ===\n`
        }
        writeFileSync(logPath, output + dump)
        flakes++
        console.log(`FLAKE at iteration ${iterations + 1} — ${logPath}`)
        if (stopOnFlake) {
            break
        }
    }
    iterations++
    if (iterations % 5 === 0) {
        console.log(`iteration ${iterations} done (${flakes} flakes)`)
    }
}

await container.stop().catch(() => {})
console.log(
    flakes === 0
        ? `FILE SOAK CLEAN: ${iterations} iterations`
        : `${flakes} flake(s) over ${iterations} iterations`,
)
process.exit(flakes === 0 ? 0 : 1)
