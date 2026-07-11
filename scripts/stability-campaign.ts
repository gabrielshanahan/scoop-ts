/**
 * Zero-flakiness proof: runs the full suite [SCOOP_STABILITY_RUNS] times (default 20) against ONE
 * shared Postgres container, alternating between declaration order and seeded-shuffle order.
 *
 * A run only counts as green at 199 passed / 0 failed (195 ported + 4 port-added regressions). If a run fails while the shared container
 * is DEAD (killed from outside — e.g. a Docker Desktop restart), the container is re-provisioned
 * and that run number is retried: that is environment resilience, not test leniency. A failure
 * with a live database is a real failure and ends the campaign argument.
 */
import { PostgreSqlContainer, StartedPostgreSqlContainer } from "@testcontainers/postgresql"
import { spawnSync } from "node:child_process"
import { writeFileSync } from "node:fs"
import { fileURLToPath } from "node:url"
import { dirname, join } from "node:path"
import postgres from "postgres"

const root = join(dirname(fileURLToPath(import.meta.url)), "..")
const totalRuns = Number(process.env.SCOOP_STABILITY_RUNS ?? "20")
const MAX_REPROVISIONS = 5

let container: StartedPostgreSqlContainer = await new PostgreSqlContainer("postgres:15").start()
console.log("shared postgres container started")

async function containerAlive(): Promise<boolean> {
    const sql = postgres(container.getConnectionUri(), {
        max: 1,
        connect_timeout: 10,
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

let failures = 0
let reprovisions = 0
try {
    let run = 1
    while (run <= totalRuns) {
        const args = run % 2 === 0 ? [`--shuffle=${run * 7919}`] : []
        const started = Date.now()
        const result = spawnSync(
            process.execPath,
            ["--import", "tsx", "scripts/run-tests.ts", ...args],
            {
                cwd: root,
                env: { ...process.env, DATABASE_URL: container.getConnectionUri() },
                encoding: "utf-8",
                maxBuffer: 64 * 1024 * 1024,
            },
        )
        const output = `${result.stdout}\n${result.stderr}`
        const pass = /pass (\d+)/.exec(output)?.[1] ?? "?"
        const fail = /fail (\d+)/.exec(output)?.[1] ?? "?"
        const seconds = Math.round((Date.now() - started) / 1000)
        const ok = result.status === 0 && fail === "0" && pass === "199"

        if (!ok && !(await containerAlive())) {
            // The database was killed out from under the suite — an infrastructure event, not a
            // suite result. Re-provision and retry this run number.
            reprovisions++
            console.log(
                `run ${run}/${totalRuns}: shared container died mid-run (re-provision ${reprovisions}/${MAX_REPROVISIONS}); retrying run ${run}`,
            )
            if (reprovisions > MAX_REPROVISIONS) {
                console.error("too many container deaths — giving up")
                failures++
                break
            }
            await container.stop().catch(() => {})
            container = await new PostgreSqlContainer("postgres:15").start()
            continue
        }

        console.log(
            `run ${run}/${totalRuns} (${args.join(" ") || "declaration order"}): pass=${pass} fail=${fail} exit=${result.status} ${seconds}s ${ok ? "OK" : "FAILED"}`,
        )
        if (!ok) {
            failures++
            const logPath = join(root, `stability-failure-run-${run}.log`)
            writeFileSync(logPath, output)
            console.log(`  full output saved to ${logPath}`)
        }
        run++
    }
} finally {
    await container.stop().catch(() => {})
}

console.log(failures === 0 ? `ALL ${totalRuns} RUNS GREEN` : `${failures} runs failed`)
process.exit(failures === 0 ? 0 : 1)
