/**
 * Per-test flakiness soak: every ported test is run in its own loop — one fresh process per
 * iteration (`node --test --test-name-pattern='^<name>$' <file>`), the harness's beforeEach
 * TRUNCATE providing DB cleanup on the edges — until a time budget elapses. Any failing
 * iteration with a live database is recorded as a flake; a dead database (external Docker event)
 * re-provisions the shared container and retries the iteration.
 *
 * Budgets: SOAK_SECONDS_PER_TEST (default 60) per DB test, 5s per pure in-memory test file (no
 * setupScoopTest — no DB, no timing), and at least SOAK_MIN_ITERATIONS (default 5) iterations
 * either way. Test inventory comes from PORT-LEDGER.md, so the soak provably covers all 195.
 */
import { PostgreSqlContainer, StartedPostgreSqlContainer } from "@testcontainers/postgresql"
import { spawnSync } from "node:child_process"
import { readFileSync, writeFileSync } from "node:fs"
import { fileURLToPath } from "node:url"
import { dirname, join } from "node:path"
import postgres from "postgres"
import { applyMigrations } from "../src/node/migrations.js"

const root = join(dirname(fileURLToPath(import.meta.url)), "..")
const secondsPerTest = Number(process.env.SOAK_SECONDS_PER_TEST ?? "60")
const minIterations = Number(process.env.SOAK_MIN_ITERATIONS ?? "5")
const MAX_REPROVISIONS = 10

// --- Inventory from the ledger ------------------------------------------------------------------
interface SoakTarget {
    tsFile: string
    testName: string
    pure: boolean
}

const ledger = readFileSync(join(root, "PORT-LEDGER.md"), "utf-8")
const targets: SoakTarget[] = []
const sectionRegex = /^### (\S+\.kt) \((\d+) tests\) → (\S+)$/gm
let sectionMatch: RegExpExecArray | null
while ((sectionMatch = sectionRegex.exec(ledger)) !== null) {
    const tsFile = sectionMatch[3]!
    const start = sectionMatch.index + sectionMatch[0].length
    const nextSection = ledger.indexOf("\n### ", start)
    const endOfBlock = ledger.indexOf("\n## ", start)
    const end = Math.min(
        nextSection === -1 ? ledger.length : nextSection,
        endOfBlock === -1 ? ledger.length : endOfBlock,
    )
    const source = readFileSync(join(root, tsFile), "utf-8")
    const pure = !source.includes("setupScoopTest")
    for (const row of ledger.slice(start, end).matchAll(/^\| \d+ \| (.+?) \| \w+ \|/gm)) {
        targets.push({ tsFile, testName: row[1]!, pure })
    }
}
// Optional subset filter (substring match on the TS file path), e.g. SOAK_ONLY=HappyPath
const only = process.env.SOAK_ONLY
const selectedTargets = only ? targets.filter(target => target.tsFile.includes(only)) : targets
console.log(`soaking ${selectedTargets.length} tests${only ? ` (filter: ${only})` : ""}`)

// --- Shared database ------------------------------------------------------------------------------
let container: StartedPostgreSqlContainer = await new PostgreSqlContainer("postgres:15").start()

async function prepareDatabase(): Promise<void> {
    const sql = postgres(container.getConnectionUri(), { max: 1 })
    await applyMigrations(sql, join(root, "db", "migration"))
    await sql.end()
}

async function containerAlive(): Promise<boolean> {
    const sql = postgres(container.getConnectionUri(), { max: 1, connect_timeout: 10 })
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
console.log("shared postgres ready")

function escapeRegex(input: string): string {
    return input.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")
}

// --- Soak loop ------------------------------------------------------------------------------------
const flakes: string[] = []
let reprovisions = 0

outer: for (const [index, target] of selectedTargets.entries()) {
    const budgetMillis = (target.pure ? 5 : secondsPerTest) * 1000
    const startedAt = Date.now()
    let iterations = 0

    while (Date.now() - startedAt < budgetMillis || iterations < minIterations) {
        const result = spawnSync(
            process.execPath,
            [
                "--import",
                "tsx",
                "--test",
                `--test-name-pattern=^${escapeRegex(target.testName)}$`,
                target.tsFile,
            ],
            {
                cwd: root,
                env: { ...process.env, DATABASE_URL: container.getConnectionUri() },
                encoding: "utf-8",
                maxBuffer: 64 * 1024 * 1024,
            },
        )
        const output = `${result.stdout}\n${result.stderr}`
        const fail = Number(/fail (\d+)/.exec(output)?.[1] ?? "1")
        const pass = Number(/pass (\d+)/.exec(output)?.[1] ?? "0")
        const ok = result.status === 0 && fail === 0 && pass >= 1

        if (!ok) {
            if (!(await containerAlive())) {
                reprovisions++
                console.log(
                    `  [${target.tsFile} :: ${target.testName}] container died (re-provision ${reprovisions}/${MAX_REPROVISIONS})`,
                )
                if (reprovisions > MAX_REPROVISIONS) {
                    console.error("too many container deaths — aborting soak")
                    flakes.push("(aborted: repeated container deaths)")
                    break outer
                }
                await container.stop().catch(() => {})
                container = await new PostgreSqlContainer("postgres:15").start()
                await prepareDatabase()
                continue // retry this iteration; not a flake
            }
            const logPath = join(root, `soak-flake-${index}.log`)
            writeFileSync(logPath, output)
            flakes.push(`${target.tsFile} :: ${target.testName} (iteration ${iterations + 1})`)
            console.log(
                `FLAKE [${index + 1}/${selectedTargets.length}] ${target.tsFile} :: ${target.testName} at iteration ${iterations + 1} — ${logPath}`,
            )
            break // record and move on to the next test
        }
        iterations++
    }

    if (iterations > 0 && !flakes.some(flake => flake.includes(target.testName))) {
        console.log(
            `ok [${index + 1}/${selectedTargets.length}] ${target.testName.slice(0, 70)} — ${iterations} iterations`,
        )
    }
}

await container.stop().catch(() => {})

if (flakes.length === 0) {
    console.log(`SOAK CLEAN: all ${selectedTargets.length} tests survived their loops`)
    process.exit(0)
} else {
    console.error(`${flakes.length} flaky test(s):`)
    for (const flake of flakes) {
        console.error(`  ${flake}`)
    }
    process.exit(1)
}
