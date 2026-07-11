/**
 * Test runner: starts ONE PostgreSQL testcontainer for the whole run, applies the V1..V5
 * migrations, then executes the node:test suite sequentially (one file at a time — the tests
 * share the database and TRUNCATE between tests, exactly like the reference suite).
 *
 * Usage:
 *   npm test                       # full suite, declaration order
 *   npm test -- --shuffle[=seed]   # randomized file order (prints the seed for reproduction)
 *   npm test -- test/coroutine/... # specific files
 */

import { spawn } from "node:child_process"
import { readdirSync, statSync } from "node:fs"
import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"
import { PostgreSqlContainer } from "@testcontainers/postgresql"
import postgres from "postgres"
import { applyMigrations } from "../src/node/migrations.js"

const root = join(dirname(fileURLToPath(import.meta.url)), "..")

function collectTestFiles(dir: string): string[] {
    const results: string[] = []
    for (const entry of readdirSync(dir)) {
        const path = join(dir, entry)
        if (statSync(path).isDirectory()) {
            results.push(...collectTestFiles(path))
        } else if (entry.endsWith(".test.ts")) {
            results.push(path)
        }
    }
    return results.sort()
}

const args = process.argv.slice(2)
const shuffleArg = args.find(a => a.startsWith("--shuffle"))
const fileArgs = args.filter(a => !a.startsWith("--"))

const files = fileArgs.length > 0 ? fileArgs : collectTestFiles(join(root, "test"))

if (shuffleArg) {
    const seed = shuffleArg.includes("=")
        ? Number(shuffleArg.split("=")[1])
        : Math.floor(Math.random() * 2 ** 31)
    console.log(`shuffling test files with seed ${seed} (reproduce with --shuffle=${seed})`)
    // Deterministic LCG-based Fisher-Yates
    let state = seed >>> 0
    const random = () => {
        state = (state * 1664525 + 1013904223) >>> 0
        return state / 2 ** 32
    }
    for (let i = files.length - 1; i > 0; i--) {
        const j = Math.floor(random() * (i + 1))
        ;[files[i]!, files[j]!] = [files[j]!, files[i]!]
    }
}

// With DATABASE_URL set, an externally-managed Postgres is used (e.g. one container shared
// across many runs); otherwise a fresh testcontainer is started for this run.
let url = process.env.DATABASE_URL
let container: Awaited<ReturnType<PostgreSqlContainer["start"]>> | null = null
if (url) {
    console.log(`using external postgres for ${files.length} test file(s)`)
} else {
    console.log(`starting postgres container for ${files.length} test file(s)...`)
    container = await new PostgreSqlContainer("postgres:15").start()
    url = container.getConnectionUri()
}
const sql = postgres(url, { max: 1 })
await applyMigrations(sql, join(root, "db", "migration"))
const [clockRow] = await sql`SELECT EXTRACT(EPOCH FROM CLOCK_TIMESTAMP()) * 1000 AS db_ms`
const skew = Math.round(Number(clockRow!.db_ms) - Date.now())
await sql.end()
console.log(`postgres ready, migrations applied (db-host clock skew: ${skew}ms)`)

const child = spawn(
    process.execPath,
    ["--import", "tsx", "--test", "--test-concurrency=1", ...files],
    {
        stdio: "inherit",
        env: { ...process.env, DATABASE_URL: url },
        cwd: root,
    },
)

const exitCode: number = await new Promise(resolve => {
    child.on("exit", code => resolve(code ?? 1))
})

if (container) {
    await container.stop()
}
process.exit(exitCode)
