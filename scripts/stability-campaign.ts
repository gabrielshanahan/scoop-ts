/**
 * Zero-flakiness proof: runs the full suite [SCOOP_STABILITY_RUNS] times (default 20) against ONE
 * shared Postgres container, alternating between declaration order and seeded-shuffle order.
 * Prints one line per run; exits non-zero if any run fails.
 */
import { PostgreSqlContainer } from "@testcontainers/postgresql"
import { spawnSync } from "node:child_process"
import { fileURLToPath } from "node:url"
import { dirname, join } from "node:path"

const root = join(dirname(fileURLToPath(import.meta.url)), "..")
const totalRuns = Number(process.env.SCOOP_STABILITY_RUNS ?? "20")

const container = await new PostgreSqlContainer("postgres:15").start()
console.log("shared postgres container started")

let failures = 0
try {
    for (let run = 1; run <= totalRuns; run++) {
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
        const ok = result.status === 0 && fail === "0" && pass === "195"
        console.log(
            `run ${run}/${totalRuns} (${args.join(" ") || "declaration order"}): pass=${pass} fail=${fail} exit=${result.status} ${seconds}s ${ok ? "OK" : "FAILED"}`,
        )
        if (!ok) {
            failures++
            const logPath = join(root, `stability-failure-run-${run}.log`)
            const { writeFileSync } = await import("node:fs")
            writeFileSync(logPath, output)
            console.log(`  full output saved to ${logPath}`)
        }
    }
} finally {
    await container.stop()
}

console.log(failures === 0 ? `ALL ${totalRuns} RUNS GREEN` : `${failures} runs failed`)
process.exit(failures === 0 ? 0 : 1)
