import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"
import postgres from "postgres"
import { applyMigrations } from "../src/node/migrations.js"

const url = process.env.DATABASE_URL ?? "postgres://postgres:postgres@localhost:5432/scoop"
const migrationsDir = join(dirname(fileURLToPath(import.meta.url)), "..", "db", "migration")

const sql = postgres(url, { max: 1 })
try {
    await applyMigrations(sql, migrationsDir)
    console.log("migrations applied")
} finally {
    await sql.end()
}
