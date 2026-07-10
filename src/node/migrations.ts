import { readdir, readFile } from "node:fs/promises"
import { join } from "node:path"
import type { Sql } from "postgres"

/**
 * Applies the V1..Vn .sql migrations carried over verbatim from the Kotlin repo.
 *
 * Replaces Flyway-on-boot from the Quarkus integration (see DECISIONS.md). Deliberately tiny:
 * files are applied in version order, each in its own transaction, and recorded in
 * scoop_schema_version so reapplication is a no-op.
 */
export async function applyMigrations(sql: Sql, migrationsDir: string): Promise<void> {
    await sql`
        CREATE TABLE IF NOT EXISTS scoop_schema_version (
            version INT PRIMARY KEY,
            filename TEXT NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    `
    const files = (await readdir(migrationsDir))
        .filter(f => /^V\d+__.*\.sql$/.test(f))
        .sort((a, b) => versionOf(a) - versionOf(b))

    for (const file of files) {
        const version = versionOf(file)
        const [already] = await sql`
            SELECT 1 FROM scoop_schema_version WHERE version = ${version}
        `
        if (already) {
            continue
        }
        const contents = await readFile(join(migrationsDir, file), "utf-8")
        await sql.begin(async tx => {
            await tx.unsafe(contents)
            await tx`
                INSERT INTO scoop_schema_version (version, filename)
                VALUES (${version}, ${file})
            `
        })
    }
}

function versionOf(filename: string): number {
    return Number(/^V(\d+)__/.exec(filename)![1])
}
