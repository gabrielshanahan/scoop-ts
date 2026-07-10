import type { Sql } from "postgres"
import { setClock } from "../util/Clock.js"

/**
 * Calibrates the injectable clock against the DATABASE clock.
 *
 * The engine's authoritative time source is Postgres (`CLOCK_TIMESTAMP()` writes every
 * `created_at`), but some client-side reads — most importantly the `ignoreOlderThan` cutoff that
 * `HandlerRegistry.eventLoopStrategy()` bakes into the readiness SQL — come from the injected
 * clock. If the client clock runs AHEAD of the database clock (e.g. Docker VM clock drift),
 * a message inserted right after a strategy is created gets `created_at < ignoreOlderThan` and is
 * ignored forever. The Kotlin original implicitly assumes host time ≈ DB time; on this stack the
 * assumption is made explicit: call this once after connecting and the injected clock follows the
 * database clock.
 *
 * Returns the measured offset in milliseconds (db − host) for observability.
 */
export async function calibrateClockToDatabase(sql: Sql): Promise<number> {
    // Take a handful of samples and keep the one with the tightest round-trip, so network jitter
    // doesn't corrupt the offset.
    let bestOffset = 0
    let bestRoundTrip = Number.POSITIVE_INFINITY
    for (let i = 0; i < 5; i++) {
        const before = Date.now()
        const [row] = await sql`SELECT EXTRACT(EPOCH FROM CLOCK_TIMESTAMP()) * 1000 AS db_ms`
        const after = Date.now()
        const roundTrip = after - before
        if (roundTrip < bestRoundTrip) {
            bestRoundTrip = roundTrip
            bestOffset = Number(row!.db_ms) - (before + after) / 2
        }
    }
    const offset = bestOffset
    setClock({ nowMillis: () => Date.now() + offset })
    return offset
}
