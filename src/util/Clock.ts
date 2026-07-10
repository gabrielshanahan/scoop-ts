/**
 * Injectable time source (a divergence from the Kotlin original, which reads
 * `OffsetDateTime.now()` / `System.currentTimeMillis()` directly — see DECISIONS.md).
 *
 * All wall-clock reads in the library go through [currentClock], so tests can control time
 * deterministically via [setClock]. Note that most load-bearing time decisions in Scoop happen in
 * Postgres (`CLOCK_TIMESTAMP()`), not in application code.
 */
export interface Clock {
    /** Current time in epoch milliseconds. */
    nowMillis(): number
}

export const systemClock: Clock = {
    nowMillis: () => Date.now(),
}

let currentClock: Clock = systemClock

export function setClock(clock: Clock): void {
    currentClock = clock
}

export function resetClock(): void {
    currentClock = systemClock
}

export function nowMillis(): number {
    return currentClock.nowMillis()
}

/**
 * Current time as an ISO-8601 UTC timestamp string — the analog of `OffsetDateTime.now()`.
 * Millisecond precision (JS clocks do not expose more).
 */
export function nowIso(): string {
    return new Date(currentClock.nowMillis()).toISOString()
}

/** ISO timestamp [millis] milliseconds from now. Analog of `OffsetDateTime.now().plus(...)`. */
export function isoFromNowMillis(millis: number): string {
    return new Date(currentClock.nowMillis() + millis).toISOString()
}

/**
 * The maximum timestamp Postgres can represent (analog of `postgresMaxTime` in the original's
 * context/util.kt). Rendered exactly like `OffsetDateTime.toString()` renders it.
 */
export const postgresMaxTime = "9999-12-31T23:59:59.999999Z"

/** Compares two ISO-8601 timestamps chronologically. */
export function compareTimestamps(a: string, b: string): number {
    const ta = Date.parse(a)
    const tb = Date.parse(b)
    return ta < tb ? -1 : ta > tb ? 1 : 0
}
