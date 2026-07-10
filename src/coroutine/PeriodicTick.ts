/**
 * Handle for a periodic tick loop started by `EventLoop.tickPeriodically`.
 *
 * All ticks for a given saga identifier — scheduled or explicitly [trigger]ed — are serialized:
 * at most one tick runs at any moment for a given worker. Parallelism comes from registering more
 * *instances* of the same saga, never from multiple entry points into one identifier.
 */
export interface PeriodicTick {
    /**
     * Ask the loop to run a tick as soon as it's idle. Intended for push-based wake-ups (e.g.
     * Postgres LISTEN/NOTIFY callbacks). Triggers that arrive while another tick is already
     * running or pending are silently dropped (coalesced) — the next scheduled tick acts as the
     * safety net. Calls after [close] are silently ignored.
     */
    trigger(): void

    /**
     * Stops the ticker and waits for any in-flight tick to finish (bounded — a genuinely stuck
     * tick is abandoned with a warning rather than deadlocking shutdown).
     */
    close(): Promise<void>
}
