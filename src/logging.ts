import { pino } from "pino"

/**
 * Root pino logger (replaces SLF4J). Level is controlled via SCOOP_LOG_LEVEL; defaults to "warn"
 * so the engine is quiet in tests unless asked otherwise.
 */
export const rootLogger = pino({
    level: process.env.SCOOP_LOG_LEVEL ?? "warn",
})

export function logger(name: string) {
    return rootLogger.child({ name })
}
