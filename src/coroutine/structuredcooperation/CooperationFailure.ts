const NO_MESSAGE = "<no message>"
const UNKNOWN_FILENAME = "<unknown filename>"
const UNKNOWN_FUNCTION = "<unknown function>"

/**
 * Language-agnostic representation of a stack trace element, serializable across service
 * boundaries between different programming languages and platforms.
 */
export interface StackTraceFrame {
    fileName: string
    lineNumber: number
    className?: string | null
    functionName?: string | null
}

/**
 * Language-agnostic representation of a distributed exception — the core data structure for
 * failures propagated across service boundaries in structured cooperation.
 *
 * - CooperationFailure: language-agnostic data for cross-service communication (this)
 * - CooperationException: this runtime's translation of CooperationFailure for use in Scoop
 * - ScoopException: implementation-specific exceptions within Scoop itself
 */
export interface CooperationFailure {
    message: string
    type: string
    source: string
    stackTrace: StackTraceFrame[]
    causes: CooperationFailure[]
}

interface MaybeAggregated {
    cause?: unknown
    suppressed?: Error[]
}

/** Parses a V8 stack trace string into language-agnostic frames. */
function stackTraceFrames(error: Error): StackTraceFrame[] {
    const stack = error.stack ?? ""
    const frames: StackTraceFrame[] = []
    for (const line of stack.split("\n")) {
        const match =
            /^\s+at\s+(?:(.+?)\s+\()?(?:(.+?):(\d+):\d+|(native))\)?$/.exec(line) ?? null
        if (!match) {
            continue
        }
        const functionName = match[1] ?? null
        const fileName = match[2] ?? UNKNOWN_FILENAME
        const lineNumber = match[3] !== undefined ? Number(match[3]) : -1
        frames.push({
            fileName,
            lineNumber,
            className: null,
            functionName: functionName !== UNKNOWN_FUNCTION ? functionName : null,
        })
    }
    return frames
}

/** Converts an Error to a language-agnostic CooperationFailure. */
export function cooperationFailureFromThrowable(
    throwable: Error,
    source: string,
): CooperationFailure {
    const stackTrace = stackTraceFrames(throwable)

    const causes: CooperationFailure[] = []
    const aggregated = throwable as MaybeAggregated
    if (aggregated.cause instanceof Error && aggregated.cause !== throwable) {
        causes.push(cooperationFailureFromThrowable(aggregated.cause, source))
    }
    for (const suppressed of aggregated.suppressed ?? []) {
        causes.push(cooperationFailureFromThrowable(suppressed, source))
    }

    if (throwable instanceof CooperationException) {
        return {
            message: stripPrefix(throwable.message, `[${throwable.source}] ${throwable.type}: `),
            type: throwable.type,
            source: throwable.source,
            stackTrace,
            causes,
        }
    }
    return {
        message: throwable.message !== "" ? throwable.message : NO_MESSAGE,
        type: throwable.name,
        source,
        stackTrace,
        causes,
    }
}

function stripPrefix(message: string, prefix: string): string {
    const index = message.indexOf(prefix)
    return index === -1 ? message : message.slice(index + prefix.length)
}

/** Converts a CooperationFailure to a CooperationException usable as a normal Error. */
export function toCooperationException(failure: CooperationFailure): CooperationException {
    return new CooperationException(
        failure.message,
        failure.type,
        failure.source,
        failure.stackTrace,
        (failure.causes ?? []).map(toCooperationException),
    )
}

/**
 * Runtime exception representation of [CooperationFailure] — failures that originated from other
 * services (or handlers) in a structured cooperation system.
 */
export class CooperationException extends Error {
    override readonly cause?: CooperationException
    readonly suppressed: CooperationException[] = []

    constructor(
        message: string,
        readonly type: string,
        readonly source: string,
        readonly stackTraceFrames: StackTraceFrame[],
        readonly causes: CooperationException[],
    ) {
        super(`[${source}] ${type}: ${message.trim() !== "" ? message : NO_MESSAGE}`)
        this.name = "CooperationException"
        if (causes.length > 0) {
            this.cause = causes[0]
        }
        for (const suppressed of causes.slice(1)) {
            this.suppressed.push(suppressed)
        }
    }

    /** Structural equality, like the Kotlin data-class-style equals (ignoring stack traces). */
    equalsStructurally(other: CooperationException): boolean {
        return (
            this.type === other.type &&
            this.source === other.source &&
            this.message === other.message &&
            this.causes.length === other.causes.length &&
            this.causes.every((cause, i) => cause.equalsStructurally(other.causes[i]!))
        )
    }
}
