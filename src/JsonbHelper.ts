import { CooperationContext } from "./coroutine/context/CooperationContext.js"
import {
    contextFromParsedJson,
    writeContextJson,
} from "./coroutine/context/CooperationContextModule.js"

/** Any JSON-representable value; the analog of the JDBC `PGobject` payload carrier. */
export type JsonValue = unknown

function isCooperationContext(value: unknown): value is CooperationContext {
    return (
        typeof value === "object" &&
        value !== null &&
        typeof (value as CooperationContext).fold === "function" &&
        typeof (value as CooperationContext).plus === "function" &&
        typeof (value as CooperationContext).minus === "function" &&
        typeof (value as CooperationContext).get === "function"
    )
}

/**
 * Helper for converting between JS values and PostgreSQL JSONB columns — the analog of the
 * Jackson-based `JsonbHelper`. postgres.js already parses JSONB columns into JS values, so the
 * "PGobject" side of the original API surface becomes plain parsed JSON here.
 */
export class JsonbHelper {
    /**
     * Serializes a value to the JSON text stored in a JSONB column. [CooperationContext] values
     * are serialized via the context codec (the analog of `CooperationContextModule`).
     */
    toJsonText(value: unknown): string {
        if (isCooperationContext(value)) {
            return writeContextJson(value)
        }
        return JSON.stringify(value)
    }

    /**
     * Prepares a value for use as a jsonb query parameter. postgres.js resolves `$n::jsonb`
     * parameter types server-side and JSON-serializes the JS value — so parameters must be the
     * parsed JSON VALUE, never pre-serialized JSON text (which would double-encode into a jsonb
     * string). [CooperationContext] values go through the context codec and back to a value.
     */
    toJsonbParam(value: unknown): unknown {
        if (isCooperationContext(value)) {
            return JSON.parse(writeContextJson(value))
        }
        return value
    }

    /**
     * Deserializes a JSONB column value (already parsed by postgres.js, or raw JSON text) into a
     * plain JS value.
     */
    fromJsonb<T>(dbValue: unknown): T {
        if (typeof dbValue === "string") {
            return JSON.parse(dbValue) as T
        }
        return dbValue as T
    }

    /** Deserializes a JSONB column value into a [CooperationContext] (lazy, single level). */
    contextFromJsonb(dbValue: unknown): CooperationContext {
        const parsed =
            typeof dbValue === "string"
                ? (JSON.parse(dbValue) as Record<string, unknown>)
                : (dbValue as Record<string, unknown>)
        return contextFromParsedJson(parsed)
    }
}
