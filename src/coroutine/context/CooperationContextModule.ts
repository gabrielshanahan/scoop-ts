import {
    type CooperationContext,
    Element,
    elementBean,
    keySerializedValue,
    OpaqueElement,
} from "./CooperationContext.js"
import { CooperationContextMap, KeyedElementMap } from "./CooperationContextMap.js"

/**
 * Serialization codec for [CooperationContext] — the analog of the Jackson
 * `CooperationContextModule` in the Kotlin original.
 *
 * ## Serialization strategy
 * Context serializes to a JSON object whose keys are element names and whose values are the
 * serialized element data:
 * - [OpaqueElement]: its raw JSON is written verbatim (no re-serialization)
 * - typed elements: serialized via `JSON.stringify` (each element's `toJSON` excludes the key)
 * - composite contexts: all elements, via `fold` (which yields deterministic, name-sorted order)
 *
 * ## Deserialization strategy
 * Only a single level of the JSON is processed: each top-level value is stored back as a JSON
 * string in the map's serialized form, preserving unknown elements and enabling lazy typed
 * deserialization on access. The Kotlin original reconstructs the value text from the token
 * stream; here the value is `JSON.parse`d and re-emitted with `JSON.stringify`, which yields the
 * same canonical form this codec itself produces (see DECISIONS.md for the caveats).
 */
export function writeContextJson(context: CooperationContext): string {
    const parts: string[] = []
    serializeInto(context, parts)
    return `{${parts.join(",")}}`
}

function serializeInto(value: CooperationContext, parts: string[]): void {
    if (value instanceof OpaqueElement) {
        parts.push(`${JSON.stringify(value.key.key)}:${value.json}`)
    } else if (value instanceof Element) {
        // Top level is unwrapped ({key: bean}); nested elements inside the bean serialize
        // context-wrapped via their toJSON.
        parts.push(
            `${JSON.stringify(keySerializedValue(value.key))}:${JSON.stringify(elementBean(value))}`,
        )
    } else {
        value.fold(undefined, (_, element) => {
            serializeInto(element, parts)
            return undefined
        })
    }
}

/** Deserializes a context from its JSON text. */
export function readContextJson(json: string): CooperationContext {
    return contextFromParsedJson(JSON.parse(json) as Record<string, unknown>)
}

/**
 * Builds a context from an already-parsed JSON object (as returned for jsonb columns by
 * postgres.js): every top-level value is re-serialized to its canonical JSON string form and kept
 * for lazy deserialization.
 */
export function contextFromParsedJson(parsed: Record<string, unknown>): CooperationContext {
    const serialized = new Map<string, string>()
    for (const [key, value] of Object.entries(parsed)) {
        serialized.set(key, JSON.stringify(value))
    }
    return new CooperationContextMap(serialized, new KeyedElementMap())
}
