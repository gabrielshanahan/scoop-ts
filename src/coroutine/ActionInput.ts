import type { JsonbHelper } from "../JsonbHelper.js"
import { VariableName } from "./VariableName.js"

/**
 * A wrapper for action payloads that includes the return value variable name — where the action
 * should store its result.
 */
export class ActionInput<P> {
    constructor(
        readonly returnValueVariableName: VariableName,
        readonly payload: P,
    ) {}
}

/**
 * Parses an [ActionInput] from a JSONB payload, resolving the polymorphic `_type` discriminator
 * of the variable name back to its registered singleton.
 */
export function parseActionInput<P>(jsonbHelper: JsonbHelper, dbValue: unknown): ActionInput<P> {
    const parsed = jsonbHelper.fromJsonb<{
        returnValueVariableName: { _type: string }
        payload: P
    }>(dbValue)
    return new ActionInput(
        VariableName.fromSerialized(parsed.returnValueVariableName._type),
        parsed.payload,
    )
}
