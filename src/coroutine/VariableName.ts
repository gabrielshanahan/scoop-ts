const variableNameRegistry = new Map<string, VariableName>()

/**
 * A type-safe identifier for a return value variable name.
 *
 * Serializes polymorphically with a `_type` discriminator, exactly like the Kotlin original's
 * `@JsonTypeInfo(use = NAME, property = "_type")`. Instances register themselves by name so that
 * deserialization (e.g. `parseActionInput`) can resolve the discriminator back to the singleton.
 */
export class VariableName {
    constructor(readonly serializedValue: string) {
        variableNameRegistry.set(serializedValue, this)
    }

    toJSON(): unknown {
        return { _type: this.serializedValue }
    }

    static fromSerialized(name: string): VariableName {
        const found = variableNameRegistry.get(name)
        if (!found) {
            throw new Error(`Unknown VariableName '${name}' — was it ever instantiated?`)
        }
        return found
    }
}
