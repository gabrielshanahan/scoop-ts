import { VariableName } from "../VariableName.js"

/**
 * Thrown when attempting to store a return value that already exists for the same combination of
 * cooperation lineage, handler, and variable name (violating the unique constraint on the
 * return_value table).
 */
export class ReturnValueAlreadyExistsException extends Error {
    constructor(cooperationLineage: string[], handlerName: string, variableName: VariableName) {
        super(
            `Return value already exists for lineage ${cooperationLineage.join(".")}, ` +
                `handler '${handlerName}', variable '${variableName.serializedValue}'`,
        )
        this.name = "ReturnValueAlreadyExistsException"
    }
}
