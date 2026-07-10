import type { JsonbHelper, JsonValue } from "../../JsonbHelper.js"
import { DbConnection, queryNamed, uuidArrayLiteral } from "../../sql.js"
import type { Handler } from "../Handler.js"
import { asScoopInfrastructure } from "../ScoopInfrastructureException.js"
import type { VariableName } from "../VariableName.js"
import { ReturnValueAlreadyExistsException } from "./ReturnValueAlreadyExistsException.js"

/**
 * Repository for storing and retrieving return values from actions, keyed by cooperation lineage,
 * handler name, and variable name.
 */
export class ReturnValueRepository {
    constructor(private readonly jsonbHelper: JsonbHelper) {}

    /**
     * Stores a return value.
     *
     * @throws ReturnValueAlreadyExistsException if a return value already exists for this tuple —
     *   a logical uniqueness outcome, so it passes through the infrastructure-failure wrapper.
     */
    async storeReturnValue(
        connection: DbConnection,
        cooperationLineage: string[],
        handlerName: string,
        variableName: VariableName,
        value: JsonValue,
    ): Promise<void> {
        await asScoopInfrastructure(async () => {
            try {
                await queryNamed(
                    connection,
                    `INSERT INTO return_value (cooperation_lineage, handler_name, variable_name, value)
                    VALUES (:lineage::uuid[], :handlerName, :variableName, :value::jsonb)`,
                    {
                        lineage: uuidArrayLiteral(cooperationLineage),
                        handlerName,
                        variableName: variableName.serializedValue,
                        value: this.jsonbHelper.toJsonText(value),
                    },
                )
            } catch (e) {
                // Unique constraint violation is a logical outcome, not an infrastructure fault
                if (
                    e instanceof Error &&
                    e.message.includes("unique_return_value_per_lineage_handler_variable")
                ) {
                    throw new ReturnValueAlreadyExistsException(
                        cooperationLineage,
                        handlerName,
                        variableName,
                    )
                }
                throw e
            }
        })
    }

    /**
     * Retrieves all return values from direct children (lineage exactly one element longer than
     * the parent's, with the parent's lineage as prefix) for the given variable name.
     */
    async getReturnValues(
        connection: DbConnection,
        parentLineage: string[],
        variableName: VariableName,
        handlerRegistry: (name: string) => Handler<unknown>,
    ): Promise<Map<Handler<unknown>, JsonValue>> {
        return asScoopInfrastructure(async () => {
            const rows = await queryNamed(
                connection,
                `SELECT handler_name, value FROM return_value
                WHERE variable_name = :variableName
                  AND :parentLineage::uuid[] <@ cooperation_lineage
                  AND cardinality(cooperation_lineage) = :childCardinality`,
                {
                    variableName: variableName.serializedValue,
                    parentLineage: uuidArrayLiteral(parentLineage),
                    childCardinality: parentLineage.length + 1,
                },
            )
            const result = new Map<Handler<unknown>, JsonValue>()
            for (const row of rows) {
                result.set(
                    handlerRegistry(row.handler_name as string),
                    this.jsonbHelper.fromJsonb(row.value),
                )
            }
            return result
        })
    }

    /** Retrieves a specific return value from a direct child by handler, or null. */
    async getReturnValue(
        connection: DbConnection,
        parentLineage: string[],
        variableName: VariableName,
        handler: Handler<unknown>,
    ): Promise<JsonValue | null> {
        return asScoopInfrastructure(async () => {
            const rows = await queryNamed(
                connection,
                `SELECT value FROM return_value
                WHERE variable_name = :variableName
                  AND handler_name = :handlerName
                  AND :parentLineage::uuid[] <@ cooperation_lineage
                  AND cardinality(cooperation_lineage) = :childCardinality`,
                {
                    variableName: variableName.serializedValue,
                    handlerName: handler.handlerName,
                    parentLineage: uuidArrayLiteral(parentLineage),
                    childCardinality: parentLineage.length + 1,
                },
            )
            const value = rows[0]?.value
            return value === undefined ? null : this.jsonbHelper.fromJsonb(value)
        })
    }
}
