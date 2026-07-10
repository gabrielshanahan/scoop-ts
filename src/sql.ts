import type { Sql, TransactionSql } from "postgres"

/** A connection-ish thing queries run on: the pool or a transaction handle. */
export type DbConnection = Sql | TransactionSql

/**
 * Converts FluentJDBC-style named parameters (`:name`) to positional (`$1`, `$2`, …) so the
 * ported SQL can stay textually close to the original. `::type` casts are left untouched. A named
 * parameter may appear multiple times; missing parameters raise immediately.
 */
export function namedParamsToPositional(
    sql: string,
    params: Record<string, unknown>,
): { text: string; values: unknown[] } {
    const order: string[] = []
    const text = sql.replace(/(?<![:\w]):([a-zA-Z_][a-zA-Z0-9_]*)/g, (_match, name: string) => {
        if (!(name in params)) {
            throw new Error(`Missing SQL parameter :${name}`)
        }
        let index = order.indexOf(name)
        if (index === -1) {
            order.push(name)
            index = order.length - 1
        }
        return `$${index + 1}`
    })
    return { text, values: order.map(name => params[name]) }
}

/** Executes SQL with named parameters on the given connection. */
export function queryNamed(
    connection: DbConnection,
    sql: string,
    params: Record<string, unknown>,
): Promise<Record<string, unknown>[]> {
    const { text, values } = namedParamsToPositional(sql, params)
    return connection.unsafe(text, values as never[]) as unknown as Promise<
        Record<string, unknown>[]
    >
}

/** Renders a lineage as a Postgres uuid[] literal (pass with a `::uuid[]` cast). */
export function uuidArrayLiteral(uuids: string[]): string {
    return `{${uuids.join(",")}}`
}
