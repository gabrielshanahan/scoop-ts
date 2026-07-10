import type { Sql, TransactionSql } from "postgres"

/**
 * Owns the database transaction that wraps a single saga step.
 *
 * Each tick of the EventLoop resumes one saga step inside a transaction. Scoop's own
 * `message_event` writes happen on the transaction handle handed to [inStepTransaction], and so
 * does any business code the step calls. Keeping those writes in one transaction is what makes a
 * step atomic: either the business effect and the structured-cooperation bookkeeping both commit,
 * or both roll back.
 *
 * This is the seam that, in the Kotlin original, has a plain-JDBC implementation in core and a
 * JTA implementation in the Quarkus module. On this stack there is exactly ONE implementation —
 * [PostgresTransactionRunner], wrapping postgres.js `sql.begin()` (see DECISIONS.md).
 */
export interface TransactionRunner {
    /**
     * Runs [block] inside the per-step transaction, passing it the transaction's connection
     * handle. The transaction is committed if [block] returns normally and rolled back if it
     * throws.
     */
    inStepTransaction<T>(block: (connection: TransactionSql) => Promise<T>): Promise<T>
}

/** The single [TransactionRunner]: postgres.js `sql.begin()` — commit on return, rollback on throw. */
export class PostgresTransactionRunner implements TransactionRunner {
    constructor(private readonly sql: Sql) {}

    inStepTransaction<T>(block: (connection: TransactionSql) => Promise<T>): Promise<T> {
        return this.sql.begin(block) as Promise<T>
    }
}

/** Convenience analog of the Kotlin `FluentJdbc.transactional` extension. */
export function transactional<T>(
    sql: Sql,
    block: (connection: TransactionSql) => Promise<T>,
): Promise<T> {
    return sql.begin(block) as Promise<T>
}
