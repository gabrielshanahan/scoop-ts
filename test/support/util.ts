import assert from "node:assert/strict"
import type { Sql } from "postgres"
import type { DistributedCoroutineIdentifier } from "../../src/coroutine/DistributedCoroutineIdentifier.js"
import {
    type CooperationException,
    type CooperationFailure,
    toCooperationException,
} from "../../src/coroutine/structuredcooperation/CooperationFailure.js"
import type { JsonbHelper } from "../../src/JsonbHelper.js"

/** One event-sequence row: [type, step, coroutine_name] — the analog of the Kotlin Triple. */
export type EventTriple = [string, string | null, string | null]

export function triple(
    type: string,
    step: string | null,
    coroutineName: string | null,
): EventTriple {
    return [type, step, coroutineName]
}

/** Port of `FluentJdbc.getEventSequence()`. */
export async function getEventSequence(sql: Sql): Promise<EventTriple[]> {
    const rows = await sql`
        SELECT type, step, coroutine_name FROM message_event ORDER BY created_at
    `
    return rows.map(row => [
        row.type as string,
        (row.step as string | null) ?? null,
        (row.coroutine_name as string | null) ?? null,
    ])
}

/** Port of `keepOnlyHandlers` — keeps rows with a null handler or one of the given handlers. */
export function keepOnlyHandlers(sequence: EventTriple[], ...handlers: string[]): EventTriple[] {
    return sequence.filter(([, , name]) => name === null || handlers.includes(name))
}

/** Port of `keepOnlyPrefixedBy`. */
export function keepOnlyPrefixedBy(items: string[], ...prefixes: string[]): string[] {
    return items.filter(item => prefixes.some(prefix => item.startsWith(prefix)))
}

/** Port of `FluentJdbc.fetchExceptions`. */
export async function fetchExceptions(
    sql: Sql,
    jsonbHelper: JsonbHelper,
    type: string,
    coroutineName: string | null,
): Promise<CooperationException[]> {
    const rows =
        coroutineName === null
            ? await sql`
                SELECT exception
                FROM message_event
                WHERE type = ${type}::message_event_type AND coroutine_name IS NULL
              `
            : await sql`
                SELECT exception
                FROM message_event
                WHERE type = ${type}::message_event_type AND coroutine_name = ${coroutineName}
              `
    return rows.map(row =>
        toCooperationException(jsonbHelper.fromJsonb<CooperationFailure>(row.exception)),
    )
}

/** Structural expectation for a CooperationException tree. */
export interface CooperationExceptionData {
    message: string
    type: string
    source: string
    causes?: CooperationExceptionData[]
}

/** Port of `assertEquivalent` — compares exception trees by message/type/source, recursively. */
export function assertEquivalent(
    expected: CooperationExceptionData[],
    actual: CooperationException[],
): void {
    assert.equal(
        actual.length,
        expected.length,
        `Sizes don't match - expected ${JSON.stringify(expected)}, but got ${actual.map(e => e.message)}`,
    )
    for (let i = 0; i < expected.length; i++) {
        const expectedItem = expected[i]!
        const actualItem = actual[i]!
        assert.equal(
            actualItem.message,
            expectedItem.message,
            `Messages don't match - expected ${expectedItem.message}, but got ${actualItem.message}`,
        )
        assert.equal(
            actualItem.type,
            expectedItem.type,
            `Types don't match - expected ${expectedItem.type}, but got ${actualItem.type}`,
        )
        assert.equal(
            actualItem.source,
            expectedItem.source,
            `Sources don't match - expected ${expectedItem.source}, but got ${actualItem.source}`,
        )
        assertEquivalent(expectedItem.causes ?? [], actualItem.causes)
    }
}

/** Port of `DistributedCoroutineIdentifier.asSource()`. */
export function asSource(identifier: DistributedCoroutineIdentifier): string {
    return `${identifier.name}[${identifier.instance}]`
}
