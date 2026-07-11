/**
 * Synthetic reproduction of the created_at microsecond-tie readiness bug (scratch; not shipped).
 *
 * Scenario (two-level distillation of the failing n-deep tests): a parent suspended in its
 * "rolling back child scopes" rollback step must NOT be ready to resume until its (COMMITTED)
 * child writes ROLLING_BACK and then ROLLED_BACK. The ROLLBACK_EMITTED and SUSPENDED rows are
 * written back-to-back in the same transaction; childRollbackEmissionsInLatestStep requires
 * `rollback_emissions.created_at < latest_suspended.created_at` (strict). If CLOCK_TIMESTAMP()
 * returns the same microsecond for both inserts, the emission vanishes from the CTE and the
 * parent becomes (wrongly) ready.
 */

import { randomUUID } from "node:crypto"
import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"
import { PostgreSqlContainer } from "@testcontainers/postgresql"
import postgres from "postgres"
import {
    ROLLING_BACK_CHILD_SCOPES_STEP_SUFFIX,
    ROLLING_BACK_PREFIX,
} from "../src/coroutine/continuation/RollbackPathContinuation.js"
import { DistributedCoroutineIdentifier } from "../src/coroutine/DistributedCoroutineIdentifier.js"
import { NO_CHILD_FAILURE } from "../src/coroutine/eventloop/SuspensionState.js"
import { StandardEventLoopStrategy } from "../src/coroutine/eventloop/strategy/StandardEventLoopStrategy.js"
import {
    buildSql,
    candidateSeensWaitingToBeProcessed,
} from "../src/coroutine/structuredcooperation/PendingCoroutineRunSql.js"
import { JsonbHelper } from "../src/JsonbHelper.js"
import { applyMigrations } from "../src/node/migrations.js"
import { queryNamed } from "../src/sql.js"
import { nowIso } from "../src/util/Clock.js"
import { SqlTestUtils } from "../test/support/SqlTestUtils.js"

const root = join(dirname(fileURLToPath(import.meta.url)), "..")
const container = await new PostgreSqlContainer("postgres:15").start()
const sql = postgres(container.getConnectionUri(), { max: 5 })
await applyMigrations(sql, join(root, "db", "migration"))

const utils = new SqlTestUtils(sql, new JsonbHelper())

const rootTopic = "root-topic"
const childTopic = "child-topic"
const rootHandler = "root-handler"
const childHandler = "child-handler"

const rootIdent = new DistributedCoroutineIdentifier(rootHandler)
const childIdent = new DistributedCoroutineIdentifier(childHandler)

function step(ident: DistributedCoroutineIdentifier, stepName: string) {
    return {
        stepName,
        stepIteration: 0,
        childFailureHandlerIteration: NO_CHILD_FAILURE,
        distributedCoroutineIdentifier: ident,
    }
}

const childScopeRollbackStep = `${ROLLING_BACK_PREFIX} 0[0,]${ROLLING_BACK_CHILD_SCOPES_STEP_SUFFIX}`

const registryStrategy = new StandardEventLoopStrategy(
    nowIso(),
    () =>
        new Map([
            [rootTopic, [rootHandler]],
            [childTopic, [childHandler]],
        ]),
)
const waiting = candidateSeensWaitingToBeProcessed(registryStrategy)

async function ready(name: string): Promise<boolean> {
    const rows = await queryNamed(sql, buildSql(waiting), {
        coroutine_name: name,
    })
    return rows.length > 0
}

async function buildScenario(): Promise<{
    emittedId: string
    suspendedId: string
}> {
    await sql`TRUNCATE TABLE message_event, message, return_value CASCADE`
    const rootScope = [randomUUID()]
    const childScope = [...rootScope, randomUUID()]
    const throwable = new Error("A thing happened")

    const rootMessage = await utils.createSimpleMessage(rootTopic)
    await utils.emitted(rootMessage.id)
    await utils.seen(rootMessage.id, rootIdent, rootScope)

    const childMessage = await utils.createSimpleMessage(childTopic)
    await utils.emitted(childMessage.id, step(rootIdent, "0"), rootScope)
    await utils.suspended(rootMessage.id, step(rootIdent, "0"), rootScope)

    await utils.seen(childMessage.id, childIdent, childScope)
    await utils.suspended(childMessage.id, step(childIdent, "0"), childScope)
    await utils.committed(childMessage.id, step(childIdent, "0"), childScope)

    // Parent enters rollback and emits rollbacks for its child scope, then suspends — the
    // engine writes these in one transaction, microseconds apart.
    await utils.rollingBack(rootMessage.id, step(rootIdent, "1"), rootScope, throwable)
    const emittedId = await utils.rollbackEmitted(
        childMessage.id,
        step(rootIdent, childScopeRollbackStep),
        rootScope,
        throwable,
    )
    const suspendedId = await utils.suspended(
        rootMessage.id,
        step(rootIdent, childScopeRollbackStep),
        rootScope,
    )
    return { emittedId, suspendedId }
}

// Control: normal timestamps (strictly increasing) — parent must NOT be ready (child has no
// ROLLING_BACK yet).
{
    await buildScenario()
    const isReady = await ready(rootHandler)
    console.log(`CONTROL (distinct created_at): root ready = ${isReady} (expected false)`)
}

// Tie: force ROLLBACK_EMITTED.created_at == SUSPENDED.created_at.
{
    const { emittedId, suspendedId } = await buildScenario()
    await sql`
        UPDATE message_event
        SET created_at = (SELECT created_at FROM message_event WHERE id = ${emittedId})
        WHERE id = ${suspendedId}
    `
    const isReady = await ready(rootHandler)
    console.log(`TIE (created_at equal): root ready = ${isReady} (bug if true)`)
}

await sql.end()
await container.stop()
