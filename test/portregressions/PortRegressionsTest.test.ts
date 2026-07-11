import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { describe, test } from "node:test"
import { saga } from "../../src/coroutine/builder/SagaBuilder.js"
import { MessageEventRepository } from "../../src/coroutine/structuredcooperation/MessageEventRepository.js"
import { transactional } from "../../src/coroutine/TransactionRunner.js"
import { eventLoopStrategy } from "../../src/messaging/HandlerRegistry.js"
import { PostgresTopicNotifier } from "../../src/node/PostgresTopicNotifier.js"
import { Scoop } from "../../src/Scoop.js"
import { rollbackPathTimeout } from "../../src/coroutine/eventloop/deadline/RollbackPathDeadline.js"
import { nowMillis, setClock } from "../../src/util/Clock.js"
import { setupScoopTest, waitUntil } from "../support/harness.js"
import { CountDownLatch, sleep } from "../support/latch.js"

const h = setupScoopTest()

/**
 * Port-added regression tests (no Kotlin counterpart) guarding the port-found bugs recorded in
 * DECISIONS.md. Each test constructs the exact failure condition deterministically, so a
 * regression fails outright instead of resurfacing as a soak flake.
 */
describe("PortRegressionsTest", () => {
    test("engine inserts keep created_at strictly increasing per lineage", async () => {
        // DECISIONS.md "created_at is strictly increasing per cooperation lineage": the
        // step-window CTEs compare same-lineage events with strict < on created_at, so an
        // emission and its SUSPENDED mark landing in the same microsecond make the emission
        // vanish from "emitted in latest step" (latent in the Kotlin original;
        // scripts/scratch-tie-repro.ts). Seeding the lineage max AHEAD of the wall clock forces
        // the tie condition: without the GREATEST(+1µs) guard, the next engine insert would
        // land at clock_timestamp(), at or before the seeded row.
        const lineage = [randomUUID()]
        const lin = `{${lineage.join(",")}}`
        const messageId = randomUUID()
        await h.sql`INSERT INTO message (id, topic, payload) VALUES (${messageId}, 'tie-topic', '{}')`
        const [seeded] = await h.sql`
            INSERT INTO message_event (message_id, type, cooperation_lineage, created_at)
            VALUES (${messageId}, 'EMITTED', ${lin}::uuid[],
                    clock_timestamp() + interval '200 milliseconds')
            RETURNING created_at::text AS created_at`

        const repository = new MessageEventRepository(h.jsonbHelper)
        await transactional(h.sql, connection =>
            repository.insertMessageEvent(
                connection,
                messageId,
                "SUSPENDED",
                "tie-handler",
                randomUUID(),
                "0",
                lineage,
                null,
                null,
                null,
                null,
            ),
        )

        const [row] = await h.sql`
            SELECT (created_at = ${seeded!.created_at}::text::timestamptz
                    + interval '1 microsecond') AS nudged
            FROM message_event
            WHERE cooperation_lineage = ${lin}::uuid[] AND type = 'SUSPENDED'`
        assert.equal(
            row!.nudged,
            true,
            "engine insert must land exactly 1µs after the lineage max, never tie with it",
        )
    })

    test("rollback window keeps microsecond precision through the suspendedAt bind", async () => {
        // DECISIONS.md "Timestamp parameters must be bound through ::text": a JS string bound
        // directly at a ::timestamptz placeholder is serialized through a JS Date, truncating
        // microseconds to milliseconds. The EMITTED row and the SUSPENDED boundary here share a
        // millisecond (.820) and differ only in microseconds — a truncated boundary lands
        // before the emission, the window comes back empty, and zero ROLLBACK_EMITTED rows are
        // written (port-specific; scripts/scratch-window-repro.ts).
        const lineage = [randomUUID(), randomUUID()]
        const lin = `{${lineage.join(",")}}`
        const childMsg = randomUUID()
        const grandMsg = randomUUID()
        await h.sql`
            INSERT INTO message (id, topic, payload)
            VALUES (${childMsg}, 'child-topic', '{}'), (${grandMsg}, 'grandchild-topic', '{}')`
        const suspendedAt = "2026-01-01 12:00:00.820963+00"
        const rows: Array<[string, string, string, string]> = [
            [grandMsg, "EMITTED", "1", "2026-01-01 12:00:00.820026+00"],
            [childMsg, "SUSPENDED", "1", suspendedAt],
        ]
        for (const [messageId, type, step, createdAt] of rows) {
            await h.sql`
                INSERT INTO message_event
                    (message_id, type, coroutine_name, coroutine_identifier, step,
                     cooperation_lineage, created_at)
                VALUES (${messageId}, ${type}::message_event_type, 'child-handler',
                        ${randomUUID()}, ${step}, ${lin}::uuid[], ${createdAt}::text::timestamptz)`
        }

        const repository = new MessageEventRepository(h.jsonbHelper)
        await transactional(h.sql, connection =>
            repository.insertRollbackEmittedEventsForStep(
                connection,
                lineage,
                "child-handler",
                randomUUID(),
                "1",
                {
                    message: "regression probe",
                    type: "TestFailure",
                    source: "PortRegressionsTest",
                    stackTrace: [],
                    causes: [],
                },
                null,
                suspendedAt,
            ),
        )

        const emitted = await h.sql`
            SELECT message_id FROM message_event
            WHERE type = 'ROLLBACK_EMITTED' AND cooperation_lineage = ${lin}::uuid[]`
        assert.equal(
            emitted.length,
            1,
            "the same-millisecond emission must fall inside the rollback window",
        )
        assert.equal(emitted[0]!.message_id, grandMsg)
    })

    test("a message published after ready() is delivered without the reconcile safety net", async () => {
        // DECISIONS.md "Subscriptions expose ready()": a NOTIFY fired before the LISTEN
        // registration round-trip completes is lost, and once the worker's startup-armed
        // reconcile window drains, only the safety net would repair it. The net is set far past
        // the latch here, so delivery can only come from the NOTIFY that the awaited ready()
        // guarantees an active LISTEN for.
        const [epochRow] = await h.sql`SELECT clock_timestamp()::text AS db_now`
        const epoch = epochRow!.db_now as string
        const scoop = Scoop.create(h.sql, {
            topicNotifier: new PostgresTopicNotifier(h.sql),
            reconcileSafetyNetMillis: 600_000,
            ignoreMessagesOlderThan: epoch,
        })
        const latch = new CountDownLatch(1)
        let subscription
        try {
            await scoop.ready()
            subscription = scoop.messageQueue.subscribe(
                "ready-topic",
                saga("ready-handler", eventLoopStrategy(scoop.messageQueue, epoch), b => {
                    b.step({
                        invoke: () => {
                            latch.countDown()
                        },
                    })
                }),
            )
            await subscription.ready()
            // Let the startup-armed reconcile window drain (QUIET_TICKS empty passes) before
            // publishing, so the tick path cannot mask a missing LISTEN.
            await sleep(500)
            await transactional(h.sql, connection =>
                scoop.messageQueue.launch(connection, "ready-topic", {}),
            )
            assert.ok(
                await latch.await(10_000),
                "message published right after ready() was not delivered via NOTIFY",
            )
        } finally {
            await subscription?.close()
            await scoop.close()
        }
    })

    test("ignoreOlderThan anchored to the database clock survives client-clock skew", async () => {
        // DECISIONS.md "Tests anchor ignoreOlderThan to the database clock": with the client
        // clock AHEAD of the database clock, a client-anchored cutoff is later than the
        // created_at of a message launched immediately afterwards — the message is "older than"
        // the cutoff and its SEEN is never created (latent in the Kotlin original, which
        // anchors to the client clock). A DB-anchored cutoff is immune; +1s of injected skew —
        // far beyond real calibration error — must change nothing.
        const baseOffset = nowMillis() - Date.now()
        setClock({ nowMillis: () => Date.now() + baseOffset + 1_000 })
        const latch = new CountDownLatch(1)
        try {
            const [epochRow] = await h.sql`SELECT clock_timestamp()::text AS db_now`
            const epoch = epochRow!.db_now as string
            const subscription = await h.subscribe(
                "skew-topic",
                saga("skew-handler", eventLoopStrategy(h.messageQueue, epoch), b => {
                    b.step({
                        invoke: () => {
                            latch.countDown()
                        },
                    })
                }),
            )
            try {
                await transactional(h.sql, connection =>
                    h.messageQueue.launch(connection, "skew-topic", {}),
                )
                assert.ok(
                    await latch.await(10_000),
                    "message was ignored under +1s client-clock skew despite the DB-anchored cutoff",
                )
            } finally {
                await subscription.close()
            }
        } finally {
            setClock({ nowMillis: () => Date.now() + baseOffset })
        }
    })

    test("a missed rollback-path deadline produces ROLLBACK_FAILED", async () => {
        // DECISIONS.md "rollback deadline key mismatch": the deadline element used to serialize
        // under 'RollbackPathDeadlineKey' while the give-up SQL checks 'RollbackDeadlineKey',
        // so rollback-path deadlines could never fire (latent in the Kotlin original — which
        // has zero rollback-deadline coverage — and inherited by the port until fixed). With an
        // already-expired rollback deadline attached at launch, the first rollback resume must
        // give up: ROLLBACK_FAILED, no rollback lambda ever runs.
        let rollbackRan = false
        const failing = new Error("boom")
        const subscription = await h.subscribe(
            "rb-deadline-topic",
            saga("rb-deadline-handler", eventLoopStrategy(h.messageQueue, h.strategyEpoch), b => {
                b.step({
                    invoke: () => {},
                    rollback: () => {
                        rollbackRan = true
                    },
                })
                b.step({
                    invoke: () => {
                        throw failing
                    },
                })
            }),
        )
        try {
            const root = await transactional(h.sql, connection =>
                h.messageQueue.launch(
                    connection,
                    "rb-deadline-topic",
                    {},
                    rollbackPathTimeout(0, "regression test"),
                ),
            )
            await waitUntil(
                async () =>
                    (
                        await h.sql`
                            SELECT 1 FROM message_event
                            WHERE type = 'ROLLBACK_FAILED' AND message_id = ${root.message.id}`
                    ).length > 0,
                10_000,
                "ROLLBACK_FAILED for the expired rollback deadline",
            )
            assert.ok(
                !rollbackRan,
                "give-up must preempt the rollback path — no rollback lambda may run",
            )
            const [row] = await h.sql`
                SELECT string_agg(exception::text, '') AS exceptions
                FROM message_event
                WHERE message_id = ${root.message.id} AND exception IS NOT NULL`
            assert.ok(
                String(row!.exceptions).includes("MissedRollbackDeadline"),
                "the failure must be attributed to the missed rollback deadline",
            )
        } finally {
            await subscription.close()
        }
    })
})
