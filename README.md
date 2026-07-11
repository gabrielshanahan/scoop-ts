# Scoop (TypeScript)

A faithful TypeScript port of [Scoop](https://github.com/gabrielshanahan/scoop) — a
proof-of-concept implementation of **structured cooperation** for distributed, message-based
systems. Scoop implements its own persisted continuation/saga machinery on top of Postgres: saga
state lives in the append-only `message_event` log, coordination happens via SQL
(`FOR UPDATE SKIP LOCKED`, LISTEN/NOTIFY), and the core rule is:

> A message handler can be broken down into consecutive steps. A subsequent step doesn't start
> executing until all handlers of all messages emitted in the previous step have finished.

For the concepts (sagas, cooperation lineage, the event model, EventLoopStrategy), read the
original repo's README and the blog posts it links — this port mirrors that codebase 1:1 (same
component names, same SQL, same semantics). Every deliberate divergence is recorded in
[DECISIONS.md](DECISIONS.md); the file-by-file and test-by-test mapping is in
[PORT-LEDGER.md](PORT-LEDGER.md).

## Stack

- **Runtime**: Node.js ≥ 22 (written runtime-agnostically; Bun is intended as a drop-in swap)
- **Language**: TypeScript, strict mode (`npm run typecheck`)
- **Postgres**: [postgres.js](https://github.com/porsager/postgres) — raw parameterized SQL,
  built-in pool, `sql.begin()` transactions, native LISTEN/NOTIFY
- **Logging**: pino (level via `SCOOP_LOG_LEVEL`, default `warn`)
- **Migrations**: the original's `V1..V5` SQL files verbatim (`db/migration/`) + a tiny runner

## Usage

```ts
import postgres from "postgres"
import {
    applyMigrations, calibrateClockToDatabase, Scoop, PostgresTopicNotifier,
    saga, eventLoopStrategy, transactional,
} from "scoop"

const sql = postgres(process.env.DATABASE_URL, { max: 20 })
await applyMigrations(sql, "db/migration")
await calibrateClockToDatabase(sql) // align the injected clock with the DB clock

const scoop = Scoop.create(sql, { topicNotifier: new PostgresTopicNotifier(sql) })
const queue = scoop.messageQueue

const subscription = queue.subscribe(
    "order-topic",
    saga("order-processor", eventLoopStrategy(queue), b => {
        b.step({
            invoke: async (scope, message) => {
                await scope.launch("payment-topic", { orderId: message.payload })
            },
            rollback: async (scope, message, error) => {
                /* compensating action */
            },
        })
        b.step({
            invoke: async (scope, message) => {
                /* runs only after ALL payment-topic handlers finished */
            },
        })
    }),
)

await transactional(sql, tx => queue.launch(tx, "order-topic", { hello: "world" }))

// shutdown
await subscription.close()
await scoop.close()
await sql.end()
```

Sleeping/scheduling (`sleepForStep`, `scheduledStep`, `periodic`), try-finally steps
(`tryFinallyStep`), loops (`Repeat`/`GoTo` via `b.controlledStep`), cancellation/rollback requests
(`scoop.capabilities.cancel/rollback`), deadlines (`happyPathTimeout`, …), cooperation context,
and return values (`scope.storeReturnValue`/`getReturnValues`) all work exactly as in the
original; the test suite doubles as usage documentation.

## Installing

```sh
npm install scoop-ts                          # from the npm registry
npm install github:gabrielshanahan/scoop-ts   # straight from git (builds via prepare)
```

Ships compiled ESM with type declarations plus the V1–V5 SQL migrations
(`node_modules/scoop-ts/db/migration`, importable as `scoop-ts/db/migration/*`); apply them with
the exported `applyMigrations(sql, dir)`. Requires Node ≥ 22; `postgres` (postgres.js) and `pino`
come along as dependencies.

## Running the tests

Requires Docker (one PostgreSQL testcontainer is started per run) and Node ≥ 22.

```sh
npm install
npm test                        # full suite (199 tests), sequential, TRUNCATE-isolated
npm test -- --shuffle           # randomized file order (prints the seed)
npm test -- --shuffle=12345     # reproduce a specific order
npm test -- test/coroutine/structuredcooperation/RollbackPathTest.test.ts   # one file
npm run typecheck               # tsc --noEmit
npx tsx scripts/reconcile.ts    # mechanical port-completeness proof against the Kotlin repo
npx tsx scripts/stability-campaign.ts   # 20 full-suite runs (mixed shuffle) against one shared Postgres
```

The suite is the Kotlin suite ported test-for-test: 195 `@Test` methods across 29 files (the
original brief's "219" substring-counts `@TestInstance`/`@TestProfile` annotations; see
PORT-LEDGER.md). JVM-specific tests assert the equivalent guarantee on this stack
(JTA atomicity → `sql.begin()` atomicity; Vert.x LISTEN/NOTIFY → postgres.js LISTEN/NOTIFY).
On top of the ported inventory, `test/portregressions/` adds 4 regression tests that
deterministically reconstruct the port-found bugs (PORT-LEDGER.md "Port-added regression
tests") — 199 tests total.

The suite is deterministic, not just green: a 2-hour whole-file soak of RollbackPathTest
(535 iterations), a per-test soak of all 195 tests (each looped with DB cleanup on the edges),
and 20 consecutive full-suite runs in mixed declaration/shuffled order all passed with zero
flakes. Evidence and the six root-caused fixes behind it are in PORT-LEDGER.md ("Stability
proof") and DECISIONS.md.
