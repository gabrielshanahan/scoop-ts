# DECISIONS

Every deliberate divergence from a literal translation of the Kotlin/JVM original, every stack
mapping, and every added dependency is recorded here with its rationale. The reference source of
truth is the Kotlin repo at `gabrielshanahan/scoop` (checked out read-only during the port).

## Stack mappings

| JVM concern | Kotlin implementation | TypeScript implementation | Rationale |
|---|---|---|---|
| Postgres access | FluentJDBC over JDBC | `postgres` (postgres.js) tagged-template SQL | Direct analog: raw parameterized SQL, built-in pool, `sql.begin()` transactions, native LISTEN/NOTIFY. No ORM, no query builder. |
| JSON/JSONB | Jackson + `PGobject` | native `JSON` + postgres.js JSONB support | See "JSONB and CooperationContext serialization" below. |
| Logging | SLF4J | pino | Thin, structured, standard. |
| Migrations | Flyway (Quarkus) / manual (core) | V1..V5 `.sql` carried over verbatim + tiny runner (`src/node/migrations.ts`) | The migrations are plain Postgres SQL; a dbmate-style version-tracking runner (~50 lines) avoids a dependency. |
| Transactions | `TransactionRunner` seam: plain-DataSource impl (core) and JTA impl (quarkus) | `TransactionRunner` seam with ONE postgres.js impl wrapping `sql.begin()` | Same seam as the original; there is no JTA equivalent (and no need — postgres.js transactions are the only transaction source). |
| Blocking threads | Threads + `Thread.sleep` + scheduled executors | single-threaded event loop, `async`/`await`, `setTimeout`-based scheduling behind an injectable clock/scheduler | Execution-model port: Scoop is IO-bound coordination; every DB op becomes `await`. No worker_threads anywhere in the library. |
| UUIDv7 | custom `UuidV7` generator (time-ordered, load-bearing ordering) | ported precisely (`src/util/UuidV7.ts`), monotonic within-process like the original | Ordering semantics are load-bearing for the engine. |
| Tests | JUnit 5 + Quarkus Test + Testcontainers | `node:test` + `@testcontainers/postgresql` | node:test is zero-dep and sufficient; every test runs against real Postgres, no mocks. |

## Dependencies (each justified)

- **postgres** (postgres.js) — the locked choice for raw SQL, pooling, transactions, LISTEN/NOTIFY.
- **pino** — logging facade replacing SLF4J.
- **@sinclair/typebox** (0.34.x, dev) — available for schema-validated decoding where the original
  relied on Jackson typed deserialization; only used where genuinely needed.
- **typescript**, **@types/node**, **tsx** (dev) — compiler, Node types, TS execution for tests and
  scripts (`node --import tsx --test`). `tsc --noEmit` stays the type gate in CI.
- **@testcontainers/postgresql** (dev) — one real Postgres per suite run, mirroring the Quarkus
  Testcontainers setup.

Nothing else. No ORM, no query builder, no DI container, no coroutine/green-thread library.

## The scoop-quarkus question

The Quarkus module's five main files each dissolve as follows (verified against the actual code):

| Kotlin file | Resolution |
|---|---|
| `ScoopProducer.kt` (CDI producers for all Scoop beans) | Plain composition root: core's `Scoop.create(...)` factory builds the same object graph. No DI container. |
| `JtaTransactionRunner.kt` | Collapsed into the single core `TransactionRunner` implementation wrapping `sql.begin()`. JTA's job (transaction demarcation for CDI beans) has no analog; postgres.js transactions are the only transaction source. |
| `PgSubscriberProducer.kt` (Vert.x PgSubscriber wiring) | postgres.js `sql.listen()` — connection management, reconnection and channel demux are provided by postgres.js itself. Lives in `src/node/` as the LISTEN/NOTIFY `TopicNotifier`. |
| `PgSubscriberTopicNotifier.kt` | `src/node/PostgresTopicNotifier.ts` implementing core's `TopicNotifier` over `sql.listen()`. |
| `QuarkusCooperationContextCustomizer.kt` (Jackson ObjectMapper customization) | Context (de)serialization lives entirely in core (`coroutine/context/`); module registration happens at `Scoop.create` / context-serializer construction. No framework hook needed. |

**Where the quarkus tests land:** the quarkus test suite is the real behavioral specification, so
ALL of it is ported into this package's single test tree (`test/`), mirroring the Kotlin package
paths. Tests that asserted JVM/Quarkus-specific mechanisms assert the equivalent TS-stack
guarantee instead; the mapping for each is recorded in PORT-LEDGER.md notes:

- `JtaAtomicityTest` → asserts `sql.begin()` atomicity (same scenarios: handler work + message emission commit or roll back together).
- `PgSubscriberTopicNotifierTest` → asserts postgres.js LISTEN/NOTIFY delivery.
- `ShutdownSpamReproTest` (Quarkus shutdown racing scoop ticks) → asserts clean `close()` behavior of the TS composition root under the same race.

## Determinism decisions

- **Clock injection**: engine logic never reads wall-clock directly; a `Clock` interface is injected
  (`SystemClock` in production, controllable in tests) — this is a divergence from the original,
  which calls `OffsetDateTime.now()` / `CLOCK_TIMESTAMP()`; DB-side `CLOCK_TIMESTAMP()` defaults are
  retained (they are part of the schema contract).
- **Test isolation**: one Postgres container per suite run, migrations applied once, `TRUNCATE
  message_event, message, return_value` before each test (the original's `@BeforeEach` does the
  same), test files run sequentially.

## Divergences from literal translation

(kept current as the port proceeds)

- **Kotlin `object` singletons** (e.g. `Topic` subclasses, `Handler` objects in tests) become
  module-level `const` instances or classes instantiated once; behavior identical.
- **Extension functions** become plain exported functions taking the receiver as first argument, or
  methods where they were conceptually part of the type.
- **Sealed classes** become discriminated unions or class hierarchies with exhaustive `switch`;
  choice per case is noted in code comments only where non-obvious.
- **`PGobject`** (JDBC's JSONB carrier) — postgres.js returns JSONB columns as parsed JS values
  already; where the Kotlin API surface exposes `PGobject` (e.g. return values), the TS API exposes
  the parsed JSON value.
