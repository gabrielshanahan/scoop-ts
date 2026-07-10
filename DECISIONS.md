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

- **Clock calibration against the database** (`src/node/calibrateClock.ts`): the engine's
  authoritative clock is Postgres (`CLOCK_TIMESTAMP()`), but `HandlerRegistry.eventLoopStrategy()`
  bakes a client-clock `ignoreOlderThan` cutoff into the readiness SQL. When the client clock runs
  ahead of the DB clock (Docker VM drift — observed transiently at several hundred ms), a message
  launched right after strategy creation gets `created_at < ignoreOlderThan` and is ignored
  forever. Proven experimentally: +1s injected skew stalls every saga; calibration fixes it. The
  Kotlin original implicitly assumes host≈DB time; the test harness calibrates the injected clock
  at startup and the calibration helper ships in `src/node` for production use.

- **Clock injection**: engine logic never reads wall-clock directly; a `Clock` interface is injected
  (`SystemClock` in production, controllable in tests) — this is a divergence from the original,
  which calls `OffsetDateTime.now()` / `CLOCK_TIMESTAMP()`; DB-side `CLOCK_TIMESTAMP()` defaults are
  retained (they are part of the schema contract).
- **Test isolation**: one Postgres container per suite run (or an external `DATABASE_URL`),
  migrations applied once, `TRUNCATE message_event, message, return_value` before each test (the
  original's `@BeforeEach` does the same), test files run sequentially.
- **NOTIFY fan-out dispatch jitter** (`PostgresTopicNotifier`): each callback is dispatched with
  a 0–10ms random delay. The original dispatches onto per-callback virtual threads whose
  scheduling naturally desynchronizes sibling workers; a same-microtask dispatch on Node's single
  event loop leaves same-topic workers phase-locked, and their armed reconcile passes can then
  collide on the parent SEEN row's FOR UPDATE SKIP LOCKED every tick until the loser's gate goes
  quiet — observed once as a 10s stall (the 30s safety net is the designed backstop, but the
  original's gate sizing assumes scheduler jitter). The jitter restores that assumption.
- **Fixed sleeps that gate a mutation became condition polls**: three original tests wait
  `latch + Thread.sleep(100)` and then issue a cancel/rollback request that is only honoured
  "after everything has finished running". Under load the 100ms settle races the final commits
  (observed once under Docker disk pressure; the race exists in the Kotlin original too). The
  ports wait for the exact precondition instead — both sagas COMMITTED — via bounded polling
  (`waitUntil`); every assertion is unchanged.
- **Fixed settle-sleeps before terminal-state assertions became terminal-quiescence polls**
  (`eventLogSettled`): the original's pattern `latch.await()` + `Thread.sleep(100..300)` +
  assert-on-event-log races the writes that land on the tick *after* the last step's latch fires
  (the root's COMMITTED, parent rollback completion). Per-test soak runs reproduced this — the
  actual log was missing exactly the final `COMMITTED`/`ROLLED_BACK` rows — and the Kotlin
  original documents the same fragility ("some tests may be flakey... increase Thread.sleep").
  The port polls until every started saga run (`SEEN`/`ROLLING_BACK` lineage) has a terminal
  event (`COMMITTED`/`ROLLED_BACK`/`ROLLBACK_FAILED`), timing out silently so a genuine mismatch
  still fails through the test's own assertion diff. Assertions are unchanged. Exceptions, kept
  as fixed sleeps: workload sleeps inside step bodies, poll intervals inside explicit wait loops,
  `StubHandlerBlockingTest` (its sagas never terminate — the assertions are about *absence* of
  progress), and `PostgresMessageQueueTest`'s trailing post-assertion hygiene sleeps.

## The scoop-quarkus main-file mapping (concrete)

| Kotlin file | TS counterpart |
|---|---|
| `ScoopProducer.kt` | `src/Scoop.ts` — `Scoop.create(sql, options)` composition root (options carry the `@ConfigProperty` knobs: tickIntervalMillis, reconcileSafetyNetMillis, retryBackoffBase/MaxMillis; the CDI disposer becomes `Scoop.close()`) |
| `JtaTransactionRunner.kt` | `src/coroutine/TransactionRunner.ts` — the single `PostgresTransactionRunner` (`sql.begin()`) |
| `PgSubscriberProducer.kt` | dissolved — postgres.js owns the LISTEN connection, reconnection and demux; no wiring needed |
| `PgSubscriberTopicNotifier.kt` | `src/node/PostgresTopicNotifier.ts` (one LISTEN per topic + fan-out to registered callbacks; `queueMicrotask` replaces per-callback virtual threads) |
| `QuarkusCooperationContextCustomizer.kt` | dissolved — context (de)serialization lives in `src/coroutine/context/CooperationContextModule.ts` and is always active via `JsonbHelper` |

## Divergences from literal translation

(kept current as the port proceeds)

- **Injectable clock** (`src/util/Clock.ts`): all wall-clock reads (`OffsetDateTime.now()`,
  `System.currentTimeMillis()` in UuidV7, deadline/sleep helpers, HandlerRegistry strategy
  creation) go through a swappable module-level clock so tests control time. Millisecond
  precision (JS clocks expose no more); load-bearing time decisions happen in Postgres
  (`CLOCK_TIMESTAMP()`), unchanged.
- **Timestamps as ISO strings**: `OffsetDateTime`/`Instant` fields in context elements
  (deadlines, SleepUntil) are ISO-8601 strings; they serialize into context JSON identically and
  are compared via epoch parsing. `suspendedAt` (used to scope rollbacks to a tick) is carried as
  the exact text Postgres produced — `finalSelect` casts it `::text` — and passed back with a
  `::timestamptz` cast, so microsecond precision survives (a JS `Date` would truncate to ms and
  break `created_at < :suspendedAt` comparisons).
- **MappedKey carries name + reviver**: Kotlin derives the context key's serialized name from the
  class simple name and the element type from generics reflection. TS has neither, so
  `new MappedKey(name, reviveElement)` takes both explicitly. Same for `Topic`, `VariableName`,
  and `Handler` names. `VariableName` keeps its polymorphic `_type` discriminator via a
  registry + `toJSON`.
- **Preserved quirk — rollback deadline key mismatch**: the original's rollback deadline element
  serializes under `RollbackPathDeadlineKey` (class simple name) while the generated give-up SQL
  checks `RollbackDeadlineKey`. Reproduced as-is; do not "fix" without also changing the ref repo.
- **Context deserialization via JSON.parse + re-stringify**: the Kotlin module reconstructs each
  top-level value's text from the token stream; here each value is `JSON.parse`d and re-emitted
  with `JSON.stringify`, which is canonical w.r.t. this codec's own output. Caveat: objects with
  *numeric-like keys* would be reordered by JS semantics (never exercised by the reference tests).
- **`CooperationContextMap` drops the nullable ObjectMapper**: revivers live on keys; mapperless
  maps in the original always have an empty serialized map, so behavior is identical. Kotlin
  HashMap key semantics (mapped keys by singleton identity, unmapped by name) are reproduced via
  an identity token.
- **SagaBuilder overloads → two methods**: Kotlin's lambda-arity overloads become `step(spec)`
  (simple; invoke returns void ⇒ Continue, handleChildFailures keeps the incoming nextStep) and
  `controlledStep(spec)` (stepIteration-aware, NextStep-returning). `tryFinallyStep`,
  `sleepForStep`, `scheduledStep`, `periodic` become free functions taking the builder.
- **Sealed hierarchies → tagged classes**: `RollbackState`'s marker interfaces (`Me.RollingBack`,
  `ThrowableExists`) become the `meRollingBack` flag and presence of `throwable`;
  `SuspensionState`, `NextStep`, `ContinuationResult`, `LastStepResult` are kind-tagged unions.
- **Threads → event-loop primitives**: the per-worker single-thread executor + semaphore becomes
  a `setTimeout` chain (fixed-delay semantics) with a boolean tick gate (JS is single-threaded,
  so check-and-set is atomic); `markRollingBack/markRollbackFailed…InSeparateTransaction`'s
  spawn-thread-and-join becomes awaiting a separate pool transaction. `PeriodicTick.close()`
  awaits the in-flight tick bounded by 10s (there is no interrupt fallback on this runtime).
- **Named SQL parameters**: FluentJDBC `:name` parameters are preserved textually and converted
  to positional `$n` at execution (`src/sql.ts`), keeping the ported SQL diffable against the
  original. `uuid[]`/jsonb parameters pass as literals/JSON text with explicit casts.
- **Suppressed exceptions**: JS has no suppressed-exception mechanism; `ScoopException` and
  `CooperationException` carry an explicit `suppressed` array with the same aggregation rules.

- **Kotlin `object` singletons** (e.g. `Topic` subclasses, `Handler` objects in tests) become
  module-level `const` instances or classes instantiated once; behavior identical.
- **Extension functions** become plain exported functions taking the receiver as first argument, or
  methods where they were conceptually part of the type.
- **Sealed classes** become discriminated unions or class hierarchies with exhaustive `switch`;
  choice per case is noted in code comments only where non-obvious.
- **`PGobject`** (JDBC's JSONB carrier) — postgres.js returns JSONB columns as parsed JS values
  already; where the Kotlin API surface exposes `PGobject` (e.g. return values), the TS API exposes
  the parsed JSON value.
