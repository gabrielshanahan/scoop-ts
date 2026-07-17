# Release Notes

All notable changes to scoop-ts are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## Unreleased

## v0.6.0 — 2026-07-17

### Changed

- **The drain's quiet tail is one pass, not three.** The drain loops until empty within a
  tick, so the only cross-tick race a retry must cover is a run hidden behind FOR UPDATE SKIP
  LOCKED — one follow-up pass suffices. At three, an idle fleet paid three confirm-empty
  candidate_seens scans per wake (the bulk of abo-uat's residual 2–3 scans/s after v0.6.0).

- **The drain is gated like the reconcile.** The pending-coroutine-run query (candidate_seens —
  Scoop's most expensive statement) used to run on EVERY tick of every worker; on an idle fleet
  of dozens of workers those scans were the database's single biggest steady load (abo-uat:
  3.5 average active sessions, individual scans 10s+ under pressure). The ReconcileGate now
  carries a drain-side twin: a tick drains only when a notification arrived, a reconcile pass
  inserted continuations, or the previous drain resumed something — plus the same safety-net
  sweep, which alone bounds the two signals Postgres never notifies about (cross-topic child
  commits and time-based wakeups; worst-case added latency = the sweep interval, default 30s).
  Direct `tick()` callers and `ReconcileGate.ALWAYS` are unaffected.

### Added

- Lint/format gate: Biome (single dev dependency) wired into `npm run lint` / `npm run format`
  and both workflows, configured to the existing house style (4-space indent, 100 columns,
  double quotes, no semicolons). Mirrors the Kotlin repo's ktfmt+detekt gate.

### Changed

- Dependency majors adopted (full gate green on each): typescript 7, pino 10,
  @testcontainers/postgresql 12. `@types/node` majors are dependabot-ignored — types track the
  oldest supported runtime (`engines: >=22`), not the newest.

## v0.5.0 — 2026-07-11

### Fixed

- **Rollback-path deadlines now actually fire.** The deadline element serialized under
  `RollbackPathDeadlineKey` while the give-up SQL (and the V2 partial index) check
  `RollbackDeadlineKey`, so a rollback exceeding its deadline just kept running — a latent bug
  inherited from the Kotlin original, now fixed in both repos: the key serializes as
  `RollbackDeadlineKey`. No migration needed (the existing V2 index already matches). Rows
  written by older versions keep the old key and remain unenforced, exactly as before.

### Added

- Port-added regression tests (`test/portregressions/`) deterministically reconstructing the
  five port-found bugs: same-lineage `created_at` microsecond ties, `::timestamptz` bind
  truncation, the subscribe/LISTEN registration window, client-clock-skew stalls of the
  `ignoreOlderThan` cutoff, and never-firing rollback-path deadlines. See PORT-LEDGER.md
  "Port-added regression tests".
- CI (`build.yml`: typecheck, ledger reconciliation, full suite against Postgres, publishable
  artifact check) and manual release workflow (`release.yml`: version bump, test, npm publish
  with provenance, release-notes stamp, tag), mirroring the Kotlin repo's setup.

## v0.4.0 — 2026-07-11

Initial release of the TypeScript port of [Scoop](https://github.com/gabrielshanahan/scoop)
(Kotlin/JVM), published as `scoop-ts` on npm. Versioning starts at 0.4.0 to match the original
at the time of the port.

### Added

- Full 1:1 port of scoop-core plus the dissolved scoop-quarkus module (composition root,
  `sql.begin()` transactions, postgres.js LISTEN/NOTIFY) — see DECISIONS.md for every stack
  mapping and deliberate divergence, PORT-LEDGER.md for the file-by-file inventory.
- Complete test parity: all 195 reference `@Test` methods ported and passing against real
  Postgres. Zero-flakiness proven via a 2-hour whole-file soak (535 iterations), a per-test
  soak of the full inventory, and 20 consecutive full-suite runs in mixed declaration/shuffled
  order (PORT-LEDGER.md "Stability proof").

### Fixed

- Two latent engine bugs present in the Kotlin original: same-lineage `created_at` microsecond
  ties silently skipping children's rollbacks (fixed with strictly-increasing per-lineage
  `created_at`), and the client-clock-anchored `ignoreOlderThan` cutoff permanently ignoring
  fresh messages under clock skew (fixed with DB-clock anchoring + a calibration helper).
- Port-specific: postgres.js `::timestamptz` binds truncating microseconds (routed through
  `::text`), LISTEN teardown unhandled rejections, and NOTIFY fan-out phase-locking
  same-topic workers on Node's single event loop (0–10ms dispatch jitter).
