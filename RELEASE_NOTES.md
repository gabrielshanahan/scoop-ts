# Release Notes

All notable changes to scoop-ts are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/).

## Unreleased

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
