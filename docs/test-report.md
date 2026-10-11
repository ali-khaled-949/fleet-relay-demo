# Fleet Relay verification report

Prepared 2026-10-11 UTC. All inputs and output records are synthetic.

## Results

- **52 automated Python tests passed.** Command: `python -m unittest discover -v`.
  Raw output: `artifacts/test-output.txt`.
- **13 asserted walkthrough checks passed.** Command:
  `python -m scripts.demo --output artifacts/walkthrough.json`.
  The walkthrough uses a real SQLite file, in-process HTTP requests, and a virtual
  clock to advance retry and lease deadlines without waiting.
- Python bytecode compilation passed for `fleet_relay`, `tests`, and `scripts`.
- JavaScript syntax validation passed with `node --check fleet_relay/static/app.js`.
- **15 UI/API interaction checks passed.** Command: `node scripts/ui_smoke.js`.
  This runs the actual application JavaScript in a minimal DOM harness and routes
  requests through a real FastAPI TestClient. Results: `artifacts/ui-qa-results.json`.
  It verifies state/request behavior, not browser rendering.
- Static HTML checks passed for balanced nesting, 70 unique IDs, label/control
  references, application element references, and absence of third-party assets.
- Dashboard HTML and both local asset routes returned HTTP 200 in API tests;
  missing assets returned 404 and the dashboard sent its content-security policy.
- The standalone worker `--once` command initialized a new database and exited
  successfully without pending events.
- OpenAPI schema generation succeeded with six path entries.

These are correctness checks, **not throughput, latency, availability, or scale
benchmarks**. Test elapsed time is only runner output.

## What the tests cover

### Tenant boundaries and idempotency

- Missing/invalid demo credentials, tenant-scoped reads and dispatch, other-tenant
  event IDs returning 404, and body-supplied tenant overrides rejected.
- Same-key/same-body replay preserves identity; a different body returns 409.
- The same key in two tenants creates separate events.
- Twenty concurrent duplicate ingestions create one event/outbox pair; competing
  different payloads have one winner.
- Database foreign keys reject a mismatched tenant receipt.
- Unicode hash serialization remains stable and equivalent timezone values
  normalize before hashing.

### Atomicity, retries, and crashes

- Injected outbox insertion failure rolls back event insertion.
- Injected attempt-log failure rolls back acknowledgment state.
- Concurrent claims do not lease one event twice while its lease remains live.
- Expired claims can be recovered; old acknowledgments and acknowledgments at the
  exact lease boundary are rejected.
- A held SQLite writer advances the clock while another operation waits; lease
  creation and acknowledgment use time sampled after lock acquisition.
- Transient failures respect backoff then succeed; poison events stop after the
  third claim; repeated crashed workers also exhaust the budget.
- An actual child process calls `os._exit(73)` immediately after its receiver
  transaction commits. A later worker recovers the outstanding lease without
  creating another receipt. Lease time is advanced with an injected clock.
- Eight concurrent worker threads dispatch 200 events across two tenants and end
  with 100 delivered events and 100 receipts in each. This tests conservation and
  race handling on one host; it is not a performance claim.
- Receiver work can write through another connection, demonstrating that delivery
  runs outside the claim transaction. Arbitrary receiver exception text is not
  persisted, preventing accidental secret/payload leakage into the attempt log.

### Query, config, and malformed input

- Stable high-watermark cursor paging excludes later insertions and rejects
  cross-tenant, tampered, and out-of-range cursors.
- Concurrent compare-and-swap configuration updates have one winner; API writes
  require a valid ETag and return 412 on stale versions.
- Events capture the configuration version current when they are accepted.
- Invalid field values, naive timestamps, UTC normalization overflow, oversized
  payloads, chunked oversized bodies, nonfinite numbers, unpaired surrogate keys
  and values, and oversized SQL integers are rejected without corrupting data.

## Review findings resolved

An independent code review reproduced malformed-input persistence/serialization
errors and a lease clock-sampling race during database lock waits. Fixes are
covered by `tests/test_review.py`. After the fixes, that review found no remaining
blocking backend correctness issue within this prototype's stated scope. No
formal security audit or penetration test was performed.

## UI interaction coverage

The Node harness verifies empty-state zeros, three-event ingestion, replay without
new events, transient retry recovery, poison-event dead lettering after three
attempts, event-dialog open/close state, ETag-backed saves, stale-write draft
preservation and reload recovery, tenant switching and isolation, cursor paging
and reset, explicit network-error state and refresh recovery, and repeated-click
guards. Retry checks wait for the real 2- and 4-second backoff intervals.

The harness does not emulate CSS layout, actual focus/keyboard behavior, native
dialog behavior, screen readers, mobile rendering, or browser history. Those
remain browser-review tasks rather than implied passes.

## Environment

- Python 3.12.14; SQLite 3.53.1
- FastAPI 0.141.1; Starlette 1.6.0; Pydantic 2.13.4
- Uvicorn 0.52.1; httpx 0.28.1
- Node.js 24.19.0
- Exact installed Python dependency versions: `requirements-lock.txt`

The test runner emits one known Starlette deprecation warning about its current
httpx-based TestClient adapter. All tests pass. Moving the tests to the successor
adapter is a future dependency-maintenance task; no warning is hidden.

## Verification limits

- Rendered browser QA and screenshots were **not completed**. Headless Chromium
  could not start because socket creation is restricted in the shell environment.
  The separate cloud browser could not reach the shell's loopback server, and its
  URL policy did not allow file previews. These restrictions were respected.
- No PostgreSQL server or Docker runtime was available. No PostgreSQL behavior,
  container image, multi-host queue behavior, or production deployment was tested.
- The receiver is a local database-backed synthetic sink. No external HTTP
  delivery, provider account, real device, or customer data was involved.
- No clean dependency installation, remote CI, external security scanning, or
  performance/load benchmark was run. The dependency versions above were already
  installed in the build workspace.
- The public demo keys and cursor secret are fixtures. This application must not
  be exposed as a production service without replacing them and addressing the
  production checklist in the README.
- These results describe the local prototype build. Publishing the source does not
  establish production deployment, remote CI success, or browser-rendering quality.
