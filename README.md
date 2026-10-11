# Fleet Relay

**A small event delivery system with failure modes you can inspect.**

Fleet Relay accepts synthetic device events, commits a delivery obligation in the
same transaction, and dispatches it through a bounded, lease-fenced worker. A
browser console makes retries, tenant boundaries, and configuration versions
visible. It is an original portfolio reference project, using no employer code,
customer records, or external service accounts.

**Scope:** Python 3.12, FastAPI, standard-library SQLite, plain JavaScript/CSS.
Single host. Public demo credentials. Local synthetic receiver. No cloud account,
paid API, Redis, or external receiver required.

## Run it

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
python -m unittest discover -v
python -m scripts.demo --output artifacts/walkthrough.json
python -m uvicorn fleet_relay.app:app --host 127.0.0.1 --port 8000
```

Open http://127.0.0.1:8000 for the console and http://127.0.0.1:8000/docs for the
generated API schema. Bind to loopback: the demo keys are intentionally public.
The console uses manual dispatch so each state transition can be inspected.

`requirements-lock.txt` records the exact installed dependency closure used for
verification. Use it instead of `requirements-dev.txt` when reproducing that
Python 3.12 environment. A clean dependency download/install was not tested in the
restricted build workspace; tests ran against the listed installed versions.

If Node.js is available, `node scripts/ui_smoke.js` additionally runs 15 UI/API
state checks using the actual app JavaScript and a socket-free API bridge. No
Node packages are needed. This is a DOM/request harness, not rendered-browser QA;
the browser verification gap is recorded in the test report.

For continuous processing, use another terminal with the same working directory:

```bash
python -m fleet_relay.worker
# Or drain only work currently due:
python -m fleet_relay.worker --once
```

The default database is `data/fleet-relay.sqlite3`. Set `FLEET_DB` for the API or
use `--db` for the worker when choosing a different file. Both must point at the
same local database. The app creates its version-1 schema on startup. Existing
unknown schema versions are rejected; general migrations are not implemented.

### A three-minute walkthrough

1. Choose **Northstar Lab**. Ingest a synthetic batch: one normal event, one
   transient failure, and one permanently failing event.
2. Replay the batch. The accepted count stays unchanged. The original event IDs
   are returned because both the idempotency keys and event bodies are unchanged.
3. Dispatch. One event is delivered; two are scheduled for retry. Wait at least
   two seconds and dispatch again. The transient event succeeds. Wait four more
   seconds and dispatch again: the poison event reaches the dead-letter state.
4. Switch to **Solstice Lab**. Northstar events, receipts, and configuration are
   absent. The same idempotency key can be used independently in each tenant.
5. Change the sampling interval. The config version increments. A save using the
   old ETag fails with HTTP 412; a fresh read is required before saving again.

`python -m scripts.demo` runs these backend invariants plus the crash-after-send
window against a fresh temporary SQLite database. It uses an **injected virtual
clock**, not real waiting or a performance benchmark. Tests also exercise actual
concurrent database connections.

## Design

```text
request + tenant identity + idempotency key
                  |
         BEGIN IMMEDIATE
           event row + outbox row
                  |
                COMMIT          <- HTTP 201 only after this boundary
                  |
         short claim transaction
         attempt + lease token + expiration
                  |
         synthetic delivery     <- outside the database transaction
                  |
         idempotent receipt     <- separate commit, keyed by event ID
                  |
         conditional finish     <- live token and unexpired lease required
```

### Acceptance and idempotency

- A unique `(tenant_id, idempotency_key)` constraint is the durable boundary.
- A SHA-256 digest covers the validated event body. JSON object keys are sorted;
  timestamps normalize to UTC and API defaults are included. This is ordinary
  deterministic JSON serialization, not a cross-language canonical-JSON standard;
  numeric values such as `1` and `1.0` can hash differently.
- Same key and same body: HTTP 200, same event identity, no new outbox row. The
  response includes current delivery state, which may have changed since creation.
- Same key and different body: HTTP 409. No silent payload replacement.
- Event and outbox insertion are atomic. A test forces the second insert to fail
  and verifies that no event survives the rollback.
- The request body is capped at 64 KiB, including chunked input; event payloads
  are limited to 16 KiB. This does not replace rate limiting or an edge proxy.

### Outbox and recovery

- Short `BEGIN IMMEDIATE` write transactions serialize claims on a local SQLite
  database. Receiver work is performed outside the transaction.
- Every claim increments the attempt count and gets a unique token with a
  30-second lease. A worker can only acknowledge its own unexpired token.
- Expired leases are eligible for reclaim. An old worker cannot overwrite the
  new worker's state, even if it returns late.
- Failures wait 2 seconds, then 4 seconds. Three claimed attempts is the entire
  budget, including worker crashes. Retry delay is capped at 30 seconds if the
  attempt budget is raised. There is no jitter in this deterministic demo.
- Exhausted failures become `dead`. If the last lease expires, the reason says
  **delivery outcome unknown**: a dead-letter state cannot prove that an external
  side effect did not happen.
- Delivery is **at least once within the retry policy**, not guaranteed eventual
  success. A crash between receipt and acknowledgment can deliver twice. The toy
  receiver's unique event ID prevents duplicate local effects. Lease fencing
  protects database state; it cannot undo an already executed remote side effect.
- There is no lease renewal, external HTTP timeout policy, dead-letter replay,
  strict per-device ordering, scheduler service, or queue-retention policy.
- Lease timestamps use the host wall clock so they survive restarts; large clock
  jumps are not compensated for. Claim and finish sample time after obtaining the
  database write lock, avoiding stale timestamps after lock contention.
- Dispatch counters describe delivery attempts in that call. Cleanup of an
  exhausted expired lease is reflected in the summary and attempt log, even when
  there was no new delivery attempt.

### Isolation, queries, and configuration

- The bearer-key lookup selects the tenant. Event bodies cannot provide a tenant.
  Reads, claims, summaries, receipts, and config writes are scoped in SQL.
- Demo fixtures: `fleet-demo-north` → Northstar Lab and `fleet-demo-south` →
  Solstice Lab. These public strings exercise authorization boundaries; they are
  **not deployable identity management**. No account provisioning, roles, rotation,
  or production secret storage is implemented. Manual dispatch is a demo control.
- Cursor pages sort by decreasing event sequence and carry a fixed high-water
  mark. Later inserts do not appear halfway through paging. Delivery statuses
  can change: this is stable event membership, not a database snapshot across
  requests. No count/offset scan is used for pagination.
- Cursors are HMAC-tagged and tenant-bound. The shipped key is public. Set a
  strong `FLEET_CURSOR_SECRET` before any non-demo use. SQL tenant predicates are
  enforced independently of cursor signatures. Secret rotation invalidates cursors.
- Config writes use `If-Match: "N"` and one SQL compare-and-swap update. Missing
  preconditions return 428; stale versions return 412. Each accepted event records
  the current tenant configuration version for traceability.
- Config describes what a hypothetical device should do. This project does not
  push config to a fleet or stop ingestion when the `enabled` flag changes.

## API examples

The following tokens and values are synthetic fixtures, safe to use locally.

```bash
curl -i http://127.0.0.1:8000/v1/events \
  -H 'Authorization: Bearer fleet-demo-north' \
  -H 'Idempotency-Key: example-001' \
  -H 'Content-Type: application/json' \
  -d '{"device_id":"lab-sensor-01","kind":"heartbeat","severity":"info","occurred_at":"2026-01-01T00:00:00Z","payload":{"cpu_pct":24,"delivery_mode":"retry_once"}}'

curl -s http://127.0.0.1:8000/v1/events?limit=10 \
  -H 'Authorization: Bearer fleet-demo-north'

curl -s http://127.0.0.1:8000/v1/dispatch \
  -H 'Authorization: Bearer fleet-demo-north' \
  -H 'Content-Type: application/json' -d '{"limit":20}'

curl -i http://127.0.0.1:8000/v1/config \
  -H 'Authorization: Bearer fleet-demo-north'

curl -i -X PUT http://127.0.0.1:8000/v1/config \
  -H 'Authorization: Bearer fleet-demo-north' \
  -H 'If-Match: "1"' -H 'Content-Type: application/json' \
  -d '{"sample_interval_seconds":60,"enabled":true,"labels":{"lab":"synthetic"}}'
```

`kind` is `heartbeat`, `metric`, or `alert`; `severity` is `info`, `warning`, or
`critical`. `occurred_at` must include a timezone. The synthetic receiver recognizes
`payload.delivery_mode` values `normal`, `retry_once`, and `poison`; other values
behave normally. These are deliberate local fault-injection controls, not business
event rules or a network-delivery implementation.

## Verification

See [docs/test-report.md](docs/test-report.md) for the final run, exact coverage,
environment, and limitations. The raw unittest transcript and repeatable scenario
output are in `artifacts/`. Unit tests use the Python standard-library runner;
`httpx` is needed for FastAPI's in-process HTTP tests. Browser QA is separate and
is not a substitute for backend concurrency and failure-injection tests.

## Why SQLite here? What changes for production?

SQLite keeps the artifact runnable with one process and no service credentials.
WAL, foreign keys, a five-second busy timeout, and `synchronous=FULL` are enabled.
Durability still depends on filesystem and hardware guarantees. No PostgreSQL
server or Docker runtime was available in the build workspace; PostgreSQL behavior
was not tested. This is **not a distributed queue or an HA deployment**.

For a real fleet:

1. Move to PostgreSQL with reviewed migrations, connection pooling, backup and
   restore verification, and measured query plans. Use row-level claim locking
   such as `FOR UPDATE SKIP LOCKED`; separately decide whether database row-level
   security is an additional boundary. Do not reuse SQLite locking assumptions.
2. Replace public fixtures with a verified identity provider, service credentials,
   tenant lifecycle, role checks, key rotation, TLS, rate limits, and audit controls.
3. Implement an actual receiver contract with bounded timeouts, retriable vs.
   permanent error classification, jitter, receiver idempotency, and cancellation.
   Decide whether leases need renewal and whether delivery ordering matters.
4. Establish explicit dead-letter triage/replay, idempotency retention, data
   minimization, payload redaction, and operational SLOs. Add telemetry for queue
   age, error rate, saturation, lease expiry, and receiver latency.
5. Load-test real workloads and multiple processes/hosts before making throughput
   or availability claims. No performance or production-readiness claim is made here.

## Layout

```text
fleet_relay/app.py       API, validation, demo identity, request bounds
fleet_relay/store.py     SQL schema, atomic ingest, cursor queries, fenced leases
fleet_relay/worker.py    Dispatcher and local idempotent synthetic receiver
fleet_relay/static/     Dependency-free browser console
tests/                  Isolation, failure, concurrency, and API regression tests
scripts/demo.py         Asserted walkthrough with a virtual clock
docs/                   Verification and design discussion
artifacts/              Recorded test evidence; no credentials or customer data
```

Prepared as a synthetic portfolio prototype. No production deployment or real
customer integration is represented. Third-party Python dependencies retain their
own licenses; no additional project license grant is implied.
