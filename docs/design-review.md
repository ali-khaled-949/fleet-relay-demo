# Design review notes

## The boundary that matters

Accepting an event creates two facts: the immutable accepted body and an obligation
to deliver it. If these are separate commits, either can survive alone. A database
transaction makes their creation indivisible; the unique tenant/key constraint
makes client retries safe. The forced outbox-insert failure regression checks the
rollback boundary rather than only checking happy-path row counts.

## Why not mark it delivered before calling the receiver?

A crash would silently discard the obligation. Calling the receiver first leaves
the opposite window: the receiver can commit, then the worker can crash before
acknowledgment. Retrying can repeat the external effect. The demo exposes this
window and gives the receiver an event-ID deduplication boundary. It does not
assert exactly-once transport or distributed transactions.

## Why a token as well as a deadline?

A deadline says when someone else may reclaim a job. It does not identify who now
owns it. When worker A pauses and worker B reclaims, A may return with success or
failure later. Its old token cannot finish B's lease. Expiration is checked after
the database write lock is acquired, so lock-wait time cannot accidentally extend
the right to acknowledge.

This fences outbox state only. If A already called a remote receiver, the token
cannot reverse that call. Real receivers need their own idempotency contract and
carefully bounded timeouts; a long-running call may outlive the lease.

## What does a dead event mean?

It exhausted the configured attempt budget. A known receiver error and an expired
final lease carry different evidence. In the second case the delivery outcome is
unknown. Operators would reconcile the receiver before replaying a consequential
effect. Automated replay is intentionally not included.

## Why keep the tenant predicate everywhere?

Request authentication chooses an identity; database predicates enforce its data
scope. A signed cursor is a paging aid, not the authorization boundary. A client
cannot select another tenant with a body field or cursor. Composite foreign keys
also prevent an internally malformed cross-tenant receipt.

The public demo keys only exercise these mechanics. They do not offer meaningful
protection against another person who has read the README. Bind this demo to
loopback and replace identity management before any shared deployment.

## What was intentionally kept small?

- SQLite instead of an unverified PostgreSQL deployment.
- A local, inspectable receiver instead of a pretend external integration.
- Deterministic retry delays instead of production backoff/jitter policy.
- Polling summaries and an attempt log instead of an untested telemetry stack.
- Optimistic configuration writes without building an entire device-control plane.
- A static console without a framework, bundler, CDN, or vendor account.

The project favors runnable failure cases and readable SQL over broad feature
claims. Its next useful extension would be a tested PostgreSQL claim adapter plus
an explicit external receiver contract, rather than adding more UI counters.
