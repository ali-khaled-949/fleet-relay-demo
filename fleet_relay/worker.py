"""Bounded outbox dispatcher and deterministic synthetic delivery faults."""

from __future__ import annotations

import argparse
import time
from typing import Callable

from .store import Lease, Store, TENANTS


class SyntheticFailure(Exception):
    pass


class SyntheticSink:
    def __init__(self, store: Store):
        self.store = store

    def __call__(self, lease: Lease):
        mode = lease.body["payload"].get("delivery_mode", "normal")
        if mode == "poison":
            raise SyntheticFailure("Injected permanent receiver failure")
        if mode == "retry_once" and lease.attempt == 1:
            raise SyntheticFailure("Injected transient receiver failure")
        self.store.record_receipt(lease)


def dispatch(store: Store, tenant_id: str, *, limit: int = 20,
             deliver: Callable[[Lease], None] | None = None) -> dict:
    if not 1 <= limit <= 100:
        raise ValueError("limit must be between 1 and 100")
    deliver = deliver or SyntheticSink(store)
    result = {key: 0 for key in ("processed", "delivered", "retried", "dead", "lease_lost")}
    for _ in range(limit):
        lease = store.claim(tenant_id)
        if lease is None:
            break
        error = None
        try:
            deliver(lease)
        except SyntheticFailure as exc:
            error = str(exc)
        except Exception as exc:
            # Avoid persisting receiver payloads, credentials, or arbitrary exception text.
            error = f"Receiver error: {type(exc).__name__}"
        outcome = store.finish(lease, error=error)
        result["processed"] += 1
        result[outcome] += 1
    return result


def main():
    parser = argparse.ArgumentParser(description="Run the local synthetic outbox worker")
    parser.add_argument("--db", default="data/fleet-relay.sqlite3")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    store = Store(args.db)
    store.initialize()
    try:
        while True:
            for tenant in TENANTS:
                result = dispatch(store, tenant)
                if result["processed"]:
                    print(tenant, result, flush=True)
            if args.once:
                break
            time.sleep(1)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
