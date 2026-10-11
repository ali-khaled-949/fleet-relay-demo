"""Repeatable API walkthrough using real SQLite and an injected clock.

No waiting, cloud services, or external delivery. Run with python -m scripts.demo.
"""

import argparse
import json
import tempfile
from pathlib import Path

from fastapi.testclient import TestClient

from fleet_relay.app import create_app
from fleet_relay.store import Store
from fleet_relay.worker import SyntheticSink


def run(path: Path):
    now = [1_800_000_000.0]
    store = Store(path, clock=lambda: now[0])
    north = {"Authorization": "Bearer fleet-demo-north"}
    south = {"Authorization": "Bearer fleet-demo-south"}
    steps = []

    def check(name, condition, evidence):
        if not condition:
            raise AssertionError(name)
        steps.append({"check": name, "passed": True, "evidence": evidence})

    with TestClient(create_app(store=store)) as client:
        def send(mode):
            body = {"device_id": f"lab-sensor-{mode}", "kind": "heartbeat", "severity": "info",
                    "occurred_at": "2026-01-01T00:00:00Z", "payload": {"delivery_mode": mode, "cpu_pct": 24}}
            return client.post("/v1/events", json=body, headers={**north, "Idempotency-Key": f"demo-{mode}"})

        for mode in ("normal", "retry_once", "poison"):
            response = send(mode)
            check(f"accept {mode}", response.status_code == 201, response.status_code)
        replay = send("normal")
        check("same key and body replay", replay.status_code == 200 and replay.json()["duplicate"], replay.status_code)
        conflict = client.post("/v1/events", json={"device_id": "changed", "kind": "heartbeat", "severity": "info",
                                                 "occurred_at": "2026-01-01T00:00:00Z", "payload": {}},
                               headers={**north, "Idempotency-Key": "demo-normal"})
        check("conflicting payload rejected", conflict.status_code == 409, conflict.status_code)
        empty = client.get("/v1/events", headers=south).json()["items"]
        check("tenant isolation", empty == [], empty)
        first = client.post("/v1/dispatch", json={"limit": 20}, headers=north).json()
        check("first delivery round", first["delivered"] == 1 and first["retried"] == 2, first)
        early = client.post("/v1/dispatch", json={"limit": 20}, headers=north).json()
        check("retry backoff enforced", early["processed"] == 0, early)
        now[0] += 2
        second = client.post("/v1/dispatch", json={"limit": 20}, headers=north).json()
        check("transient failure recovers", second["delivered"] == 1 and second["retried"] == 1, second)
        now[0] += 4
        third = client.post("/v1/dispatch", json={"limit": 20}, headers=north).json()
        check("poison event terminates", third["dead"] == 1, third)
        config = {"sample_interval_seconds": 60, "enabled": True, "labels": {"lab": "synthetic"}}
        fresh = client.put("/v1/config", json=config, headers={**north, "If-Match": '"1"'})
        stale = client.put("/v1/config", json=config, headers={**north, "If-Match": '"1"'})
        check("stale config write rejected", fresh.status_code == 200 and stale.status_code == 412, {"first": 200, "stale": 412})
        body = {"device_id": "crash-sensor", "kind": "metric", "severity": "info",
                "occurred_at": "2026-01-01T00:00:00Z", "payload": {"value": 8}}
        client.post("/v1/events", json=body, headers={**north, "Idempotency-Key": "demo-crash"}).raise_for_status()
        lease = store.claim("northstar")
        SyntheticSink(store)(lease)
        # Intentionally omit finish: model a crash after the receiver commits.
        now[0] += 30
        recovered = client.post("/v1/dispatch", json={"limit": 20}, headers=north).json()
        summary = client.get("/v1/summary", headers=north).json()
        check("crash recovery deduplicates receiver effect", recovered["delivered"] == 1 and summary["receipt_count"] == 3,
              {"dispatch": recovered, "receipts": summary["receipt_count"]})
        check("final conservation", summary["event_count"] == 4 and summary["outbox"] ==
              {"pending": 0, "inflight": 0, "delivered": 3, "dead": 1}, summary["outbox"])
    return {"project": "Fleet Relay", "mode": "real SQLite; in-process HTTP client; deterministic virtual clock",
            "checks_passed": len(steps), "steps": steps, "final_summary": summary}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="Optional JSON transcript path")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="fleet-relay-demo-") as temp:
        result = run(Path(temp) / "demo.sqlite3")
    encoded = json.dumps(result, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded + "\n")
    print(encoded)


if __name__ == "__main__":
    main()
