import concurrent.futures
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from fleet_relay.store import Conflict, InvalidCursor, StaleVersion, Store, canonical
from fleet_relay.worker import SyntheticSink, dispatch


def event(mode="normal", device="sensor-01"):
    return {"device_id": device, "kind": "heartbeat", "severity": "info",
            "occurred_at": "2026-01-01T00:00:00Z", "payload": {"delivery_mode": mode, "cpu_pct": 24}}


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.now = 1_800_000_000.0
        self.path = Path(self.temp.name) / "test.sqlite3"
        self.store = Store(self.path, clock=lambda: self.now, lease_seconds=10)
        self.store.initialize()

    def ingest(self, key="key-1", tenant="northstar", mode="normal"):
        return self.store.ingest(tenant, key, event(mode))[0]

    def test_event_and_outbox_commit_together(self):
        row = self.ingest()
        self.assertEqual(row["delivery"]["status"], "pending")
        self.assertEqual(self.store.summary("northstar")["event_count"], 1)

    def test_outbox_insert_failure_rolls_back_event(self):
        with self.store.connection(write=True) as conn:
            conn.execute("CREATE TRIGGER abort_outbox BEFORE INSERT ON outbox BEGIN SELECT RAISE(ABORT,'injected'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.ingest()
        with self.store.connection() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0], 0)

    def test_duplicate_returns_same_event_and_canonical_key_order(self):
        original = self.ingest()
        body = dict(reversed(list(event().items())))
        replay, duplicate = self.store.ingest("northstar", "key-1", body)
        self.assertTrue(duplicate)
        self.assertEqual(replay["event_id"], original["event_id"])
        self.assertEqual(self.store.summary("northstar")["outbox"]["pending"], 1)

    def test_duplicate_changed_payload_conflicts(self):
        self.ingest()
        with self.assertRaises(Conflict):
            self.store.ingest("northstar", "key-1", event(device="different"))

    def test_unicode_hash_format_remains_stable(self):
        body = {"message": "Synthetic café sensor · مرحبا"}
        self.assertEqual(canonical(body), json.dumps(body, sort_keys=True, separators=(",", ":"), allow_nan=False))

    def test_same_key_in_other_tenant_is_independent(self):
        first = self.ingest()
        second = self.ingest(tenant="solstice")
        self.assertNotEqual(first["event_id"], second["event_id"])
        self.assertIsNone(self.store.get_event("solstice", first["event_id"]))
        self.assertEqual(len(self.store.list_events("northstar")["items"]), 1)

    def test_concurrent_duplicate_ingestion_creates_one_event(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: self.store.ingest("northstar", "shared", event()), range(20)))
        self.assertEqual(sum(not duplicate for _, duplicate in results), 1)
        self.assertEqual(len({row["event_id"] for row, _ in results}), 1)
        self.assertEqual(self.store.summary("northstar")["outbox"]["pending"], 1)

    def test_concurrent_conflicting_ingestion_has_one_winner(self):
        def write(i):
            try:
                self.store.ingest("northstar", "shared", event(device=f"sensor-{i}"))
                return "accepted"
            except Conflict:
                return "conflict"
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            outcomes = list(pool.map(write, range(8)))
        self.assertEqual(outcomes.count("accepted"), 1)
        self.assertEqual(outcomes.count("conflict"), 7)

    def test_cursor_snapshot_excludes_later_inserts(self):
        original_ids = {self.ingest(str(i))["event_id"] for i in range(5)}
        first = self.store.list_events("northstar", 2)
        late_id = self.ingest("late")["event_id"]
        ids = [row["event_id"] for row in first["items"]]
        cursor = first["next_cursor"]
        while cursor:
            page = self.store.list_events("northstar", 2, cursor)
            ids.extend(row["event_id"] for row in page["items"])
            cursor = page["next_cursor"]
        self.assertEqual(set(ids), original_ids)
        self.assertEqual(len(ids), 5)
        self.assertNotIn(late_id, ids)

    def test_cursor_rejects_other_tenant_and_tampering(self):
        for i in range(3):
            self.ingest(str(i))
        token = self.store.list_events("northstar", 1)["next_cursor"]
        for tenant, cursor in [("solstice", token), ("northstar", token + "x"), ("northstar", "garbage")]:
            with self.subTest(tenant=tenant, cursor=cursor), self.assertRaises(InvalidCursor):
                self.store.list_events(tenant, 1, cursor)

    def test_config_version_is_captured_at_ingestion(self):
        first = self.ingest("before")
        config = {"sample_interval_seconds": 60, "enabled": True, "labels": {"site": "synthetic"}}
        self.store.update_config("northstar", 1, config)
        second = self.ingest("after")
        self.assertEqual((first["config_version"], second["config_version"]), (1, 2))
        self.assertEqual(self.store.get_config("solstice")["version"], 1)

    def test_config_compare_and_swap_has_one_concurrent_winner(self):
        def write(i):
            try:
                self.store.update_config("northstar", 1, {"sample_interval_seconds": 30 + i, "enabled": True, "labels": {}})
                return True
            except StaleVersion:
                return False
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            outcomes = list(pool.map(write, range(8)))
        self.assertEqual(sum(outcomes), 1)
        self.assertEqual(self.store.get_config("northstar")["version"], 2)

    def test_concurrent_claims_are_distinct(self):
        for i in range(12):
            self.ingest(str(i))
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            leases = list(pool.map(lambda _: self.store.claim("northstar"), range(20)))
        leases = [lease for lease in leases if lease]
        self.assertEqual(len(leases), 12)
        self.assertEqual(len({lease.event_id for lease in leases}), 12)

    def test_unexpired_lease_cannot_be_claimed(self):
        self.ingest()
        self.assertIsNotNone(self.store.claim("northstar"))
        self.assertIsNone(self.store.claim("northstar"))

    def test_expired_lease_is_reclaimed_and_old_ack_is_fenced(self):
        row = self.ingest()
        old = self.store.claim("northstar")
        self.now += 10
        new = self.store.claim("northstar")
        self.assertEqual(new.attempt, 2)
        self.assertNotEqual(new.token, old.token)
        self.assertEqual(self.store.finish(old), "lease_lost")
        self.assertEqual(self.store.get_event("northstar", row["event_id"])["delivery"]["status"], "inflight")
        self.assertEqual(self.store.finish(new), "delivered")

    def test_ack_at_lease_boundary_is_rejected_even_without_reclaim(self):
        self.ingest()
        lease = self.store.claim("northstar")
        self.now += 10
        self.assertEqual(self.store.finish(lease), "lease_lost")

    def test_transient_failure_retries_after_backoff(self):
        self.ingest(mode="retry_once")
        self.assertEqual(dispatch(self.store, "northstar")["retried"], 1)
        self.assertEqual(dispatch(self.store, "northstar")["processed"], 0)
        self.now += 2
        self.assertEqual(dispatch(self.store, "northstar")["delivered"], 1)
        summary = self.store.summary("northstar")
        self.assertEqual(summary["receipt_count"], 1)
        self.assertEqual(summary["outbox"]["delivered"], 1)

    def test_poison_event_stops_after_three_attempts(self):
        self.ingest(mode="poison")
        self.assertEqual(dispatch(self.store, "northstar")["retried"], 1)
        self.now += 2
        self.assertEqual(dispatch(self.store, "northstar")["retried"], 1)
        self.now += 4
        self.assertEqual(dispatch(self.store, "northstar")["dead"], 1)
        self.now += 100
        self.assertEqual(dispatch(self.store, "northstar")["processed"], 0)
        self.assertEqual(self.store.summary("northstar")["receipt_count"], 0)

    def test_crash_after_receipt_before_ack_is_deduplicated_on_recovery(self):
        self.ingest()
        old = self.store.claim("northstar")
        SyntheticSink(self.store)(old)  # External side effect committed; process dies before finish.
        restarted = Store(self.path, clock=lambda: self.now, lease_seconds=10)
        restarted.initialize()
        self.now += 10
        self.assertEqual(dispatch(restarted, "northstar")["delivered"], 1)
        summary = restarted.summary("northstar")
        self.assertEqual(summary["receipt_count"], 1)
        self.assertEqual(summary["outbox"]["delivered"], 1)

    def test_repeated_worker_crashes_exhaust_budget_and_record_unknown_outcome(self):
        row = self.ingest()
        for attempt in range(1, 4):
            lease = self.store.claim("northstar")
            self.assertEqual(lease.attempt, attempt)
            self.now += 10
        self.assertIsNone(self.store.claim("northstar"))
        current = self.store.get_event("northstar", row["event_id"])
        self.assertEqual(current["delivery"]["status"], "dead")
        self.assertIn("unknown", current["delivery"]["last_error"])

    def test_retry_schedule_survives_database_reopen(self):
        self.ingest(mode="retry_once")
        dispatch(self.store, "northstar")
        reopened = Store(self.path, clock=lambda: self.now)
        reopened.initialize()
        self.assertEqual(dispatch(reopened, "northstar")["processed"], 0)
        self.now += 2
        self.assertEqual(dispatch(reopened, "northstar")["delivered"], 1)

    def test_worker_respects_tenant_scope(self):
        self.ingest(tenant="solstice")
        self.assertEqual(dispatch(self.store, "northstar")["processed"], 0)
        self.assertEqual(self.store.summary("solstice")["outbox"]["pending"], 1)

    def test_receiver_runs_outside_database_transaction(self):
        self.ingest()
        def receiver(lease):
            self.store.ingest("solstice", "during-delivery", event())
            self.store.record_receipt(lease)
        self.assertEqual(dispatch(self.store, "northstar", deliver=receiver)["delivered"], 1)
        self.assertEqual(self.store.summary("solstice")["event_count"], 1)

    def test_arbitrary_receiver_errors_are_sanitized(self):
        row = self.ingest()
        def fail(lease):
            raise RuntimeError("password=do-not-persist-this")
        dispatch(self.store, "northstar", deliver=fail)
        detail = self.store.get_event("northstar", row["event_id"])["delivery"]["last_error"]
        self.assertEqual(detail, "Receiver error: RuntimeError")


if __name__ == "__main__":
    unittest.main()
