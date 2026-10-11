"""Independent review regressions, including hostile input and lock-wait timing."""

import concurrent.futures
import json
import sqlite3
import tempfile
import threading
import unittest
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from fleet_relay.app import create_app
from fleet_relay.store import Store
from tests.test_store import event


NORTH = {"Authorization": "Bearer fleet-demo-north"}
SOUTH = {"Authorization": "Bearer fleet-demo-south"}
CONFIG = {"sample_interval_seconds": 30, "enabled": True, "labels": {}}


class ReviewApiTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = Store(Path(self.temp.name) / "review.db")
        self.client = TestClient(create_app(store=self.store), raise_server_exceptions=False)
        self.client.__enter__()
        self.addCleanup(self.client.__exit__, None, None, None)

    def post_raw(self, body):
        return self.client.post(
            "/v1/events", content=json.dumps(body),
            headers={**NORTH, "Content-Type": "application/json", "Idempotency-Key": "review"},
        )

    def test_oversized_if_match_is_a_client_error_without_mutation(self):
        response = self.client.put(
            "/v1/config", json=CONFIG,
            headers={**NORTH, "If-Match": '"999999999999999999999999999999"'},
        )
        self.assertIn(response.status_code, (400, 412, 422))
        self.assertEqual(self.store.get_config("northstar")["version"], 1)

    def test_correctly_signed_cursor_still_checks_sqlite_integer_bounds(self):
        for field in ("ceiling", "before"):
            with self.subTest(field=field):
                data = {"tenant": "northstar", "version": 1, "ceiling": 1, "before": 1}
                data[field] = 2**70
                cursor = self.store._encode_cursor(data)
                response = self.client.get("/v1/events", headers=NORTH, params={"cursor": cursor})
                self.assertEqual(response.status_code, 400)

    def test_utc_normalization_overflow_is_a_client_error(self):
        for timestamp in ("0001-01-01T00:00:00+14:00", "9999-12-31T23:59:59-14:00"):
            with self.subTest(timestamp=timestamp):
                response = self.post_raw({**event(), "occurred_at": timestamp})
                self.assertEqual(response.status_code, 422)
        self.assertEqual(self.store.summary("northstar")["event_count"], 0)

    def test_nonfinite_payload_is_a_serializable_client_error(self):
        for number in (float("nan"), float("inf"), -float("inf")):
            with self.subTest(number=number):
                response = self.post_raw({**event(), "payload": {"nested": [number]}})
                self.assertIn(response.status_code, (400, 422))
        self.assertEqual(self.store.summary("northstar")["event_count"], 0)

    def test_surrogate_payload_does_not_poison_persisted_event_reads(self):
        response = self.post_raw({**event(), "payload": {"nested": ["\ud800"]}})
        self.assertIn(response.status_code, (400, 422))
        self.assertEqual(self.store.summary("northstar")["event_count"], 0)
        self.assertEqual(self.client.get("/v1/events", headers=NORTH).status_code, 200)

    def test_surrogate_label_does_not_poison_persisted_config_reads(self):
        response = self.client.put(
            "/v1/config", content=json.dumps({**CONFIG, "labels": {"site": "\ud800"}}),
            headers={**NORTH, "If-Match": '"1"', "Content-Type": "application/json"},
        )
        self.assertIn(response.status_code, (400, 422))
        self.assertEqual(self.store.get_config("northstar")["version"], 1)
        self.assertEqual(self.client.get("/v1/config", headers=NORTH).status_code, 200)

    def test_surrogate_mapping_key_is_rejected_before_persistence(self):
        response = self.post_raw({**event(), "payload": {"\ud800": "value"}})
        self.assertIn(response.status_code, (400, 422))
        self.assertEqual(self.store.summary("northstar")["event_count"], 0)


class ReviewStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.now = 1_800_000_000.0
        self.store = Store(Path(self.temp.name) / "review.db", clock=lambda: self.now, lease_seconds=10)
        self.store.initialize()
        self.row = self.store.ingest("northstar", "review", event())[0]

    def run_after_writer_wait(self, operation):
        """Hold a real SQLite writer while advancing the injected wall clock.

        Signal before the worker acquires its transaction. This works whether
        the implementation samples time before or after the lock wait.
        """
        original_connection = self.store.connection
        about_to_begin = threading.Event()

        @contextmanager
        def observed_connection(*, write=False):
            about_to_begin.set()
            with original_connection(write=write) as conn:
                yield conn

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            with patch.object(self.store, "connection", observed_connection):
                with original_connection(write=True):
                    future = pool.submit(operation)
                    self.assertTrue(about_to_begin.wait(timeout=2), "Worker did not start")
                    self.now += 10
                return future.result(timeout=2)

    def test_claim_lease_duration_starts_after_obtaining_writer_lock(self):
        lease = self.run_after_writer_wait(lambda: self.store.claim("northstar"))
        self.assertIsNotNone(lease)
        with self.store.connection() as conn:
            until = conn.execute("SELECT lease_until FROM outbox WHERE event_id=?", (lease.event_id,)).fetchone()[0]
        self.assertGreater(until, self.now, "Claim returned an already-expired lease after waiting for the writer")
        self.assertIsNone(self.store.claim("northstar"))

    def test_finish_rechecks_expiry_after_obtaining_writer_lock(self):
        lease = self.store.claim("northstar")
        outcome = self.run_after_writer_wait(lambda: self.store.finish(lease))
        self.assertEqual(outcome, "lease_lost")
        self.assertEqual(self.store.get_event("northstar", lease.event_id)["delivery"]["status"], "inflight")

    def test_failed_attempt_log_rolls_back_entire_ack(self):
        lease = self.store.claim("northstar")
        with self.store.connection(write=True) as conn:
            conn.execute("CREATE TRIGGER abort_log BEFORE INSERT ON attempt_log BEGIN SELECT RAISE(ABORT,'review'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.finish(lease)
        current = self.store.get_event("northstar", lease.event_id)
        self.assertEqual(current["delivery"]["status"], "inflight")
        with self.store.connection(write=True) as conn:
            conn.execute("DROP TRIGGER abort_log")
        self.assertEqual(self.store.finish(lease), "delivered")

    def test_composite_foreign_key_blocks_cross_tenant_receipt(self):
        lease = self.store.claim("northstar")
        wrong_tenant = replace(lease, tenant_id="solstice")
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.record_receipt(wrong_tenant)
        self.assertEqual(self.store.finish(wrong_tenant), "lease_lost")
        self.assertEqual(self.store.summary("solstice")["receipt_count"], 0)
        self.assertEqual(self.store.summary("northstar")["outbox"]["inflight"], 1)


if __name__ == "__main__":
    unittest.main()
