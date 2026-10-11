import tempfile
import unittest

from fastapi.testclient import TestClient

from fleet_relay.app import create_app
from fleet_relay.store import Store
from tests.test_store import event


NORTH = {"Authorization": "Bearer fleet-demo-north"}
SOUTH = {"Authorization": "Bearer fleet-demo-south"}


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = Store(self.temp.name + "/test.db")
        self.client = TestClient(create_app(store=self.store))
        self.client.__enter__()
        self.addCleanup(self.client.__exit__, None, None, None)

    def ingest(self, key="test-1", body=None, headers=None):
        return self.client.post("/v1/events", json=body or event(),
                                headers={**(headers or NORTH), "Idempotency-Key": key})

    def test_auth_required_and_unknown_credentials_rejected(self):
        for path in ("/v1/events", "/v1/summary", "/v1/config"):
            for headers in ({}, {"Authorization": "Bearer wrong"}):
                with self.subTest(path=path, headers=headers):
                    self.assertEqual(self.client.get(path, headers=headers).status_code, 401)

    def test_new_duplicate_and_conflicting_event_statuses(self):
        created = self.ingest()
        replay = self.ingest()
        conflict = self.ingest(body=event(device="sensor-other"))
        self.assertEqual((created.status_code, replay.status_code, conflict.status_code), (201, 200, 409))
        self.assertTrue(replay.json()["duplicate"])
        self.assertEqual(created.json()["event"]["event_id"], replay.json()["event"]["event_id"])

    def test_tenant_cannot_read_other_tenant_event(self):
        event_id = self.ingest().json()["event"]["event_id"]
        self.assertEqual(self.client.get(f"/v1/events/{event_id}", headers=SOUTH).status_code, 404)
        self.assertEqual(self.client.get("/v1/events", headers=SOUTH).json()["items"], [])
        self.assertEqual(self.client.get("/v1/summary", headers=SOUTH).json()["event_count"], 0)

    def test_body_cannot_override_tenant_identity(self):
        self.assertEqual(self.ingest(body={**event(), "tenant_id": "solstice"}).status_code, 422)

    def test_naive_timestamp_rejected(self):
        self.assertEqual(self.ingest(body={**event(), "occurred_at": "2026-01-01T00:00:00"}).status_code, 422)

    def test_equivalent_timestamps_normalize_before_idempotency_hash(self):
        self.assertEqual(self.ingest().status_code, 201)
        self.assertEqual(self.ingest(body={**event(), "occurred_at": "2025-12-31T16:00:00-08:00"}).status_code, 200)

    def test_missing_or_invalid_idempotency_key_rejected(self):
        self.assertEqual(self.client.post("/v1/events", json=event(), headers=NORTH).status_code, 422)
        self.assertEqual(self.ingest(key="contains spaces").status_code, 422)

    def test_payload_size_limit(self):
        self.assertEqual(self.ingest(body={**event(), "payload": {"large": "x" * 17_000}}).status_code, 422)

    def test_request_size_limit_with_chunked_body(self):
        def chunks():
            yield b'{"payload":"'
            for _ in range(7):
                yield b"x" * 10_000
            yield b'"}'
        response = self.client.post("/v1/events", content=chunks(),
                                    headers={**NORTH, "Content-Type": "application/json", "Idempotency-Key": "large"})
        self.assertEqual(response.status_code, 413)

    def test_pagination_bounds_and_cursor_scope(self):
        for i in range(3):
            self.ingest(str(i))
        result = self.client.get("/v1/events?limit=1", headers=NORTH).json()
        self.assertEqual(len(result["items"]), 1)
        self.assertEqual(self.client.get("/v1/events", params={"cursor": result["next_cursor"]}, headers=SOUTH).status_code, 400)
        self.assertEqual(self.client.get("/v1/events?limit=101", headers=NORTH).status_code, 422)

    def test_configuration_requires_etag_and_rejects_stale_writes(self):
        initial = self.client.get("/v1/config", headers=NORTH)
        self.assertEqual(initial.headers["ETag"], '"1"')
        config = {"sample_interval_seconds": 60, "enabled": True, "labels": {"site": "demo"}}
        self.assertEqual(self.client.put("/v1/config", json=config, headers=NORTH).status_code, 428)
        self.assertEqual(self.client.put("/v1/config", json=config, headers={**NORTH, "If-Match": "1"}).status_code, 400)
        updated = self.client.put("/v1/config", json=config, headers={**NORTH, "If-Match": '"1"'})
        self.assertEqual(updated.status_code, 200)
        self.assertEqual(updated.headers["ETag"], '"2"')
        self.assertEqual(self.client.put("/v1/config", json=config, headers={**NORTH, "If-Match": '"1"'}).status_code, 412)
        self.assertEqual(self.client.get("/v1/config", headers=SOUTH).json()["version"], 1)

    def test_config_rejects_invalid_values(self):
        for config in ({"sample_interval_seconds": 1, "enabled": True},
                       {"sample_interval_seconds": "30", "enabled": True},
                       {"sample_interval_seconds": 30, "enabled": "true"}):
            self.assertEqual(self.client.put("/v1/config", json=config, headers={**NORTH, "If-Match": '"1"'}).status_code, 422)

    def test_manual_dispatch_is_tenant_scoped(self):
        self.ingest()
        self.ingest(headers=SOUTH)
        response = self.client.post("/v1/dispatch", json={"limit": 20}, headers=NORTH)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["delivered"], 1)
        self.assertEqual(self.client.get("/v1/summary", headers=SOUTH).json()["outbox"]["pending"], 1)

    def test_health_and_security_headers(self):
        response = self.client.get("/healthz")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["X-Content-Type-Options"], "nosniff")
        self.assertEqual(response.headers["Cache-Control"], "no-store")

    def test_dashboard_and_local_assets_are_served(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Fleet Relay", response.text)
        self.assertIn("script-src 'self'", response.headers["Content-Security-Policy"])
        for path in ("/static/app.js", "/static/styles.css"):
            with self.subTest(path=path):
                self.assertEqual(self.client.get(path).status_code, 200)
        self.assertEqual(self.client.get("/static/missing-file.js").status_code, 404)


if __name__ == "__main__":
    unittest.main()
