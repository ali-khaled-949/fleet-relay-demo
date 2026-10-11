"""Real process termination and concurrent dispatcher integration tests."""

import concurrent.futures
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from fleet_relay.store import Store
from fleet_relay.worker import dispatch
from tests.test_store import event


class ProcessTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.now = 1_800_000_000.0
        self.path = Path(self.temp.name) / "process.sqlite3"
        self.store = Store(self.path, clock=lambda: self.now, lease_seconds=10)
        self.store.initialize()

    def test_real_process_exit_after_receipt_recovers_without_duplicate_effect(self):
        self.store.ingest("northstar", "crash", event())
        code = """
import os,sys
from fleet_relay.store import Store
from fleet_relay.worker import SyntheticSink
store=Store(sys.argv[1], clock=lambda: 1800000000.0, lease_seconds=10)
lease=store.claim('northstar')
SyntheticSink(store)(lease)
os._exit(73)
"""
        result = subprocess.run([sys.executable, "-c", code, str(self.path)],
                                cwd=Path(__file__).resolve().parent.parent, timeout=15, capture_output=True)
        self.assertEqual(result.returncode, 73, result.stderr.decode())
        self.assertEqual(self.store.summary("northstar")["outbox"]["inflight"], 1)
        self.assertEqual(self.store.summary("northstar")["receipt_count"], 1)
        self.now += 10
        self.assertEqual(dispatch(self.store, "northstar")["delivered"], 1)
        self.assertEqual(self.store.summary("northstar")["receipt_count"], 1)

    def test_concurrent_dispatchers_preserve_two_tenant_event_conservation(self):
        for tenant in ("northstar", "solstice"):
            for index in range(100):
                self.store.ingest(tenant, f"load-{index}", event(device=f"sensor-{index}"))
        tenants = ["northstar", "solstice"] * 8
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda tenant: dispatch(self.store, tenant, limit=100), tenants))
        self.assertEqual(sum(result["delivered"] for result in results), 200)
        for tenant in ("northstar", "solstice"):
            summary = self.store.summary(tenant)
            self.assertEqual(summary["event_count"], 100)
            self.assertEqual(summary["receipt_count"], 100)
            self.assertEqual(summary["outbox"], {"pending": 0, "inflight": 0, "delivered": 100, "dead": 0})


if __name__ == "__main__":
    unittest.main()
