"""SQLite persistence and a lease-fenced transactional outbox.

Every write owns a short BEGIN IMMEDIATE transaction. Delivery deliberately runs
outside that transaction: an external side effect cannot join our database commit.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import sqlite3
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable


TENANTS = {
    "northstar": "Northstar Lab",
    "solstice": "Solstice Lab",
}
DEFAULT_CONFIG = {"sample_interval_seconds": 30, "enabled": True, "labels": {}}


class Conflict(Exception):
    pass


class StaleVersion(Exception):
    pass


class InvalidCursor(Exception):
    pass


def canonical(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False, ensure_ascii=False)
    # Reject unpaired surrogates before persistence: response encoders require valid UTF-8.
    encoded.encode("utf-8")
    # Keep the stable escaped-ASCII hash format, including for non-ASCII payloads.
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def iso(value: float) -> str:
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class Lease:
    event_id: str
    tenant_id: str
    token: str
    attempt: int
    body: dict


class Store:
    def __init__(self, path: str | Path, *, clock: Callable[[], float] = time.time,
                 cursor_secret: bytes = b"public-demo-cursor-secret-change-before-deployment",
                 lease_seconds: float = 30, max_attempts: int = 3):
        self.path = str(path)
        self.clock = clock
        self.cursor_secret = cursor_secret
        self.lease_seconds = lease_seconds
        self.max_attempts = max_attempts

    @contextmanager
    def connection(self, *, write: bool = False):
        conn = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        # FULL makes committed WAL transactions durable subject to OS/storage guarantees.
        conn.execute("PRAGMA synchronous=FULL")
        try:
            conn.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def initialize(self):
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path, timeout=5)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            version = conn.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1):
                raise RuntimeError(f"Unsupported schema version: {version}")
            conn.executescript("""
                BEGIN IMMEDIATE;
                CREATE TABLE IF NOT EXISTS tenants (
                    tenant_id TEXT PRIMARY KEY, name TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS configs (
                    tenant_id TEXT PRIMARY KEY REFERENCES tenants(tenant_id),
                    version INTEGER NOT NULL CHECK(version > 0), body TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    tenant_id TEXT NOT NULL REFERENCES tenants(tenant_id),
                    idempotency_key TEXT NOT NULL, body_hash TEXT NOT NULL,
                    body TEXT NOT NULL, received_at REAL NOT NULL,
                    config_version INTEGER NOT NULL,
                    UNIQUE(tenant_id, idempotency_key), UNIQUE(event_id, tenant_id)
                );
                CREATE INDEX IF NOT EXISTS events_tenant_sequence
                    ON events(tenant_id, sequence DESC);
                CREATE TABLE IF NOT EXISTS outbox (
                    event_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('pending','inflight','delivered','dead')),
                    attempts INTEGER NOT NULL DEFAULT 0, available_at REAL NOT NULL,
                    lease_until REAL, lease_token TEXT, delivered_at REAL, last_error TEXT,
                    FOREIGN KEY(event_id, tenant_id) REFERENCES events(event_id, tenant_id)
                );
                CREATE INDEX IF NOT EXISTS outbox_claim
                    ON outbox(tenant_id, status, available_at, lease_until);
                CREATE TABLE IF NOT EXISTS attempt_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL, tenant_id TEXT NOT NULL,
                    attempt INTEGER NOT NULL, outcome TEXT NOT NULL,
                    detail TEXT NOT NULL, at REAL NOT NULL,
                    FOREIGN KEY(event_id, tenant_id) REFERENCES events(event_id, tenant_id)
                );
                CREATE INDEX IF NOT EXISTS log_tenant_id ON attempt_log(tenant_id, id DESC);
                CREATE TABLE IF NOT EXISTS receipts (
                    event_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL,
                    body TEXT NOT NULL, received_at REAL NOT NULL,
                    FOREIGN KEY(event_id, tenant_id) REFERENCES events(event_id, tenant_id)
                );
                PRAGMA user_version=1;
                COMMIT;
            """)
        finally:
            conn.close()
        with self.connection(write=True) as conn:
            for tenant_id, name in TENANTS.items():
                conn.execute("INSERT OR IGNORE INTO tenants VALUES (?, ?)", (tenant_id, name))
                conn.execute("INSERT OR IGNORE INTO configs VALUES (?, 1, ?)",
                             (tenant_id, canonical(DEFAULT_CONFIG)))

    def _event(self, row) -> dict:
        result = json.loads(row["body"])
        result.update(event_id=row["event_id"], sequence=row["sequence"],
                      received_at=iso(row["received_at"]), config_version=row["config_version"],
                      delivery={"status": row["status"], "attempts": row["attempts"],
                                "next_attempt_at": iso(row["available_at"]) if row["status"] == "pending" else None,
                                "last_error": row["last_error"]})
        return result

    @staticmethod
    def _select_event(conn, tenant_id, event_id):
        return conn.execute("""SELECT e.*, o.status, o.attempts, o.available_at, o.last_error
            FROM events e JOIN outbox o USING(event_id, tenant_id)
            WHERE e.tenant_id=? AND e.event_id=?""", (tenant_id, event_id)).fetchone()

    def ingest(self, tenant_id: str, key: str, body: dict) -> tuple[dict, bool]:
        encoded = canonical(body)
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        with self.connection(write=True) as conn:
            existing = conn.execute("SELECT event_id, body_hash FROM events WHERE tenant_id=? AND idempotency_key=?",
                                    (tenant_id, key)).fetchone()
            if existing:
                if existing["body_hash"] != digest:
                    raise Conflict("Idempotency key already belongs to a different event body")
                return self._event(self._select_event(conn, tenant_id, existing["event_id"])), True
            now, event_id = self.clock(), str(uuid.uuid4())
            config_version = conn.execute("SELECT version FROM configs WHERE tenant_id=?", (tenant_id,)).fetchone()[0]
            conn.execute("""INSERT INTO events
                (event_id,tenant_id,idempotency_key,body_hash,body,received_at,config_version)
                VALUES (?,?,?,?,?,?,?)""", (event_id, tenant_id, key, digest, encoded, now, config_version))
            conn.execute("INSERT INTO outbox(event_id,tenant_id,status,available_at) VALUES (?,?,'pending',?)",
                         (event_id, tenant_id, now))
            return self._event(self._select_event(conn, tenant_id, event_id)), False

    def get_event(self, tenant_id: str, event_id: str) -> dict | None:
        with self.connection() as conn:
            row = self._select_event(conn, tenant_id, event_id)
            return self._event(row) if row else None

    def _encode_cursor(self, data: dict) -> str:
        payload = base64.urlsafe_b64encode(canonical(data).encode()).rstrip(b"=")
        signature = hmac.new(self.cursor_secret, payload, hashlib.sha256).hexdigest()
        return payload.decode() + "." + signature

    def _decode_cursor(self, token: str, tenant_id: str) -> dict:
        try:
            payload, signature = token.split(".")
            expected = hmac.new(self.cursor_secret, payload.encode(), hashlib.sha256).hexdigest()
            if not hmac.compare_digest(signature, expected):
                raise ValueError()
            data = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
            if data["tenant"] != tenant_id or type(data["version"]) is not int or data["version"] != 1:
                raise ValueError()
            if any(type(data[k]) is not int or not 0 <= data[k] <= 9_223_372_036_854_775_807 for k in ("ceiling", "before")):
                raise ValueError()
            return data
        except (ValueError, KeyError, TypeError, UnicodeError) as exc:
            raise InvalidCursor("Invalid cursor for this tenant") from exc

    def list_events(self, tenant_id: str, limit: int = 25, cursor: str | None = None) -> dict:
        if not 1 <= limit <= 100:
            raise ValueError("limit must be between 1 and 100")
        data = self._decode_cursor(cursor, tenant_id) if cursor else None
        with self.connection() as conn:
            ceiling = data["ceiling"] if data else conn.execute(
                "SELECT COALESCE(MAX(sequence),0) FROM events WHERE tenant_id=?", (tenant_id,)).fetchone()[0]
            before = data["before"] if data else ceiling + 1
            rows = conn.execute("""SELECT e.*,o.status,o.attempts,o.available_at,o.last_error
                FROM events e JOIN outbox o USING(event_id,tenant_id)
                WHERE e.tenant_id=? AND e.sequence<=? AND e.sequence<?
                ORDER BY e.sequence DESC LIMIT ?""", (tenant_id, ceiling, before, limit + 1)).fetchall()
        items = [self._event(row) for row in rows[:limit]]
        next_cursor = self._encode_cursor({"tenant": tenant_id, "version": 1, "ceiling": ceiling,
                                          "before": items[-1]["sequence"]}) if len(rows) > limit else None
        return {"items": items, "next_cursor": next_cursor}

    def get_config(self, tenant_id: str) -> dict:
        with self.connection() as conn:
            row = conn.execute("SELECT version,body FROM configs WHERE tenant_id=?", (tenant_id,)).fetchone()
            return {"version": row["version"], "config": json.loads(row["body"])}

    def update_config(self, tenant_id: str, version: int, body: dict) -> dict:
        with self.connection(write=True) as conn:
            result = conn.execute("UPDATE configs SET body=?,version=version+1 WHERE tenant_id=? AND version=?",
                                  (canonical(body), tenant_id, version))
            if result.rowcount != 1:
                raise StaleVersion("Configuration changed; reload before saving again")
            return {"version": version + 1, "config": body}

    @staticmethod
    def _log(conn, event_id, tenant_id, attempt, outcome, detail, now):
        conn.execute("INSERT INTO attempt_log(event_id,tenant_id,attempt,outcome,detail,at) VALUES (?,?,?,?,?,?)",
                     (event_id, tenant_id, attempt, outcome, detail, now))

    def claim(self, tenant_id: str) -> Lease | None:
        with self.connection(write=True) as conn:
            # Read the clock after acquiring the write lock, which can block.
            now = self.clock()
            exhausted = conn.execute("""SELECT * FROM outbox WHERE tenant_id=? AND status='inflight'
                AND lease_until<=? AND attempts>=?""", (tenant_id, now, self.max_attempts)).fetchall()
            for row in exhausted:
                conn.execute("""UPDATE outbox SET status='dead',lease_token=NULL,lease_until=NULL,
                    last_error='Lease expired on final attempt; delivery outcome is unknown' WHERE event_id=?""", (row["event_id"],))
                self._log(conn, row["event_id"], tenant_id, row["attempts"], "dead",
                          "Final lease expired; delivery outcome unknown", now)
            row = conn.execute("""SELECT o.*,e.body FROM outbox o JOIN events e USING(event_id,tenant_id)
                WHERE o.tenant_id=? AND o.attempts<? AND
                  ((o.status='pending' AND o.available_at<=?) OR (o.status='inflight' AND o.lease_until<=?))
                ORDER BY o.available_at,e.sequence LIMIT 1""", (tenant_id, self.max_attempts, now, now)).fetchone()
            if not row:
                return None
            if row["status"] == "inflight":
                self._log(conn, row["event_id"], tenant_id, row["attempts"], "expired", "Lease expired; reclaimed", now)
            token, attempt = str(uuid.uuid4()), row["attempts"] + 1
            conn.execute("""UPDATE outbox SET status='inflight',attempts=?,lease_token=?,lease_until=?
                WHERE event_id=? AND tenant_id=?""", (attempt, token, now + self.lease_seconds, row["event_id"], tenant_id))
            return Lease(row["event_id"], tenant_id, token, attempt, json.loads(row["body"]))

    def finish(self, lease: Lease, *, error: str | None = None) -> str:
        with self.connection(write=True) as conn:
            now = self.clock()
            row = conn.execute("""SELECT * FROM outbox WHERE event_id=? AND tenant_id=?
                AND status='inflight' AND lease_token=? AND lease_until>?""",
                               (lease.event_id, lease.tenant_id, lease.token, now)).fetchone()
            if not row:
                return "lease_lost"
            status = "delivered" if error is None else "dead" if lease.attempt >= self.max_attempts else "pending"
            safe_error = error[:240] if error else None
            next_at = now + min(2 ** lease.attempt, 30) if status == "pending" else now
            conn.execute("""UPDATE outbox SET status=?,available_at=?,lease_token=NULL,lease_until=NULL,
                delivered_at=?,last_error=? WHERE event_id=? AND tenant_id=?""",
                         (status, next_at, now if status == "delivered" else None, safe_error, lease.event_id, lease.tenant_id))
            outcome = "retried" if status == "pending" else status
            self._log(conn, lease.event_id, lease.tenant_id, lease.attempt, outcome,
                      safe_error or "Synthetic receiver acknowledged", now)
            return outcome

    def record_receipt(self, lease: Lease) -> bool:
        """Toy receiver's idempotency boundary, intentionally separate from finish()."""
        with self.connection(write=True) as conn:
            result = conn.execute("INSERT OR IGNORE INTO receipts VALUES (?,?,?,?)",
                                  (lease.event_id, lease.tenant_id, canonical(lease.body), self.clock()))
            return result.rowcount == 1

    def summary(self, tenant_id: str) -> dict:
        with self.connection() as conn:
            counts = {key: 0 for key in ("pending", "inflight", "delivered", "dead")}
            for row in conn.execute("SELECT status,COUNT(*) n FROM outbox WHERE tenant_id=? GROUP BY status", (tenant_id,)):
                counts[row["status"]] = row["n"]
            logs = [dict(row) for row in conn.execute("""SELECT event_id,attempt,outcome,detail,at FROM attempt_log
                WHERE tenant_id=? ORDER BY id DESC LIMIT 20""", (tenant_id,))]
            for row in logs:
                row["at"] = iso(row["at"])
            return {"tenant_id": tenant_id, "tenant_name": TENANTS[tenant_id],
                    "event_count": conn.execute("SELECT COUNT(*) FROM events WHERE tenant_id=?", (tenant_id,)).fetchone()[0],
                    "outbox": counts,
                    "receipt_count": conn.execute("SELECT COUNT(*) FROM receipts WHERE tenant_id=?", (tenant_id,)).fetchone()[0],
                    "config_version": conn.execute("SELECT version FROM configs WHERE tenant_id=?", (tenant_id,)).fetchone()[0],
                    "recent_attempts": logs}
