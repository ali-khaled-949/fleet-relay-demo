"""Tenant-scoped HTTP API. Demo keys are public fixtures, not real authentication."""

import hmac
import os
import re
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field, field_validator

from .store import Conflict, InvalidCursor, StaleVersion, Store, canonical
from .worker import dispatch


DEMO_KEYS = {"fleet-demo-north": "northstar", "fleet-demo-south": "solstice"}
STATIC = Path(__file__).parent / "static"


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class EventIn(StrictModel):
    device_id: str = Field(min_length=1, max_length=80, pattern=r"^[A-Za-z0-9_.:-]+$")
    kind: Literal["heartbeat", "alert", "metric"]
    severity: Literal["info", "warning", "critical"] = "info"
    occurred_at: datetime
    payload: dict = Field(default_factory=dict)

    @field_validator("occurred_at")
    @classmethod
    def require_timezone(cls, value):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("occurred_at must include a timezone")
        try:
            return value.astimezone(timezone.utc)
        except (OverflowError, ValueError) as exc:
            raise ValueError("occurred_at is outside the supported UTC range") from exc

    @field_validator("payload")
    @classmethod
    def bounded_payload(cls, value):
        try:
            encoded = canonical(value)
        except (ValueError, TypeError, RecursionError) as exc:
            raise ValueError("payload must contain finite JSON values") from exc
        if len(encoded.encode()) > 16_384:
            raise ValueError("payload must be no larger than 16 KiB")
        return value


class DeviceConfig(StrictModel):
    sample_interval_seconds: int = Field(ge=5, le=3600, strict=True)
    enabled: bool = Field(strict=True)
    labels: dict[str, str] = Field(default_factory=dict, max_length=20)

    @field_validator("labels")
    @classmethod
    def bounded_labels(cls, value):
        if any(not 1 <= len(key) <= 64 or len(val) > 128 for key, val in value.items()):
            raise ValueError("labels require 1–64 character keys and values up to 128 characters")
        try:
            canonical(value)
        except (ValueError, TypeError) as exc:
            raise ValueError("labels must contain valid Unicode strings") from exc
        return value


class DispatchIn(StrictModel):
    limit: int = Field(default=20, ge=1, le=100, strict=True)


class BodyLimitMiddleware:
    """Bound actual request bytes, including requests without Content-Length."""

    def __init__(self, app, max_bytes=65_536):
        self.app, self.max_bytes = app, max_bytes

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["method"] not in ("POST", "PUT", "PATCH"):
            return await self.app(scope, receive, send)
        chunks, size = [], 0
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            chunk = message.get("body", b"")
            size += len(chunk)
            if size > self.max_bytes:
                response = JSONResponse(status_code=413, content={"detail": "Request body exceeds 64 KiB"})
                return await response(scope, receive, send)
            chunks.append(chunk)
            if not message.get("more_body", False):
                break
        sent = False

        async def replay():
            nonlocal sent
            if not sent:
                sent = True
                return {"type": "http.request", "body": b"".join(chunks), "more_body": False}
            return await receive()

        await self.app(scope, replay, send)


def create_app(db_path: str | Path | None = None, *, store: Store | None = None) -> FastAPI:
    store = store or Store(db_path or os.environ.get("FLEET_DB", "data/fleet-relay.sqlite3"),
                          cursor_secret=os.environ.get("FLEET_CURSOR_SECRET", "public-demo-cursor-secret-change-before-deployment").encode())

    @asynccontextmanager
    async def lifespan(app):
        store.initialize()
        yield

    app = FastAPI(title="Fleet Relay", version="0.1.0", lifespan=lifespan,
                  description="Synthetic event delivery reference project. Public demo keys; single-host SQLite.")
    app.state.store = store
    app.add_middleware(BodyLimitMiddleware)

    @app.exception_handler(RequestValidationError)
    async def invalid_input(request, exc):
        # Never echo raw input: it may contain NaN, invalid Unicode, or private payloads.
        def clean(value):
            return str(value).encode("utf-8", errors="replace").decode("utf-8")
        errors = [{"loc": [item if isinstance(item, int) else clean(item) for item in error["loc"]],
                   "type": clean(error["type"]), "msg": clean(error["msg"])} for error in exc.errors()]
        return JSONResponse(status_code=422, content={"detail": errors})

    @app.middleware("http")
    async def headers(request, call_next):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Cache-Control"] = "no-store"
        if request.url.path == "/":
            response.headers["Content-Security-Policy"] = "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'"
        return response

    def tenant(authorization: Annotated[str | None, Header()] = None) -> str:
        if authorization and authorization.startswith("Bearer "):
            token = authorization.removeprefix("Bearer ")
            for key, tenant_id in DEMO_KEYS.items():
                if hmac.compare_digest(token.encode(), key.encode()):
                    return tenant_id
        raise HTTPException(401, "A valid public demo Bearer key is required", headers={"WWW-Authenticate": "Bearer"})

    Tenant = Annotated[str, Depends(tenant)]

    @app.exception_handler(sqlite3.OperationalError)
    async def database_error(request, exc):
        if "locked" in str(exc).lower() or "busy" in str(exc).lower():
            return JSONResponse(status_code=503, content={"detail": "Database busy; retry shortly"}, headers={"Retry-After": "1"})
        # Details are intentionally not returned to the client.
        return JSONResponse(status_code=500, content={"detail": "Database operation failed"})

    @app.get("/", include_in_schema=False)
    def dashboard():
        return FileResponse(STATIC / "index.html")

    @app.get("/healthz", tags=["operations"])
    def health():
        with store.connection() as conn:
            conn.execute("SELECT 1").fetchone()
        return {"status": "ok", "storage": "sqlite", "scope": "single-host synthetic prototype"}

    @app.post("/v1/events", tags=["events"])
    def ingest(body: EventIn, response: Response, tenant_id: Tenant,
               idempotency_key: Annotated[str, Header(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")]):
        try:
            event, duplicate = store.ingest(tenant_id, idempotency_key, body.model_dump(mode="json"))
        except Conflict as exc:
            raise HTTPException(409, str(exc)) from exc
        response.status_code = 200 if duplicate else 201
        return {"event": event, "duplicate": duplicate}

    @app.get("/v1/events", tags=["events"])
    def events(tenant_id: Tenant, limit: Annotated[int, Query(ge=1, le=100)] = 25,
               cursor: Annotated[str | None, Query(max_length=2048)] = None):
        try:
            return store.list_events(tenant_id, limit, cursor)
        except InvalidCursor as exc:
            raise HTTPException(400, str(exc)) from exc

    @app.get("/v1/events/{event_id}", tags=["events"])
    def event(event_id: str, tenant_id: Tenant):
        result = store.get_event(tenant_id, event_id)
        if result is None:
            raise HTTPException(404, "Event not found")
        return result

    @app.get("/v1/config", tags=["configuration"])
    def config(response: Response, tenant_id: Tenant):
        result = store.get_config(tenant_id)
        response.headers["ETag"] = f'"{result["version"]}"'
        return result

    @app.put("/v1/config", tags=["configuration"])
    def update_config(body: DeviceConfig, response: Response, tenant_id: Tenant,
                      if_match: Annotated[str | None, Header(max_length=32)] = None):
        if if_match is None:
            raise HTTPException(428, "If-Match is required; GET config to obtain its ETag")
        if not re.fullmatch(r'"[1-9][0-9]*"', if_match):
            raise HTTPException(400, 'If-Match must be a quoted version, such as "1"')
        version = int(if_match.strip('"'))
        if version >= 9_223_372_036_854_775_807:
            raise HTTPException(400, "If-Match version is outside the supported integer range")
        try:
            result = store.update_config(tenant_id, version, body.model_dump())
        except StaleVersion as exc:
            raise HTTPException(412, str(exc)) from exc
        response.headers["ETag"] = f'"{result["version"]}"'
        return result

    @app.get("/v1/summary", tags=["operations"])
    def summary(tenant_id: Tenant):
        return store.summary(tenant_id)

    @app.post("/v1/dispatch", tags=["demo controls"])
    def run_dispatch(body: DispatchIn, tenant_id: Tenant):
        """Manual, bounded synthetic dispatch. No external receiver is contacted."""
        return dispatch(store, tenant_id, limit=body.limit)

    app.mount("/static", StaticFiles(directory=STATIC), name="static")
    return app


app = create_app()
