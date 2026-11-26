"""FastAPI application: customer setup and event ingestion.

This process only *writes* state.  Delivery happens in separate worker
processes (``python -m webhook_delivery.worker``), which is what makes crash
recovery and multi-process leases meaningful.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .config import Clock, RealClock, Settings
from .store import Store
from .util import DomainError


class CustomerIn(BaseModel):
    name: str = Field(min_length=1, max_length=200)


class SubscriptionIn(BaseModel):
    customer_id: str = Field(min_length=1, max_length=200)
    target_url: str = Field(min_length=1, max_length=2000)


class SubscriptionPatch(BaseModel):
    customer_id: str = Field(min_length=1, max_length=200)
    target_url: str = Field(min_length=1, max_length=2000)
    expected_target_version: int | None = None


class EventIn(BaseModel):
    customer_id: str = Field(min_length=1, max_length=200)
    flow_id: str = Field(min_length=1, max_length=200)
    caller_event_id: str = Field(min_length=1, max_length=200)
    payload: Any


class AdminActionIn(BaseModel):
    reason: str = Field(min_length=1, max_length=2000)
    actor: str = Field(default="admin", max_length=200)


class SubscriptionOut(BaseModel):
    id: str
    customer_id: str
    target_url: str
    target_version: int
    ordinal: int
    created_at: str | None
    updated_at: str | None


class SubscriptionTargetOut(BaseModel):
    target_url: str
    target_version: int


class DeliverySummary(BaseModel):
    delivery_id: str
    subscription_id: str
    status: str


class EventOut(BaseModel):
    id: str
    customer_id: str
    flow_id: str
    caller_event_id: str
    seq: int
    payload: Any
    content_hash: str
    created_at: str | None
    replayed: bool
    deliveries: list[DeliverySummary]


class AttemptOut(BaseModel):
    attempt_number: int
    target_url: str
    target_version: int
    started_at: str | None
    finished_at: str | None
    outcome: str
    http_status: int | None
    error: str | None


class AdminActionOut(BaseModel):
    action: str
    reason: str | None
    actor: str | None
    target_version: int | None
    created_at: str | None


class DeliveryOut(BaseModel):
    delivery_id: str
    subscription_id: str
    customer_id: str
    flow_id: str
    seq: int
    event: dict[str, Any]
    status: str
    attempts_count: int
    target_url: str
    target_version: int
    subscription_target: SubscriptionTargetOut
    next_attempt_at: str | None
    blocked: bool
    block_reason: str | None
    blocking_delivery_ids: list[str]
    last_error: str | None
    created_at: str | None
    updated_at: str | None
    completed_at: str | None
    attempts: list[AttemptOut]
    admin_actions: list[AdminActionOut]
    late_results: list[dict[str, Any]]


def create_app(
    *, settings: Settings | None = None, store: Store | None = None, clock: Clock | None = None
) -> FastAPI:
    settings = settings or Settings(database_path=os.environ.get("WEBHOOK_DB", "webhook_delivery.db"))
    store = store or Store(settings)
    clock = clock or RealClock()

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        store.initialize()
        yield

    app = FastAPI(title="Webhook Delivery — Ingest API", version="0.1.0", lifespan=lifespan)
    app.state.settings = settings
    app.state.store = store
    app.state.clock = clock

    def api_error(exc: DomainError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": {"code": exc.code, "message": exc.message}},
        )

    @app.exception_handler(DomainError)
    async def _domain_error(_: Request, exc: DomainError) -> JSONResponse:
        return api_error(exc)

    def now() -> float:
        return clock.now()

    # ------------------------------------------------------------ customers
    @app.post("/customers", status_code=201, response_model=dict)
    def create_customer(body: CustomerIn) -> dict[str, Any]:
        return store.create_customer(name=body.name, now=now())

    # -------------------------------------------------------- subscriptions
    @app.post("/subscriptions", status_code=201, response_model=SubscriptionOut)
    def create_subscription(body: SubscriptionIn) -> dict[str, Any]:
        return store.create_subscription(customer_id=body.customer_id, target_url=body.target_url, now=now())

    @app.get("/subscriptions/{subscription_id}", response_model=SubscriptionOut)
    def get_subscription(subscription_id: str) -> dict[str, Any]:
        return store.get_subscription(subscription_id)

    @app.patch("/subscriptions/{subscription_id}", response_model=SubscriptionOut)
    def update_subscription(subscription_id: str, body: SubscriptionPatch) -> dict[str, Any]:
        if body.customer_id != store.get_subscription(subscription_id)["customer_id"]:
            raise DomainError(
                "customer_id does not own this subscription", status_code=403, code="forbidden"
            )
        return store.update_subscription_target(
            subscription_id,
            target_url=body.target_url,
            now=now(),
            expected_version=body.expected_target_version,
        )

    # --------------------------------------------------------------- events
    @app.post("/events", response_model=EventOut)
    def ingest_event(body: EventIn) -> dict[str, Any]:
        # 200 for both fresh and replayed events; ``replayed`` distinguishes
        # them so a client can tell that its retry was idempotent.
        return store.ingest_event(
            customer_id=body.customer_id,
            flow_id=body.flow_id,
            caller_event_id=body.caller_event_id,
            payload=body.payload,
            now=now(),
        )

    # ------------------------------------------------------------ read side
    @app.get("/subscriptions/{subscription_id}/deliveries", response_model=list[DeliveryOut])
    def list_deliveries(subscription_id: str, limit: int = 200) -> list[dict[str, Any]]:
        return store.list_deliveries(subscription_id=subscription_id, limit=limit)

    @app.get("/deliveries/{delivery_id}", response_model=DeliveryOut)
    def get_delivery(delivery_id: str) -> dict[str, Any]:
        return store.get_delivery(delivery_id)

    @app.get("/events", response_model=list[dict])
    def list_events(customer_id: str | None = None, flow_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return store.list_events(customer_id=customer_id, flow_id=flow_id, limit=limit)

    # ----------------------------------------------------------- admin ops
    @app.post("/deliveries/{delivery_id}/retry", response_model=DeliveryOut)
    def admin_retry(delivery_id: str, body: AdminActionIn) -> dict[str, Any]:
        return store.retry_blocked(delivery_id, now=now(), reason=body.reason, actor=body.actor)

    @app.post("/deliveries/{delivery_id}/skip", response_model=DeliveryOut)
    def admin_skip(delivery_id: str, body: AdminActionIn) -> dict[str, Any]:
        return store.skip_delivery(delivery_id, now=now(), reason=body.reason, actor=body.actor)

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


app = create_app()
