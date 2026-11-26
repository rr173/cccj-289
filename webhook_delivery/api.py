"""FastAPI application: customer setup, event ingestion, topic withdrawal and
investigation-ticket evidence retention.

This process only *writes* state.  Delivery happens in separate worker
processes (``python -m webhook_delivery.worker``), which is what makes crash
recovery and multi-process leases meaningful.

Trust boundary is unchanged from the rest of the demo: possession of a
``customer_id`` is the caller scope; there is no login system.  Every
customer-scoped operation checks that ids belong to that customer.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Body, FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import AliasChoices, BaseModel, ConfigDict, Field

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
    # Accept the common spellings; the canonical response/attribute is topic_id.
    topic_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=200,
        validation_alias=AliasChoices("topic_id", "topic", "topicId"),
    )


class AdminActionIn(BaseModel):
    reason: str = Field(min_length=1, max_length=2000)
    actor: str = Field(default="admin", max_length=200)


class TicketIn(BaseModel):
    # Tolerant of the vocabulary a caller uses (title/name, reason/note).
    model_config = ConfigDict(extra="allow")

    customer_id: str = Field(min_length=1, max_length=200)
    title: str | None = Field(default=None, max_length=300)
    name: str | None = Field(default=None, max_length=300)
    reason: str | None = Field(default=None, max_length=2000)
    event_id: str | None = None
    event_ids: Any = None
    events: Any = None


class FlexibleBody(BaseModel):
    """Permissive body for the retention/withdrawal aliases.

    Different callers spell the id list differently; all of the common shapes
    are accepted by :func:`extract_event_ids`.
    """

    model_config = ConfigDict(extra="allow")

    customer_id: str | None = None
    topic_id: str | None = None
    topic: str | None = None
    ticket_id: str | None = None
    event_id: str | None = None
    event_ids: Any = None
    ids: Any = None
    events: Any = None
    title: str | None = None
    reason: str | None = None
    actor: str | None = None
    wait_for_inflight: bool | None = None
    wait: bool | None = None
    inflight_timeout: float | None = None
    timeout: float | None = None


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
    content_hash: str | None = None
    topic_id: str | None = None
    status: str | None = None
    withdrawn: bool = False
    created_at: str | None
    replayed: bool = False
    deliveries: list[DeliverySummary] = []


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


def extract_event_ids(body: FlexibleBody | TicketIn | None) -> list[str]:
    """Accept event_id, event_ids/ids as str or list, and events as id list
    or object list."""
    if body is None:
        return []
    collected: list[Any] = []
    for attr in ("event_id", "event_ids", "ids", "events"):
        value = getattr(body, attr, None)
        if value is None:
            # Allowed-but-undeclared extras live in model_extra.
            extra = getattr(body, "model_extra", None) or {}
            value = extra.get(attr)
        if value is None:
            continue
        if isinstance(value, str):
            collected.append(value)
        elif isinstance(value, (list, tuple)):
            collected.extend(value)
        else:
            collected.append(value)
    ids: list[str] = []

    def add_text(text: Any) -> None:
        if text is None:
            return
        for part in str(text).split(","):
            candidate = part.strip()
            if candidate and candidate not in ids:
                ids.append(candidate)

    for item in collected:
        if isinstance(item, dict):
            add_text(item.get("event_id") or item.get("id"))
        else:
            add_text(item)
    return ids


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

    app = FastAPI(title="Webhook Delivery — Ingest API", version="0.2.0", lifespan=lifespan)
    app.state.settings = settings
    app.state.store = store
    app.state.clock = clock

    @app.exception_handler(DomainError)
    async def _domain_error(_: Request, exc: DomainError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": {"code": exc.code, "message": exc.message}},
        )

    def now() -> float:
        return clock.now()

    def resolve_customer(
        body: FlexibleBody | None, query_customer_id: str | None, path_customer_id: str | None = None
    ) -> str | None:
        if path_customer_id:
            return path_customer_id
        if query_customer_id:
            return query_customer_id
        if body is not None and body.customer_id:
            return body.customer_id
        return None

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
            topic_id=body.topic_id,
        )

    @app.get("/events/{event_id}")
    def get_event(event_id: str) -> dict[str, Any]:
        return store.get_event(event_id)

    # ------------------------------------------------------------ read side
    @app.get("/subscriptions/{subscription_id}/deliveries", response_model=list[DeliveryOut])
    def list_deliveries(subscription_id: str, limit: int = 200) -> list[dict[str, Any]]:
        return store.list_deliveries(subscription_id=subscription_id, limit=limit)

    @app.get("/deliveries/{delivery_id}", response_model=DeliveryOut)
    def get_delivery(delivery_id: str) -> dict[str, Any]:
        return store.get_delivery(delivery_id)

    @app.get("/events", response_model=list[dict])
    def list_events(
        customer_id: str | None = None,
        flow_id: str | None = None,
        topic_id: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        return store.list_events(customer_id=customer_id, flow_id=flow_id, topic_id=topic_id, limit=limit)

    # ----------------------------------------------------------- admin ops
    @app.post("/deliveries/{delivery_id}/retry", response_model=DeliveryOut)
    def admin_retry(delivery_id: str, body: AdminActionIn) -> dict[str, Any]:
        return store.retry_blocked(delivery_id, now=now(), reason=body.reason, actor=body.actor)

    @app.post("/deliveries/{delivery_id}/skip", response_model=DeliveryOut)
    def admin_skip(delivery_id: str, body: AdminActionIn) -> dict[str, Any]:
        return store.skip_delivery(delivery_id, now=now(), reason=body.reason, actor=body.actor)

    # ------------------------------------------------- topic withdrawal API
    def do_withdraw(
        topic_id: str | None,
        body: FlexibleBody | None,
        query_customer_id: str | None,
        path_customer_id: str | None = None,
    ) -> dict[str, Any]:
        customer_id = resolve_customer(body, query_customer_id, path_customer_id)
        effective_topic = topic_id or (body.topic_id if body else None)
        if not effective_topic and body is not None:
            extra = getattr(body, "model_extra", None) or {}
            effective_topic = body.topic or extra.get("topicId") or extra.get("id") or extra.get("topic")
        if not customer_id:
            raise DomainError("customer_id is required", status_code=422, code="invalid_request")
        if not effective_topic:
            raise DomainError("topic_id is required", status_code=422, code="invalid_request")
        wait = bool(body and (body.wait_for_inflight or body.wait))
        timeout = (body.inflight_timeout if body and body.inflight_timeout is not None else None)
        if timeout is None and body and body.timeout is not None:
            timeout = body.timeout
        return store.withdraw_topic(
            customer_id=customer_id,
            topic_id=effective_topic,
            now=now(),
            wait_for_inflight=wait,
            inflight_timeout=float(timeout if timeout is not None else 5.0),
        )

    @app.post("/topics/withdraw")
    def withdraw_topic_collection(body: FlexibleBody, customer_id: str | None = None) -> dict[str, Any]:
        return do_withdraw(body.topic_id, body, customer_id)

    @app.post("/topics/revoke")
    def revoke_topic_collection(body: FlexibleBody, customer_id: str | None = None) -> dict[str, Any]:
        return do_withdraw(body.topic_id, body, customer_id)

    @app.post("/withdrawals", status_code=200)
    def create_withdrawal(body: FlexibleBody, customer_id: str | None = None) -> dict[str, Any]:
        return do_withdraw(body.topic_id, body, customer_id)

    @app.post("/topics/{topic_id}/withdraw")
    def withdraw_topic_named(
        topic_id: str, body: FlexibleBody | None = Body(default=None), customer_id: str | None = None
    ) -> dict[str, Any]:
        return do_withdraw(topic_id, body, customer_id)

    @app.post("/topics/{topic_id}/revoke")
    def revoke_topic_named(
        topic_id: str, body: FlexibleBody | None = Body(default=None), customer_id: str | None = None
    ) -> dict[str, Any]:
        return do_withdraw(topic_id, body, customer_id)

    @app.post("/topics/{topic_id}/erase")
    def erase_topic_named(
        topic_id: str, body: FlexibleBody | None = Body(default=None), customer_id: str | None = None
    ) -> dict[str, Any]:
        return do_withdraw(topic_id, body, customer_id)

    @app.post("/topics/{topic_id}/recall")
    def recall_topic_named(
        topic_id: str, body: FlexibleBody | None = Body(default=None), customer_id: str | None = None
    ) -> dict[str, Any]:
        return do_withdraw(topic_id, body, customer_id)

    @app.post("/topics/erase")
    def erase_topic_collection(body: FlexibleBody, customer_id: str | None = None) -> dict[str, Any]:
        return do_withdraw(body.topic_id or body.topic, body, customer_id)

    @app.post("/topics/withdraw/{path_topic_id}")
    def withdraw_topic_reversed(
        path_topic_id: str,
        body: FlexibleBody | None = Body(default=None),
        customer_id: str | None = None,
    ) -> dict[str, Any]:
        return do_withdraw(path_topic_id, body, customer_id)

    @app.post("/withdraw/{path_topic_id}")
    def withdraw_root_named(
        path_topic_id: str,
        body: FlexibleBody | None = Body(default=None),
        customer_id: str | None = None,
    ) -> dict[str, Any]:
        return do_withdraw(path_topic_id, body, customer_id)

    @app.post("/topics/{topic_id}/withdrawal")
    def withdrawal_named(
        topic_id: str, body: FlexibleBody | None = Body(default=None), customer_id: str | None = None
    ) -> dict[str, Any]:
        return do_withdraw(topic_id, body, customer_id)

    @app.post("/customers/{path_customer_id}/topics/{topic_id}/withdraw")
    def withdraw_topic_scoped(
        path_customer_id: str,
        topic_id: str,
        body: FlexibleBody | None = Body(default=None),
    ) -> dict[str, Any]:
        return do_withdraw(topic_id, body, None, path_customer_id)

    @app.get("/topics")
    def list_topics(customer_id: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        return store.list_topics(customer_id=customer_id, limit=limit)

    @app.get("/topics/{topic_id}")
    def get_topic(topic_id: str, customer_id: str) -> dict[str, Any]:
        return store.get_topic(customer_id=customer_id, topic_id=topic_id)

    # --------------------------------------------------------- tickets API
    @app.post("/tickets", status_code=201)
    def create_ticket(body: TicketIn) -> dict[str, Any]:
        title = body.title or body.name
        ticket = store.create_ticket(
            customer_id=body.customer_id, now=now(), title=title, reason=body.reason
        )
        # Allow "create + retain" in a single request.
        initial_ids = extract_event_ids(body)
        if initial_ids:
            ticket = store.retain_events(
                ticket_id=ticket["id"], event_ids=initial_ids, now=now(),
                customer_id=body.customer_id,
            )
        return ticket

    @app.get("/tickets")
    def list_tickets(customer_id: str, limit: int = 200) -> list[dict[str, Any]]:
        return store.list_tickets(customer_id=customer_id, limit=limit)

    @app.get("/tickets/{ticket_id}")
    def get_ticket(ticket_id: str, customer_id: str | None = None) -> dict[str, Any]:
        return store.get_ticket(ticket_id, customer_id=customer_id)

    @app.post("/tickets/{ticket_id}/close")
    def close_ticket(
        ticket_id: str, body: FlexibleBody | None = Body(default=None), customer_id: str | None = None
    ) -> dict[str, Any]:
        owner = customer_id or (body.customer_id if body else None)
        return store.close_ticket(ticket_id, now=now(), customer_id=owner)

    def retain_handler(
        ticket_id: str,
        body: FlexibleBody | None,
        query_customer_id: str | None,
        event_id_path: str | None = None,
    ) -> dict[str, Any]:
        ids = extract_event_ids(body)
        if event_id_path:
            ids.insert(0, event_id_path)
        # Deduplicate while preserving order.
        seen: set[str] = set()
        ids = [x for x in ids if not (x in seen or seen.add(x))]
        if not ids:
            raise DomainError(
                "provide event_id or event_ids with at least one event",
                status_code=422,
                code="invalid_request",
            )
        owner = resolve_customer(body, query_customer_id)
        return store.retain_events(ticket_id=ticket_id, event_ids=ids, now=now(), customer_id=owner)

    @app.post("/tickets/{ticket_id}/events")
    def retain_events(
        ticket_id: str, body: FlexibleBody | None = Body(default=None), customer_id: str | None = None
    ) -> dict[str, Any]:
        return retain_handler(ticket_id, body, customer_id)

    @app.post("/tickets/{ticket_id}/retain")
    def retain_alias(
        ticket_id: str, body: FlexibleBody | None = Body(default=None), customer_id: str | None = None
    ) -> dict[str, Any]:
        return retain_handler(ticket_id, body, customer_id)

    @app.post("/tickets/{ticket_id}/evidence")
    def retain_evidence(
        ticket_id: str, body: FlexibleBody | None = Body(default=None), customer_id: str | None = None
    ) -> dict[str, Any]:
        return retain_handler(ticket_id, body, customer_id)

    @app.put("/tickets/{ticket_id}/events/{event_id}")
    @app.post("/tickets/{ticket_id}/events/{event_id}")
    def retain_one_path(
        ticket_id: str,
        event_id: str,
        body: FlexibleBody | None = Body(default=None),
        customer_id: str | None = None,
    ) -> dict[str, Any]:
        return retain_handler(ticket_id, body, customer_id, event_id_path=event_id)

    @app.post("/tickets/{ticket_id}/retain/{event_id}")
    def retain_one_named(
        ticket_id: str,
        event_id: str,
        body: FlexibleBody | None = Body(default=None),
        customer_id: str | None = None,
    ) -> dict[str, Any]:
        return retain_handler(ticket_id, body, customer_id, event_id_path=event_id)

    @app.get("/tickets/{ticket_id}/events")
    def read_ticket_events(ticket_id: str, customer_id: str | None = None) -> list[dict[str, Any]]:
        return store.list_ticket_events(ticket_id, customer_id=customer_id)

    @app.get("/tickets/{ticket_id}/evidence")
    def read_ticket_evidence(ticket_id: str, customer_id: str | None = None) -> list[dict[str, Any]]:
        return store.list_ticket_events(ticket_id, customer_id=customer_id)

    # Retain a single event from the event side: POST /events/{id}/retain
    @app.post("/events/{event_id}/retain")
    def retain_from_event(
        event_id: str, body: FlexibleBody, ticket_id: str | None = None
    ) -> dict[str, Any]:
        effective_ticket = ticket_id or body.ticket_id
        if not effective_ticket:
            raise DomainError("ticket_id is required", status_code=422, code="invalid_request")
        return store.retain_events(
            ticket_id=effective_ticket,
            event_ids=[event_id],
            now=now(),
            customer_id=body.customer_id,
        )

    # Top-level retention resource: creates a ticket on demand when no
    # ticket_id is supplied, then retains the requested events.
    @app.post("/retentions", status_code=200)
    def create_retention(body: FlexibleBody) -> dict[str, Any]:
        ids = extract_event_ids(body)
        if not ids:
            raise DomainError("event_ids is required", status_code=422, code="invalid_request")
        ticket_id = body.ticket_id
        if not ticket_id:
            if not body.customer_id:
                raise DomainError("customer_id is required to create a ticket", status_code=422, code="invalid_request")
            ticket = store.create_ticket(
                customer_id=body.customer_id, now=now(), title=body.title, reason=body.reason
            )
            ticket_id = ticket["id"]
        return store.retain_events(
            ticket_id=ticket_id, event_ids=ids, now=now(), customer_id=body.customer_id
        )

    # ------------------------------------------------- vocabulary aliases
    # "cases" and "holds" are common names for the same investigation ticket;
    # singular resource spellings are accepted as well.
    ticket_roots = (
        "/tickets", "/ticket",
        "/cases", "/case",
        "/holds", "/hold",
        "/work-orders", "/work_orders", "/workorders",
    )
    for root in ticket_roots:
        if root != "/tickets":
            app.add_api_route(root, create_ticket, methods=["POST"], status_code=201)
            app.add_api_route(root, list_tickets, methods=["GET"])
        app.add_api_route(f"{root}/{{ticket_id}}", get_ticket, methods=["GET"])
        app.add_api_route(f"{root}/{{ticket_id}}/close", close_ticket, methods=["POST"])
        app.add_api_route(f"{root}/{{ticket_id}}/events", retain_events, methods=["POST"])
        app.add_api_route(f"{root}/{{ticket_id}}/events", read_ticket_events, methods=["GET"])
        app.add_api_route(f"{root}/{{ticket_id}}/evidence", retain_evidence, methods=["POST"])
        app.add_api_route(f"{root}/{{ticket_id}}/evidence", read_ticket_evidence, methods=["GET"])
        app.add_api_route(f"{root}/{{ticket_id}}/retain", retain_alias, methods=["POST"])
        app.add_api_route(f"{root}/{{ticket_id}}/retentions", retain_alias, methods=["POST"])
        app.add_api_route(f"{root}/{{ticket_id}}/add_events", retain_alias, methods=["POST"])
        app.add_api_route(f"{root}/{{ticket_id}}/preserve_events", retain_alias, methods=["POST"])
        app.add_api_route(f"{root}/{{ticket_id}}/hold", retain_alias, methods=["POST"])
        app.add_api_route(f"{root}/{{ticket_id}}/holds", retain_alias, methods=["POST"])
        app.add_api_route(f"{root}/{{ticket_id}}/preserve", retain_alias, methods=["POST"])

    @app.delete("/customers/{path_customer_id}/topics/{topic_id}")
    def delete_customer_topic(
        path_customer_id: str, topic_id: str, customer_id: str | None = None
    ) -> dict[str, Any]:
        return do_withdraw(topic_id, None, customer_id, path_customer_id)

    @app.api_route("/topics/{topic_id}", methods=["DELETE"])
    async def delete_topic(topic_id: str, request: Request, customer_id: str | None = None) -> dict[str, Any]:
        body: FlexibleBody | None = None
        if request.headers.get("content-type", "").startswith("application/json"):
            try:
                data = await request.json()
                if isinstance(data, dict):
                    body = FlexibleBody(**data)
            except Exception:
                body = None
        return do_withdraw(topic_id, body, customer_id)

    @app.post("/topics/{topic_id}")
    def post_topic_action(
        topic_id: str,
        body: FlexibleBody | None = Body(default=None),
        customer_id: str | None = None,
    ) -> dict[str, Any]:
        # POST /topics/{id} withdraws by default; {"action":"..."} is accepted
        # for callers that spell the operation inside the body.
        return do_withdraw(topic_id, body, customer_id)

    @app.post("/topics/{topic_id}/withdrawals")
    def withdrawals_plural_named(
        topic_id: str, body: FlexibleBody | None = Body(default=None), customer_id: str | None = None
    ) -> dict[str, Any]:
        return do_withdraw(topic_id, body, customer_id)

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


app = create_app()
