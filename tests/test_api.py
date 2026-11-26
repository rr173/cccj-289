"""API surface tests through the real ASGI app (no network listener)."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from webhook_delivery.api import create_app
from webhook_delivery.config import FakeClock, Settings


@pytest.fixture()
def client(settings, clock):
    app = create_app(settings=settings, clock=clock)
    with TestClient(app) as client:
        yield client


def test_full_api_flow(client):
    customer = client.post("/customers", json={"name": "Acme"}).json()
    sub = client.post(
        "/subscriptions",
        json={"customer_id": customer["id"], "target_url": "http://example.test/hook"},
    )
    assert sub.status_code == 201
    sub = sub.json()
    assert sub["target_version"] == 1

    event = client.post(
        "/events",
        json={
            "customer_id": customer["id"],
            "flow_id": "f1",
            "caller_event_id": "c1",
            "payload": {"hello": "world"},
        },
    )
    assert event.status_code == 200
    body = event.json()
    assert body["seq"] == 1
    assert body["replayed"] is False
    assert body["deliveries"][0]["subscription_id"] == sub["id"]

    replay = client.post(
        "/events",
        json={
            "customer_id": customer["id"],
            "flow_id": "f1",
            "caller_event_id": "c1",
            "payload": {"hello": "world"},
        },
    ).json()
    assert replay["id"] == body["id"] and replay["replayed"] is True

    conflict = client.post(
        "/events",
        json={
            "customer_id": customer["id"],
            "flow_id": "f1",
            "caller_event_id": "c1",
            "payload": {"hello": "changed"},
        },
    )
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "caller_event_conflict"

    delivery = client.get(f"/deliveries/{body['deliveries'][0]['delivery_id']}").json()
    assert delivery["status"] == "pending"
    assert delivery["subscription_target"]["target_version"] == 1
    assert delivery["block_reason"] == "pending claim"

    listed = client.get(f"/subscriptions/{sub['id']}/deliveries").json()
    assert [d["delivery_id"] for d in listed] == [delivery["delivery_id"]]


def test_patch_subscription_bumps_version(client):
    customer = client.post("/customers", json={"name": "Acme"}).json()
    sub = client.post(
        "/subscriptions", json={"customer_id": customer["id"], "target_url": "http://a.test/hook"}
    ).json()
    patched = client.patch(
        f"/subscriptions/{sub['id']}",
        json={"customer_id": customer["id"], "target_url": "http://b.test/hook"},
    )
    assert patched.status_code == 200
    assert patched.json()["target_version"] == 2
    assert patched.json()["target_url"] == "http://b.test/hook"


def test_patch_subscription_rejects_other_customer(client):
    a = client.post("/customers", json={"name": "A"}).json()
    b = client.post("/customers", json={"name": "B"}).json()
    sub = client.post(
        "/subscriptions", json={"customer_id": a["id"], "target_url": "http://a.test/hook"}
    ).json()
    r = client.patch(
        f"/subscriptions/{sub['id']}",
        json={"customer_id": b["id"], "target_url": "http://evil.test/hook"},
    )
    assert r.status_code == 403


def test_validation_errors(client):
    assert client.post("/customers", json={"name": ""}).status_code == 422
    customer = client.post("/customers", json={"name": "A"}).json()
    bad_url = client.post(
        "/subscriptions", json={"customer_id": customer["id"], "target_url": "ftp://nope"}
    )
    assert bad_url.status_code == 400
    assert bad_url.json()["error"]["code"] == "invalid_target_url"


def test_admin_endpoints_require_reason(client, store, clock, customer, subscribe):
    ev = store.ingest_event(
        customer_id=customer["id"], flow_id="f", caller_event_id="e", payload={}, now=clock.now()
    )
    dlv = ev["deliveries"][0]["delivery_id"]
    r = client.post(f"/deliveries/{dlv}/skip", json={"reason": ""})
    assert r.status_code == 422
    r = client.post(f"/deliveries/{dlv}/retry", json={"reason": "try"})
    assert r.status_code == 409  # not parked yet
    assert r.json()["error"]["code"] == "invalid_state"


def test_unknown_delivery_404(client):
    assert client.get("/deliveries/dlv_nope").status_code == 404
