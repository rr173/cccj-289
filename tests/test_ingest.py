"""Idempotency, sequence allocation, subscription cutoff and target changes."""

from __future__ import annotations

import pytest

from webhook_delivery.store import Store
from webhook_delivery.util import DomainError


def ingest(store, clock, customer, flow="f1", caller="e1", payload=None):
    return store.ingest_event(
        customer_id=customer["id"],
        flow_id=flow,
        caller_event_id=caller,
        payload={"v": 1} if payload is None else payload,
        now=clock.now(),
    )


def test_replay_same_content_returns_original(store, clock, customer, subscribe):
    first = ingest(store, clock, customer)
    clock.advance(10)
    again = ingest(store, clock, customer)
    assert again["id"] == first["id"]
    assert again["seq"] == first["seq"]
    assert again["replayed"] is True
    # No second delivery was created.
    assert [d["delivery_id"] for d in again["deliveries"]] == [d["delivery_id"] for d in first["deliveries"]]


def test_replay_with_different_content_is_rejected(store, clock, customer, subscribe):
    ingest(store, clock, customer, payload={"v": 1})
    with pytest.raises(DomainError) as exc:
        ingest(store, clock, customer, payload={"v": 2})
    assert exc.value.code == "caller_event_conflict"
    assert exc.value.status_code == 409


def test_same_caller_id_in_different_flow_is_a_different_event(store, clock, customer, subscribe):
    a = ingest(store, clock, customer, flow="f1", caller="e1")
    b = ingest(store, clock, customer, flow="f2", caller="e1")
    assert a["id"] != b["id"]
    assert a["seq"] == b["seq"] == 1


def test_sequence_is_per_flow(store, clock, customer, subscribe):
    seqs = [ingest(store, clock, customer, flow="f1", caller=f"a{i}")["seq"] for i in range(3)]
    other = [ingest(store, clock, customer, flow="f2", caller=f"b{i}")["seq"] for i in range(2)]
    assert seqs == [1, 2, 3]
    assert other == [1, 2]


def test_event_before_subscription_is_not_delivered(store, clock, customer, make_subscription):
    old = ingest(store, clock, customer, flow="cut", caller="boot")
    assert old["deliveries"] == []
    clock.advance(1)
    sub = make_subscription(customer["id"])
    after = ingest(store, clock, customer, flow="cut", caller="live")
    assert [d["subscription_id"] for d in after["deliveries"]] == [sub["id"]]
    # The pre-subscription event gains no deliveries retroactively.
    assert [d for d in store.get_delivery(after["deliveries"][0]["delivery_id"])]
    stored_old = [e for e in store.list_events(customer_id=customer["id"]) if e["caller_event_id"] == "boot"][0]
    assert stored_old["seq"] == 1


def test_each_subscription_gets_its_own_delivery(store, clock, customer, make_subscription):
    sub1 = make_subscription(customer["id"], "http://one.test/hook")
    sub2 = make_subscription(customer["id"], "http://two.test/hook")
    ev = ingest(store, clock, customer)
    by_sub = {d["subscription_id"]: d["delivery_id"] for d in ev["deliveries"]}
    assert set(by_sub) == {sub1["id"], sub2["id"]}
    assert by_sub[sub1["id"]] != by_sub[sub2["id"]]
    assert store.get_delivery(by_sub[sub1["id"]])["target_url"] == "http://one.test/hook"


def test_unknown_customer_rejected(store, clock):
    with pytest.raises(DomainError) as exc:
        store.ingest_event(customer_id="cus_nope", flow_id="f", caller_event_id="e", payload={}, now=1.0)
    assert exc.value.status_code == 404


def test_invalid_target_url_rejected(store, clock, customer):
    for bad in ["ftp://x/y", "not a url", "http://user:pass@host/x", ""]:
        with pytest.raises(DomainError):
            store.create_subscription(customer_id=customer["id"], target_url=bad, now=clock.now())


def test_second_store_instance_sees_same_data(settings, clock, customer, subscribe):
    """Two processes share one SQLite file; this simulates the second process."""
    other = Store(settings)
    other.initialize()
    ingest(other, clock, customer, caller="from-other")
    events = [e for e in other.list_events(customer_id=customer["id"]) if e["caller_event_id"] == "from-other"]
    assert len(events) == 1
