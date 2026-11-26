"""Deterministic retry-time, ordering, failure and per-flow concurrency tests.

A FakeClock is injected, so the real 1s/4s/16s schedule is asserted exactly,
with no waiting.  ``ScriptedSender`` returns pre-programmed outcomes and
records when each attempt started (in fake-clock time).
"""

from __future__ import annotations

import pytest

from webhook_delivery.config import DeliveryPolicy
from webhook_delivery.worker import SendResult, Worker


def make_worker(store, settings, clock, sender, concurrency: int = 4) -> Worker:
    return Worker(
        store=store,
        policy=DeliveryPolicy(settings),
        clock=clock,
        sender=sender,
        concurrency=concurrency,
        poll_seconds=0.0,
    )


def pump_until(store, worker, clock, *, max_cycles: int = 200):
    """Run the worker while advancing the fake clock to the next wake-up.

    This is the "no real waiting" control: scheduled retries at now+1/now+4/
    now+16 are reached by jumping the fake clock forward, not by sleeping.
    """
    for _ in range(max_cycles):
        claimed = worker.run_once()
        nxt = store.next_wakeup(now=clock.now())
        if nxt is None:
            return
        if not claimed and nxt > clock.now():
            clock.advance(nxt - clock.now())


def seed(store, clock, customer, make_subscription, *, url="http://target.test/hook", count=1, flow="f"):
    sub = make_subscription(customer["id"], url)
    deliveries = []
    for i in range(count):
        ev = store.ingest_event(
            customer_id=customer["id"],
            flow_id=flow,
            caller_event_id=f"{flow}-{i}",
            payload={"i": i},
            now=clock.now(),
        )
        deliveries.extend(d["delivery_id"] for d in ev["deliveries"])
    return sub, deliveries


def test_backoff_is_exactly_1_4_16_then_needs_attention(store, settings, clock, customer, make_subscription, sender):
    sub, (dlv,) = seed(store, clock, customer, make_subscription)
    sender.script[dlv] = [SendResult("failed_http", 500)] * 10
    worker = make_worker(store, settings, clock, sender)
    pump_until(store, worker, clock)

    times = [t for (d, _a, t, _u, _o) in sender.sent if d == dlv]
    assert len(times) == 4
    assert [round(b - a) for a, b in zip(times, times[1:])] == [1, 4, 16]
    detail = store.get_delivery(dlv)
    assert detail["status"] == "needs_attention"
    assert [a["outcome"] for a in detail["attempts"]] == ["failed_http"] * 4
    # No further attempt happens no matter how far time advances.
    clock.advance(3600)
    worker.run_once()
    assert len([s for s in sender.sent if s[0] == dlv]) == 4


def test_timeout_and_transport_errors_use_the_same_schedule(store, settings, clock, customer, make_subscription, sender):
    sub, (dlv,) = seed(store, clock, customer, make_subscription)
    sender.script[dlv] = [
        SendResult("timeout", None, "ReadTimeout"),
        SendResult("transport_error", None, "ConnectError"),
        SendResult("succeeded", 200),
    ]
    pump_until(store, make_worker(store, settings, clock, sender), clock)
    detail = store.get_delivery(dlv)
    assert detail["status"] == "succeeded"
    assert [a["outcome"] for a in detail["attempts"]] == ["timeout", "transport_error", "succeeded"]
    times = [t for (d, _a, t, _u, _o) in sender.sent if d == dlv]
    assert [round(b - a) for a, b in zip(times, times[1:])] == [1, 4]


def test_success_on_first_attempt(store, settings, clock, customer, make_subscription, sender):
    sub, (dlv,) = seed(store, clock, customer, make_subscription)
    pump_until(store, make_worker(store, settings, clock, sender), clock)
    detail = store.get_delivery(dlv)
    assert detail["status"] == "succeeded"
    assert len(detail["attempts"]) == 1
    assert detail["completed_at"] is not None


def test_same_flow_is_serialised_and_strictly_ordered(store, settings, clock, customer, make_subscription, sender):
    sub, [d1, d2, d3] = seed(store, clock, customer, make_subscription, count=3)
    order: list[str] = []
    sender.on_send = lambda claim, result: order.append(claim.id)
    pump_until(store, make_worker(store, settings, clock, sender), clock)
    assert order == [d1, d2, d3]
    assert all(store.get_delivery(d)["status"] == "succeeded" for d in (d1, d2, d3))


def test_successor_waits_while_predecessor_retries(store, settings, clock, customer, make_subscription, sender):
    sub, (d1, d2) = seed(store, clock, customer, make_subscription, count=2)
    sender.script[d1] = [SendResult("failed_http", 500), SendResult("failed_http", 500), SendResult("succeeded", 200)]
    seen_before_d1_success: list[str] = []

    def on_send(claim, result):
        if claim.id == d2 and store.get_delivery(d1)["status"] != "succeeded":
            seen_before_d1_success.append(claim.id)

    sender.on_send = on_send
    pump_until(store, make_worker(store, settings, clock, sender), clock)
    assert seen_before_d1_success == []
    assert store.get_delivery(d2)["status"] == "succeeded"


def test_different_flows_do_not_block_each_other(store, settings, clock, customer, make_subscription, sender):
    sub = make_subscription(customer["id"])
    e1 = store.ingest_event(customer_id=customer["id"], flow_id="flow-a", caller_event_id="a1", payload={"a": 1}, now=clock.now())
    e2 = store.ingest_event(customer_id=customer["id"], flow_id="flow-b", caller_event_id="b1", payload={"b": 1}, now=clock.now())
    dlv_a = e1["deliveries"][0]["delivery_id"]
    dlv_b = e2["deliveries"][0]["delivery_id"]
    sender.script[dlv_a] = [SendResult("failed_http", 500)] * 10

    worker = make_worker(store, settings, clock, sender, concurrency=2)
    worker.run_once()  # both flows claimed in the same round
    assert store.get_delivery(dlv_b)["status"] == "succeeded"
    assert store.get_delivery(dlv_a)["status"] == "retry_wait"


def test_needs_attention_blocks_successor_until_admin_skip(store, settings, clock, customer, make_subscription, sender):
    sub, (d1, d2) = seed(store, clock, customer, make_subscription, count=2)
    sender.script[d1] = [SendResult("failed_http", 503)] * 10
    pump_until(store, make_worker(store, settings, clock, sender), clock)

    assert store.get_delivery(d1)["status"] == "needs_attention"
    blocked = store.get_delivery(d2)
    assert blocked["status"] == "pending"
    assert blocked["blocked"] is True
    assert "needs_attention" in blocked["block_reason"]
    assert [s for s in sender.sent if s[0] == d2] == []

    clock.advance(3600)
    pump_until(store, make_worker(store, settings, clock, sender), clock)
    assert [s for s in sender.sent if s[0] == d2] == [], "successor must never be silently released"

    store.skip_delivery(d1, now=clock.now(), reason="customer confirmed event obsolete", actor="ops")
    pump_until(store, make_worker(store, settings, clock, sender), clock)
    assert store.get_delivery(d2)["status"] == "succeeded"


def test_admin_retry_resets_budget(store, settings, clock, customer, make_subscription, sender):
    sub, (d1,) = seed(store, clock, customer, make_subscription)
    sender.script[d1] = [SendResult("failed_http", 500)] * 10
    pump_until(store, make_worker(store, settings, clock, sender), clock)
    assert store.get_delivery(d1)["status"] == "needs_attention"

    store.retry_blocked(d1, now=clock.now(), reason="endpoint fixed", actor="ops")
    assert store.get_delivery(d1)["status"] == "pending"
    pump_until(store, make_worker(store, settings, clock, sender), clock)
    detail = store.get_delivery(d1)
    assert detail["status"] == "needs_attention"
    retries = [a for a in detail["admin_actions"] if a["action"] == "retry"]
    assert retries and retries[0]["reason"] == "endpoint fixed"


def test_admin_actions_require_reason_and_correct_state(store, clock, customer, make_subscription):
    from webhook_delivery.util import DomainError

    sub, (d1,) = seed(store, clock, customer, make_subscription)
    with pytest.raises(DomainError) as exc:
        store.skip_delivery(d1, now=clock.now(), reason="", actor="ops")
    assert exc.value.status_code == 422
    with pytest.raises(DomainError):
        store.skip_delivery(d1, now=clock.now(), reason="reason", actor="ops")  # not parked yet
    with pytest.raises(DomainError):
        store.retry_blocked(d1, now=clock.now(), reason="reason", actor="ops")


def test_successor_of_skipped_flow_head_runs(store, settings, clock, customer, make_subscription, sender):
    """Skipping a head makes the whole remaining queue runnable in order."""
    sub, (d1, d2, d3) = seed(store, clock, customer, make_subscription, count=3)
    sender.script[d1] = [SendResult("failed_http", 500)] * 10
    pump_until(store, make_worker(store, settings, clock, sender), clock)
    store.skip_delivery(d1, now=clock.now(), reason="manual", actor="ops")
    pump_until(store, make_worker(store, settings, clock, sender), clock)
    statuses = [store.get_delivery(d)["status"] for d in (d1, d2, d3)]
    assert statuses == ["administratively_skipped", "succeeded", "succeeded"]
