"""Leases, concurrency, target-version fencing and crash recovery.

These tests deliberately use two in-process ``Store`` objects against the same
SQLite file to emulate two worker processes: the only shared state is the
database, which is exactly the production situation.
"""

from __future__ import annotations

import pytest

from webhook_delivery.config import DeliveryPolicy, FakeClock, Settings
from webhook_delivery.store import Store
from webhook_delivery.worker import SendResult, Worker


class HoldSender:
    """Sender that lets a test decide when (and with what) each attempt ends."""

    def __init__(self) -> None:
        self.results: dict[str, SendResult] = {}
        self.sent: list[tuple[str, str]] = []  # (delivery, worker name)
        self.name = "w"

    def send(self, claim, timeout: float) -> SendResult:
        self.sent.append((claim.id, self.name, claim.target_url, claim.target_version))
        return self.results.get(claim.id, SendResult("succeeded", 200))


class HoldSenderFactory:
    def __init__(self) -> None:
        self.senders: dict[str, HoldSender] = {}

    def __call__(self, name: str) -> HoldSender:
        sender = HoldSender()
        sender.name = name
        self.senders[name] = sender
        return sender


def seed(store, clock, customer, make_subscription, *, url="http://target.test/hook", count=1, flow="f"):
    sub = make_subscription(customer["id"], url)
    ids = []
    for i in range(count):
        ev = store.ingest_event(
            customer_id=customer["id"],
            flow_id=flow,
            caller_event_id=f"{flow}-{i}",
            payload={"i": i},
            now=clock.now(),
        )
        ids.extend(d["delivery_id"] for d in ev["deliveries"])
    return sub, ids


# --------------------------------------------------------------------- leases
def test_claim_creates_lease_and_second_claim_gets_nothing(store, clock, customer, make_subscription):
    sub, (dlv,) = seed(store, clock, customer, make_subscription)
    a = store.claim_due(now=clock.now(), lease_seconds=30)
    b = store.claim_due(now=clock.now(), lease_seconds=30)
    assert [c.id for c in a] == [dlv]
    assert b == []
    detail = store.get_delivery(dlv)
    assert detail["status"] == "in_progress"
    assert detail["attempts"][0]["outcome"] == "in_flight"


def test_expired_lease_is_recoverable_by_another_worker(store, clock, customer, make_subscription):
    sub, (dlv,) = seed(store, clock, customer, make_subscription)
    first = store.claim_due(now=clock.now(), lease_seconds=30)
    assert len(first) == 1
    # Lease not yet expired: nobody else may take it.
    assert store.claim_due(now=clock.now() + 10, lease_seconds=30) == []
    clock.advance(31)
    recovered = store.recover_expired_leases(now=clock.now())
    assert recovered == 1
    second = store.claim_due(now=clock.now(), lease_seconds=30)
    assert [c.id for c in second] == [dlv]
    assert second[0].lease_token != first[0].lease_token
    assert second[0].attempt_seq == 2
    detail = store.get_delivery(dlv)
    assert detail["attempts"][0]["outcome"] == "expired"
    assert detail["attempts"][1]["outcome"] == "in_flight"


def test_stale_worker_cannot_overwrite_newer_result(store, clock, customer, make_subscription):
    """Two processes: A claims, its lease expires, B claims and succeeds; A's
    late reply must not change the outcome."""
    sub, (dlv,) = seed(store, clock, customer, make_subscription)
    claim_a = store.claim_due(now=clock.now(), lease_seconds=5)[0]
    clock.advance(6)
    store.recover_expired_leases(now=clock.now())
    claim_b = store.claim_due(now=clock.now(), lease_seconds=5)[0]

    # B finishes successfully.
    assert store.finish_attempt(claim_b, outcome="succeeded", http_status=200, error=None, now=clock.now(), retry_delay=None) == "succeeded"

    # A's response finally arrives, claiming failure.
    clock.advance(1)
    outcome = store.finish_attempt(
        claim_a, outcome="failed_http", http_status=500, error="late", now=clock.now(), retry_delay=1.0
    )
    assert outcome == "lease_lost"
    detail = store.get_delivery(dlv)
    assert detail["status"] == "succeeded", "stale worker must not overwrite a newer success"
    assert [a["outcome"] for a in detail["attempts"]] == ["expired", "succeeded"]
    assert [l["observed"] for l in detail["late_results"]] == ["failed_http"]


def test_stale_success_cannot_override_newer_failure(store, clock, customer, make_subscription):
    sub, (dlv,) = seed(store, clock, customer, make_subscription)
    claim_a = store.claim_due(now=clock.now(), lease_seconds=5)[0]
    clock.advance(6)
    store.recover_expired_leases(now=clock.now())
    claim_b = store.claim_due(now=clock.now(), lease_seconds=5)[0]
    store.finish_attempt(claim_b, outcome="failed_http", http_status=500, error=None, now=clock.now(), retry_delay=1.0)
    clock.advance(1)
    outcome = store.finish_attempt(claim_a, outcome="succeeded", http_status=200, error=None, now=clock.now(), retry_delay=None)
    assert outcome == "lease_lost"
    detail = store.get_delivery(dlv)
    assert detail["status"] == "retry_wait", "a stale success must not cancel a newer failure"


def test_two_workers_run_concurrently_without_double_claims(store, settings, clock, sender_factory, customer, make_subscription):
    """A second Worker (separate Store, same DB) takes the other flow's work."""
    sub, ids = seed(store, clock, customer, make_subscription, count=1, flow="flow-a")
    ev = store.ingest_event(customer_id=customer["id"], flow_id="flow-b", caller_event_id="b-1", payload={"b": 1}, now=clock.now())
    ids_b = [d["delivery_id"] for d in ev["deliveries"]]
    other = Store(settings)
    other.initialize()
    sender_one = sender_factory("one")
    sender_two = sender_factory("two")
    w1 = Worker(store=store, policy=DeliveryPolicy(settings), clock=clock, sender=sender_one, concurrency=1)
    w2 = Worker(store=other, policy=DeliveryPolicy(settings), clock=clock, sender=sender_two, concurrency=1)
    total = w1.run_once() + w2.run_once()
    assert total == 2
    seen = [s[0] for s in sender_one.sent] + [s[0] for s in sender_two.sent]
    assert sorted(seen) == sorted(ids + ids_b)
    assert len(set(seen)) == len(seen), "no delivery may be attempted by both workers"


# ----------------------------------------------------------- target versioning
def test_late_reply_after_target_change_is_discarded(store, clock, customer, make_subscription):
    old = "http://old.test/hook"
    new = "http://new.test/hook"
    sub, (dlv,) = seed(store, clock, customer, make_subscription, url=old)

    claim_old = store.claim_due(now=clock.now(), lease_seconds=60)[0]
    assert claim_old.target_url == old and claim_old.target_version == 1

    store.update_subscription_target(sub["id"], target_url=new, now=clock.now())

    # The old request "comes back" with 200.
    outcome = store.finish_attempt(claim_old, outcome="succeeded", http_status=200, error=None, now=clock.now(), retry_delay=None)
    assert outcome == "superseded_target"

    detail = store.get_delivery(dlv)
    assert detail["status"] == "pending", "late old-target success must not mark the delivery delivered"
    assert detail["target_url"] == new
    assert detail["target_version"] == 2
    assert detail["attempts"][0]["outcome"] == "superseded_target"
    assert detail["attempts"][0]["target_url"] == old
    assert detail["completed_at"] is None

    claim_new = store.claim_due(now=clock.now(), lease_seconds=60)[0]
    assert (claim_new.target_url, claim_new.target_version) == (new, 2)
    store.finish_attempt(claim_new, outcome="succeeded", http_status=200, error=None, now=clock.now(), retry_delay=None)
    detail = store.get_delivery(dlv)
    assert detail["status"] == "succeeded"
    assert [(a["target_version"], a["outcome"]) for a in detail["attempts"]] == [
        (1, "superseded_target"),
        (2, "succeeded"),
    ]


def test_target_change_does_not_touch_succeeded_deliveries(store, clock, customer, make_subscription):
    old = "http://old.test/hook"
    new = "http://new.test/hook"
    sub, (dlv,) = seed(store, clock, customer, make_subscription, url=old)
    claim = store.claim_due(now=clock.now(), lease_seconds=60)[0]
    store.finish_attempt(claim, outcome="succeeded", http_status=200, error=None, now=clock.now(), retry_delay=None)

    store.update_subscription_target(sub["id"], target_url=new, now=clock.now())
    detail = store.get_delivery(dlv)
    assert detail["status"] == "succeeded"
    assert detail["target_url"] == old, "already delivered work keeps its historical target"
    assert detail["attempts"][0]["target_url"] == old


def test_pending_and_parked_work_use_new_target(store, clock, customer, make_subscription):
    old = "http://old.test/hook"
    new = "http://new.test/hook"
    sub, (dlv,) = seed(store, clock, customer, make_subscription, url=old)
    store.update_subscription_target(sub["id"], target_url=new, now=clock.now())
    assert store.get_delivery(dlv)["target_url"] == new

    # A parked delivery is retried by an administrator against the new target.
    for _ in range(4):
        claim = store.claim_due(now=clock.now(), lease_seconds=60)[0]
        store.finish_attempt(claim, outcome="failed_http", http_status=500, error=None, now=clock.now(), retry_delay=0)
        clock.advance(1)
    assert store.get_delivery(dlv)["status"] == "needs_attention"
    store.update_subscription_target(sub["id"], target_url="http://third.test/hook", now=clock.now())
    assert store.get_delivery(dlv)["target_url"] == "http://third.test/hook"
    store.retry_blocked(dlv, now=clock.now(), reason="fixed", actor="ops")
    claim = store.claim_due(now=clock.now(), lease_seconds=60)[0]
    assert claim.target_url == "http://third.test/hook"


def test_optimistic_target_version_check(store, clock, customer, subscribe):
    from webhook_delivery.util import DomainError

    store.update_subscription_target(subscribe["id"], target_url="http://x.test/h", now=clock.now())
    with pytest.raises(DomainError) as exc:
        store.update_subscription_target(
            subscribe["id"], target_url="http://y.test/h", now=clock.now(), expected_version=1
        )
    assert exc.value.code == "target_version_conflict"


def test_stale_worker_does_not_consume_retry_budget_on_target_change(store, settings, clock, customer, make_subscription, sender):
    """Repointing the target is a configuration fix, not a failed attempt."""
    old = "http://old.test/hook"
    new = "http://new.test/hook"
    sub, (dlv,) = seed(store, clock, customer, make_subscription, url=old)
    for _ in range(3):
        claim = store.claim_due(now=clock.now(), lease_seconds=60)[0]
        store.finish_attempt(claim, outcome="failed_http", http_status=500, error=None, now=clock.now(), retry_delay=1.0)
        assert store.get_delivery(dlv)["status"] == "retry_wait"
        clock.advance(2)
    claim = store.claim_due(now=clock.now(), lease_seconds=60)[0]
    store.update_subscription_target(sub["id"], target_url=new, now=clock.now())
    store.finish_attempt(claim, outcome="succeeded", http_status=200, error=None, now=clock.now(), retry_delay=None)
    detail = store.get_delivery(dlv)
    assert detail["status"] == "pending"
    assert detail["attempts_count"] == 3, "the superseded attempt is refunded"
    pump = Worker(store=store, policy=DeliveryPolicy(settings), clock=clock, sender=sender, concurrency=1)
    pump.run_once()
    detail = store.get_delivery(dlv)
    assert detail["status"] == "succeeded"
    assert detail["target_url"] == new


# ------------------------------------------------------------------- recovery
def test_worker_restart_picks_up_waiting_work(store, settings, clock, customer, make_subscription, sender):
    """A scheduled retry survives a "process restart" (new Worker instance)."""
    sub, (dlv,) = seed(store, clock, customer, make_subscription)
    sender.script[dlv] = [SendResult("failed_http", 500), SendResult("succeeded", 200)]
    w1 = Worker(store=store, policy=DeliveryPolicy(settings), clock=clock, sender=sender, concurrency=1)
    w1.run_once()
    assert store.get_delivery(dlv)["status"] == "retry_wait"

    # "Restart": a brand new worker, and the clock reaches the retry time.
    clock.advance(1)
    w2 = Worker(store=store, policy=DeliveryPolicy(settings), clock=clock, sender=sender, concurrency=1)
    w2.run_once()
    assert store.get_delivery(dlv)["status"] == "succeeded"


def test_worker_restart_after_crash_retries_inflight_attempt(store, settings, clock, customer, make_subscription, sender):
    sub, (dlv,) = seed(store, clock, customer, make_subscription)
    sender.results = {}
    w1 = Worker(store=store, policy=DeliveryPolicy(settings), clock=clock, sender=sender, concurrency=1)
    claimed = store.claim_due(now=clock.now(), lease_seconds=5)
    assert len(claimed) == 1
    # Crash: no finish_attempt.  Lease expires; new worker recovers.
    clock.advance(6)
    w2 = Worker(store=store, policy=DeliveryPolicy(settings), clock=clock, sender=sender, concurrency=1)
    w2.recover()
    w2.run_once()
    detail = store.get_delivery(dlv)
    assert detail["status"] == "succeeded"
    assert [a["outcome"] for a in detail["attempts"]] == ["expired", "succeeded"]
    assert detail["attempts"][0]["attempt_number"] == detail["attempts"][1]["attempt_number"] - 1


def test_delivery_id_is_stable_across_all_attempts_and_target_versions(store, clock, customer, make_subscription):
    old = "http://old.test/hook"
    new = "http://new.test/hook"
    sub, (dlv,) = seed(store, clock, customer, make_subscription, url=old)
    claim = store.claim_due(now=clock.now(), lease_seconds=60)[0]
    assert claim.id == dlv
    store.update_subscription_target(sub["id"], target_url=new, now=clock.now())
    store.finish_attempt(claim, outcome="failed_http", http_status=500, error=None, now=clock.now(), retry_delay=0)
    claim2 = store.claim_due(now=clock.now(), lease_seconds=60)[0]
    assert claim2.id == dlv
    store.finish_attempt(claim2, outcome="succeeded", http_status=200, error=None, now=clock.now(), retry_delay=None)
    assert store.get_delivery(dlv)["attempts"][0]["target_version"] == 1
    assert store.get_delivery(dlv)["attempts"][1]["target_version"] == 2
