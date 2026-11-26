"""Delivery termination, send fencing, late-result safety and process restart
behaviour for topic withdrawal.

These tests use two Store objects against one SQLite file to emulate multiple
API/worker processes, plus scripted senders and a real-socket echo test.
"""

from __future__ import annotations

import sqlite3
import threading
import time

import pytest

from webhook_delivery.config import DeliveryPolicy
from webhook_delivery.store import Store
from webhook_delivery.worker import SendResult, Worker

ECHO_SECRET = "ECHO_SECRET_551"


def seed_event(store, clock, customer, *, caller, flow="f", topic="t", payload=None):
    return store.ingest_event(
        customer_id=customer["id"],
        flow_id=flow,
        caller_event_id=caller,
        payload=payload if payload is not None else {"secret": ECHO_SECRET},
        now=clock.now(),
        topic_id=topic,
    )


def worker_for(store, settings, clock, sender):
    return Worker(
        store=store, policy=DeliveryPolicy(settings), clock=clock, sender=sender, concurrency=1
    )


def pump(store, settings, clock, sender, *, max_cycles=200):
    w = worker_for(store, settings, clock, sender)
    for _ in range(max_cycles):
        claimed = w.run_once()
        nxt = store.next_wakeup(now=clock.now())
        if nxt is None:
            return
        if not claimed and nxt > clock.now():
            clock.advance(nxt - clock.now())


def assert_secret_absent_everywhere(db_path: str, secret: str = ECHO_SECRET) -> None:
    con = sqlite3.connect(db_path)
    try:
        tables = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        for table in tables:
            for row in con.execute(f"SELECT * FROM {table}"):
                assert secret not in repr(row), f"{table}: {row!r}"
    finally:
        con.close()


# ------------------------------------------------------------ ordering/blocking
def test_withdrawn_delivery_does_not_block_next_event_in_flow(
    store, settings, clock, customer, subscribe, sender
):
    e1 = seed_event(store, clock, customer, caller="e1", topic="t")
    e2 = store.ingest_event(
        customer_id=customer["id"], flow_id="f", caller_event_id="e2", payload={"ok": True}, now=clock.now()
    )
    d1 = e1["deliveries"][0]["delivery_id"]
    d2 = e2["deliveries"][0]["delivery_id"]

    store.withdraw_topic(customer_id=customer["id"], topic_id="t", now=clock.now())
    assert store.get_delivery(d1)["status"] == "withdrawn"

    # The successor is claimable immediately even though its predecessor never
    # reached succeeded/skipped, because withdrawal is terminal.
    claims = store.claim_due(now=clock.now(), lease_seconds=30)
    assert [c.id for c in claims] == [d2]

    pump(store, settings, clock, sender)
    assert store.get_delivery(d2)["status"] == "succeeded"
    assert [s[0] for s in sender.sent] == [d2]
    assert all(s[0] != d1 for s in sender.sent)


def test_pending_retry_wait_and_needs_attention_are_terminated(
    store, settings, clock, customer, subscribe, sender
):
    e1 = seed_event(store, clock, customer, caller="e1", topic="t")
    d1 = e1["deliveries"][0]["delivery_id"]
    # Park it in needs_attention with four 500s.
    sender.script[d1] = [SendResult("failed_http", 500)] * 10
    pump(store, settings, clock, sender)
    assert store.get_delivery(d1)["status"] == "needs_attention"

    result = store.withdraw_topic(customer_id=customer["id"], topic_id="t", now=clock.now())
    assert result["deliveries_terminated"] == 1
    assert store.get_delivery(d1)["status"] == "withdrawn"
    clock.advance(3600)
    pump(store, settings, clock, sender)
    assert len([s for s in sender.sent if s[0] == d1]) == 4  # no extra attempt after withdrawal

    from webhook_delivery.util import DomainError

    with pytest.raises(DomainError) as retry_exc:
        store.retry_blocked(d1, now=clock.now(), reason="x", actor="a")
    assert retry_exc.value.code == "delivery_withdrawn"


def test_succeeded_delivery_fact_is_preserved(store, settings, clock, customer, subscribe, sender):
    e1 = seed_event(store, clock, customer, caller="e1", topic="t")
    d1 = e1["deliveries"][0]["delivery_id"]
    pump(store, settings, clock, sender)
    assert store.get_delivery(d1)["status"] == "succeeded"

    result = store.withdraw_topic(customer_id=customer["id"], topic_id="t", now=clock.now())
    assert result["deliveries_succeeded"] == 1
    detail = store.get_delivery(d1)
    assert detail["status"] == "succeeded"
    assert [a["outcome"] for a in detail["attempts"]] == ["succeeded"]
    assert detail["completed_at"] is not None


def test_admin_skipped_delivery_fact_is_preserved(store, settings, clock, customer, subscribe, sender):
    e1 = seed_event(store, clock, customer, caller="e1", topic="t")
    d1 = e1["deliveries"][0]["delivery_id"]
    sender.script[d1] = [SendResult("failed_http", 503)] * 10
    pump(store, settings, clock, sender)
    store.skip_delivery(d1, now=clock.now(), reason="confirmed obsolete", actor="ops")

    store.withdraw_topic(customer_id=customer["id"], topic_id="t", now=clock.now())
    detail = store.get_delivery(d1)
    assert detail["status"] == "administratively_skipped"
    assert any(a["action"] == "skip" and a["reason"] == "confirmed obsolete" for a in detail["admin_actions"])


def test_target_change_does_not_revive_withdrawn_delivery(
    store, clock, customer, make_subscription
):
    sub = make_subscription(customer["id"], "http://old.test/h")
    e1 = seed_event(store, clock, customer, caller="e1", topic="t")
    d1 = e1["deliveries"][0]["delivery_id"]
    store.withdraw_topic(customer_id=customer["id"], topic_id="t", now=clock.now())
    store.update_subscription_target(sub["id"], target_url="http://new.test/h", now=clock.now())
    detail = store.get_delivery(d1)
    assert detail["status"] == "withdrawn"
    assert store.claim_due(now=clock.now(), lease_seconds=30) == []


# ------------------------------------------------------------- send gate/race
def test_claimed_but_unsent_work_does_not_send_after_withdrawal(
    store, settings, clock, customer, subscribe, sender
):
    e1 = seed_event(store, clock, customer, caller="e1", topic="t")
    d1 = e1["deliveries"][0]["delivery_id"]
    claim = store.claim_due(now=clock.now(), lease_seconds=30)[0]

    store.withdraw_topic(customer_id=customer["id"], topic_id="t", now=clock.now())
    outcome = worker_for(store, settings, clock, sender).attempt(claim)
    assert outcome == "not_sent"
    assert sender.sent == []
    assert store.get_delivery(d1)["status"] == "withdrawn"


def test_late_result_after_withdrawal_cannot_rewrite_or_leak_echo(
    store, clock, customer, subscribe
):
    e1 = seed_event(store, clock, customer, caller="e1", topic="t")
    d1 = e1["deliveries"][0]["delivery_id"]
    claim = store.claim_due(now=clock.now(), lease_seconds=30)[0]
    assert store.mark_sending(claim, now=clock.now()) is True

    store.withdraw_topic(customer_id=customer["id"], topic_id="t", now=clock.now())
    # The already-sent request now returns a 500 whose body echoes content.
    outcome = store.finish_attempt(
        claim,
        outcome="failed_http",
        http_status=500,
        error=f"upstream echo: {ECHO_SECRET}",
        now=clock.now(),
        retry_delay=1.0,
    )
    assert outcome == "withdrawn"
    detail = store.get_delivery(d1)
    assert detail["status"] == "withdrawn"
    assert ECHO_SECRET not in repr(detail)
    assert detail["late_results"][0]["error"] is None
    assert_secret_absent_everywhere(store._path)


def test_preexisting_echo_diagnostic_text_is_scrubbed_on_withdrawal(
    store, clock, customer, subscribe
):
    e1 = seed_event(store, clock, customer, caller="e1", topic="t")
    d1 = e1["deliveries"][0]["delivery_id"]
    claim = store.claim_due(now=clock.now(), lease_seconds=30)[0]
    store.mark_sending(claim, now=clock.now())
    store.finish_attempt(
        claim,
        outcome="failed_http",
        http_status=500,
        error=f"receiver echoed {ECHO_SECRET}",
        now=clock.now(),
        retry_delay=1.0,
    )
    assert ECHO_SECRET in repr(store.get_delivery(d1))

    store.withdraw_topic(customer_id=customer["id"], topic_id="t", now=clock.now())
    detail = store.get_delivery(d1)
    assert ECHO_SECRET not in repr(detail)
    # The delivery fact (attempt outcome + http status) survives.
    assert detail["attempts"][0]["outcome"] == "failed_http"
    assert detail["attempts"][0]["http_status"] == 500
    assert_secret_absent_everywhere(store._path)


def test_recovery_settles_withdrawn_inflight_after_lease_expiry(
    store, clock, customer, subscribe
):
    e1 = seed_event(store, clock, customer, caller="e1", topic="t")
    d1 = e1["deliveries"][0]["delivery_id"]
    claim = store.claim_due(now=clock.now(), lease_seconds=5)[0]
    store.mark_sending(claim, now=clock.now())
    store.withdraw_topic(customer_id=customer["id"], topic_id="t", now=clock.now())

    clock.advance(6)
    recovered = store.recover_expired_leases(now=clock.now())
    assert recovered >= 1
    detail = store.get_delivery(d1)
    assert detail["status"] == "withdrawn"  # never requeued
    assert detail["attempts"][0]["outcome"] == "expired"
    assert store.claim_due(now=clock.now(), lease_seconds=30) == []


# --------------------------------------------------------- multi-process/restart
def test_withdrawal_and_tickets_survive_restart(settings, clock, customer, subscribe, sender):
    from webhook_delivery.store import Store

    e1 = seed_event(Store(settings), clock, customer, caller="e1", topic="t")
    ticket = Store(settings).create_ticket(customer_id=customer["id"], now=clock.now())
    Store(settings).retain_events(ticket_id=ticket["id"], event_ids=[e1["id"]], now=clock.now())
    Store(settings).withdraw_topic(customer_id=customer["id"], topic_id="t", now=clock.now())

    # "Restart": brand new Store/Worker instances on the same database file.
    restarted = Store(settings)
    restarted.initialize()
    with pytest.raises(Exception):
        restarted.ingest_event(
            customer_id=customer["id"], flow_id="f", caller_event_id="brand-new",
            payload={}, now=clock.now(), topic_id="t",
        )
    assert restarted.claim_due(now=clock.now(), lease_seconds=30) == []
    assert restarted.list_ticket_events(ticket["id"])[0]["payload"] == {"secret": ECHO_SECRET}

    w = worker_for(restarted, settings, clock, sender)
    assert w.run_once() == 0
    assert sender.sent == []


def test_new_subscription_after_withdrawal_never_delivers_topic_event(
    store, clock, customer, make_subscription
):
    sub1 = make_subscription(customer["id"], "http://one.test/h")
    e1 = seed_event(store, clock, customer, caller="e1", topic="t")
    d1 = e1["deliveries"][0]["delivery_id"]
    store.withdraw_topic(customer_id=customer["id"], topic_id="t", now=clock.now())
    assert store.get_delivery(d1)["status"] == "withdrawn"

    # A brand-new subscription (including its ordinal back-fill) must not
    # create fresh work for the withdrawn event.
    sub2 = make_subscription(customer["id"], "http://two.test/h")
    dlvs = store.list_deliveries(subscription_id=sub2["id"])
    assert all(d["event"]["id"] != e1["id"] for d in dlvs)
    assert store.claim_due(now=clock.now(), lease_seconds=30) == []


def test_other_topics_keep_delivering_during_withdrawal(
    store, settings, clock, customer, make_subscription, sender
):
    make_subscription(customer["id"], "http://target.test/h")
    a = seed_event(store, clock, customer, caller="a", flow="fa", topic="secret-topic")
    b = seed_event(
        store, clock, customer, caller="b", flow="fb", topic="other-topic", payload={"b": 1}
    )
    store.withdraw_topic(customer_id=customer["id"], topic_id="secret-topic", now=clock.now())
    pump(store, settings, clock, sender)
    assert store.get_delivery(b["deliveries"][0]["delivery_id"])["status"] == "succeeded"
    assert store.get_delivery(a["deliveries"][0]["delivery_id"])["status"] == "withdrawn"
    sent_ids = {s[0] for s in sender.sent}
    assert b["deliveries"][0]["delivery_id"] in sent_ids
    assert a["deliveries"][0]["delivery_id"] not in sent_ids


# ------------------------------------------------------------- real HTTP echo
def test_real_http_late_echo_cannot_persist_content(tmp_path):
    """End-to-end: a real receiver echo in a late 500 must not survive."""
    from webhook_delivery.receiver import Receiver, Response
    from webhook_delivery.config import RealClock, Settings as S

    settings = S(
        database_path=str(tmp_path / "echo.db"),
        backoff_seconds=(0.02, 0.02),
        request_timeout_seconds=2.0,
        lease_seconds=5.0,
        worker_poll_seconds=0.01,
        max_attempts=3,
    )
    st = Store(settings)
    st.initialize()
    recv = Receiver().start()
    try:
        # 500 whose body echoes the secret, delayed so withdrawal commits first.
        recv.set_default(Response(status=500, delay_seconds=0.4, body=f'{{"echo":"{ECHO_SECRET}"}}'))
        cus = st.create_customer(name="c", now=time.time())
        st.create_subscription(customer_id=cus["id"], target_url=recv.url, now=time.time())
        ev = st.ingest_event(
            customer_id=cus["id"], flow_id="f", caller_event_id="e",
            payload={"secret": ECHO_SECRET}, now=time.time(), topic_id="t",
        )
        dlv = ev["deliveries"][0]["delivery_id"]

        w = Worker(store=st, policy=DeliveryPolicy(settings), clock=RealClock(), concurrency=1)
        thread = threading.Thread(target=w.run_once)
        thread.start()
        assert recv.wait_for(lambda rs: len(rs) >= 1, timeout=5)
        # Request is in flight (receiver delaying its response): withdraw now.
        res = st.withdraw_topic(
            customer_id=cus["id"], topic_id="t", now=time.time(),
            wait_for_inflight=True, inflight_timeout=5.0,
        )
        thread.join(timeout=5)
        assert not thread.is_alive()
        assert res["deliveries_terminated"] == 1

        detail = st.get_delivery(dlv)
        assert detail["status"] == "withdrawn"
        assert ECHO_SECRET not in repr(detail)
        assert_secret_absent_everywhere(settings.database_path)
        w.sender.close()
    finally:
        recv.stop()
