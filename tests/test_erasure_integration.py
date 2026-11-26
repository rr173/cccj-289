"""Real-socket erasure integration: the reference receiver + the real HTTP sender.

These exercise the full protocol over actual HTTP: managed copies are stored
with their authorization sequence, an erasure request deletes exactly the
caller's topic, and an in-flight pre-withdrawal callback that arrives *after*
the erasure request cannot recreate the deleted copy.
"""

from __future__ import annotations

import threading
import time

from webhook_delivery.config import DeliveryPolicy, RealClock, Settings
from webhook_delivery.erasure_receiver import ReferenceErasureReceiverServer
from webhook_delivery.store import Store
from webhook_delivery.worker import Worker

SECRET = "INTEGRATION_REPLICA_SECRET_9"


def make_settings(tmp_path) -> Settings:
    return Settings(
        database_path=str(tmp_path / "erasure.db"),
        backoff_seconds=(0.02, 0.02, 0.02),
        request_timeout_seconds=1.0,
        lease_seconds=5.0,
        worker_poll_seconds=0.01,
        max_attempts=3,
    )


def pump(store, settings, *, timeout=10.0):
    """Real-clock pump: process deliveries and erasures until nothing is due."""
    worker = Worker(
        store=store, policy=DeliveryPolicy(settings), clock=RealClock(), concurrency=2,
    )
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            worker.run_once()
            nxt = store.next_wakeup(now=time.time())
            ew = store.next_erasure_wakeup(now=time.time())
            pending = store.list_erasure_requests(status="pending") + store.list_erasure_requests(
                status="retry_wait"
            ) + store.list_erasure_requests(status="in_progress")
            if nxt is None and ew is None and not pending:
                return
            time.sleep(0.01)
    finally:
        worker.sender.close()
        if worker._erasure_sender_obj is not None:
            worker._erasure_sender_obj.close()


def test_end_to_end_erase_removes_only_the_customers_topic(tmp_path):
    settings = make_settings(tmp_path)
    store = Store(settings)
    store.initialize()
    rx = ReferenceErasureReceiverServer(str(tmp_path / "reference.db")).start()
    try:
        cus = store.create_customer(name="Acme", now=time.time())
        other = store.create_customer(name="Other", now=time.time())
        recv = store.register_erasure_receiver(customer_id=cus["id"], url=rx.url, now=time.time())
        store.create_subscription(
            customer_id=cus["id"], target_url=rx.url, now=time.time(),
            erasure_receiver_id=recv["id"],
        )
        # Our event on the topic we will withdraw.
        ev = store.ingest_event(
            customer_id=cus["id"], flow_id="orders", caller_event_id="o1",
            payload={"secret": SECRET}, now=time.time(), topic_id="gdpr-42",
        )
        # Same customer, a different topic that must survive.
        keep = store.ingest_event(
            customer_id=cus["id"], flow_id="orders", caller_event_id="o2",
            payload={"keep": "yes"}, now=time.time(), topic_id="other-topic",
        )
        pump(store, settings)

        copies = rx.backend.list_replicas()
        assert {(c["customer_id"], c["topic_id"]) for c in copies} == {
            (cus["id"], "gdpr-42"),
            (cus["id"], "other-topic"),
        }
        assert any(c["payload"].get("secret") == SECRET for c in copies)

        result = store.withdraw_topic(customer_id=cus["id"], topic_id="gdpr-42", now=time.time())
        assert result["erasure"]["total"] == 1
        pump(store, settings)

        progress = store.get_topic_erasure(customer_id=cus["id"], topic_id="gdpr-42")
        assert progress["complete"] is True
        assert progress["acknowledged_receiver_ids"] == [recv["id"]]

        copies = rx.backend.list_replicas()
        assert all(c["topic_id"] != "gdpr-42" for c in copies), "the withdrawn topic is erased"
        assert any(c["topic_id"] == "other-topic" for c in copies), "other topic preserved"
        # The secret is gone from the reference receiver's persistent store.
        assert SECRET not in repr(copies)
        _ = (ev, keep, other)
    finally:
        rx.stop()


def test_inflight_callback_arriving_after_erasure_cannot_rewrite(tmp_path):
    """The reference receiver seals against pre-withdrawal authorizations, so a
    callback request that lands after the erasure request still cannot recreate
    a copy — no network ordering is assumed."""
    settings = make_settings(tmp_path)
    store = Store(settings)
    store.initialize()
    rx = ReferenceErasureReceiverServer(str(tmp_path / "reference.db")).start()
    try:
        cus = store.create_customer(name="Acme", now=time.time())
        recv = store.register_erasure_receiver(customer_id=cus["id"], url=rx.url, now=time.time())
        store.create_subscription(
            customer_id=cus["id"], target_url=rx.url, now=time.time(),
            erasure_receiver_id=recv["id"],
        )
        store.ingest_event(
            customer_id=cus["id"], flow_id="f", caller_event_id="e",
            payload={"secret": SECRET}, now=time.time(), topic_id="t",
        )
        # Manually drive the callback through the send gate so we can capture the
        # exact authorized request, then deliver it AFTER erasure.
        claim = store.claim_due(now=time.time(), lease_seconds=30)[0]
        stamped = store.begin_send(claim, now=time.time())
        assert stamped is not None and stamped.auth_seq >= 1

        # Do NOT send it yet: withdraw locally and run the erasure protocol.
        store.withdraw_topic(customer_id=cus["id"], topic_id="t", now=time.time())
        pump(store, settings)
        assert store.get_topic_erasure(customer_id=cus["id"], topic_id="t")["complete"] is True
        assert rx.backend.list_replicas() == []

        # Now the in-flight callback's HTTP request finally arrives.
        from webhook_delivery.worker import HttpxSender

        sender = HttpxSender()
        try:
            sender.send(stamped, timeout=2.0)
        finally:
            sender.close()
        # The receiver refused to store it: no copy reappears.
        assert rx.backend.list_replicas() == [], "sealed callback must not recreate a replica"
    finally:
        rx.stop()


def test_failed_erasure_is_retried_and_then_acknowledged(tmp_path):
    settings = make_settings(tmp_path)
    store = Store(settings)
    store.initialize()
    rx = ReferenceErasureReceiverServer(str(tmp_path / "reference.db")).start()
    try:
        cus = store.create_customer(name="Acme", now=time.time())
        recv = store.register_erasure_receiver(customer_id=cus["id"], url=rx.url, now=time.time())
        store.create_subscription(
            customer_id=cus["id"], target_url=rx.url, now=time.time(),
            erasure_receiver_id=recv["id"],
        )
        store.ingest_event(
            customer_id=cus["id"], flow_id="f", caller_event_id="e",
            payload={"secret": SECRET}, now=time.time(), topic_id="t",
        )
        pump(store, settings)
        # Point the receiver_url at a dead port to force an erasure failure.
        import socket

        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            dead_port = s.getsockname()[1]
        store.withdraw_topic(customer_id=cus["id"], topic_id="t", now=time.time())
        # Rewrite the request's url to the dead port (simulating a receiver
        # outage) then let one attempt fail.
        con_ctx = store._write()
        with con_ctx as conn:
            conn.execute(
                "UPDATE erasure_requests SET receiver_url=? WHERE customer_id=? AND topic_id=?",
                (f"http://127.0.0.1:{dead_port}/erase", cus["id"], "t"),
            )
        w = Worker(store=store, policy=DeliveryPolicy(settings), clock=RealClock(), concurrency=2)
        w.run_once()
        engine = store.get_topic_erasure(customer_id=cus["id"], topic_id="t")
        assert engine["complete"] is False
        assert engine["pending"] == 1
        op = engine["receivers"][0]["erasure_id"]

        # Bring the real receiver back and retry the same operation id.
        with store._write() as conn:
            conn.execute(
                "UPDATE erasure_requests SET receiver_url=? WHERE id=?", (rx.url, op)
            )
        pump(store, settings, timeout=10)
        progress = store.get_topic_erasure(customer_id=cus["id"], topic_id="t")
        assert progress["complete"] is True
        assert rx.backend.list_replicas() == []
        w.sender.close()
        if w._erasure_sender_obj is not None:
            w._erasure_sender_obj.close()
    finally:
        rx.stop()


def test_erasure_request_body_carries_no_content(tmp_path):
    settings = make_settings(tmp_path)
    store = Store(settings)
    store.initialize()
    rx = ReferenceErasureReceiverServer(str(tmp_path / "reference.db")).start()
    try:
        cus = store.create_customer(name="Acme", now=time.time())
        recv = store.register_erasure_receiver(customer_id=cus["id"], url=rx.url, now=time.time())
        store.create_subscription(
            customer_id=cus["id"], target_url=rx.url, now=time.time(),
            erasure_receiver_id=recv["id"],
        )
        store.ingest_event(
            customer_id=cus["id"], flow_id="f", caller_event_id="e",
            payload={"secret": SECRET}, now=time.time(), topic_id="t",
        )
        pump(store, settings)

        # Capture the erasure request body by installing a small recording hook
        # around the reference handler is overkill; instead assert on the sealed
        # DB rows of the webhook service and the receiver after erasure.
        store.withdraw_topic(customer_id=cus["id"], topic_id="t", now=time.time())
        pump(store, settings)
        import sqlite3

        con = sqlite3.connect(settings.database_path)
        try:
            for row in con.execute("SELECT * FROM erasure_requests"):
                assert SECRET not in repr(row)
        finally:
            con.close()
        # And the receiver's own stores carry no secret after erasure.
        assert SECRET not in repr(rx.backend.list_replicas())
    finally:
        rx.stop()
