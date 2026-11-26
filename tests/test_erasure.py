"""External replica erasure: scope, obligations, protocol, durability, retries.

Covers the product rules end to end at the Store and Worker level, plus the
reference receiver's guarantees (scope isolation, sealing, idempotent
acknowledgement).  Real-socket integration lives in
``test_erasure_integration.py``.
"""

from __future__ import annotations

import sqlite3

import pytest

from webhook_delivery.config import DeliveryPolicy
from webhook_delivery.erasure_receiver import ReferenceErasureReceiver
from webhook_delivery.store import Store
from webhook_delivery.util import DomainError
from webhook_delivery.worker import ErasureResult, SendResult, Worker

SECRET = "EXTERNAL_REPLICA_SECRET_77"


class RecordingSender:
    """Sender that records callbacks and acknowledges/ignores erasures."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, str | None, int]] = []  # (delivery, receiver, auth_seq)
        self.erasures: list[tuple[str, str, int]] = []  # (operation, receiver, watermark)
        self.event_outcome = SendResult("succeeded", 200)
        self.erasure_ack = True
        self.erasure_status = 200
        # Optional per-operation script of acknowledgement booleans; popped in
        # order, then ``erasure_ack`` is used.
        self.erasure_script: dict[str, list[bool]] = {}
        self.on_send = None

    def send(self, claim, timeout: float) -> SendResult:
        self.sent.append((claim.id, claim.erasure_receiver_id, claim.auth_seq))
        if self.on_send is not None:
            self.on_send(claim)
        return self.event_outcome

    def send_erasure(self, claim, timeout: float) -> ErasureResult:
        self.erasures.append((claim.id, claim.receiver_id, claim.watermark))
        script = self.erasure_script.get(claim.id)
        ack = script.pop(0) if script else self.erasure_ack
        if ack:
            return ErasureResult(True, self.erasure_status)
        return ErasureResult(False, self.erasure_status, "receiver unavailable")


def worker_for(store, settings, clock, sender, concurrency=2):
    return Worker(
        store=store, policy=DeliveryPolicy(settings), clock=clock, sender=sender,
        concurrency=concurrency,
    )


@pytest.fixture()
def sender() -> RecordingSender:
    return RecordingSender()


def pump(store, settings, clock, sender, *, max_cycles=200):
    w = worker_for(store, settings, clock, sender)
    for _ in range(max_cycles):
        w.run_once()
        nxt = store.next_wakeup(now=clock.now())
        ew = store.next_erasure_wakeup(now=clock.now())
        candidates = [t for t in (nxt, ew) if t is not None]
        if not candidates:
            return
        wake = min(candidates)
        if wake > clock.now():
            clock.advance(wake - clock.now())


def setup_receiver(store, clock, customer, url="http://receiver.test/erase"):
    return store.register_erasure_receiver(customer_id=customer["id"], url=url, now=clock.now())


def ingest(store, clock, customer, *, caller="e1", flow="f", topic_id="topic-a", payload=None):
    return store.ingest_event(
        customer_id=customer["id"],
        flow_id=flow,
        caller_event_id=caller,
        payload=payload if payload is not None else {"secret": SECRET},
        now=clock.now(),
        topic_id=topic_id,
    )


# ------------------------------------------------------- registration/target
def test_register_and_select_erasure_receiver(store, clock, customer):
    recv = setup_receiver(store, clock, customer)
    assert recv["id"] and recv["url"] == "http://receiver.test/erase"
    sub = store.create_subscription(
        customer_id=customer["id"], target_url="http://ignored/x", now=clock.now(),
        erasure_receiver_id=recv["id"],
    )
    # The registration pins the HTTP address; the caller's URL is advisory.
    assert sub["target_url"] == recv["url"]
    assert sub["erasure_receiver_id"] == recv["id"]


def test_multiple_receivers_per_customer_and_switch_target(store, clock, customer):
    a = setup_receiver(store, clock, customer, "http://a.test/erase")
    b = setup_receiver(store, clock, customer, "http://b.test/erase")
    sub = store.create_subscription(
        customer_id=customer["id"], target_url=a["url"], now=clock.now(), erasure_receiver_id=a["id"]
    )
    sub2 = store.update_subscription_target(
        sub["id"], target_url=b["url"], now=clock.now(), erasure_receiver_id=b["id"]
    )
    assert sub2["erasure_receiver_id"] == b["id"]
    assert sub2["target_url"] == b["url"]


def test_cannot_select_other_customers_receiver(store, clock, customer):
    other = store.create_customer(name="Other", now=clock.now())
    foreign = setup_receiver(store, clock, other)
    with pytest.raises(DomainError) as exc:
        store.create_subscription(
            customer_id=customer["id"], target_url=foreign["url"], now=clock.now(),
            erasure_receiver_id=foreign["id"],
        )
    assert exc.value.status_code == 403


def test_plain_target_keeps_original_behaviour(store, clock, customer, sender):
    store.create_subscription(customer_id=customer["id"], target_url="http://plain.test/h", now=clock.now())
    ingest(store, clock, customer)
    claim = store.claim_due(now=clock.now(), lease_seconds=30)[0]
    assert claim.erasure_receiver_id is None
    assert claim.auth_seq == 0
    # Withdrawal has no obligations -> no erasure requests.
    result = store.withdraw_topic(customer_id=customer["id"], topic_id="topic-a", now=clock.now())
    assert result["erasure"]["total"] == 0


def test_topicless_events_to_participating_receiver_stay_ordinary(store, clock, customer):
    recv = setup_receiver(store, clock, customer)
    store.create_subscription(
        customer_id=customer["id"], target_url=recv["url"], now=clock.now(),
        erasure_receiver_id=recv["id"],
    )
    ev = ingest(store, clock, customer, topic_id=None)
    claim = store.claim_due(now=clock.now(), lease_seconds=30)[0]
    assert claim.erasure_receiver_id is None
    assert claim.auth_seq == 0


# ---------------------------------------------------------- obligation scope
def test_obligation_recorded_at_send_gate_even_if_callback_fails(
    store, settings, clock, customer, sender
):
    recv = setup_receiver(store, clock, customer)
    store.create_subscription(
        customer_id=customer["id"], target_url=recv["url"], now=clock.now(),
        erasure_receiver_id=recv["id"],
    )
    ingest(store, clock, customer)
    sender.event_outcome = SendResult("failed_http", 500)
    w = worker_for(store, settings, clock, sender)
    w.run_once()
    # Failed callback -> still an obligation and an erasure request on withdraw.
    result = store.withdraw_topic(customer_id=customer["id"], topic_id="topic-a", now=clock.now())
    assert result["erasure"]["total"] == 1
    assert result["erasure"]["pending_receiver_ids"] == [recv["id"]]


def test_claim_without_send_gate_produces_no_obligation(store, clock, customer):
    recv = setup_receiver(store, clock, customer)
    store.create_subscription(
        customer_id=customer["id"], target_url=recv["url"], now=clock.now(),
        erasure_receiver_id=recv["id"],
    )
    ingest(store, clock, customer)
    # A claim alone (lease handed out) is NOT "obtained permission to send".
    store.claim_due(now=clock.now(), lease_seconds=30)
    result = store.withdraw_topic(customer_id=customer["id"], topic_id="topic-a", now=clock.now())
    assert result["erasure"]["total"] == 0


def test_configured_only_receiver_without_auth_is_not_notified(store, clock, customer):
    recv_a = setup_receiver(store, clock, customer, "http://a.test/erase")
    recv_b = setup_receiver(store, clock, customer, "http://b.test/erase")
    # Subscription is for receiver A only.
    store.create_subscription(
        customer_id=customer["id"], target_url=recv_a["url"], now=clock.now(),
        erasure_receiver_id=recv_a["id"],
    )
    ingest(store, clock, customer)
    store.claim_due(now=clock.now(), lease_seconds=30)  # claim but never send
    # No send gate reached for B, and not even for A yet (claim != send).
    result = store.withdraw_topic(customer_id=customer["id"], topic_id="topic-a", now=clock.now())
    assert result["erasure"]["total"] == 0, "crucially, a configured-only target is not notified"


def test_sent_receiver_is_in_scope_configured_only_is_not(store, settings, clock, customer, sender):
    recv_a = setup_receiver(store, clock, customer, "http://a.test/erase")
    recv_b = setup_receiver(store, clock, customer, "http://b.test/erase")
    store.create_subscription(
        customer_id=customer["id"], target_url=recv_a["url"], now=clock.now(),
        erasure_receiver_id=recv_a["id"],
    )
    ingest(store, clock, customer)
    pump(store, settings, clock, sender)  # sends to A only
    result = store.withdraw_topic(customer_id=customer["id"], topic_id="topic-a", now=clock.now())
    ids = result["erasure"]["acknowledged_receiver_ids"] + result["erasure"]["pending_receiver_ids"]
    assert ids == [recv_a["id"]]
    assert recv_b["id"] not in ids


def test_repointing_before_send_does_not_obligate_old_target(
    store, settings, clock, customer, sender
):
    """Only the receiver actually authorized for the callback is notified."""
    recv_a = setup_receiver(store, clock, customer, "http://a.test/erase")
    recv_b = setup_receiver(store, clock, customer, "http://b.test/erase")
    sub = store.create_subscription(
        customer_id=customer["id"], target_url=recv_a["url"], now=clock.now(),
        erasure_receiver_id=recv_a["id"],
    )
    ingest(store, clock, customer)
    # Switch to B before any send; A was only configured.
    store.update_subscription_target(
        sub["id"], target_url=recv_b["url"], now=clock.now(), erasure_receiver_id=recv_b["id"]
    )
    pump(store, settings, clock, sender)
    assert [s[1] for s in sender.sent] == [recv_b["id"]]
    result = store.withdraw_topic(customer_id=customer["id"], topic_id="topic-a", now=clock.now())
    ids = result["erasure"]["acknowledged_receiver_ids"] + result["erasure"]["pending_receiver_ids"]
    assert ids == [recv_b["id"]]


def test_repointing_after_send_keeps_old_obligation(store, settings, clock, customer, sender):
    """Address changes never transfer an already-produced erasure obligation."""
    recv_a = setup_receiver(store, clock, customer, "http://a.test/erase")
    recv_b = setup_receiver(store, clock, customer, "http://b.test/erase")
    sub = store.create_subscription(
        customer_id=customer["id"], target_url=recv_a["url"], now=clock.now(),
        erasure_receiver_id=recv_a["id"],
    )
    ingest(store, clock, customer)
    pump(store, settings, clock, sender)  # authorized + sent to A
    store.update_subscription_target(
        sub["id"], target_url=recv_b["url"], now=clock.now(), erasure_receiver_id=recv_b["id"]
    )
    result = store.withdraw_topic(customer_id=customer["id"], topic_id="topic-a", now=clock.now())
    ids = set(result["erasure"]["pending_receiver_ids"] + result["erasure"]["acknowledged_receiver_ids"])
    assert recv_a["id"] in ids, "the receiver that actually received the callback stays in scope"
    assert recv_b["id"] not in ids


def test_same_topic_name_other_customer_not_erased(store, settings, clock, customer, sender):
    other = store.create_customer(name="Other", now=clock.now())
    recv = setup_receiver(store, clock, customer)
    store.create_subscription(
        customer_id=customer["id"], target_url=recv["url"], now=clock.now(),
        erasure_receiver_id=recv["id"],
    )
    ingest(store, clock, customer, topic_id="shared")
    # Other customer's same-name topic must be untouched by our withdrawal.
    other_ev = ingest(store, clock, other, topic_id="shared")
    pump(store, settings, clock, sender)
    result = store.withdraw_topic(customer_id=customer["id"], topic_id="shared", now=clock.now())
    assert result["erasure"]["total"] == 1
    assert not store.get_event(other_ev["id"])["withdrawn"]


def test_other_topic_same_customer_not_in_scope(store, settings, clock, customer, sender):
    recv = setup_receiver(store, clock, customer)
    store.create_subscription(
        customer_id=customer["id"], target_url=recv["url"], now=clock.now(),
        erasure_receiver_id=recv["id"],
    )
    ingest(store, clock, customer, caller="e1", topic_id="topic-a")
    ingest(store, clock, customer, caller="e2", topic_id="topic-b")
    pump(store, settings, clock, sender)
    result = store.withdraw_topic(customer_id=customer["id"], topic_id="topic-a", now=clock.now())
    assert len(result["erasure"]["receivers"]) == 1
    assert result["erasure"]["receivers"][0]["topic_id"] == "topic-a"


# ------------------------------------------------------------- confirmation
def test_only_matching_acknowledgement_confirms(store, settings, clock, customer, sender):
    recv = setup_receiver(store, clock, customer)
    store.create_subscription(
        customer_id=customer["id"], target_url=recv["url"], now=clock.now(),
        erasure_receiver_id=recv["id"],
    )
    ingest(store, clock, customer)
    pump(store, settings, clock, sender)
    store.withdraw_topic(customer_id=customer["id"], topic_id="topic-a", now=clock.now())

    # Fail the first two attempts, then succeed.  A failed erasure is NOT
    # confirmation even though the local withdrawal is done and the original
    # callback succeeded.
    op = store.get_topic_erasure(customer_id=customer["id"], topic_id="topic-a")["receivers"][0][
        "erasure_id"
    ]
    sender.erasure_script[op] = [False, False, True]
    pump(store, settings, clock, sender)
    progress = store.get_topic_erasure(customer_id=customer["id"], topic_id="topic-a")
    assert progress["complete"] is True
    assert progress["acknowledged_receiver_ids"] == [recv["id"]]
    assert len([e for e in sender.erasures if e[0] == op]) == 3


def test_erasure_request_has_no_event_body(store, settings, clock, customer, sender):
    recv = setup_receiver(store, clock, customer)
    store.create_subscription(
        customer_id=customer["id"], target_url=recv["url"], now=clock.now(),
        erasure_receiver_id=recv["id"],
    )
    ingest(store, clock, customer)
    pump(store, settings, clock, sender)
    store.withdraw_topic(customer_id=customer["id"], topic_id="topic-a", now=clock.now())
    pump(store, settings, clock, sender)
    # The recorded erasure call carried no payload — only identity + watermark.
    assert sender.erasures
    op, rid, watermark = sender.erasures[0]
    assert rid == recv["id"] and watermark >= 1
    # The DB rows for the erasure request contain no secret content.
    con = sqlite3.connect(store._path)
    try:
        for row in con.execute("SELECT * FROM erasure_requests"):
            assert SECRET not in repr(row)
    finally:
        con.close()


def test_failed_erasure_does_not_block_other_receivers_or_topics(
    store, settings, clock, customer, sender
):
    recv_a = setup_receiver(store, clock, customer, "http://a.test/erase")
    recv_b = setup_receiver(store, clock, customer, "http://b.test/erase")
    # Two subscriptions, one per receiver: the same topic callback reaches both.
    store.create_subscription(
        customer_id=customer["id"], target_url=recv_a["url"], now=clock.now(),
        erasure_receiver_id=recv_a["id"],
    )
    store.create_subscription(
        customer_id=customer["id"], target_url=recv_b["url"], now=clock.now(),
        erasure_receiver_id=recv_b["id"],
    )
    ingest(store, clock, customer, caller="e1", topic_id="topic-a")
    pump(store, settings, clock, sender)

    # A keeps failing; B must still be acknowledged on its own.
    class SelectiveSender(RecordingSender):
        def send_erasure(self, claim, timeout):
            self.erasures.append((claim.id, claim.receiver_id, claim.watermark))
            if claim.receiver_id == recv_a["id"]:
                return ErasureResult(False, 500, "a down")
            return ErasureResult(True, 200)

    sel = SelectiveSender()
    store.withdraw_topic(customer_id=customer["id"], topic_id="topic-a", now=clock.now())
    w = worker_for(store, settings, clock, sel)
    for _ in range(4):
        w.run_once()
        clock.advance(0.01)
    prog = store.get_topic_erasure(customer_id=customer["id"], topic_id="topic-a")
    assert prog["complete"] is False, "one receiver's failure keeps the withdrawal uncertified"
    assert prog["acknowledged_receiver_ids"] == [recv_b["id"]]
    assert prog["pending_receiver_ids"] == [recv_a["id"]]


def test_erasure_retry_reuses_same_operation_id(store, settings, clock, customer, sender):
    recv = setup_receiver(store, clock, customer)
    store.create_subscription(
        customer_id=customer["id"], target_url=recv["url"], now=clock.now(),
        erasure_receiver_id=recv["id"],
    )
    ingest(store, clock, customer)
    pump(store, settings, clock, sender)
    store.withdraw_topic(customer_id=customer["id"], topic_id="topic-a", now=clock.now())

    op = store.get_topic_erasure(customer_id=customer["id"], topic_id="topic-a")["receivers"][0][
        "erasure_id"
    ]
    sender.erasure_script[op] = [False, False, True]
    pump(store, settings, clock, sender)
    attempted = [e[0] for e in sender.erasures]
    assert attempted and all(o == op for o in attempted), "retries keep the same operation id"
    assert store.get_topic_erasure(customer_id=customer["id"], topic_id="topic-a")["complete"]


# --------------------------------------------------- durability/restart
def test_erasure_requests_survive_restart(store, settings, clock, customer, sender):
    recv = setup_receiver(store, clock, customer)
    store.create_subscription(
        customer_id=customer["id"], target_url=recv["url"], now=clock.now(),
        erasure_receiver_id=recv["id"],
    )
    ingest(store, clock, customer)
    pump(store, settings, clock, sender)
    # Local withdrawal commits, then "the process dies" before any erasure is
    # sent (no worker run afterwards).
    store.withdraw_topic(customer_id=customer["id"], topic_id="topic-a", now=clock.now())

    # New Store/Worker instances on the same file must still deliver every
    # required erasure.
    fresh = Store(settings)
    fresh.initialize()
    progress = fresh.get_topic_erasure(customer_id=customer["id"], topic_id="topic-a")
    assert progress["pending"] == 1
    new_sender = RecordingSender()
    pump(fresh, settings, clock, new_sender)
    assert fresh.get_topic_erasure(customer_id=customer["id"], topic_id="topic-a")["complete"]
    assert new_sender.erasures[0][1] == recv["id"]


def test_crash_after_withdrawal_before_ack_is_retried(store, settings, clock, customer, sender):
    recv = setup_receiver(store, clock, customer)
    store.create_subscription(
        customer_id=customer["id"], target_url=recv["url"], now=clock.now(),
        erasure_receiver_id=recv["id"],
    )
    ingest(store, clock, customer)
    pump(store, settings, clock, sender)
    store.withdraw_topic(customer_id=customer["id"], topic_id="topic-a", now=clock.now())
    # Claim an erasure and crash (no finish).  Lease expires; recovery requeues.
    claims = store.claim_due_erasures(now=clock.now(), lease_seconds=5)
    assert len(claims) == 1
    clock.advance(6)
    recovered = store.recover_expired_erasure_leases(now=clock.now())
    assert recovered == 1
    new_sender = RecordingSender()
    pump(store, settings, clock, new_sender)
    assert store.get_topic_erasure(customer_id=customer["id"], topic_id="topic-a")["complete"]


def test_repeat_withdrawal_is_idempotent_for_erasure(store, settings, clock, customer, sender):
    recv = setup_receiver(store, clock, customer)
    store.create_subscription(
        customer_id=customer["id"], target_url=recv["url"], now=clock.now(),
        erasure_receiver_id=recv["id"],
    )
    ingest(store, clock, customer)
    pump(store, settings, clock, sender)
    store.withdraw_topic(customer_id=customer["id"], topic_id="topic-a", now=clock.now())
    second = store.withdraw_topic(customer_id=customer["id"], topic_id="topic-a", now=clock.now())
    assert second["already_withdrawn"] is True
    assert second["erasure"]["total"] == 1  # no duplicate rows


def test_send_gate_after_withdrawal_creates_no_obligation(store, settings, clock, customer):
    """Two stores on one file: withdrawal commits first, so the later send gate
    must not send and must not create an erasure obligation."""
    recv = setup_receiver(store, clock, customer)
    store.create_subscription(
        customer_id=customer["id"], target_url=recv["url"], now=clock.now(),
        erasure_receiver_id=recv["id"],
    )
    ingest(store, clock, customer)
    claim = store.claim_due(now=clock.now(), lease_seconds=30)[0]
    # Withdraw in the first store; the second store's gate must be fenced off.
    store.withdraw_topic(customer_id=customer["id"], topic_id="topic-a", now=clock.now())
    other = Store(settings)
    other.initialize()
    assert other.begin_send(claim, now=clock.now()) is None
    # No obligation appeared after withdrawal.
    con = sqlite3.connect(store._path)
    try:
        assert con.execute(
            "SELECT COUNT(*) FROM erasure_obligations WHERE customer_id=? AND topic_id=?",
            (customer["id"], "topic-a"),
        ).fetchone()[0] == 0
    finally:
        con.close()


def test_inflight_authorization_watermark_covers_withdrawal(store, settings, clock, customer):
    """The authorization sequence stamped at the gate is at or below the
    withdrawal watermark, so the reference receiver seals the late send."""
    recv = setup_receiver(store, clock, customer)
    store.create_subscription(
        customer_id=customer["id"], target_url=recv["url"], now=clock.now(),
        erasure_receiver_id=recv["id"],
    )
    ingest(store, clock, customer)
    claim = store.claim_due(now=clock.now(), lease_seconds=30)[0]
    stamped = store.begin_send(claim, now=clock.now())  # request "in flight"
    auth_seq = stamped.auth_seq
    assert auth_seq >= 1
    result = store.withdraw_topic(customer_id=customer["id"], topic_id="topic-a", now=clock.now())
    watermark = result["erasure"]["receivers"][0]["watermark"]
    assert auth_seq <= watermark, "the in-flight callback is sealed by the withdrawal watermark"


# ------------------------------------------------ reference receiver behavior
def test_reference_receiver_scopes_erase_to_exact_customer_topic(tmp_path):
    rx = ReferenceErasureReceiver(str(tmp_path / "rx.db"))
    # Two customers share a topic name; one customer has another topic too.
    rx.store_callback(
        {"customer_id": "c1", "topic_id": "shared", "delivery_id": "d1",
         "erasure_receiver_id": "r", "authorization_seq": 1, "payload": {"x": SECRET}}, now=1.0)
    rx.store_callback(
        {"customer_id": "c2", "topic_id": "shared", "delivery_id": "d2",
         "erasure_receiver_id": "r", "authorization_seq": 1, "payload": {"x": "other"}}, now=1.0)
    rx.store_callback(
        {"customer_id": "c1", "topic_id": "other", "delivery_id": "d3",
         "erasure_receiver_id": "r", "authorization_seq": 1, "payload": {"x": "keep"}}, now=1.0)

    ack = rx.erase_topic(
        {"operation_id": "op1", "customer_id": "c1", "topic_id": "shared",
         "receiver_id": "r", "watermark": 5}, now=2.0)
    assert ack["acknowledged"] is True and ack["deleted"] == 1

    remaining = {(r["customer_id"], r["topic_id"]) for r in rx.list_replicas()}
    assert remaining == {("c2", "shared"), ("c1", "other")}


def test_reference_receiver_rejects_inflight_callback_after_erase(tmp_path):
    rx = ReferenceErasureReceiver(str(tmp_path / "rx.db"))
    rx.store_callback(
        {"customer_id": "c1", "topic_id": "t", "delivery_id": "d1",
         "erasure_receiver_id": "r", "authorization_seq": 3, "payload": {"x": 1}}, now=1.0)
    rx.erase_topic(
        {"operation_id": "op1", "customer_id": "c1", "topic_id": "t",
         "receiver_id": "r", "watermark": 3}, now=2.0)
    assert rx.list_replicas() == []
    # A callback authorized before withdrawal (seq <= 3) arrives late; it must
    # not recreate a copy even though its HTTP request is after the erasure.
    result = rx.store_callback(
        {"customer_id": "c1", "topic_id": "t", "delivery_id": "d1",
         "erasure_receiver_id": "r", "authorization_seq": 3, "payload": {"x": 1}}, now=3.0)
    assert result["stored"] is False and result["reason"] == "sealed"
    assert rx.list_replicas() == []
    # A genuinely new authorization above the seal may store again (not a
    # pre-withdrawal callback).
    later = rx.store_callback(
        {"customer_id": "c1", "topic_id": "t", "delivery_id": "d9",
         "erasure_receiver_id": "r", "authorization_seq": 9, "payload": {"x": 2}}, now=4.0)
    assert later["stored"] is True


def test_reference_receiver_ack_is_idempotent_and_survives_restart(tmp_path):
    path = str(tmp_path / "rx.db")
    rx = ReferenceErasureReceiver(path)
    rx.store_callback(
        {"customer_id": "c1", "topic_id": "t", "delivery_id": "d1",
         "erasure_receiver_id": "r", "authorization_seq": 1, "payload": {"x": 1}}, now=1.0)
    ack1 = rx.erase_topic(
        {"operation_id": "op1", "customer_id": "c1", "topic_id": "t",
         "receiver_id": "r", "watermark": 4}, now=2.0)
    assert ack1["acknowledged"] and ack1["deleted"] == 1
    # Repeating the same operation is idempotent.
    ack2 = rx.erase_topic(
        {"operation_id": "op1", "customer_id": "c1", "topic_id": "t",
         "receiver_id": "r", "watermark": 4}, now=3.0)
    assert ack2["acknowledged"] and ack2["repeated"] is True and ack2["deleted"] == 0

    # Restart: seals are durable, so a late pre-withdrawal callback is still
    # rejected and a repeat acknowledgement still holds.
    rx2 = ReferenceErasureReceiver(path)
    late = rx2.store_callback(
        {"customer_id": "c1", "topic_id": "t", "delivery_id": "d1",
         "erasure_receiver_id": "r", "authorization_seq": 4, "payload": {"x": 1}}, now=4.0)
    assert late["stored"] is False
    ack3 = rx2.erase_topic(
        {"operation_id": "op1", "customer_id": "c1", "topic_id": "t",
         "receiver_id": "r", "watermark": 4}, now=5.0)
    assert ack3["acknowledged"] and ack3["repeated"] is True


def test_reference_receiver_does_not_ack_without_watermark(tmp_path):
    rx = ReferenceErasureReceiver(str(tmp_path / "rx.db"))
    ack = rx.erase_topic(
        {"operation_id": "op1", "customer_id": "c1", "topic_id": "t", "receiver_id": "r"}, now=1.0)
    assert ack["acknowledged"] is False


# ------------------------------------------------------------- migration
LEGACY_SCHEMA = """
CREATE TABLE customers(id TEXT PRIMARY KEY, name TEXT NOT NULL, created_at REAL NOT NULL);
CREATE TABLE counters(name TEXT PRIMARY KEY, value INTEGER NOT NULL);
CREATE TABLE subscriptions(id TEXT PRIMARY KEY, customer_id TEXT NOT NULL, target_url TEXT NOT NULL,
 target_version INTEGER NOT NULL DEFAULT 1, ordinal INTEGER NOT NULL, created_at REAL NOT NULL, updated_at REAL NOT NULL);
CREATE TABLE events(id TEXT PRIMARY KEY, customer_id TEXT NOT NULL, flow_id TEXT NOT NULL, caller_event_id TEXT NOT NULL,
 payload_json TEXT NOT NULL, content_hash TEXT NOT NULL, seq INTEGER NOT NULL, ordinal INTEGER NOT NULL, created_at REAL NOT NULL,
 UNIQUE(customer_id,flow_id,caller_event_id), UNIQUE(customer_id,flow_id,seq));
"""


def test_initialize_migrates_old_database_for_erasure(tmp_path):
    from webhook_delivery.config import Settings as S

    db = tmp_path / "legacy.db"
    con = sqlite3.connect(str(db))
    con.executescript(LEGACY_SCHEMA)
    con.execute("INSERT INTO counters VALUES('ordinal', 1)")
    con.execute("INSERT INTO customers VALUES('cus_1','Oldie',1.0)")
    con.execute(
        "INSERT INTO subscriptions VALUES('sub_1','cus_1','http://old/h',1,0,1.0,1.0)"
    )
    con.commit()
    con.close()

    migrated = Store(S(database_path=str(db)))
    migrated.initialize()
    migrated.initialize()  # idempotent

    # Old subscription is an ordinary target: no erasure receiver.
    sub = migrated.get_subscription("sub_1")
    assert sub["erasure_receiver_id"] is None
    # New capability works on the migrated file.
    recv = migrated.register_erasure_receiver(customer_id="cus_1", url="http://new/erase", now=2.0)
    ev = migrated.ingest_event(
        customer_id="cus_1", flow_id="f", caller_event_id="new", payload={"a": 1},
        now=3.0, topic_id="tp",
    )
    assert ev["topic_id"] == "tp"
    assert migrated.list_erasure_receivers(customer_id="cus_1")[0]["id"] == recv["id"]


# ------------------------------------------------------------------- API
def test_api_register_select_and_query_progress(client, store, clock):
    cus = client.post("/customers", json={"name": "Acme"}).json()
    reg = client.post(
        "/erasure-receivers", json={"customer_id": cus["id"], "url": "http://recv.test/erase"}
    )
    assert reg.status_code == 201
    recv = reg.json()
    assert recv["customer_id"] == cus["id"]

    listed = client.get("/erasure-receivers", params={"customer_id": cus["id"]}).json()
    assert [r["id"] for r in listed] == [recv["id"]]

    sub = client.post(
        "/subscriptions",
        json={"customer_id": cus["id"], "target_url": "http://ignored/h",
              "erasure_receiver_id": recv["id"]},
    ).json()
    assert sub["erasure_receiver_id"] == recv["id"]
    assert sub["target_url"] == "http://recv.test/erase"

    ev = client.post(
        "/events",
        json={"customer_id": cus["id"], "flow_id": "f", "caller_event_id": "e1",
              "payload": {"s": SECRET}, "topic_id": "tp"},
    ).json()
    assert ev["deliveries"]

    # Drive the send gate so the receiver becomes obligated, then withdraw.
    claim = store.claim_due(now=clock.now(), lease_seconds=30)[0]
    assert store.begin_send(claim, now=clock.now()) is not None

    w = client.post(f"/topics/tp/withdraw", json={"customer_id": cus["id"]})
    assert w.status_code == 200
    body = w.json()
    assert body["withdrawn"] is True
    assert body["erasure"]["total"] == 1
    assert body["erasure"]["pending_receiver_ids"] == [recv["id"]]

    progress = client.get(f"/topics/tp/erasure", params={"customer_id": cus["id"]}).json()
    assert progress["complete"] is False
    assert progress["acknowledged"] == 0

    # The erasure request is queryable and, once acknowledged, confirms.
    reqs = client.get("/erasure-requests", params={"customer_id": cus["id"]}).json()
    assert len(reqs) == 1 and reqs[0]["acknowledged"] is False
    store.finish_erasure(
        store.claim_due_erasures(now=clock.now(), lease_seconds=30)[0],
        acknowledged=True, http_status=200, error=None, now=clock.now(), retry_delay=None,
    )
    progress = client.get(f"/topics/tp/erasure", params={"customer_id": cus["id"]}).json()
    assert progress["complete"] is True
    assert progress["acknowledged_receiver_ids"] == [recv["id"]]


def test_api_switch_receiver_and_patch(client, store, clock):
    cus = client.post("/customers", json={"name": "Acme"}).json()
    a = client.post("/erasure-receivers", json={"customer_id": cus["id"], "url": "http://a.test/e"}).json()
    b = client.post("/erasure-receivers", json={"customer_id": cus["id"], "url": "http://b.test/e"}).json()
    sub = client.post(
        "/subscriptions",
        json={"customer_id": cus["id"], "target_url": a["url"], "erasure_receiver_id": a["id"]},
    ).json()
    patched = client.patch(
        f"/subscriptions/{sub['id']}",
        json={"customer_id": cus["id"], "target_url": b["url"], "erasure_receiver_id": b["id"]},
    ).json()
    assert patched["erasure_receiver_id"] == b["id"]
    # Switching back to an ordinary target is explicit.
    cleared = client.patch(
        f"/subscriptions/{sub['id']}",
        json={"customer_id": cus["id"], "target_url": "http://plain.test/h",
              "clear_erasure_receiver": True},
    ).json()
    assert cleared["erasure_receiver_id"] is None


def test_api_rejects_foreign_receiver(client):
    a = client.post("/customers", json={"name": "A"}).json()
    b = client.post("/customers", json={"name": "B"}).json()
    recv = client.post(
        "/erasure-receivers", json={"customer_id": b["id"], "url": "http://b.test/e"}
    ).json()
    r = client.post(
        "/subscriptions",
        json={"customer_id": a["id"], "target_url": "http://x/h", "erasure_receiver_id": recv["id"]},
    )
    assert r.status_code == 403
