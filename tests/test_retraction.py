"""Topic withdrawal and ticket evidence-retention semantics.

Covers the product rules end to end at the Store and FastAPI level:

* optional customer-scoped ``topic_id`` (at most one per event);
* tickets retaining already-received, non-withdrawn events;
* multi-ticket retention and closure;
* idempotent irreversible topic withdrawal and content/digest erasure;
* tombstoned event keys and rejection of future topic events;
* additive migration of pre-topic databases (no backfill).

Delivery/worker fencing lives in ``test_retraction_delivery.py``.
"""

from __future__ import annotations

import sqlite3

import pytest

from webhook_delivery.store import Store
from webhook_delivery.util import DomainError

SECRET = "TOPIC_SECRET_TOKEN_42"


def ingest(store, clock, customer, *, caller="e1", flow="f", payload=None, topic_id=None):
    return store.ingest_event(
        customer_id=customer["id"],
        flow_id=flow,
        caller_event_id=caller,
        payload={SECRET: SECRET} if payload is None else payload,
        now=clock.now(),
        topic_id=topic_id,
    )


def assert_secret_absent_from_database(db_path: str, secret: str = SECRET) -> None:
    """Scan every stored row's representation for the secret.

    This is the "normal persistent data" check (SQL-visible state).  Disk
    page carving / WAL forensics and external backups are out of scope.
    """
    con = sqlite3.connect(db_path)
    try:
        tables = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        for table in tables:
            for row in con.execute(f"SELECT * FROM {table}"):
                assert secret not in repr(row), f"secret still present in table {table}: {row!r}"
    finally:
        con.close()


# --------------------------------------------------------------------- topics
def test_event_carries_optional_topic_id(store, clock, customer):
    ev = ingest(store, clock, customer, topic_id="topic-a")
    assert ev["topic_id"] == "topic-a"
    plain = ingest(store, clock, customer, caller="e2")
    assert plain["topic_id"] is None
    loaded = store.get_event(ev["id"])
    assert loaded["topic_id"] == "topic-a" and loaded["withdrawn"] is False


def test_without_topic_events_are_unaffected_by_other_withdrawals(store, clock, customer):
    plain = ingest(store, clock, customer, caller="plain")
    store.withdraw_topic(customer_id=customer["id"], topic_id="does-not-exist", now=clock.now())
    loaded = store.get_event(plain["id"])
    assert loaded["payload"] == {SECRET: SECRET}
    assert loaded["content_hash"]
    assert loaded["withdrawn"] is False


def test_topic_is_scoped_to_customer(store, clock, customer):
    other = store.create_customer(name="Other", now=clock.now())
    mine = ingest(store, clock, customer, topic_id="shared-name")
    theirs = ingest(store, clock, other, topic_id="shared-name")
    store.withdraw_topic(customer_id=customer["id"], topic_id="shared-name", now=clock.now())
    assert store.get_event(mine["id"])["withdrawn"] is True
    assert store.get_event(theirs["id"])["withdrawn"] is False
    # The other customer can still post to the same topic name.
    again = ingest(store, clock, other, caller="theirs2", topic_id="shared-name")
    assert again["seq"] == 2


# --------------------------------------------------------------- withdrawal UX
def test_withdraw_is_idempotent_and_keeps_identity_records(store, clock, customer):
    ev = ingest(store, clock, customer, topic_id="t1")
    first = store.withdraw_topic(customer_id=customer["id"], topic_id="t1", now=clock.now())
    assert first["already_withdrawn"] is False
    assert first["events_affected"] == 1
    second = store.withdraw_topic(customer_id=customer["id"], topic_id="t1", now=clock.now() + 1)
    assert second["already_withdrawn"] is True
    assert second["replayed"] is True
    assert second["events_affected"] == 1  # event rows still match the topic

    tombstone = store.list_events(customer_id=customer["id"])[0]
    assert tombstone["id"] == ev["id"]
    assert tombstone["seq"] == ev["seq"]
    assert tombstone["caller_event_id"] == "e1"
    assert tombstone["withdrawn"] is True
    assert tombstone["payload"] is None
    assert tombstone["content_hash"] is None


def test_withdrawal_erases_content_from_every_read_model(store, clock, customer, subscribe):
    ev = ingest(store, clock, customer, topic_id="t1")
    dlv_id = ev["deliveries"][0]["delivery_id"]
    store.withdraw_topic(customer_id=customer["id"], topic_id="t1", now=clock.now())

    assert SECRET not in repr(store.list_events(customer_id=customer["id"]))
    assert SECRET not in repr(store.get_event(ev["id"]))
    detail = store.get_delivery(dlv_id)
    assert SECRET not in repr(detail)
    assert detail["event"]["payload"] is None
    assert detail["event"]["withdrawn"] is True


def test_without_holds_database_rows_contain_no_content(settings, store, clock, customer):
    ingest(store, clock, customer, topic_id="erase-me")
    store.withdraw_topic(customer_id=customer["id"], topic_id="erase-me", now=clock.now())
    assert_secret_absent_from_database(settings.database_path)


def test_erased_content_is_not_left_in_database_files(settings, store, clock, customer):
    secret = "RAW_FILE_SECRET_31337"
    ingest(store, clock, customer, topic_id="erase-file", payload={"k": secret})
    store.withdraw_topic(customer_id=customer["id"], topic_id="erase-file", now=clock.now())
    for suffix in ("", "-wal"):
        path = settings.database_path + suffix
        try:
            blob = open(path, "rb").read()
        except FileNotFoundError:
            continue
        assert secret.encode() not in blob, f"secret bytes remain in {path}"


def test_withdrawn_key_cannot_be_resent_or_reused(store, clock, customer):
    payload = {"k": SECRET}
    ingest(store, clock, customer, payload=payload, topic_id="t")
    store.withdraw_topic(customer_id=customer["id"], topic_id="t", now=clock.now())

    with pytest.raises(DomainError) as same:
        ingest(store, clock, customer, payload=payload, topic_id="t")
    assert same.value.status_code == 409
    assert same.value.code in {"topic_withdrawn", "event_withdrawn"}

    with pytest.raises(DomainError) as changed:
        ingest(store, clock, customer, payload={"k": "different"}, topic_id="t")
    assert changed.value.status_code == 409

    # Omitting topic_id on replay still hits the event tombstone.
    with pytest.raises(DomainError) as omitted:
        store.ingest_event(
            customer_id=customer["id"], flow_id="f", caller_event_id="e1",
            payload=payload, now=clock.now(),
        )
    assert omitted.value.code == "event_withdrawn"


def test_new_topic_events_rejected_without_sequence_or_delivery(store, clock, customer, subscribe):
    first = ingest(store, clock, customer, topic_id="t")
    assert first["seq"] == 1
    store.withdraw_topic(customer_id=customer["id"], topic_id="t", now=clock.now())

    with pytest.raises(DomainError) as exc:
        ingest(store, clock, customer, caller="later", topic_id="t")
    assert exc.value.code == "topic_withdrawn"

    # The rejected event allocated neither a seq nor deliveries: the next
    # topic-less event in the same flow gets seq 2, not 3.
    nxt = ingest(store, clock, customer, caller="ok", topic_id=None)
    assert nxt["seq"] == 2
    assert len(nxt["deliveries"]) == 1
    assert store.list_events(customer_id=customer["id"], topic_id="t") == [
        e for e in store.list_events(customer_id=customer["id"]) if e["topic_id"] == "t"
    ]


# ------------------------------------------------------------------- tickets
def test_ticket_retains_reads_and_closes(store, clock, customer):
    ev = ingest(store, clock, customer)
    ticket = store.create_ticket(customer_id=customer["id"], now=clock.now(), title="INV-1")
    updated = store.retain_events(ticket_id=ticket["id"], event_ids=[ev["id"]], now=clock.now())
    assert updated["event_ids"] == [ev["id"]]

    evidence = store.list_ticket_events(ticket["id"])
    assert len(evidence) == 1
    assert evidence[0]["payload"] == {SECRET: SECRET}
    assert evidence[0]["content_hash"]

    closed = store.close_ticket(ticket["id"], now=clock.now())
    assert closed["status"] == "closed"
    with pytest.raises(DomainError) as read_exc:
        store.list_ticket_events(ticket["id"])
    assert read_exc.value.code == "ticket_closed"
    with pytest.raises(DomainError) as add_exc:
        store.retain_events(ticket_id=ticket["id"], event_ids=[ev["id"]], now=clock.now())
    assert add_exc.value.code == "ticket_closed"
    # Closing twice is an idempotent no-op.
    assert store.close_ticket(ticket["id"], now=clock.now() + 1)["status"] == "closed"


def test_event_can_be_held_by_several_tickets(settings, store, clock, customer):
    ev = ingest(store, clock, customer, topic_id="t")
    a = store.create_ticket(customer_id=customer["id"], now=clock.now())
    b = store.create_ticket(customer_id=customer["id"], now=clock.now())
    store.retain_events(ticket_id=a["id"], event_ids=[ev["id"]], now=clock.now())
    store.retain_events(ticket_id=b["id"], event_ids=[ev["id"]], now=clock.now())

    store.withdraw_topic(customer_id=customer["id"], topic_id="t", now=clock.now())
    # Both open tickets still read the original content.
    assert store.list_ticket_events(a["id"])[0]["payload"] == {SECRET: SECRET}
    assert store.list_ticket_events(b["id"])[0]["payload"] == {SECRET: SECRET}

    store.close_ticket(a["id"], now=clock.now())
    with pytest.raises(DomainError):
        store.list_ticket_events(a["id"])
    # Ticket B is unaffected by A's closure/purge.
    assert store.list_ticket_events(b["id"])[0]["payload"] == {SECRET: SECRET}

    # One valid retention remains, so content is still in the DB (B's copy).
    con = sqlite3.connect(settings.database_path)
    rows = list(con.execute("SELECT payload_json FROM ticket_evidence WHERE ticket_id=?", (b["id"],)))
    con.close()
    assert SECRET in rows[0][0]

    store.close_ticket(b["id"], now=clock.now())
    # No open hold remains: all normal tables are free of the content.
    assert_secret_absent_from_database(settings.database_path)


def test_closed_ticket_embedded_detail_has_no_content(store, clock, customer):
    ev = ingest(store, clock, customer)
    ticket = store.create_ticket(customer_id=customer["id"], now=clock.now())
    store.retain_events(ticket_id=ticket["id"], event_ids=[ev["id"]], now=clock.now())
    store.close_ticket(ticket["id"], now=clock.now())
    detail = store.get_ticket(ticket["id"])
    assert all(e["payload"] is None for e in detail["events"])
    assert SECRET not in repr(detail)


def test_retain_rejects_unknown_or_other_customer_event(store, clock, customer):
    ticket = store.create_ticket(customer_id=customer["id"], now=clock.now())
    with pytest.raises(DomainError) as exc:
        store.retain_events(ticket_id=ticket["id"], event_ids=["evt_does_not_exist"], now=clock.now())
    assert exc.value.status_code == 404
    assert exc.value.code == "event_not_found"

    other = store.create_customer(name="X", now=clock.now())
    foreign = ingest(store, clock, other)
    with pytest.raises(DomainError) as exc2:
        store.retain_events(ticket_id=ticket["id"], event_ids=[foreign["id"]], now=clock.now())
    assert exc2.value.code == "event_not_found"


def test_retain_batch_is_all_or_nothing(store, clock, customer):
    ev = ingest(store, clock, customer)
    ticket = store.create_ticket(customer_id=customer["id"], now=clock.now())
    with pytest.raises(DomainError):
        store.retain_events(
            ticket_id=ticket["id"], event_ids=[ev["id"], "evt_missing"], now=clock.now()
        )
    assert store.get_ticket(ticket["id"])["event_ids"] == []
    # Repeating an already-retained id is idempotent.
    store.retain_events(ticket_id=ticket["id"], event_ids=[ev["id"], ev["id"]], now=clock.now())
    again = store.retain_events(ticket_id=ticket["id"], event_ids=[ev["id"]], now=clock.now())
    assert again["event_ids"] == [ev["id"]]


def test_cross_customer_ticket_access_is_denied(store, clock, customer):
    other = store.create_customer(name="X", now=clock.now())
    ticket = store.create_ticket(customer_id=customer["id"], now=clock.now())
    with pytest.raises(DomainError) as exc:
        store.get_ticket(ticket["id"], customer_id=other["id"])
    assert exc.value.status_code == 403
    with pytest.raises(DomainError):
        store.close_ticket(ticket["id"], customer_id=other["id"])


# ------------------------------------------------- retain/withdraw race order
def test_retain_before_withdrawal_wins_and_preserves(store, clock, customer):
    ev = ingest(store, clock, customer, topic_id="t")
    ticket = store.create_ticket(customer_id=customer["id"], now=clock.now())
    # Server-effective order #1: retention commits first.
    store.retain_events(ticket_id=ticket["id"], event_ids=[ev["id"]], now=clock.now())
    store.withdraw_topic(customer_id=customer["id"], topic_id="t", now=clock.now())
    assert store.list_ticket_events(ticket["id"])[0]["payload"] == {SECRET: SECRET}


def test_withdrawal_before_retention_rejects(store, clock, customer):
    ev = ingest(store, clock, customer, topic_id="t")
    ticket = store.create_ticket(customer_id=customer["id"], now=clock.now())
    # Server-effective order #2: withdrawal commits first.
    store.withdraw_topic(customer_id=customer["id"], topic_id="t", now=clock.now())
    with pytest.raises(DomainError) as exc:
        store.retain_events(ticket_id=ticket["id"], event_ids=[ev["id"]], now=clock.now())
    assert exc.value.status_code == 409


# --------------------------------------------------------------------- migration
LEGACY_SCHEMA = """
CREATE TABLE customers(id TEXT PRIMARY KEY, name TEXT NOT NULL, created_at REAL NOT NULL);
CREATE TABLE counters(name TEXT PRIMARY KEY, value INTEGER NOT NULL);
CREATE TABLE subscriptions(id TEXT PRIMARY KEY, customer_id TEXT NOT NULL, target_url TEXT NOT NULL,
 target_version INTEGER NOT NULL DEFAULT 1, ordinal INTEGER NOT NULL, created_at REAL NOT NULL, updated_at REAL NOT NULL);
CREATE TABLE events(id TEXT PRIMARY KEY, customer_id TEXT NOT NULL, flow_id TEXT NOT NULL, caller_event_id TEXT NOT NULL,
 payload_json TEXT NOT NULL, content_hash TEXT NOT NULL, seq INTEGER NOT NULL, ordinal INTEGER NOT NULL, created_at REAL NOT NULL,
 UNIQUE(customer_id,flow_id,caller_event_id), UNIQUE(customer_id,flow_id,seq));
"""


def test_initialize_migrates_existing_database_without_topic_backfill(tmp_path):
    from webhook_delivery.config import Settings as S

    db = tmp_path / "legacy.db"
    con = sqlite3.connect(str(db))
    con.executescript(LEGACY_SCHEMA)
    con.execute("INSERT INTO counters VALUES('ordinal', 1)")
    con.execute("INSERT INTO customers VALUES('cus_1','Oldie',1.0)")
    con.execute(
        "INSERT INTO events VALUES('evt_old','cus_1','flow','old-key','{\"a\":1}','h',1,1,1.0)"
    )
    con.commit()
    con.close()

    settings = S(database_path=str(db))
    migrated = Store(settings)
    migrated.initialize()  # must ALTER-add topic_id/status and create new tables
    migrated.initialize()  # idempotent

    old = migrated.list_events(customer_id="cus_1")[0]
    assert old["topic_id"] is None and old["payload"] == {"a": 1}
    # Old key still replays under the legacy idempotency rule only if content
    # matches; here we just assert the row is readable and topic-less.
    ticket = migrated.create_ticket(customer_id="cus_1", now=2.0)
    migrated.retain_events(ticket_id=ticket["id"], event_ids=["evt_old"], now=2.0)
    assert migrated.list_ticket_events(ticket["id"])[0]["payload"] == {"a": 1}
    # New topic capability works on the migrated file.
    ev = migrated.ingest_event(
        customer_id="cus_1", flow_id="f2", caller_event_id="new", payload={"b": 2}, now=3.0, topic_id="tp"
    )
    assert ev["topic_id"] == "tp"
    migrated.withdraw_topic(customer_id="cus_1", topic_id="tp", now=4.0)
    assert migrated.get_event(ev["id"])["withdrawn"] is True


# ------------------------------------------------------------------------ API
def test_api_full_withdraw_and_ticket_journey(client):
    cus = client.post("/customers", json={"name": "Acme"}).json()
    ev = client.post(
        "/events",
        json={
            "customer_id": cus["id"], "flow_id": "f", "caller_event_id": "e1",
            "payload": {"secret": SECRET}, "topic_id": "case-topic",
        },
    ).json()

    ticket = client.post("/tickets", json={"customer_id": cus["id"], "title": "INV-9"}).json()
    r = client.post(f"/tickets/{ticket['id']}/events", json={"event_ids": [ev["id"]]})
    assert r.status_code == 200

    w = client.post(f"/topics/case-topic/withdraw", json={"customer_id": cus["id"]})
    assert w.status_code == 200 and w.json()["events_affected"] == 1
    assert client.post("/topics/case-topic/withdraw", json={"customer_id": cus["id"]}).status_code == 200

    listed = client.get("/events", params={"customer_id": cus["id"], "topic_id": "case-topic"}).json()
    assert listed[0]["payload"] is None and listed[0]["content_hash"] is None

    evidence = client.get(f"/tickets/{ticket['id']}/events").json()
    assert evidence[0]["payload"] == {"secret": SECRET}

    closed = client.post(f"/tickets/{ticket['id']}/close", json={})
    assert closed.status_code == 200
    assert client.get(f"/tickets/{ticket['id']}/events").status_code == 409
    assert client.post(f"/tickets/{ticket['id']}/events", json={"event_id": ev["id"]}).status_code == 409

    replay = client.post(
        "/events",
        json={
            "customer_id": cus["id"], "flow_id": "f", "caller_event_id": "e1",
            "payload": {"secret": SECRET}, "topic_id": "case-topic",
        },
    )
    assert replay.status_code == 409


def test_api_withdraw_requires_customer(client):
    r = client.post("/topics/t/withdraw", json={})
    assert r.status_code == 422


def test_api_alias_routes(client):
    cus = client.post("/customers", json={"name": "A"}).json()
    ev = client.post(
        "/events",
        json={"customer_id": cus["id"], "flow_id": "f", "caller_event_id": "e", "payload": {"x": 1}},
    ).json()
    case = client.post("/cases", json={"customer_id": cus["id"]}).json()
    assert client.post(f"/cases/{case['id']}/retain", json={"event_id": ev["id"]}).status_code == 200
    assert client.get(f"/cases/{case['id']}/evidence").status_code == 200
    assert client.post(f"/cases/{case['id']}/close").status_code == 200

    w = client.post("/withdrawals", json={"customer_id": cus["id"], "topic_id": "missing"})
    assert w.status_code == 200 and w.json()["withdrawn"] is True
    deleted = client.request("DELETE", "/topics/missing?customer_id=" + cus["id"])
    assert deleted.status_code == 200


def test_api_unknown_ticket_404(client):
    assert client.get("/tickets/tkt_nope").status_code == 404


def test_api_accepts_topic_alias_and_create_ticket_with_events(client):
    cus = client.post("/customers", json={"name": "A"}).json()
    # "topic" spelling is accepted in addition to topic_id.
    ev = client.post(
        "/events",
        json={"customer_id": cus["id"], "flow_id": "f", "caller_event_id": "k",
              "payload": {"v": 1}, "topic": "alias-topic"},
    ).json()
    assert ev["topic_id"] == "alias-topic"

    # Create + retain in one request.
    ticket = client.post(
        "/tickets",
        json={"customer_id": cus["id"], "title": "one-shot", "event_ids": [ev["id"]]},
    ).json()
    assert ticket["event_ids"] == [ev["id"]]
    assert ticket["events"][0]["payload"] == {"v": 1}

    # camelCase topic id on the withdraw body also resolves.
    r = client.post("/topics/withdraw", json={"customer_id": cus["id"], "topicId": "alias-topic"})
    assert r.status_code == 200 and r.json()["already_withdrawn"] is False


# ------------------------------------------------------------ concurrency
def test_parallel_retain_and_withdraw_serialise_on_database(settings, store, clock, customer):
    """Many retaining threads vs one withdrawal: only clean DomainErrors are
    allowed and the final erasure invariant must hold."""
    import threading

    events = [
        ingest(store, clock, customer, caller=f"c{i}", topic_id="hot", payload={"i": i, "s": SECRET})
        for i in range(12)
    ]
    tickets = [store.create_ticket(customer_id=customer["id"], now=clock.now()) for _ in range(5)]
    errors: list[BaseException] = []
    barrier = threading.Barrier(len(tickets) + 1)

    def retainer(ticket_id: str) -> None:
        barrier.wait()
        for ev in events:
            try:
                store.retain_events(ticket_id=ticket_id, event_ids=[ev["id"]], now=clock.now())
            except DomainError:
                pass  # withdrawal won the race for this event
            except BaseException as exc:  # pragma: no cover - failure signal
                errors.append(exc)

    threads = [threading.Thread(target=retainer, args=(t["id"],)) for t in tickets]
    for t in threads:
        t.start()
    barrier.wait()
    clock.advance(1)
    try:
        store.withdraw_topic(customer_id=customer["id"], topic_id="hot", now=clock.now())
    except BaseException as exc:  # pragma: no cover
        errors.append(exc)
    for t in threads:
        t.join(timeout=10)

    assert errors == [], f"unexpected concurrency errors: {errors!r}"
    # Whatever each open ticket managed to retain is readable and intact.
    for ticket in tickets:
        for item in store.list_ticket_events(ticket["id"]):
            assert item["payload"]["s"] == SECRET
    # Closing every ticket removes all copies.
    for ticket in tickets:
        store.close_ticket(ticket["id"], now=clock.now())
    assert_secret_absent_from_database(settings.database_path)
