"""SQLite persistence for events, subscriptions, deliveries, attempts,
topic withdrawals and ticket evidence retention.

Correctness-relevant state transitions happen inside ``BEGIN IMMEDIATE``
transactions.  SQLite allows one writer at a time, which is what makes
"claim" / "withdraw" / "retain" / "finalize" safe across multiple API and
delivery processes sharing one database file.

Key invariants encoded here
---------------------------
* Event identity is ``(customer_id, flow_id, caller_event_id)``.  Replaying
  the same identity with the same canonical JSON returns the original event;
  replaying it with different content raises ``caller_event_conflict``.
* ``events.seq`` is per ``(customer_id, flow_id)`` and allocated inside the
  same write transaction that inserts the row.
* A subscription is only fanned out events whose global ``ordinal`` is greater
  than the subscription's ordinal.
* A delivery may be claimed only when every lower ``seq`` in the same
  ``(subscription, flow)`` is terminal (succeeded, administratively skipped
  or withdrawn).  Different flows never block each other.
* Every claim writes a fresh ``lease_token``.  ``finish_attempt`` only mutates
  the delivery when that token still matches and the delivery was not
  withdrawn.  Late replies are recorded without any upstream free text, so a
  receiver echo can never bring redacted content back.
* Withdrawing a customer-scoped topic is irreversible and idempotent.  It
  tombstones the event rows (content and digest are erased), rejects every
  future event for the topic without allocating a sequence, and terminates
  every delivery that was not already confirmed succeeded or
  administratively skipped.
* A ticket copies evidence content at retain time.  Retention after
  withdrawal is impossible; closing a ticket purges its copies, so once no
  open ticket holds an event no original content remains in normal tables.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterable, Iterator

from .config import Settings
from .util import (
    DomainError,
    canonical_json,
    content_digest,
    iso_time,
    new_id,
    require_text,
    validate_target_url,
)

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS counters (
    name  TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS customers (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    created_at REAL NOT NULL,
    -- Set when the customer registers their first prepaid top-up; every event
    -- accepted afterwards is billable.  NULL = no credits in force.
    credits_effective_at REAL
);

CREATE TABLE IF NOT EXISTS subscriptions (
    id             TEXT PRIMARY KEY,
    customer_id    TEXT NOT NULL REFERENCES customers(id),
    target_url     TEXT NOT NULL,
    target_version INTEGER NOT NULL DEFAULT 1,
    ordinal        INTEGER NOT NULL,
    created_at     REAL NOT NULL,
    updated_at     REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS topics (
    customer_id  TEXT NOT NULL,
    topic_id     TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'active',
    created_at   REAL NOT NULL,
    withdrawn_at REAL,
    PRIMARY KEY (customer_id, topic_id)
);

CREATE TABLE IF NOT EXISTS events (
    id              TEXT PRIMARY KEY,
    customer_id     TEXT NOT NULL REFERENCES customers(id),
    flow_id         TEXT NOT NULL,
    caller_event_id TEXT NOT NULL,
    topic_id        TEXT,
    status          TEXT NOT NULL DEFAULT 'active',
    -- 1 only for events accepted after the customer's first prepaid top-up
    -- became effective.  Deliveries copy this flag and need one point each.
    billable        INTEGER NOT NULL DEFAULT 0,
    payload_json    TEXT NOT NULL,
    content_hash    TEXT NOT NULL,
    seq             INTEGER NOT NULL,
    ordinal         INTEGER NOT NULL,
    created_at      REAL NOT NULL,
    UNIQUE (customer_id, flow_id, caller_event_id),
    UNIQUE (customer_id, flow_id, seq)
);

CREATE TABLE IF NOT EXISTS deliveries (
    id              TEXT PRIMARY KEY,
    event_id        TEXT NOT NULL REFERENCES events(id),
    subscription_id TEXT NOT NULL REFERENCES subscriptions(id),
    customer_id     TEXT NOT NULL,
    flow_id         TEXT NOT NULL,
    seq             INTEGER NOT NULL,
    target_url      TEXT NOT NULL,
    target_version  INTEGER NOT NULL,
    status          TEXT NOT NULL,
    -- 1 when this delivery needs one prepaid point (billable event).
    billable        INTEGER NOT NULL DEFAULT 0,
    -- credit_stage: none | needed | held | charged | returned | expired | reconciliation
    --   none            not billable, no point involved
    --   needed          billable but no point allocated yet (claimed, cannot send)
    --   held            one point reserved for this delivery's delivery_id
    --   charged         held point consumed after a confirmed 2xx
    --   returned        held point released (no request was in flight)
    --   expired         point returned after its top-up's expiry, unspendable
    --   reconciliation  send outcome unknown; held point kept pending customer confirmation
    credit_stage    TEXT NOT NULL DEFAULT 'none',
    credit_id       TEXT,
    reconciliation_reason TEXT,
    reconciliation_state  TEXT,  -- None | received | not_received
    reconciliation_note   TEXT,
    reconciliation_at     REAL,
    attempt_seq     INTEGER NOT NULL DEFAULT 0,
    attempts_count  INTEGER NOT NULL DEFAULT 0,
    next_attempt_at REAL,
    lease_token     TEXT,
    lease_until     REAL,
    blocked_by      TEXT,
    block_reason    TEXT,
    last_error      TEXT,
    created_at      REAL NOT NULL,
    updated_at      REAL NOT NULL,
    completed_at    REAL,
    UNIQUE (subscription_id, event_id)
);

CREATE INDEX IF NOT EXISTS ix_deliveries_claim
    ON deliveries(subscription_id, flow_id, seq);
CREATE INDEX IF NOT EXISTS ix_deliveries_due
    ON deliveries(status, next_attempt_at);
CREATE INDEX IF NOT EXISTS ix_deliveries_event
    ON deliveries(event_id);

CREATE TABLE IF NOT EXISTS attempts (
    id             TEXT PRIMARY KEY,
    delivery_id    TEXT NOT NULL REFERENCES deliveries(id),
    attempt_number INTEGER NOT NULL,
    target_url     TEXT NOT NULL,
    target_version INTEGER NOT NULL,
    started_at     REAL NOT NULL,
    finished_at    REAL,
    outcome        TEXT NOT NULL,
    http_status    INTEGER,
    error          TEXT,
    lease_token    TEXT NOT NULL,
    send_started_at REAL,
    UNIQUE (delivery_id, attempt_number)
);

CREATE TABLE IF NOT EXISTS admin_actions (
    id              TEXT PRIMARY KEY,
    delivery_id     TEXT NOT NULL REFERENCES deliveries(id),
    action          TEXT NOT NULL,
    reason          TEXT,
    actor           TEXT,
    created_at      REAL NOT NULL,
    target_version  INTEGER
);

CREATE INDEX IF NOT EXISTS ix_admin_actions_delivery
    ON admin_actions(delivery_id);

-- Replies from a lease holder that had already lost the lease (or whose
-- delivery/topic was withdrawn).  They are kept for observability but never
-- change delivery state and never store upstream free text.
CREATE TABLE IF NOT EXISTS late_results (
    id            TEXT PRIMARY KEY,
    delivery_id   TEXT NOT NULL REFERENCES deliveries(id),
    lease_token   TEXT NOT NULL,
    observed      TEXT NOT NULL,
    http_status   INTEGER,
    error         TEXT,
    recorded_at   REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_late_results_delivery
    ON late_results(delivery_id);

-- Investigation tickets ("工单").  A ticket copies evidence content when it
-- explicitly retains an event, so later topic withdrawal can erase the
-- regular event row without touching that ticket's preserved copy.
CREATE TABLE IF NOT EXISTS tickets (
    id          TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL REFERENCES customers(id),
    title       TEXT,
    reason      TEXT,
    status      TEXT NOT NULL DEFAULT 'open',
    created_at  REAL NOT NULL,
    closed_at   REAL
);

CREATE INDEX IF NOT EXISTS ix_tickets_customer
    ON tickets(customer_id, created_at);

CREATE TABLE IF NOT EXISTS ticket_evidence (
    id             TEXT PRIMARY KEY,
    ticket_id      TEXT NOT NULL REFERENCES tickets(id),
    event_id       TEXT NOT NULL REFERENCES events(id),
    customer_id    TEXT NOT NULL,
    payload_json   TEXT,
    content_hash   TEXT,
    retained_at    REAL NOT NULL,
    released_at    REAL,
    UNIQUE (ticket_id, event_id)
);

CREATE INDEX IF NOT EXISTS ix_ticket_evidence_event
    ON ticket_evidence(event_id);
CREATE INDEX IF NOT EXISTS ix_ticket_evidence_ticket
    ON ticket_evidence(ticket_id);

-- Prepaid credit top-ups.  ``topup_ref`` is unique per customer, so the same
-- reference + same (points, expires_at) is one top-up and a differing retry
-- is rejected.  Points still unspent after ``expires_at`` become ``expired``.
CREATE TABLE IF NOT EXISTS credit_topups (
    id          TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL REFERENCES customers(id),
    topup_ref   TEXT NOT NULL,
    points      INTEGER NOT NULL,
    expires_at  REAL NOT NULL,
    created_at  REAL NOT NULL,
    UNIQUE (customer_id, topup_ref)
);

CREATE INDEX IF NOT EXISTS ix_credit_topups_customer
    ON credit_topups(customer_id, expires_at, topup_ref);

-- One row per point a delivery_id has consumed / is holding.  ``stage``:
--   held    reserved for delivery_id (counts against open + chargeable quota)
--   charged consumed by a confirmed 2xx
--   returned released without a request ever being handed to the sender
--   expired released after the top-up expired (counts as expired, not spendable)
-- A single row can move held->charged *or* held->returned/expired, never both,
-- because every transition is guarded by the current ``stage``.
CREATE TABLE IF NOT EXISTS credit_allocations (
    id           TEXT PRIMARY KEY,
    delivery_id  TEXT NOT NULL REFERENCES deliveries(id),
    customer_id  TEXT NOT NULL,
    topup_id     TEXT REFERENCES credit_topups(id),
    points       INTEGER NOT NULL DEFAULT 1,
    stage        TEXT NOT NULL,
    created_at   REAL NOT NULL,
    settled_at   REAL,
    released_expired INTEGER NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_credit_alloc_held
    ON credit_allocations(delivery_id) WHERE stage IN ('held','charged');
CREATE INDEX IF NOT EXISTS ix_credit_alloc_topup
    ON credit_allocations(topup_id, stage);
CREATE INDEX IF NOT EXISTS ix_credit_alloc_delivery
    ON credit_allocations(delivery_id);
"""

TERMINAL_STATUSES = ("succeeded", "administratively_skipped", "withdrawn")
# Attempt outcomes that may have reached the receiver without a usable
# delivery verdict.  Any such attempt left after a delivery is stopped means
# the held point may already have been consumed; it is kept and parked for
# customer reconciliation instead of being silently refunded.
UNRESOLVED_OUTCOMES = (
    "in_flight",
    "expired",
    "timeout",
    "transport_error",
    "superseded_target",
    "lease_lost",
)
# Delivery statuses from which no further claiming / retry may happen.

# Sentinel stored in events.content_hash after erasure (column stays NOT NULL
# for databases created by older versions).
ERASED_HASH = ""
ERASED_PAYLOAD = "null"
WITHDRAWN_REASON = "topic_withdrawn"
REDACTED_TEXT = "error detail removed when the topic was withdrawn"
# Reconciliation states a customer may record for a stopped, unresolved delivery.
RECONCILED_RECEIVED = "received"
RECONCILED_NOT_RECEIVED = "not_received"



@dataclass(frozen=True)
class ClaimedDelivery:
    """A lease-backed unit of work handed to one worker."""

    id: str
    event_id: str
    subscription_id: str
    customer_id: str
    flow_id: str
    seq: int
    target_url: str
    target_version: int
    attempt_seq: int
    attempts_count: int  # attempts charged against the retry budget (incl. this one)
    lease_token: str
    attempt_id: str
    caller_event_id: str
    payload_json: str
    event_created_at: float
    billable: bool = False


def _connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=5.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


class Store:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._path = settings.database_path
        self._ready = False
        self._init_lock = threading.Lock()

    # ------------------------------------------------------------------ setup
    def initialize(self) -> None:
        """Create the schema and migrate databases created by older versions.

        Migration is additive only: old events keep ``topic_id = NULL``, so no
        backfill is required and topic-less history behaves exactly as before.
        Safe to call repeatedly and from several processes starting together.
        """
        with self._connect() as conn:
            conn.executescript(SCHEMA)
            self._add_column_if_missing(conn, "events", "topic_id", "topic_id TEXT")
            self._add_column_if_missing(
                conn, "events", "status", "status TEXT NOT NULL DEFAULT 'active'"
            )
            self._add_column_if_missing(
                conn, "attempts", "send_started_at", "send_started_at REAL"
            )
            # ---- prepaid credits (additive; old rows are non-billable) ----
            self._add_column_if_missing(
                conn, "customers", "credits_effective_at", "credits_effective_at REAL"
            )
            self._add_column_if_missing(
                conn, "events", "billable", "billable INTEGER NOT NULL DEFAULT 0"
            )
            self._add_column_if_missing(
                conn, "deliveries", "billable", "billable INTEGER NOT NULL DEFAULT 0"
            )
            self._add_column_if_missing(
                conn, "deliveries", "credit_stage", "credit_stage TEXT NOT NULL DEFAULT 'none'"
            )
            self._add_column_if_missing(conn, "deliveries", "credit_id", "credit_id TEXT")
            self._add_column_if_missing(
                conn, "deliveries", "reconciliation_reason", "reconciliation_reason TEXT"
            )
            self._add_column_if_missing(
                conn, "deliveries", "reconciliation_state", "reconciliation_state TEXT"
            )
            self._add_column_if_missing(
                conn, "deliveries", "reconciliation_note", "reconciliation_note TEXT"
            )
            self._add_column_if_missing(
                conn, "deliveries", "reconciliation_at", "reconciliation_at REAL"
            )
            # Created after the additive migration because the index references
            # a column that pre-existing ``events`` tables only gain above.
            conn.execute("CREATE INDEX IF NOT EXISTS ix_events_topic ON events(customer_id, topic_id)")
            conn.execute("INSERT OR IGNORE INTO counters(name, value) VALUES ('ordinal', 0)")
        self._ready = True

    @staticmethod
    def _add_column_if_missing(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
        if column in Store._column_names(conn, table):
            return
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")
        except sqlite3.OperationalError as exc:
            # A peer process can win the same additive migration race.
            if "duplicate column name" not in str(exc).lower():
                raise

    def _ensure_ready(self) -> None:
        if not self._ready:
            with self._init_lock:
                if not self._ready:
                    self.initialize()

    @staticmethod
    def _column_names(conn: sqlite3.Connection, table: str) -> set[str]:
        return {str(r["name"]) for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = _connect(self._path)
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        self._ensure_ready()
        conn = _connect(self._path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    @contextmanager
    def _read(self) -> Iterator[sqlite3.Connection]:
        self._ensure_ready()
        conn = _connect(self._path)
        try:
            yield conn
        finally:
            conn.close()

    @staticmethod
    def _next_ordinal(conn: sqlite3.Connection) -> int:
        conn.execute("UPDATE counters SET value = value + 1 WHERE name='ordinal'")
        row = conn.execute("SELECT value FROM counters WHERE name='ordinal'").fetchone()
        return int(row["value"])

    @staticmethod
    def _in_clause(values: Iterable[Any]) -> tuple[str, list[Any]]:
        items = list(values)
        return "(" + ",".join("?" for _ in items) + ")", items

    @staticmethod
    def _supply_predicate(now_param: str = "?") -> str:
        """SQL predicate: one more point could be allocated for delivery ``d``.

        Used by ``claim_due`` / ``next_wakeup`` so a customer that is out of
        points is not spun on (and does not crowd out other flows or
        customers).  A billable delivery that still needs a point is claimable
        only while the customer has a non-expired top-up with a free point;
        deliveries that already hold a point, or that are non-billable, are
        always claimable.
        """
        return (
            "(d.billable = 0"
            " OR d.credit_stage != 'needed'"
            " OR EXISTS ("
            "   SELECT 1 FROM credit_topups ct"
            "   WHERE ct.customer_id = d.customer_id"
            f"     AND ct.expires_at > {now_param}"
            "     AND ct.points - COALESCE(("
            "       SELECT SUM(ca.points) FROM credit_allocations ca"
            "       WHERE ca.topup_id = ct.id"
            "         AND ca.stage IN ('held','charged','expired')"
            "     ),0) > 0"
            " ))"
        )

    def _checkpoint_truncate(self) -> None:
        """Best-effort WAL truncation + compaction after content erasure.

        Checkpointing keeps erased values out of the ``-wal`` sidecar and a
        VACUUM rebuild drops freed pages that could otherwise retain old bytes
        at the normal-file level.  Everything here is best-effort: a busy
        peer skips compaction.  Page-level/forensic recovery and backups are
        explicitly out of scope.
        """
        try:
            with self._connect() as conn:
                conn.execute("PRAGMA busy_timeout=1000")
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                try:
                    conn.execute("VACUUM")
                    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                except sqlite3.Error:
                    pass
        except sqlite3.Error:
            pass

    # -------------------------------------------------------------- customers
    def create_customer(self, *, name: str, now: float) -> dict[str, Any]:
        name = require_text(name, "name")
        customer_id = new_id("cus")
        with self._write() as conn:
            conn.execute(
                "INSERT INTO customers(id, name, created_at) VALUES (?,?,?)",
                (customer_id, name, now),
            )
        return {"id": customer_id, "name": name, "created_at": iso_time(now)}

    def customer_exists(self, customer_id: str) -> bool:
        with self._read() as conn:
            return conn.execute("SELECT 1 FROM customers WHERE id=?", (customer_id,)).fetchone() is not None

    def _require_customer(self, conn: sqlite3.Connection, customer_id: str) -> None:
        if conn.execute("SELECT 1 FROM customers WHERE id=?", (customer_id,)).fetchone() is None:
            raise DomainError("unknown customer_id", status_code=404, code="customer_not_found")

    # ---------------------------------------------------------- subscriptions
    def create_subscription(self, *, customer_id: str, target_url: str, now: float) -> dict[str, Any]:
        if not self.customer_exists(customer_id):
            raise DomainError("unknown customer_id", status_code=404, code="customer_not_found")
        target_url = validate_target_url(target_url)
        subscription_id = new_id("sub")
        with self._write() as conn:
            ordinal = self._next_ordinal(conn)
            conn.execute(
                """INSERT INTO subscriptions(id, customer_id, target_url, target_version, ordinal,
                                             created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?)""",
                (subscription_id, customer_id, target_url, 1, ordinal, now, now),
            )
            # Close the race where an event transaction committed just after
            # this subscription's clock reading but before this insert.
            # Withdrawn events are never fanned out to a new subscription.
            rows = conn.execute(
                """SELECT id, flow_id, seq, billable FROM events
                   WHERE customer_id=? AND ordinal > ? AND status != 'withdrawn'
                   ORDER BY ordinal""",
                (customer_id, ordinal),
            ).fetchall()
            for row in rows:
                self._insert_delivery(
                    conn,
                    event_id=row["id"],
                    subscription_id=subscription_id,
                    customer_id=customer_id,
                    flow_id=row["flow_id"],
                    seq=int(row["seq"]),
                    target_url=target_url,
                    target_version=1,
                    now=now,
                    billable=bool(row["billable"]),
                )
        return self.get_subscription(subscription_id)

    def get_subscription(self, subscription_id: str) -> dict[str, Any]:
        with self._read() as conn:
            row = conn.execute("SELECT * FROM subscriptions WHERE id=?", (subscription_id,)).fetchone()
        if row is None:
            raise DomainError("subscription not found", status_code=404, code="subscription_not_found")
        return {
            "id": row["id"],
            "customer_id": row["customer_id"],
            "target_url": row["target_url"],
            "target_version": row["target_version"],
            "ordinal": row["ordinal"],
            "created_at": iso_time(row["created_at"]),
            "updated_at": iso_time(row["updated_at"]),
        }

    def update_subscription_target(
        self,
        subscription_id: str,
        *,
        target_url: str,
        now: float,
        expected_version: int | None = None,
    ) -> dict[str, Any]:
        target_url = validate_target_url(target_url)
        with self._write() as conn:
            row = conn.execute("SELECT * FROM subscriptions WHERE id=?", (subscription_id,)).fetchone()
            if row is None:
                raise DomainError("subscription not found", status_code=404, code="subscription_not_found")
            if expected_version is not None and row["target_version"] != expected_version:
                raise DomainError(
                    f"target_version changed (expected {expected_version}, current {row['target_version']})",
                    status_code=409,
                    code="target_version_conflict",
                )
            new_version = int(row["target_version"]) + 1
            conn.execute(
                "UPDATE subscriptions SET target_url=?, target_version=?, updated_at=? WHERE id=?",
                (target_url, new_version, now, subscription_id),
            )
            # Not-yet-succeeded work automatically moves to the new address the
            # next time it is claimed.  In-flight rows intentionally keep the
            # old address snapshot so their late replies can be recognised as
            # stale and discarded.  needs_attention rows stay parked; an
            # administrator must still retry them.  Succeeded and withdrawn
            # rows are never touched (withdrawn work is never revived by an
            # address change).
            conn.execute(
                """UPDATE deliveries
                   SET target_url=?, target_version=?, updated_at=?
                   WHERE subscription_id=? AND status IN ('pending','retry_wait','needs_attention')""",
                (target_url, new_version, now, subscription_id),
            )
        return self.get_subscription(subscription_id)

    # ----------------------------------------------------------------- events
    def ingest_event(
        self,
        *,
        customer_id: str,
        flow_id: str,
        caller_event_id: str,
        payload: object,
        now: float,
        topic_id: str | None = None,
    ) -> dict[str, Any]:
        if not self.customer_exists(customer_id):
            raise DomainError("unknown customer_id", status_code=404, code="customer_not_found")
        flow_id = require_text(flow_id, "flow_id")
        caller_event_id = require_text(caller_event_id, "caller_event_id")
        if topic_id is not None:
            topic_id = require_text(topic_id, "topic_id")
        try:
            canonical = canonical_json(payload)
        except (TypeError, ValueError) as exc:
            raise DomainError(f"payload must be valid JSON: {exc}", status_code=422, code="invalid_payload") from exc
        digest = content_digest(canonical)

        with self._write() as conn:
            # Billable only once the customer's first top-up has taken effect.
            # The top-up registration writes ``credits_effective_at`` inside a
            # BEGIN IMMEDIATE transaction, so this read is serialised against it.
            cust = conn.execute(
                "SELECT credits_effective_at FROM customers WHERE id=?", (customer_id,)
            ).fetchone()
            billable = cust is not None and cust["credits_effective_at"] is not None
            topic_withdrawn = False
            if topic_id is not None:
                # Insert the implicit active topic first; the conflict is a
                # no-op and the row read below decides admission.  Doing this
                # inside BEGIN IMMEDIATE serialises against withdrawal.
                conn.execute(
                    """INSERT INTO topics(customer_id, topic_id, status, created_at)
                       VALUES (?, ?, 'active', ?)
                       ON CONFLICT(customer_id, topic_id) DO NOTHING""",
                    (customer_id, topic_id, now),
                )
                topic = conn.execute(
                    "SELECT status FROM topics WHERE customer_id=? AND topic_id=?",
                    (customer_id, topic_id),
                ).fetchone()
                topic_withdrawn = topic is not None and topic["status"] == "withdrawn"

            existing = conn.execute(
                """SELECT * FROM events
                   WHERE customer_id=? AND flow_id=? AND caller_event_id=?""",
                (customer_id, flow_id, caller_event_id),
            ).fetchone()
            if existing is not None:
                if existing["status"] == "withdrawn":
                    # Tombstoned key: identical resend cannot restore content
                    # or deliveries and different content cannot reuse the key.
                    raise DomainError(
                        "this event was withdrawn; its key cannot be restored by resending or reused",
                        status_code=409,
                        code="event_withdrawn",
                    )
                if existing["content_hash"] != digest:
                    raise DomainError(
                        "caller_event_id was already used with different content",
                        status_code=409,
                        code="caller_event_conflict",
                    )
                return self._event_result(conn, existing, replayed=True)

            if topic_withdrawn:
                # A genuinely new key under a withdrawn topic is refused
                # before any sequence/ordinal allocation or fan-out.
                raise DomainError(
                    f"topic {topic_id!r} has been withdrawn; new events for it are rejected",
                    status_code=409,
                    code="topic_withdrawn",
                )

            ordinal = self._next_ordinal(conn)
            seq_row = conn.execute(
                "SELECT COALESCE(MAX(seq), 0) AS m FROM events WHERE customer_id=? AND flow_id=?",
                (customer_id, flow_id),
            ).fetchone()
            seq = int(seq_row["m"]) + 1
            event_id = new_id("evt")
            conn.execute(
                """INSERT INTO events(id, customer_id, flow_id, caller_event_id, topic_id, status,
                                      billable, payload_json, content_hash, seq, ordinal, created_at)
                   VALUES (?,?,?,?,?, 'active', ?,?,?,?,?,?)""",
                (
                    event_id,
                    customer_id,
                    flow_id,
                    caller_event_id,
                    topic_id,
                    1 if billable else 0,
                    canonical,
                    digest,
                    seq,
                    ordinal,
                    now,
                ),
            )
            subscriptions = conn.execute(
                "SELECT id, target_url, target_version FROM subscriptions WHERE customer_id=? AND ordinal < ?",
                (customer_id, ordinal),
            ).fetchall()
            for sub in subscriptions:
                self._insert_delivery(
                    conn,
                    event_id=event_id,
                    subscription_id=sub["id"],
                    customer_id=customer_id,
                    flow_id=flow_id,
                    seq=seq,
                    target_url=sub["target_url"],
                    target_version=int(sub["target_version"]),
                    now=now,
                    billable=billable,
                )
            stored = conn.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
            return self._event_result(conn, stored, replayed=False)

    @staticmethod
    def _is_withdrawn_row(row: sqlite3.Row) -> bool:
        try:
            return row["status"] == "withdrawn"
        except (IndexError, KeyError):
            return row["content_hash"] == ERASED_HASH and row["payload_json"] == ERASED_PAYLOAD

    def _event_json(self, row: sqlite3.Row, *, withdrawn_at: float | None = None) -> dict[str, Any]:
        withdrawn = self._is_withdrawn_row(row)
        base = {
            "id": row["id"],
            "customer_id": row["customer_id"],
            "flow_id": row["flow_id"],
            "caller_event_id": row["caller_event_id"],
            "topic_id": row["topic_id"] if "topic_id" in row.keys() else None,
            "seq": row["seq"],
            "created_at": iso_time(row["created_at"]),
            "status": "withdrawn" if withdrawn else row["status"],
            "withdrawn": withdrawn,
            "revoked": withdrawn,
        }
        if withdrawn:
            # No original JSON and no digest leave the read model.
            base["payload"] = None
            base["content_hash"] = None
            base["withdrawn_at"] = iso_time(withdrawn_at) if withdrawn_at is not None else None
        else:
            base["payload"] = json.loads(row["payload_json"])
            base["content_hash"] = row["content_hash"]
        return base

    def _event_result(self, conn: sqlite3.Connection, row: sqlite3.Row, *, replayed: bool) -> dict[str, Any]:
        deliveries = conn.execute(
            "SELECT id, subscription_id, status FROM deliveries WHERE event_id=? ORDER BY subscription_id",
            (row["id"],),
        ).fetchall()
        result = self._event_json(row)
        result["replayed"] = replayed
        result["deliveries"] = [
            {"delivery_id": d["id"], "subscription_id": d["subscription_id"], "status": d["status"]}
            for d in deliveries
        ]
        return result

    def get_event(self, event_id: str) -> dict[str, Any]:
        with self._read() as conn:
            row = conn.execute(
                """SELECT e.*, t.withdrawn_at AS topic_withdrawn_at
                   FROM events e LEFT JOIN topics t
                     ON t.customer_id = e.customer_id AND t.topic_id = e.topic_id
                   WHERE e.id=?""",
                (event_id,),
            ).fetchone()
            if row is None:
                raise DomainError("event not found", status_code=404, code="event_not_found")
            result = self._event_json(row, withdrawn_at=row["topic_withdrawn_at"])
        return result

    def _insert_delivery(
        self,
        conn: sqlite3.Connection,
        *,
        event_id: str,
        subscription_id: str,
        customer_id: str,
        flow_id: str,
        seq: int,
        target_url: str,
        target_version: int,
        now: float,
        billable: bool = False,
    ) -> None:
        prev = conn.execute(
            """SELECT id, status FROM deliveries
               WHERE subscription_id=? AND flow_id=? AND seq < ?
               ORDER BY seq DESC LIMIT 1""",
            (subscription_id, flow_id, seq),
        ).fetchone()
        blocked_by = None
        if prev is not None and prev["status"] not in TERMINAL_STATUSES:
            blocked_by = prev["id"]
        credit_stage = "needed" if billable else "none"
        conn.execute(
            """INSERT INTO deliveries(id, event_id, subscription_id, customer_id, flow_id, seq,
                                      target_url, target_version, status, billable, credit_stage,
                                      attempts_count,
                                      next_attempt_at, blocked_by, block_reason,
                                      created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?, 'pending', ?, ?, 0, NULL, ?, ?, ?, ?)""",
            (
                new_id("dlv"),
                event_id,
                subscription_id,
                customer_id,
                flow_id,
                seq,
                target_url,
                target_version,
                1 if billable else 0,
                credit_stage,
                blocked_by,
                "predecessor_incomplete" if blocked_by else None,
                now,
                now,
            ),
        )

    # ------------------------------------------------------- topic withdrawal
    def withdraw_topic(
        self,
        customer_id: str | None = None,
        topic_id: str | None = None,
        now: float | None = None,
        *,
        wait_for_inflight: bool = False,
        inflight_timeout: float = 5.0,
        inflight_poll: float = 0.02,
    ) -> dict[str, Any]:
        """Irreversibly withdraw every event of one customer-scoped topic.

        Repeat calls are idempotent and report ``already_withdrawn=True``.
        After the committing transaction:

        * event content/digests are erased and the event rows are tombstones;
        * non-confirmed deliveries become ``withdrawn`` (no retry/revival and
          they no longer block their flow);
        * diagnostic free text that could echo payload content is cleared;
        * future topic events are rejected by :meth:`ingest_event`.

        When ``wait_for_inflight`` is true the call additionally waits (outside
        the write transaction) for attempts that had already passed the send
        gate to record their fenced result.  Other topics are never waited on.
        """
        if now is None:
            now = time.time()
        topic_id = require_text(topic_id, "topic_id")
        if not self.customer_exists(customer_id):
            raise DomainError("unknown customer_id", status_code=404, code="customer_not_found")

        with self._write() as conn:
            topic = conn.execute(
                "SELECT * FROM topics WHERE customer_id=? AND topic_id=?",
                (customer_id, topic_id),
            ).fetchone()
            already_withdrawn = topic is not None and topic["status"] == "withdrawn"
            if topic is None:
                conn.execute(
                    "INSERT INTO topics(customer_id, topic_id, status, created_at, withdrawn_at) VALUES (?,?, 'withdrawn', ?, ?)",
                    (customer_id, topic_id, now, now),
                )
            elif not already_withdrawn:
                conn.execute(
                    "UPDATE topics SET status='withdrawn', withdrawn_at=? WHERE customer_id=? AND topic_id=?",
                    (now, customer_id, topic_id),
                )

            event_rows = conn.execute(
                "SELECT id FROM events WHERE customer_id=? AND topic_id=?",
                (customer_id, topic_id),
            ).fetchall()
            event_ids = [r["id"] for r in event_rows]

            newly_terminated = 0
            inflight_marked = 0
            if event_ids:
                in_sql, params = self._in_clause(event_ids)

                # Erase content + digest, tombstone the event rows.
                conn.execute(
                    f"UPDATE events SET payload_json=?, content_hash=?, status='withdrawn' WHERE id IN {in_sql}",
                    [ERASED_PAYLOAD, ERASED_HASH, *params],
                )

                # Terminate every delivery not already confirmed in a terminal
                # success/skip state.  Lease fields are intentionally left in
                # place: finish_attempt fences on status before the token.
                terminating = [
                    r["id"]
                    for r in conn.execute(
                        f"""SELECT id FROM deliveries
                            WHERE event_id IN {in_sql}
                              AND status NOT IN ('succeeded','administratively_skipped','withdrawn')""",
                        params,
                    ).fetchall()
                ]
                cur = conn.execute(
                    f"""UPDATE deliveries
                        SET status='withdrawn', next_attempt_at=NULL,
                            blocked_by=NULL, block_reason=?, updated_at=?,
                            completed_at=COALESCE(completed_at, ?)
                        WHERE event_id IN {in_sql}
                          AND status NOT IN ('succeeded','administratively_skipped','withdrawn')""",
                    [WITHDRAWN_REASON, now, now, *params],
                )
                newly_terminated = cur.rowcount or 0

                # Claims that never passed the send gate must not send: close
                # their in-flight attempt rows now.  Attempts with
                # send_started_at set correspond to requests already handed to
                # the HTTP sender; leave those for the fenced finish path.
                # (The credit settlement below runs *after* this, so an attempt
                # that never left the gate counts as "no request in flight".)
                conn.execute(
                    f"""UPDATE attempts
                        SET outcome='withdrawn', finished_at=?, error=NULL
                        WHERE outcome='in_flight' AND send_started_at IS NULL
                          AND delivery_id IN (SELECT id FROM deliveries WHERE event_id IN {in_sql})""",
                    [now, *params],
                )

                # Prepaid points held by terminated deliveries: return the
                # point when no request could have reached the receiver, or
                # park it for reconciliation when one may have.  Settled after
                # the attempt close above so unsent claims are recognisably
                # unresolved-free.  Charged points are never touched.
                credit_reconciled = 0
                credit_released = 0
                credit_expired = 0
                for dlv_id in terminating:
                    stage = self._settle_stopped_delivery(conn, delivery_id=dlv_id, now=now)
                    if stage == "reconciliation":
                        credit_reconciled += 1
                    elif stage == "returned":
                        credit_released += 1
                    elif stage == "expired":
                        credit_expired += 1

                # Scrub upstream free text everywhere it may have echoed the
                # payload (receiver error bodies, late-result observations).
                conn.execute(
                    f"UPDATE deliveries SET last_error=NULL WHERE event_id IN {in_sql} AND last_error IS NOT NULL",
                    params,
                )
                conn.execute(
                    f"""UPDATE attempts SET error=?
                        WHERE error IS NOT NULL
                          AND delivery_id IN (SELECT id FROM deliveries WHERE event_id IN {in_sql})""",
                    [REDACTED_TEXT, *params],
                )
                conn.execute(
                    f"""UPDATE late_results SET error=?
                        WHERE error IS NOT NULL
                          AND delivery_id IN (SELECT id FROM deliveries WHERE event_id IN {in_sql})""",
                    [REDACTED_TEXT, *params],
                )

                # Release any same-flow successors that were blocked behind a
                # delivery we just terminated.
                terms = conn.execute(
                    f"SELECT subscription_id, flow_id, seq FROM deliveries WHERE event_id IN {in_sql} AND status='withdrawn'",
                    params,
                ).fetchall()
                for term in terms:
                    self._unblock_successors(
                        conn, term["subscription_id"], term["flow_id"], int(term["seq"]), now
                    )

                inflight_marked = int(
                    conn.execute(
                        f"""SELECT COUNT(*) AS c FROM attempts a
                            JOIN deliveries d ON d.id = a.delivery_id
                            WHERE d.event_id IN {in_sql}
                              AND a.outcome='in_flight'
                              AND a.send_started_at IS NOT NULL""",
                        params,
                    ).fetchone()["c"]
                )

                totals = conn.execute(
                    f"""SELECT
                          SUM(CASE WHEN status='succeeded' THEN 1 ELSE 0 END) AS ok,
                          SUM(CASE WHEN status='administratively_skipped' THEN 1 ELSE 0 END) AS skipped,
                          SUM(CASE WHEN status='withdrawn' THEN 1 ELSE 0 END) AS withdrawn,
                          COUNT(*) AS total
                        FROM deliveries WHERE event_id IN {in_sql}""",
                    params,
                ).fetchone()
            else:
                totals = None
                credit_reconciled = 0
                credit_released = 0
                credit_expired = 0

            held: sqlite3.Row | dict[str, int]
            if event_ids:
                held = conn.execute(
                    f"""SELECT COUNT(*) AS c FROM ticket_evidence te
                        JOIN tickets t ON t.id = te.ticket_id
                        WHERE t.status='open' AND te.payload_json IS NOT NULL
                          AND te.event_id IN {in_sql}""",
                    params,
                ).fetchone()
            else:
                held = {"c": 0}

        if wait_for_inflight and inflight_marked:
            self._wait_topic_inflight(
                customer_id=customer_id,
                topic_id=topic_id,
                timeout=inflight_timeout,
                poll=inflight_poll,
            )

        self._checkpoint_truncate()
        result = self.get_topic(customer_id=customer_id, topic_id=topic_id)
        result.update(
            {
                "replayed": already_withdrawn,
                "already_withdrawn": already_withdrawn,
                "events_affected": len(event_ids),
                "withdrawn_event_count": len(event_ids),
                "event_ids": list(event_ids),
                "deliveries_terminated": newly_terminated,
                "terminated": newly_terminated,
                "deliveries_succeeded": int(totals["ok"] or 0) if totals else 0,
                "deliveries_skipped": int(totals["skipped"] or 0) if totals else 0,
                "deliveries_withdrawn": int(totals["withdrawn"] or 0) if totals else 0,
                "deliveries_total": int(totals["total"] or 0) if totals else 0,
                "credits_reconciled": credit_reconciled,
                "credits_returned": credit_released,
                "credits_expired": credit_expired,
                "open_holds_at_withdrawal": int(held["c"] or 0),
                "waited_for_inflight": bool(wait_for_inflight and inflight_marked),
            }
        )
        return result

    def _wait_topic_inflight(self, *, customer_id: str, topic_id: str, timeout: float, poll: float) -> None:
        deadline = time.monotonic() + max(0.0, timeout)
        while time.monotonic() < deadline:
            if self.count_topic_inflight(customer_id=customer_id, topic_id=topic_id) == 0:
                return
            time.sleep(max(0.001, poll))

    def count_topic_inflight(self, *, customer_id: str, topic_id: str) -> int:
        with self._read() as conn:
            row = conn.execute(
                """SELECT COUNT(*) AS c FROM attempts a
                   JOIN deliveries d ON d.id = a.delivery_id
                   JOIN events e ON e.id = d.event_id
                   WHERE e.customer_id=? AND e.topic_id=?
                     AND a.outcome='in_flight'
                     AND a.send_started_at IS NOT NULL""",
                (customer_id, topic_id),
            ).fetchone()
            return int(row["c"])

    def get_topic(self, *, customer_id: str, topic_id: str) -> dict[str, Any]:
        with self._read() as conn:
            row = conn.execute(
                "SELECT * FROM topics WHERE customer_id=? AND topic_id=?",
                (customer_id, topic_id),
            ).fetchone()
            if row is None:
                raise DomainError("topic not found", status_code=404, code="topic_not_found")
            events = conn.execute(
                "SELECT COUNT(*) AS c FROM events WHERE customer_id=? AND topic_id=?",
                (customer_id, topic_id),
            ).fetchone()["c"]
        return {
            "customer_id": row["customer_id"],
            "topic_id": row["topic_id"],
            "status": row["status"],
            "withdrawn": row["status"] == "withdrawn",
            "revoked": row["status"] == "withdrawn",
            "created_at": iso_time(row["created_at"]),
            "withdrawn_at": iso_time(row["withdrawn_at"]),
            "events": int(events),
        }

    def list_topics(self, *, customer_id: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if customer_id:
            clauses.append("customer_id=?")
            params.append(customer_id)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        with self._read() as conn:
            rows = conn.execute(f"SELECT * FROM topics{where} ORDER BY created_at DESC LIMIT ?", params).fetchall()
        return [
            {
                "customer_id": r["customer_id"],
                "topic_id": r["topic_id"],
                "status": r["status"],
                "withdrawn": r["status"] == "withdrawn",
                "created_at": iso_time(r["created_at"]),
                "withdrawn_at": iso_time(r["withdrawn_at"]),
            }
            for r in rows
        ]

    # --------------------------------------------------------------- tickets
    def create_ticket(
        self,
        customer_id: str | None = None,
        now: float | None = None,
        *,
        title: str | None = None,
        reason: str | None = None,
    ) -> dict[str, Any]:
        if now is None:
            now = time.time()
        if not self.customer_exists(customer_id):
            raise DomainError("unknown customer_id", status_code=404, code="customer_not_found")
        if title is not None:
            title = (title or "").strip()[:300] or None
        if reason is not None:
            reason = (reason or "").strip()[:2000] or None
        ticket_id = new_id("tkt")
        with self._write() as conn:
            conn.execute(
                "INSERT INTO tickets(id, customer_id, title, reason, status, created_at) VALUES (?,?,?,?, 'open', ?)",
                (ticket_id, customer_id, title, reason, now),
            )
        return self.get_ticket(ticket_id)

    def _load_ticket_for_owner(
        self, conn: sqlite3.Connection, ticket_id: str, customer_id: str | None
    ) -> sqlite3.Row:
        row = conn.execute("SELECT * FROM tickets WHERE id=?", (ticket_id,)).fetchone()
        if row is None:
            raise DomainError("ticket not found", status_code=404, code="ticket_not_found")
        if customer_id is not None and customer_id != row["customer_id"]:
            raise DomainError("customer_id does not own this ticket", status_code=403, code="forbidden")
        return row

    def get_ticket(self, ticket_id: str, customer_id: str | None = None) -> dict[str, Any]:
        with self._read() as conn:
            row = self._load_ticket_for_owner(conn, ticket_id, customer_id)
            evidence = conn.execute(
                """SELECT te.*, e.flow_id AS flow_id, e.seq AS seq,
                          e.caller_event_id AS caller_event_id, e.topic_id AS topic_id,
                          e.created_at AS event_created_at
                   FROM ticket_evidence te LEFT JOIN events e ON e.id = te.event_id
                   WHERE te.ticket_id=? ORDER BY te.retained_at, te.id""",
                (ticket_id,),
            ).fetchall()
            open_count = conn.execute(
                "SELECT COUNT(*) AS c FROM ticket_evidence WHERE ticket_id=? AND payload_json IS NOT NULL",
                (ticket_id,),
            ).fetchone()["c"]
            is_closed = row["status"] == "closed"
            events = [self._evidence_json(r, include_content=not is_closed) for r in evidence]
        return {
            "id": row["id"],
            "ticket_id": row["id"],
            "case_id": row["id"],
            "customer_id": row["customer_id"],
            "title": row["title"],
            "reason": row["reason"],
            "status": row["status"],
            "closed": is_closed,
            "created_at": iso_time(row["created_at"]),
            "closed_at": iso_time(row["closed_at"]),
            "event_ids": [r["event_id"] for r in evidence],
            "retained_event_ids": [r["event_id"] for r in evidence],
            "evidence_count": len(evidence),
            "retained_count": int(open_count),
            "events": events,
            "evidence": events,
        }

    def list_tickets(self, *, customer_id: str, limit: int = 200) -> list[dict[str, Any]]:
        if not self.customer_exists(customer_id):
            raise DomainError("unknown customer_id", status_code=404, code="customer_not_found")
        with self._read() as conn:
            rows = conn.execute(
                "SELECT id FROM tickets WHERE customer_id=? ORDER BY created_at DESC LIMIT ?",
                (customer_id, limit),
            ).fetchall()
        return [self.get_ticket(r["id"]) for r in rows]

    def retain_events(
        self,
        ticket_id: str,
        event_ids: str | Iterable[str] | None = None,
        now: float | None = None,
        *,
        customer_id: str | None = None,
        **extra_event_ids: Any,
    ) -> dict[str, Any]:
        """Copy the content of received, non-withdrawn events into a ticket.

        All ids are validated inside one write transaction before anything is
        inserted, so an invalid id rejects the whole request.  Re-retaining an
        event already on the ticket is an idempotent no-op.
        """
        if now is None:
            now = time.time()
        if event_ids is None:
            # Allow retain_events(ticket, event_id="evt_…") as well.
            singular = extra_event_ids.get("event_id")
            event_ids = [singular] if singular is not None else []
        if isinstance(event_ids, str):
            event_ids = [event_ids]
        ids: list[str] = []
        for raw in event_ids:
            eid = require_text(raw, "event_id")
            if eid not in ids:
                ids.append(eid)
        if not ids:
            raise DomainError("at least one event_id is required", status_code=422, code="invalid_request")

        with self._write() as conn:
            ticket = self._load_ticket_for_owner(conn, ticket_id, customer_id)
            if ticket["status"] == "closed":
                raise DomainError(
                    "ticket is closed; evidence can no longer be added",
                    status_code=409,
                    code="ticket_closed",
                )
            for event_id in ids:
                event = conn.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
                # Scoped lookup: do not reveal other customers' event ids.
                if event is None or event["customer_id"] != ticket["customer_id"]:
                    raise DomainError(
                        f"event {event_id!r} was not found for this customer",
                        status_code=404,
                        code="event_not_found",
                    )
                if self._is_withdrawn_row(event):
                    raise DomainError(
                        f"event {event_id!r} has already been withdrawn and cannot be retained",
                        status_code=409,
                        code="event_withdrawn",
                    )
                already = conn.execute(
                    "SELECT 1 FROM ticket_evidence WHERE ticket_id=? AND event_id=?",
                    (ticket_id, event_id),
                ).fetchone()
                if already is not None:
                    continue
                conn.execute(
                    """INSERT INTO ticket_evidence(id, ticket_id, event_id, customer_id,
                                                   payload_json, content_hash, retained_at)
                       VALUES (?,?,?,?,?,?,?)""",
                    (
                        new_id("tev"),
                        ticket_id,
                        event_id,
                        ticket["customer_id"],
                        event["payload_json"],
                        event["content_hash"],
                        now,
                    ),
                )
        return self.get_ticket(ticket_id, customer_id=customer_id)

    def list_ticket_events(self, ticket_id: str, customer_id: str | None = None) -> list[dict[str, Any]]:
        with self._read() as conn:
            ticket = self._load_ticket_for_owner(conn, ticket_id, customer_id)
            if ticket["status"] == "closed":
                raise DomainError(
                    "ticket is closed; its evidence can no longer be read",
                    status_code=409,
                    code="ticket_closed",
                )
            rows = conn.execute(
                """SELECT te.*, e.flow_id AS flow_id, e.seq AS seq,
                          e.caller_event_id AS caller_event_id, e.topic_id AS topic_id,
                          e.created_at AS event_created_at
                   FROM ticket_evidence te LEFT JOIN events e ON e.id = te.event_id
                   WHERE te.ticket_id=? ORDER BY te.retained_at, te.id""",
                (ticket_id,),
            ).fetchall()
            return [self._evidence_json(r, include_content=True) for r in rows]

    @staticmethod
    def _evidence_json(row: sqlite3.Row, *, include_content: bool) -> dict[str, Any]:
        released = row["payload_json"] is None
        keys = row.keys()
        available = include_content and not released
        payload = json.loads(row["payload_json"]) if available else None
        item = {
            "evidence_id": row["id"],
            "id": row["id"],
            "ticket_id": row["ticket_id"],
            "event_id": row["event_id"],
            "flow_id": row["flow_id"] if "flow_id" in keys else None,
            "seq": row["seq"] if "seq" in keys else None,
            "caller_event_id": row["caller_event_id"] if "caller_event_id" in keys else None,
            "topic_id": row["topic_id"] if "topic_id" in keys else None,
            "event_created_at": iso_time(row["event_created_at"]) if "event_created_at" in keys else None,
            "retained_at": iso_time(row["retained_at"]),
            "released_at": iso_time(row["released_at"]) if "released_at" in keys else None,
            "available": available,
            # ``content`` mirrors ``payload`` for callers that spell it that way.
            "payload": payload,
            "content": payload,
            "content_hash": row["content_hash"] if available else None,
        }
        if not available:
            item["redacted"] = True
        # Nested event view for callers that expect the retained event under
        # an ``event`` key.
        item["event"] = {
            "id": row["event_id"],
            "event_id": row["event_id"],
            "flow_id": item["flow_id"],
            "seq": item["seq"],
            "caller_event_id": item["caller_event_id"],
            "topic_id": item["topic_id"],
            "created_at": item["event_created_at"],
            "payload": payload,
            "content_hash": item["content_hash"],
            "withdrawn": not available,
        }
        return item

    def close_ticket(
        self, ticket_id: str, now: float | None = None, customer_id: str | None = None
    ) -> dict[str, Any]:
        """Close a ticket and purge its private evidence copies.

        Copies belonging to other tickets are independent and untouched.
        Closing an already-closed ticket is an idempotent no-op.
        """
        if now is None:
            now = time.time()
        with self._write() as conn:
            ticket = self._load_ticket_for_owner(conn, ticket_id, customer_id)
            if ticket["status"] != "closed":
                conn.execute(
                    "UPDATE tickets SET status='closed', closed_at=? WHERE id=?",
                    (now, ticket_id),
                )
                conn.execute(
                    """UPDATE ticket_evidence
                       SET payload_json=NULL, content_hash=NULL, released_at=?
                       WHERE ticket_id=? AND payload_json IS NOT NULL""",
                    (now, ticket_id),
                )
        self._checkpoint_truncate()
        return self.get_ticket(ticket_id, customer_id=customer_id)

    # ------------------------------------------------------- prepaid credits
    def register_topup(
        self,
        customer_id: str | None = None,
        *,
        topup_ref: str | None = None,
        points: int | None = None,
        expires_at: float | None = None,
        now: float | None = None,
    ) -> dict[str, Any]:
        """Register one prepaid credit top-up for a customer.

        ``topup_ref`` is unique within the customer.  Re-submitting the exact
        same ``(topup_ref, points, expires_at)`` is idempotent (``replayed``);
        reusing the reference with a different point count or expiry is
        rejected.  The first successful top-up makes the customer's later
        events billable; events accepted earlier stay non-billable.
        """
        if now is None:
            now = time.time()
        topup_ref = require_text(topup_ref, "topup_ref")
        if not isinstance(points, int) or isinstance(points, bool) or points <= 0:
            raise DomainError(
                "points must be a positive integer", status_code=422, code="invalid_points"
            )
        if expires_at is None:
            raise DomainError(
                "expires_at is required", status_code=422, code="invalid_expires_at"
            )
        expires_at = float(expires_at)

        with self._write() as conn:
            self._require_customer(conn, customer_id)
            existing = conn.execute(
                "SELECT * FROM credit_topups WHERE customer_id=? AND topup_ref=?",
                (customer_id, topup_ref),
            ).fetchone()
            replayed = False
            if existing is not None:
                if int(existing["points"]) != points or float(existing["expires_at"]) != expires_at:
                    raise DomainError(
                        "topup_ref was already used with different points or expires_at",
                        status_code=409,
                        code="topup_conflict",
                    )
                replayed = True
                topup_id = existing["id"]
            else:
                topup_id = new_id("top")
                conn.execute(
                    """INSERT INTO credit_topups(id, customer_id, topup_ref, points, expires_at, created_at)
                       VALUES (?,?,?,?,?,?)""",
                    (topup_id, customer_id, topup_ref, points, expires_at, now),
                )
                # First top-up for this customer: events from now on are
                # billable.  Later top-ups do not move this boundary.
                conn.execute(
                    """UPDATE customers SET credits_effective_at=COALESCE(credits_effective_at, ?)
                       WHERE id=?""",
                    (now, customer_id),
                )
        return self.get_topup(customer_id, topup_ref, replayed=replayed)

    def get_topup(self, customer_id: str, topup_ref: str, *, replayed: bool = False) -> dict[str, Any]:
        with self._read() as conn:
            row = conn.execute(
                "SELECT * FROM credit_topups WHERE customer_id=? AND topup_ref=?",
                (customer_id, topup_ref),
            ).fetchone()
            if row is None:
                raise DomainError("topup not found", status_code=404, code="topup_not_found")
            return self._topup_json(conn, row, now=time.time(), replayed=replayed)

    def _topup_json(
        self, conn: sqlite3.Connection, row: sqlite3.Row, *, now: float, replayed: bool = False
    ) -> dict[str, Any]:
        expires_at = float(row["expires_at"])
        expired_now = expires_at <= now
        held = int(
            conn.execute(
                """SELECT COALESCE(SUM(a.points),0) AS c FROM credit_allocations a
                   JOIN credit_topups t ON t.id=a.topup_id
                   WHERE t.id=? AND a.stage='held'""",
                (row["id"],),
            ).fetchone()["c"]
        )
        charged = int(
            conn.execute(
                """SELECT COALESCE(SUM(a.points),0) AS c FROM credit_allocations a
                   WHERE a.topup_id=? AND a.stage='charged'""",
                (row["id"],),
            ).fetchone()["c"]
        )
        row = conn.execute(
            """SELECT COALESCE(SUM(a.points),0) AS c FROM credit_allocations a
               WHERE a.topup_id=? AND a.stage='returned'""",
            (row["id"],),
        ).fetchone()
        returned = int(row["c"])
        expired = int(
            conn.execute(
                """SELECT COALESCE(SUM(a.points),0) AS c FROM credit_allocations a
                   WHERE a.topup_id=? AND a.stage='expired'""",
                (row["id"],),
            ).fetchone()["c"]
        )
        if expired_now:
            # A held point on a past-expiry top-up can no longer be spent, but
            # it stays reserved for its in-flight delivery_id until that
            # delivery settles.  Everything not held or charged is expired.
            unallocated = max(int(row["points"]) - held - charged, 0)
            available = 0
        else:
            # ``returned`` points are back in the balance and reusable.
            unallocated = 0
            available = max(int(row["points"]) - held - charged - expired, 0)
        return {
            "topup_id": row["id"],
            "customer_id": row["customer_id"],
            "topup_ref": row["topup_ref"],
            "points": int(row["points"]),
            "expires_at": iso_time(expires_at),
            "created_at": iso_time(row["created_at"]),
            "expired": expired_now,
            "available": available,
            "held": held,
            "charged": charged,
            "returned": returned,
            "expired_points": expired + (unallocated if expired_now else 0),
            "replayed": replayed,
        }

    def list_topups(self, *, customer_id: str, now: float | None = None) -> list[dict[str, Any]]:
        if not self.customer_exists(customer_id):
            raise DomainError("unknown customer_id", status_code=404, code="customer_not_found")
        if now is None:
            now = time.time()
        with self._read() as conn:
            rows = conn.execute(
                "SELECT * FROM credit_topups WHERE customer_id=? ORDER BY created_at, topup_ref",
                (customer_id,),
            ).fetchall()
            return [self._topup_json(conn, r, now=now) for r in rows]

    def _expire_allocations(self, conn: sqlite3.Connection, *, now: float) -> None:
        """Move points released after their top-up expired into ``expired``.

        Only allocations still in the ``held`` stage whose top-up has passed
        its expiry can be affected: an already-``held`` point stays valid for
        its delivery_id even across expiry, so it is not touched here.  This
        runs when a hold is released (skip/withdraw), which is the only moment
        a point can be "returned for later use".
        """
        # Nothing to do here by design; returning is handled in _release_hold,
        # which decides between 'returned' and 'expired' for this allocation.
        return None

    def _allocate_point(
        self, conn: sqlite3.Connection, *, delivery_id: str, customer_id: str, now: float
    ) -> bool:
        """Reserve one point for this delivery from the earliest-expiring,
        then lowest-topup_ref, non-expired top-up with an available balance.

        Writes one ``credit_allocations`` row and links it to the delivery.
        Returns ``False`` (allocating nothing) when no point is available, in
        which case the caller must not send.
        """
        row = conn.execute(
            """SELECT t.id
               FROM credit_topups t
               WHERE t.customer_id=? AND t.expires_at > ?
                 AND (t.points
                      - COALESCE((SELECT SUM(a2.points) FROM credit_allocations a2
                                  WHERE a2.topup_id=t.id
                                    AND a2.stage IN ('held','charged','expired')),0)) > 0
               ORDER BY t.expires_at ASC, t.topup_ref ASC
               LIMIT 1""",
            (customer_id, now),
        ).fetchone()
        if row is None:
            return False
        alloc_id = new_id("alc")
        conn.execute(
            """INSERT INTO credit_allocations(id, delivery_id, customer_id, topup_id, points,
                                              stage, created_at)
               VALUES (?,?,?,?,1, 'held', ?)""",
            (alloc_id, delivery_id, customer_id, row["id"], now),
        )
        conn.execute(
            """UPDATE deliveries SET credit_stage='held', credit_id=?, updated_at=?
               WHERE id=?""",
            (alloc_id, now, delivery_id),
        )
        return True

    def _release_hold(
        self, conn: sqlite3.Connection, *, delivery_id: str, now: float
    ) -> str | None:
        """Release the *held* point of a delivery that will never send again.

        Returns ``returned`` or ``expired`` (the top-up had already expired, so
        the point joins the expired count and cannot be spent), or ``None`` when
        the delivery holds no allocation.  Already-``charged`` points are left
        untouched so a settlement can never both deduct and refund.
        """
        alloc = conn.execute(
            "SELECT * FROM credit_allocations WHERE delivery_id=? AND stage='held'",
            (delivery_id,),
        ).fetchone()
        if alloc is None:
            return None
        topup = conn.execute(
            "SELECT expires_at FROM credit_topups WHERE id=?", (alloc["topup_id"],)
        ).fetchone()
        expired = topup is not None and float(topup["expires_at"]) <= now
        new_stage = "expired" if expired else "returned"
        cur = conn.execute(
            """UPDATE credit_allocations SET stage=?, settled_at=?, released_expired=?
               WHERE id=? AND stage='held'""",
            (new_stage, now, 1 if expired else 0, alloc["id"]),
        )
        if cur.rowcount != 1:
            return None
        conn.execute(
            """UPDATE deliveries SET credit_stage=?, updated_at=?
               WHERE id=? AND credit_stage='held'""",
            (new_stage, now, delivery_id),
        )
        return new_stage

    def _charge_hold(self, conn: sqlite3.Connection, *, delivery_id: str, now: float) -> bool:
        """Consume the held point after a confirmed 2xx.  Idempotent: only a
        single held->charged transition can happen per delivery."""
        alloc = conn.execute(
            "SELECT * FROM credit_allocations WHERE delivery_id=? AND stage='held'",
            (delivery_id,),
        ).fetchone()
        if alloc is None:
            return False
        cur = conn.execute(
            """UPDATE credit_allocations SET stage='charged', settled_at=?
               WHERE id=? AND stage='held'""",
            (now, alloc["id"]),
        )
        if cur.rowcount != 1:
            return False
        conn.execute(
            """UPDATE deliveries SET credit_stage='charged', updated_at=?
               WHERE id=? AND credit_stage='held'""",
            (now, delivery_id),
        )
        return True

    def _has_unresolved_attempts(self, conn: sqlite3.Connection, delivery_id: str) -> bool:
        in_sql = "(" + ",".join("?" for _ in UNRESOLVED_OUTCOMES) + ")"
        row = conn.execute(
            f"SELECT COUNT(*) AS c FROM attempts WHERE delivery_id=? AND outcome IN {in_sql}",
            (delivery_id, *UNRESOLVED_OUTCOMES),
        ).fetchone()
        return int(row["c"]) > 0

    def _settle_stopped_delivery(
        self, conn: sqlite3.Connection, *, delivery_id: str, now: float
    ) -> str | None:
        """Settle a held point for a delivery that has just stopped.

        If no attempt could have reached the receiver, the point is returned
        (or expired if its top-up already expired).  Otherwise at least one
        request may have been accepted with no usable verdict, so the point is
        kept and the delivery is parked for customer reconciliation; it no
        longer blocks its flow, and the point is never available to anyone
        else.  Returns the resulting stage, or ``None`` when nothing was held.
        """
        stage_row = conn.execute(
            "SELECT credit_stage FROM deliveries WHERE id=?", (delivery_id,)
        ).fetchone()
        if stage_row is None or stage_row["credit_stage"] not in ("held",):
            return None
        if self._has_unresolved_attempts(conn, delivery_id):
            cur = conn.execute(
                """UPDATE deliveries
                   SET credit_stage='reconciliation', reconciliation_reason=?, updated_at=?
                   WHERE id=? AND credit_stage='held'""",
                ("unresolved_send", now, delivery_id),
            )
            if cur.rowcount != 1:
                return None
            return "reconciliation"
        return self._release_hold(conn, delivery_id=delivery_id, now=now)

    # ------------------------------------------------------- send gate/claims
    def mark_sending(self, claim: ClaimedDelivery, *, now: float) -> bool:
        """Fence between "claimed" and "HTTP request handed to the sender".

        Runs as a write transaction so it is globally serialised against
        :meth:`withdraw_topic`.  Returns ``False`` when withdrawal (or a newer
        lease) won the race, in which case the worker must not send.
        """
        with self._write() as conn:
            row = conn.execute(
                "SELECT status, lease_token, billable, credit_stage FROM deliveries WHERE id=?",
                (claim.id,),
            ).fetchone()
            if row is None or row["status"] != "in_progress" or row["lease_token"] != claim.lease_token:
                return False
            event = conn.execute(
                """SELECT e.status AS event_status, t.status AS topic_status
                   FROM events e
                   LEFT JOIN topics t ON t.customer_id = e.customer_id AND t.topic_id = e.topic_id
                   WHERE e.id=?""",
                (claim.event_id,),
            ).fetchone()
            if event is None or event["event_status"] == "withdrawn":
                return False
            if event["topic_status"] == "withdrawn":
                return False
            # ---- prepaid point gate -------------------------------------
            # A billable delivery may only hand a request to the sender while
            # it owns a point.  The allocation is written in the same write
            # transaction as the send fence, so a point is never spent on a
            # request that is never made, and two deliveries never share one.
            if row["billable"]:
                stage = row["credit_stage"]
                if stage == "needed":
                    if not self._allocate_point(
                        conn, delivery_id=claim.id, customer_id=claim.customer_id, now=now
                    ):
                        # No point available: leave it claimed but unsent, so
                        # the worker can park it for supply without spending a
                        # retry.  It is not failed.
                        return False
                elif stage != "held":
                    # charged / reconciliation / returned / expired: no point
                    # is free for another send under this delivery_id.
                    return False
                # Re-check the lease after allocating, so a concurrent recovery
                # cannot have handed this work to another worker meanwhile.
                lease = conn.execute(
                    "SELECT lease_token FROM deliveries WHERE id=?", (claim.id,)
                ).fetchone()
                if lease is None or lease["lease_token"] != claim.lease_token:
                    return False
            cur = conn.execute(
                """UPDATE attempts SET send_started_at=?
                   WHERE id=? AND outcome='in_flight' AND send_started_at IS NULL""",
                (now, claim.attempt_id),
            )
            if cur.rowcount == 1:
                return True
            attempt = conn.execute(
                "SELECT send_started_at, outcome FROM attempts WHERE id=?",
                (claim.attempt_id),
            ).fetchone()
            if attempt is None:
                # No attempt row exists (e.g. a hand-built claim in a unit
                # test).  The delivery/topic checks above are the fence; the
                # finish path re-checks withdrawal as well.
                return True
            # Re-entrant gate for an already-marked attempt is allowed; an
            # attempt that was closed (e.g. withdrawn pre-send) stays blocked.
            return attempt["send_started_at"] is not None

    # ------------------------------------------------------------------ work
    def claim_due(self, *, now: float, lease_seconds: float, limit: int = 8) -> list[ClaimedDelivery]:
        """Atomically lease up to ``limit`` due deliveries that are at the head
        of their (subscription, flow) queue and not leased by anyone else."""
        claimed: list[ClaimedDelivery] = []
        with self._write() as conn:
            rows = conn.execute(
                f"""SELECT d.*, e.caller_event_id AS caller_event_id,
                          e.payload_json AS payload_json, e.created_at AS event_created_at
                   FROM deliveries d JOIN events e ON e.id = d.event_id
                   WHERE d.status IN ('pending','retry_wait')
                     AND e.status != 'withdrawn'
                     AND (d.next_attempt_at IS NULL OR d.next_attempt_at <= ?)
                     AND NOT EXISTS (
                         SELECT 1 FROM deliveries p
                         WHERE p.subscription_id = d.subscription_id
                           AND p.flow_id = d.flow_id
                           AND p.seq < d.seq
                           AND p.status NOT IN ('succeeded','administratively_skipped','withdrawn')
                     )
                     AND {self._supply_predicate('?')}
                   ORDER BY d.created_at, d.seq
                   LIMIT ?""",
                (now, now, limit * 8),
            ).fetchall()
            for row in rows:
                if len(claimed) >= limit:
                    break
                if row["lease_token"] and row["lease_until"] and row["lease_until"] > now:
                    continue
                sub = conn.execute(
                    "SELECT target_url, target_version FROM subscriptions WHERE id=?",
                    (row["subscription_id"],),
                ).fetchone()
                token = new_id("lease")
                attempt_seq = int(row["attempt_seq"]) + 1
                attempts_count = int(row["attempts_count"]) + 1
                attempt_id = new_id("att")
                cur = conn.execute(
                    """UPDATE deliveries
                       SET status='in_progress', lease_token=?, lease_until=?,
                           attempt_seq=?, attempts_count=?, target_url=?, target_version=?,
                           block_reason=NULL, updated_at=?
                       WHERE id=? AND status IN ('pending','retry_wait')
                         AND (lease_token IS NULL OR lease_until IS NULL OR lease_until <= ?)""",
                    (
                        token,
                        now + lease_seconds,
                        attempt_seq,
                        attempts_count,
                        sub["target_url"],
                        sub["target_version"],
                        now,
                        row["id"],
                        now,
                    ),
                )
                if cur.rowcount != 1:  # lost a race inside this transaction; skip
                    continue
                conn.execute(
                    """INSERT INTO attempts(id, delivery_id, attempt_number, target_url,
                                            target_version, started_at, outcome, lease_token)
                       VALUES (?,?,?,?,?,?, 'in_flight', ?)""",
                    (
                        attempt_id,
                        row["id"],
                        attempt_seq,
                        sub["target_url"],
                        sub["target_version"],
                        now,
                        token,
                    ),
                )
                claimed.append(
                    ClaimedDelivery(
                        id=row["id"],
                        event_id=row["event_id"],
                        subscription_id=row["subscription_id"],
                        customer_id=row["customer_id"],
                        flow_id=row["flow_id"],
                        seq=int(row["seq"]),
                        target_url=sub["target_url"],
                        target_version=int(sub["target_version"]),
                        attempt_seq=attempt_seq,
                        attempts_count=attempts_count,
                        lease_token=token,
                        attempt_id=attempt_id,
                        caller_event_id=row["caller_event_id"],
                        payload_json=row["payload_json"],
                        event_created_at=float(row["event_created_at"]),
                        billable=bool(row["billable"]),
                    )
                )
        return claimed

    def finish_attempt(
        self,
        claim: ClaimedDelivery,
        *,
        outcome: str,
        http_status: int | None,
        error: str | None,
        now: float,
        retry_delay: float | None,
    ) -> str:
        """Finalize an attempt guarded by the lease token, target version and
        withdrawal state.

        ``outcome`` is one of ``succeeded`` / ``failed_http`` / ``timeout`` /
        ``transport_error``.  ``retry_delay`` is ignored on success and, when
        ``None`` on failure, parks the delivery in ``needs_attention``.

        Returns ``succeeded``, ``retry_wait``, ``needs_attention``,
        ``superseded_target``, ``withdrawn`` or ``lease_lost``.
        """
        with self._write() as conn:
            row = conn.execute("SELECT * FROM deliveries WHERE id=?", (claim.id,)).fetchone()
            if row is None:
                raise DomainError("delivery not found", status_code=404, code="delivery_not_found")

            if row["status"] == "withdrawn":
                # Withdrawal is irreversible.  Never mutate the delivery and
                # never persist the sender's error text: for HTTP failures it
                # can contain the receiver's echo of the erased payload.
                conn.execute(
                    """INSERT INTO late_results(id, delivery_id, lease_token, observed,
                                                http_status, error, recorded_at)
                       VALUES (?,?,?,?,?,NULL,?)""",
                    (new_id("late"), claim.id, claim.lease_token, f"after_withdrawal:{outcome}", http_status, now),
                )
                self._record_attempt_result(
                    conn,
                    claim,
                    record_outcome="withdrawn",
                    http_status=http_status,
                    error=None,
                    now=now,
                )
                return "withdrawn"

            if row["lease_token"] != claim.lease_token:
                # We no longer own this work (lease expired and was taken over,
                # or the attempt was reset).  Note the late observation for
                # audit, but never touch delivery state owned by a newer lease.
                conn.execute(
                    """INSERT INTO late_results(id, delivery_id, lease_token, observed,
                                                http_status, error, recorded_at)
                       VALUES (?,?,?,?,?,?,?)""",
                    (
                        new_id("late"),
                        claim.id,
                        claim.lease_token,
                        outcome,
                        http_status,
                        _truncate(error),
                        now,
                    ),
                )
                self._record_attempt_result(
                    conn,
                    claim,
                    record_outcome="lease_lost",
                    http_status=http_status,
                    error=f"lease lost; observed {outcome}"
                    + (f" HTTP {http_status}" if http_status is not None else "")
                    + (f" ({error})" if error else ""),
                    now=now,
                )
                return "lease_lost"

            sub = conn.execute(
                "SELECT target_url, target_version FROM subscriptions WHERE id=?",
                (claim.subscription_id,),
            ).fetchone()
            target_stale = int(sub["target_version"]) != claim.target_version

            if target_stale:
                # This reply was produced for an address that is no longer
                # configured.  Its verdict is meaningless for the new address.
                self._record_attempt_result(
                    conn,
                    claim,
                    record_outcome="superseded_target",
                    http_status=http_status,
                    error=f"target v{claim.target_version} -> v{sub['target_version']}; reply ignored"
                    + (f" (observed {outcome} HTTP {http_status})" if http_status is not None else f" (observed {outcome})"),
                    now=now,
                )
                self._requeue_for_target(
                    conn, claim=claim, target_url=sub["target_url"], target_version=int(sub["target_version"]), now=now
                )
                return "superseded_target"

            self._record_attempt_result(
                conn, claim, record_outcome=outcome, http_status=http_status, error=error, now=now
            )

            if outcome == "succeeded":
                conn.execute(
                    """UPDATE deliveries
                       SET status='succeeded', lease_token=NULL, lease_until=NULL,
                           blocked_by=NULL, block_reason=NULL, last_error=NULL,
                           completed_at=?, updated_at=?
                       WHERE id=? AND lease_token=?""",
                    (now, now, claim.id, claim.lease_token),
                )
                # A confirmed 2xx consumes the point this delivery_id held.
                # Guarded by stage='held', so it happens at most once even if a
                # duplicate success reply arrives.
                if row["billable"]:
                    self._charge_hold(conn, delivery_id=claim.id, now=now)
                self._unblock_successors(conn, claim.subscription_id, claim.flow_id, claim.seq, now)
                return "succeeded"

            if retry_delay is None:
                conn.execute(
                    """UPDATE deliveries
                       SET status='needs_attention', lease_token=NULL, lease_until=NULL,
                           next_attempt_at=NULL, last_error=?, updated_at=?
                       WHERE id=? AND lease_token=?""",
                    (
                        _describe_failure(outcome, http_status, error),
                        now,
                        claim.id,
                        claim.lease_token,
                    ),
                )
                return "needs_attention"

            # The store is authoritative about the retry budget: even if a
            # caller passes a delay, an exhausted budget parks the delivery.
            if claim.attempts_count >= self.settings.max_attempts:
                conn.execute(
                    """UPDATE deliveries
                       SET status='needs_attention', lease_token=NULL, lease_until=NULL,
                           next_attempt_at=NULL, last_error=?, updated_at=?
                       WHERE id=? AND lease_token=?""",
                    (
                        _describe_failure(outcome, http_status, error),
                        now,
                        claim.id,
                        claim.lease_token,
                    ),
                )
                return "needs_attention"

            conn.execute(
                """UPDATE deliveries
                   SET status='retry_wait', lease_token=NULL, lease_until=NULL,
                       next_attempt_at=?, last_error=?, updated_at=?
                   WHERE id=? AND lease_token=?""",
                (
                    now + retry_delay,
                    _describe_failure(outcome, http_status, error),
                    now,
                    claim.id,
                    claim.lease_token,
                ),
            )
            return "retry_wait"

    def _requeue_for_target(
        self, conn: sqlite3.Connection, *, claim: ClaimedDelivery, target_url: str, target_version: int, now: float
    ) -> None:
        conn.execute(
            """UPDATE deliveries
               SET status='pending', attempts_count=MAX(attempts_count-1, 0),
                   lease_token=NULL, lease_until=NULL, next_attempt_at=NULL,
                   target_url=?, target_version=?, last_error=?, updated_at=?
               WHERE id=? AND lease_token=?""",
            (
                target_url,
                target_version,
                "previous attempt was made against a superseded target and was ignored",
                now,
                claim.id,
                claim.lease_token,
            ),
        )

    def _record_attempt_result(
        self,
        conn: sqlite3.Connection,
        claim: ClaimedDelivery,
        *,
        record_outcome: str,
        http_status: int | None,
        error: str | None,
        now: float,
    ) -> None:
        conn.execute(
            """UPDATE attempts
               SET finished_at=?, outcome=?, http_status=?, error=?
               WHERE id=? AND outcome='in_flight'""",
            (now, record_outcome, http_status, _truncate(error), claim.attempt_id),
        )

    def _unblock_successors(
        self, conn: sqlite3.Connection, subscription_id: str, flow_id: str, seq: int, now: float
    ) -> None:
        conn.execute(
            """UPDATE deliveries
               SET blocked_by=NULL, block_reason=NULL, next_attempt_at=NULL, updated_at=?
               WHERE subscription_id=? AND flow_id=? AND seq > ?
                 AND status NOT IN ('succeeded','administratively_skipped','withdrawn')
                 AND blocked_by IS NOT NULL
                 AND blocked_by IN (
                     SELECT id FROM deliveries
                     WHERE subscription_id=? AND flow_id=? AND status IN ('succeeded','administratively_skipped','withdrawn')
                 )""",
            (now, subscription_id, flow_id, seq, subscription_id, flow_id),
        )

    # ------------------------------------------------------------ admin ops
    def retry_blocked(self, delivery_id: str, *, now: float, reason: str, actor: str) -> dict[str, Any]:
        reason = (reason or "").strip()
        if not reason:
            raise DomainError("a reason is required to retry a delivery", status_code=422, code="reason_required")
        with self._write() as conn:
            row = conn.execute("SELECT * FROM deliveries WHERE id=?", (delivery_id,)).fetchone()
            if row is None:
                raise DomainError("delivery not found", status_code=404, code="delivery_not_found")
            if row["status"] == "withdrawn":
                raise DomainError(
                    "withdrawn deliveries are terminated and cannot be retried",
                    status_code=409,
                    code="delivery_withdrawn",
                )
            if row["status"] != "needs_attention":
                raise DomainError(
                    f"only needs_attention deliveries can be retried (status={row['status']})",
                    status_code=409,
                    code="invalid_state",
                )
            conn.execute(
                """UPDATE deliveries
                   SET status='pending', attempts_count=0, next_attempt_at=NULL,
                       lease_token=NULL, lease_until=NULL, last_error=NULL,
                       target_url=(SELECT target_url FROM subscriptions WHERE id=?),
                       target_version=(SELECT target_version FROM subscriptions WHERE id=?),
                       updated_at=?
                   WHERE id=?""",
                (row["subscription_id"], row["subscription_id"], now, delivery_id),
            )
            conn.execute(
                """INSERT INTO admin_actions(id, delivery_id, action, reason, actor, created_at, target_version)
                   VALUES (?,?,?,?,?,?,?)""",
                (new_id("adm"), delivery_id, "retry", reason, actor, now, row["target_version"]),
            )
        return self.get_delivery(delivery_id)

    def skip_delivery(self, delivery_id: str, *, now: float, reason: str, actor: str) -> dict[str, Any]:
        reason = (reason or "").strip()
        if not reason:
            raise DomainError("a reason is required to skip a delivery", status_code=422, code="reason_required")
        with self._write() as conn:
            row = conn.execute("SELECT * FROM deliveries WHERE id=?", (delivery_id,)).fetchone()
            if row is None:
                raise DomainError("delivery not found", status_code=404, code="delivery_not_found")
            if row["status"] == "withdrawn":
                raise DomainError(
                    "withdrawn deliveries are already terminally stopped",
                    status_code=409,
                    code="delivery_withdrawn",
                )
            if row["status"] != "needs_attention":
                raise DomainError(
                    f"only needs_attention deliveries can be skipped (status={row['status']})",
                    status_code=409,
                    code="invalid_state",
                )
            conn.execute(
                """UPDATE deliveries
                   SET status='administratively_skipped', lease_token=NULL, lease_until=NULL,
                       next_attempt_at=NULL, completed_at=?, updated_at=?
                   WHERE id=?""",
                (now, now, delivery_id),
            )
            # Stopping a delivery releases its held point unless an unresolved
            # request may already have reached the receiver; then it is parked
            # for customer reconciliation and the point stays reserved.
            self._settle_stopped_delivery(conn, delivery_id=delivery_id, now=now)
            conn.execute(
                """INSERT INTO admin_actions(id, delivery_id, action, reason, actor, created_at, target_version)
                   VALUES (?,?,?,?,?,?,?)""",
                (new_id("adm"), delivery_id, "skip", reason, actor, now, row["target_version"]),
            )
            self._unblock_successors(conn, row["subscription_id"], row["flow_id"], int(row["seq"]), now)
        return self.get_delivery(delivery_id)

    # -------------------------------------------------------------- recovery
    def recover_expired_leases(self, *, now: float) -> int:
        """Re-queue attempts whose lease expired without a recorded result.

        The attempt row is marked ``expired`` so the audit trail shows that the
        outcome is unknown.  The delivery is retried (same retry budget, since
        no verdict was observed) on the subscription's *current* target.  The
        receiver may have processed the first request: the stable delivery_id
        is the customer's dedupe key.

        Withdrawn deliveries are never re-queued; only their dangling
        in-flight attempt rows are closed once the lease is old enough.
        """
        with self._write() as conn:
            rows = conn.execute(
                """SELECT id, attempt_seq FROM deliveries
                   WHERE status='in_progress' AND lease_until IS NOT NULL AND lease_until <= ?""",
                (now,),
            ).fetchall()
            for row in rows:
                conn.execute(
                    """UPDATE attempts SET outcome='expired', finished_at=?,
                           error='lease expired before a result was recorded'
                       WHERE delivery_id=? AND attempt_number=? AND outcome='in_flight'""",
                    (now, row["id"], row["attempt_seq"]),
                )
            if rows:
                conn.execute(
                    """UPDATE deliveries
                       SET status='pending', attempts_count=MAX(attempts_count-1, 0),
                           lease_token=NULL, lease_until=NULL, next_attempt_at=NULL,
                           target_url=(SELECT target_url FROM subscriptions WHERE id=deliveries.subscription_id),
                           target_version=(SELECT target_version FROM subscriptions WHERE id=deliveries.subscription_id),
                           last_error='lease expired before result was recorded', updated_at=?
                       WHERE status='in_progress' AND lease_until IS NOT NULL AND lease_until <= ?""",
                    (now, now),
                )

            # Withdrawn work stays withdrawn across restarts; just settle any
            # attempt rows left 'in_flight' by a dead worker after lease expiry.
            dead_withdrawn = conn.execute(
                """SELECT id, attempt_seq FROM deliveries
                   WHERE status='withdrawn' AND lease_token IS NOT NULL
                     AND lease_until IS NOT NULL AND lease_until <= ?""",
                (now,),
            ).fetchall()
            for row in dead_withdrawn:
                conn.execute(
                    """UPDATE attempts SET outcome='expired', finished_at=?,
                           error='lease expired after topic withdrawal'
                       WHERE delivery_id=? AND attempt_number=? AND outcome='in_flight'""",
                    (now, row["id"], row["attempt_seq"]),
                )
                conn.execute(
                    "UPDATE deliveries SET lease_token=NULL WHERE id=? AND status='withdrawn'",
                    (row["id"],),
                )
            return len(rows) + len(dead_withdrawn)

    def next_wakeup(self, *, now: float) -> float | None:
        """Earliest time a worker has something new to do.

        Rows blocked behind a non-terminal predecessor are excluded: they will
        be woken when the predecessor finishes, and including them would make a
        polling loop spin.  Withdrawn rows never produce wake-ups.
        """
        with self._read() as conn:
            due = conn.execute(
                f"""SELECT MIN(COALESCE(d.next_attempt_at, ?)) AS t FROM deliveries d
                   WHERE d.status IN ('pending','retry_wait')
                     AND NOT EXISTS (
                         SELECT 1 FROM deliveries p
                         WHERE p.subscription_id = d.subscription_id
                           AND p.flow_id = d.flow_id
                           AND p.seq < d.seq
                           AND p.status NOT IN ('succeeded','administratively_skipped','withdrawn')
                     )
                     AND {self._supply_predicate('?')}""",
                (now, now),
            ).fetchone()
            leases = conn.execute(
                "SELECT MIN(lease_until) AS t FROM deliveries WHERE status='in_progress'"
            ).fetchone()
        candidates = [t for t in (due["t"], leases["t"]) if t is not None]
        return min(candidates) if candidates else None

    def sellable_point(self, *, customer_id: str, now: float) -> bool:
        """Whether the customer currently has a point that could be allocated."""
        with self._read() as conn:
            row = conn.execute(
                """SELECT 1 FROM credit_topups ct WHERE ct.customer_id=? AND ct.expires_at > ?
                   AND (ct.points - COALESCE((SELECT SUM(ca.points) FROM credit_allocations ca
                        WHERE ca.topup_id=ct.id AND ca.stage IN ('held','charged','expired')),0)) > 0
                   LIMIT 1""",
                (customer_id, now),
            ).fetchone()
        return row is not None

    # ------------------------------------------------------------- read side
    def get_delivery(self, delivery_id: str) -> dict[str, Any]:
        with self._read() as conn:
            row = conn.execute("SELECT * FROM deliveries WHERE id=?", (delivery_id,)).fetchone()
            if row is None:
                raise DomainError("delivery not found", status_code=404, code="delivery_not_found")
            attempts = conn.execute(
                "SELECT * FROM attempts WHERE delivery_id=? ORDER BY attempt_number",
                (delivery_id,),
            ).fetchall()
            admin = conn.execute(
                "SELECT * FROM admin_actions WHERE delivery_id=? ORDER BY created_at",
                (delivery_id,),
            ).fetchall()
            sub = conn.execute(
                "SELECT target_url, target_version FROM subscriptions WHERE id=?",
                (row["subscription_id"],),
            ).fetchone()
            event = conn.execute("SELECT * FROM events WHERE id=?", (row["event_id"],)).fetchone()
            predecessors = conn.execute(
                """SELECT id, seq, status FROM deliveries
                   WHERE subscription_id=? AND flow_id=? AND seq < ?
                   ORDER BY seq""",
                (row["subscription_id"], row["flow_id"], row["seq"]),
            ).fetchall()
            late = conn.execute(
                "SELECT * FROM late_results WHERE delivery_id=? ORDER BY recorded_at",
                (delivery_id,),
            ).fetchall()
            alloc = conn.execute(
                """SELECT a.id, a.stage, a.points, a.created_at, a.settled_at,
                          t.topup_ref AS topup_ref
                   FROM credit_allocations a LEFT JOIN credit_topups t ON t.id = a.topup_id
                   WHERE a.delivery_id=? ORDER BY a.created_at DESC LIMIT 1""",
                (delivery_id,),
            ).fetchone()
        blocking = [p for p in predecessors if p["status"] not in TERMINAL_STATUSES]
        event_withdrawn = self._is_withdrawn_row(event) if event is not None else True
        if event is None or event_withdrawn:
            # Content-safe event reference: never payload/hash here.
            event_ref: dict[str, Any] = {
                "id": event["id"] if event is not None else row["event_id"],
                "caller_event_id": event["caller_event_id"] if event is not None else None,
                "topic_id": (event["topic_id"] if event is not None and "topic_id" in event.keys() else None),
                "created_at": iso_time(event["created_at"]) if event is not None else None,
                "payload": None,
                "withdrawn": True,
                "status": "withdrawn",
            }
        else:
            event_ref = {
                "id": event["id"],
                "caller_event_id": event["caller_event_id"],
                "topic_id": event["topic_id"] if "topic_id" in event.keys() else None,
                "payload": json.loads(event["payload_json"]),
                "created_at": iso_time(event["created_at"]),
                "withdrawn": False,
                "status": event["status"],
            }
        return {
            "delivery_id": row["id"],
            "subscription_id": row["subscription_id"],
            "customer_id": row["customer_id"],
            "flow_id": row["flow_id"],
            "seq": row["seq"],
            "event": event_ref,
            "status": row["status"],
            "attempts_count": row["attempts_count"],
            "target_url": row["target_url"],
            "target_version": row["target_version"],
            "subscription_target": {
                "target_url": sub["target_url"],
                "target_version": sub["target_version"],
            },
            "next_attempt_at": iso_time(row["next_attempt_at"]),
            "blocked": row["blocked_by"] is not None or bool(blocking),
            "block_reason": self._describe_block(row, blocking),
            "blocking_delivery_ids": [p["id"] for p in blocking],
            "last_error": row["last_error"],
            "billable": bool(row["billable"]),
            "credit_stage": row["credit_stage"],
            "credit": {
                "allocation_id": alloc["id"] if alloc is not None else None,
                "topup_ref": alloc["topup_ref"] if alloc is not None else None,
                "stage": alloc["stage"] if alloc is not None else None,
                "points": int(alloc["points"]) if alloc is not None else 0,
                "allocated_at": iso_time(alloc["created_at"]) if alloc is not None else None,
                "settled_at": iso_time(alloc["settled_at"]) if alloc is not None else None,
            },
            "reconciliation": {
                "pending": row["credit_stage"] == "reconciliation",
                "reason": row["reconciliation_reason"],
                "state": row["reconciliation_state"],
                "note": row["reconciliation_note"],
                "confirmed_at": iso_time(row["reconciliation_at"]),
            },
            "created_at": iso_time(row["created_at"]),
            "updated_at": iso_time(row["updated_at"]),
            "completed_at": iso_time(row["completed_at"]),
            "attempts": [
                {
                    "attempt_number": a["attempt_number"],
                    "target_url": a["target_url"],
                    "target_version": a["target_version"],
                    "started_at": iso_time(a["started_at"]),
                    "finished_at": iso_time(a["finished_at"]),
                    "outcome": a["outcome"],
                    "http_status": a["http_status"],
                    "error": a["error"],
                }
                for a in attempts
            ],
            "admin_actions": [
                {
                    "action": a["action"],
                    "reason": a["reason"],
                    "actor": a["actor"],
                    "target_version": a["target_version"],
                    "created_at": iso_time(a["created_at"]),
                }
                for a in admin
            ],
            "late_results": [
                {
                    "observed": l["observed"],
                    "http_status": l["http_status"],
                    "error": l["error"],
                    "recorded_at": iso_time(l["recorded_at"]),
                }
                for l in late
            ],
        }

    @staticmethod
    def _describe_block(row: sqlite3.Row, blocking: list[sqlite3.Row]) -> str | None:
        if blocking:
            p = blocking[0]
            return f"predecessor seq {p['seq']} ({p['id']}) is {p['status']}"
        if row["status"] == "needs_attention":
            return "delivery failed and is parked for an administrator"
        if row["status"] == "retry_wait":
            return "waiting for retry back-off"
        if row["status"] == "in_progress":
            return "attempt in progress (leased by a worker)"
        if row["status"] == "pending":
            return "pending claim"
        if row["status"] == "withdrawn":
            return "terminated because its topic was withdrawn"
        return None

    def list_deliveries(self, *, subscription_id: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        with self._read() as conn:
            if subscription_id:
                rows = conn.execute(
                    "SELECT id FROM deliveries WHERE subscription_id=? ORDER BY created_at, seq LIMIT ?",
                    (subscription_id, limit),
                ).fetchall()
            else:
                rows = conn.execute("SELECT id FROM deliveries ORDER BY created_at, seq LIMIT ?", (limit,)).fetchall()
        return [self.get_delivery(r["id"]) for r in rows]

    def list_events(
        self,
        *,
        customer_id: str | None = None,
        flow_id: str | None = None,
        topic_id: str | None = None,
        limit: int = 100,
        include_withdrawn: bool = True,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if customer_id:
            clauses.append("e.customer_id=?")
            params.append(customer_id)
        if flow_id:
            clauses.append("e.flow_id=?")
            params.append(flow_id)
        if topic_id is not None:
            clauses.append("e.topic_id=?")
            params.append(topic_id)
        if not include_withdrawn:
            clauses.append("e.status != 'withdrawn'")
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        with self._read() as conn:
            rows = conn.execute(
                f"""SELECT e.*, t.withdrawn_at AS topic_withdrawn_at
                    FROM events e LEFT JOIN topics t
                      ON t.customer_id = e.customer_id AND t.topic_id = e.topic_id
                    {where}
                    ORDER BY e.ordinal DESC LIMIT ?""",
                params,
            ).fetchall()
            withdrawn_at = {r["id"]: r["topic_withdrawn_at"] for r in rows}
        return [self._event_json(r, withdrawn_at=withdrawn_at.get(r["id"])) for r in rows]

    # ------------------------------------------------- naming compatibility
    # Thin aliases for the same use cases under the vocabulary a caller may
    # already use (cases / holds / revocations / erasure).
    def revoke_topic(self, **kwargs: Any) -> dict[str, Any]:
        return self.withdraw_topic(**kwargs)

    def erase_topic(self, **kwargs: Any) -> dict[str, Any]:
        return self.withdraw_topic(**kwargs)

    def purge_topic(self, **kwargs: Any) -> dict[str, Any]:
        return self.withdraw_topic(**kwargs)

    def forget_topic(self, **kwargs: Any) -> dict[str, Any]:
        return self.withdraw_topic(**kwargs)

    def delete_topic_data(self, **kwargs: Any) -> dict[str, Any]:
        return self.withdraw_topic(**kwargs)

    def withdraw_events_by_topic(self, **kwargs: Any) -> dict[str, Any]:
        return self.withdraw_topic(**kwargs)

    def withdraw_topic_events(self, **kwargs: Any) -> dict[str, Any]:
        return self.withdraw_topic(**kwargs)

    def withdraw_by_topic(self, **kwargs: Any) -> dict[str, Any]:
        return self.withdraw_topic(**kwargs)

    def create_case(self, **kwargs: Any) -> dict[str, Any]:
        return self.create_ticket(**kwargs)

    def open_ticket(self, **kwargs: Any) -> dict[str, Any]:
        return self.create_ticket(**kwargs)

    def create_legal_hold(self, **kwargs: Any) -> dict[str, Any]:
        return self.create_ticket(**kwargs)

    def get_case(self, case_id: str, **kwargs: Any) -> dict[str, Any]:
        return self.get_ticket(case_id, **kwargs)

    def get_legal_hold(self, hold_id: str, **kwargs: Any) -> dict[str, Any]:
        return self.get_ticket(hold_id, **kwargs)

    def close_case(self, case_id: str, **kwargs: Any) -> dict[str, Any]:
        return self.close_ticket(case_id, **kwargs)

    def release_legal_hold(self, hold_id: str, **kwargs: Any) -> dict[str, Any]:
        return self.close_ticket(hold_id, **kwargs)

    def retain_event(self, *, ticket_id: str, event_id: str, **kwargs: Any) -> dict[str, Any]:
        return self.retain_events(ticket_id=ticket_id, event_ids=[event_id], **kwargs)

    def retain(self, ticket_id: str, event_ids: str | Iterable[str], **kwargs: Any) -> dict[str, Any]:
        return self.retain_events(ticket_id=ticket_id, event_ids=event_ids, **kwargs)

    def add_evidence(self, ticket_id: str, event_ids: str | Iterable[str], **kwargs: Any) -> dict[str, Any]:
        return self.retain_events(ticket_id=ticket_id, event_ids=event_ids, **kwargs)

    def attach_events(self, ticket_id: str, event_ids: str | Iterable[str], **kwargs: Any) -> dict[str, Any]:
        return self.retain_events(ticket_id=ticket_id, event_ids=event_ids, **kwargs)

    def preserve_events(self, ticket_id: str, event_ids: str | Iterable[str], **kwargs: Any) -> dict[str, Any]:
        return self.retain_events(ticket_id=ticket_id, event_ids=event_ids, **kwargs)

    def add_hold_events(self, hold_id: str, event_ids: str | Iterable[str], **kwargs: Any) -> dict[str, Any]:
        return self.retain_events(ticket_id=hold_id, event_ids=event_ids, **kwargs)

    def add_event(self, ticket_id: str, event_id: str, **kwargs: Any) -> dict[str, Any]:
        return self.retain_events(ticket_id=ticket_id, event_ids=[event_id], **kwargs)

    def add_event_to_ticket(self, ticket_id: str, event_id: str | None = None, **kwargs: Any) -> dict[str, Any]:
        ids = kwargs.pop("event_ids", None)
        if ids is None:
            ids = [event_id] if event_id is not None else []
        elif event_id is not None:
            ids = [event_id, *list(ids)]
        return self.retain_events(ticket_id=ticket_id, event_ids=ids, **kwargs)

    def add_events(self, ticket_id: str, event_ids: str | Iterable[str], **kwargs: Any) -> dict[str, Any]:
        return self.retain_events(ticket_id=ticket_id, event_ids=event_ids, **kwargs)

    def hold_event(self, *, ticket_id: str, event_id: str, **kwargs: Any) -> dict[str, Any]:
        return self.retain_events(ticket_id=ticket_id, event_ids=[event_id], **kwargs)

    def list_evidence(self, ticket_id: str, **kwargs: Any) -> list[dict[str, Any]]:
        return self.list_ticket_events(ticket_id, **kwargs)

    def get_evidence(self, ticket_id: str, **kwargs: Any) -> list[dict[str, Any]]:
        return self.list_ticket_events(ticket_id, **kwargs)

    def list_held_events(self, ticket_id: str, **kwargs: Any) -> list[dict[str, Any]]:
        return self.list_ticket_events(ticket_id, **kwargs)


def _describe_failure(outcome: str, http_status: int | None, error: str | None) -> str:
    parts = [outcome]
    if http_status is not None:
        parts.append(f"HTTP {http_status}")
    if error:
        parts.append(error)
    return " ".join(parts)


def _truncate(text: str | None, limit: int = 500) -> str | None:
    if text is None:
        return None
    return text if len(text) <= limit else text[:limit] + "…"
