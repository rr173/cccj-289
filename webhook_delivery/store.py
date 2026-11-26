"""SQLite persistence for events, subscriptions, deliveries and attempts.

Correctness-relevant state transitions happen inside ``BEGIN IMMEDIATE``
transactions.  SQLite allows one writer at a time, which is what makes
"claim" and "finalize" safe across multiple delivery processes.

Key invariants encoded here
---------------------------
* Event identity is ``(customer_id, flow_id, caller_event_id)``.  Replaying
  the same identity with the same canonical JSON returns the original event;
  replaying it with different content raises ``caller_event_conflict``.
* ``events.seq`` is per ``(customer_id, flow_id)`` and allocated inside the
  same write transaction that inserts the row.
* A subscription is only fanned out events whose global ``ordinal`` is greater
  than the subscription's ordinal.  Ordinals come from a counter bumped in the
  same transaction, so ordering is unambiguous even if two operations share a
  clock tick.
* A delivery may be claimed only when every lower ``seq`` in the same
  ``(subscription, flow)`` is terminal (succeeded or administratively skipped).
  Different flows never block each other.
* Every claim writes a fresh ``lease_token``.  ``finish_attempt`` only mutates
  the delivery when that token still matches.  A response produced against an
  older subscription target version is discarded ("superseded_target") and the
  delivery is re-queued against the current target, so a late answer for the
  old address can never mark the new address's record as delivered.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator

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
    created_at REAL NOT NULL
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

CREATE TABLE IF NOT EXISTS events (
    id              TEXT PRIMARY KEY,
    customer_id     TEXT NOT NULL REFERENCES customers(id),
    flow_id         TEXT NOT NULL,
    caller_event_id TEXT NOT NULL,
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

-- Replies from a lease holder that had already lost the lease.  They are kept
-- for observability but never change delivery state.
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
"""

TERMINAL_STATUSES = ("succeeded", "administratively_skipped")


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

    # ------------------------------------------------------------------ setup
    def initialize(self) -> None:
        with self._connect() as conn:
            conn.executescript(SCHEMA)
            conn.execute("INSERT OR IGNORE INTO counters(name, value) VALUES ('ordinal', 0)")

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = _connect(self._path)
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
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
            # this subscription's clock reading but before this insert: any
            # event ordered after the subscription still fans out to it.
            rows = conn.execute(
                """SELECT id, flow_id, seq FROM events
                   WHERE customer_id=? AND ordinal > ?
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
            # administrator must still retry them (and will then use the new
            # address).  Succeeded rows are never touched.
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
    ) -> dict[str, Any]:
        if not self.customer_exists(customer_id):
            raise DomainError("unknown customer_id", status_code=404, code="customer_not_found")
        flow_id = require_text(flow_id, "flow_id")
        caller_event_id = require_text(caller_event_id, "caller_event_id")
        try:
            canonical = canonical_json(payload)
        except (TypeError, ValueError) as exc:
            raise DomainError(f"payload must be valid JSON: {exc}", status_code=422, code="invalid_payload") from exc
        digest = content_digest(canonical)

        with self._write() as conn:
            existing = conn.execute(
                """SELECT * FROM events
                   WHERE customer_id=? AND flow_id=? AND caller_event_id=?""",
                (customer_id, flow_id, caller_event_id),
            ).fetchone()
            if existing is not None:
                if existing["content_hash"] != digest:
                    raise DomainError(
                        "caller_event_id was already used with different content",
                        status_code=409,
                        code="caller_event_conflict",
                    )
                return self._event_result(conn, existing, replayed=True)

            ordinal = self._next_ordinal(conn)
            seq_row = conn.execute(
                "SELECT COALESCE(MAX(seq), 0) AS m FROM events WHERE customer_id=? AND flow_id=?",
                (customer_id, flow_id),
            ).fetchone()
            seq = int(seq_row["m"]) + 1
            event_id = new_id("evt")
            conn.execute(
                """INSERT INTO events(id, customer_id, flow_id, caller_event_id, payload_json,
                                      content_hash, seq, ordinal, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (event_id, customer_id, flow_id, caller_event_id, canonical, digest, seq, ordinal, now),
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
                )
            stored = conn.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
            return self._event_result(conn, stored, replayed=False)

    def _event_result(self, conn: sqlite3.Connection, row: sqlite3.Row, *, replayed: bool) -> dict[str, Any]:
        deliveries = conn.execute(
            "SELECT id, subscription_id, status FROM deliveries WHERE event_id=? ORDER BY subscription_id",
            (row["id"],),
        ).fetchall()
        return {
            "id": row["id"],
            "customer_id": row["customer_id"],
            "flow_id": row["flow_id"],
            "caller_event_id": row["caller_event_id"],
            "seq": row["seq"],
            "payload": json.loads(row["payload_json"]),
            "content_hash": row["content_hash"],
            "created_at": iso_time(row["created_at"]),
            "replayed": replayed,
            "deliveries": [
                {"delivery_id": d["id"], "subscription_id": d["subscription_id"], "status": d["status"]}
                for d in deliveries
            ],
        }

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
        conn.execute(
            """INSERT INTO deliveries(id, event_id, subscription_id, customer_id, flow_id, seq,
                                      target_url, target_version, status, attempts_count,
                                      next_attempt_at, blocked_by, block_reason,
                                      created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?, 'pending', 0, NULL, ?, ?, ?, ?)""",
            (
                new_id("dlv"),
                event_id,
                subscription_id,
                customer_id,
                flow_id,
                seq,
                target_url,
                target_version,
                blocked_by,
                "predecessor_incomplete" if blocked_by else None,
                now,
                now,
            ),
        )

    # ------------------------------------------------------------------ work
    def claim_due(self, *, now: float, lease_seconds: float, limit: int = 8) -> list[ClaimedDelivery]:
        """Atomically lease up to ``limit`` due deliveries that are at the head
        of their (subscription, flow) queue and not leased by anyone else."""
        claimed: list[ClaimedDelivery] = []
        with self._write() as conn:
            rows = conn.execute(
                """SELECT d.*, e.caller_event_id AS caller_event_id,
                          e.payload_json AS payload_json, e.created_at AS event_created_at
                   FROM deliveries d JOIN events e ON e.id = d.event_id
                   WHERE d.status IN ('pending','retry_wait')
                     AND (d.next_attempt_at IS NULL OR d.next_attempt_at <= ?)
                     AND NOT EXISTS (
                         SELECT 1 FROM deliveries p
                         WHERE p.subscription_id = d.subscription_id
                           AND p.flow_id = d.flow_id
                           AND p.seq < d.seq
                           AND p.status NOT IN ('succeeded','administratively_skipped')
                     )
                   ORDER BY d.created_at, d.seq
                   LIMIT ?""",
                (now, limit * 8),
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
        """Finalize an attempt guarded by the lease token and target version.

        ``outcome`` is one of ``succeeded`` / ``failed_http`` / ``timeout`` /
        ``transport_error``.  ``retry_delay`` is ignored on success and, when
        ``None`` on failure, parks the delivery in ``needs_attention``.

        Returns ``succeeded``, ``retry_wait``, ``needs_attention``,
        ``superseded_target`` or ``lease_lost``.
        """
        with self._write() as conn:
            row = conn.execute("SELECT * FROM deliveries WHERE id=?", (claim.id,)).fetchone()
            if row is None or row["lease_token"] != claim.lease_token:
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
                 AND status NOT IN ('succeeded','administratively_skipped')
                 AND blocked_by IS NOT NULL
                 AND blocked_by IN (
                     SELECT id FROM deliveries
                     WHERE subscription_id=? AND flow_id=? AND status IN ('succeeded','administratively_skipped')
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
            return len(rows)

    def next_wakeup(self, *, now: float) -> float | None:
        """Earliest time a worker has something new to do.

        Rows blocked behind a non-terminal predecessor are excluded: they will
        be woken when the predecessor finishes, and including them would make a
        polling loop spin.
        """
        with self._read() as conn:
            due = conn.execute(
                """SELECT MIN(COALESCE(d.next_attempt_at, ?)) AS t FROM deliveries d
                   WHERE d.status IN ('pending','retry_wait')
                     AND NOT EXISTS (
                         SELECT 1 FROM deliveries p
                         WHERE p.subscription_id = d.subscription_id
                           AND p.flow_id = d.flow_id
                           AND p.seq < d.seq
                           AND p.status NOT IN ('succeeded','administratively_skipped')
                     )""",
                (now,),
            ).fetchone()
            leases = conn.execute(
                "SELECT MIN(lease_until) AS t FROM deliveries WHERE status='in_progress'"
            ).fetchone()
        candidates = [t for t in (due["t"], leases["t"]) if t is not None]
        return min(candidates) if candidates else None

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
        blocking = [p for p in predecessors if p["status"] not in TERMINAL_STATUSES]
        return {
            "delivery_id": row["id"],
            "subscription_id": row["subscription_id"],
            "customer_id": row["customer_id"],
            "flow_id": row["flow_id"],
            "seq": row["seq"],
            "event": {
                "id": event["id"],
                "caller_event_id": event["caller_event_id"],
                "payload": json.loads(event["payload_json"]),
                "created_at": iso_time(event["created_at"]),
            },
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
        self, *, customer_id: str | None = None, flow_id: str | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if customer_id:
            clauses.append("customer_id=?")
            params.append(customer_id)
        if flow_id:
            clauses.append("flow_id=?")
            params.append(flow_id)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        with self._read() as conn:
            rows = conn.execute(
                f"SELECT * FROM events{where} ORDER BY ordinal DESC LIMIT ?", params
            ).fetchall()
        return [
            {
                "id": r["id"],
                "customer_id": r["customer_id"],
                "flow_id": r["flow_id"],
                "caller_event_id": r["caller_event_id"],
                "seq": r["seq"],
                "payload": json.loads(r["payload_json"]),
                "content_hash": r["content_hash"],
                "created_at": iso_time(r["created_at"]),
            }
            for r in rows
        ]


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
