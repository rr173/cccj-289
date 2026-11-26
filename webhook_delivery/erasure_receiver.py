"""Reference participating receiver for the external-replica erasure protocol.

This is a deliberately small, self-contained HTTP service a partner could run.
It keeps *managed copies* of topic callbacks in its own persistent SQLite file
and honours erasure requests from the webhook service.

Guarantees implemented here
---------------------------
* It only ever deletes copies scoped to the exact ``(customer_id, topic_id)``
  named by an erasure request.  A same-named topic belonging to a different
  customer, or another topic of the same customer, is never touched.
* It acknowledges an erasure request **only** after (a) every stored managed
  copy for that customer+topic is deleted, and (b) no callback that was
  authorized before withdrawal can create a new copy again.  The seal is the
  withdrawal ``watermark``: any callback whose ``authorization_seq`` is at or
  below the acknowledged seal is rejected without storing anything.  This
  holds even if the callback's HTTP request arrives *after* the erasure
  request, so no network ordering is assumed.
* Erasure requests are idempotent: repeating one returns the same
  acknowledgement and never resurrects or double-deletes anything.  State is
  durable, so a restart preserves seals and copies.

The reference receiver is a *participant*: it makes no attempt to control
receivers that do not implement the protocol.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

SCHEMA = """
PRAGMA journal_mode=WAL;

-- Managed copies of topic callbacks this receiver has accepted.
CREATE TABLE IF NOT EXISTS managed_replicas (
    customer_id       TEXT NOT NULL,
    topic_id          TEXT NOT NULL,
    delivery_id       TEXT NOT NULL,
    receiver_id       TEXT NOT NULL,
    authorization_seq INTEGER NOT NULL,
    payload_json      TEXT,
    stored_at         REAL NOT NULL,
    PRIMARY KEY (customer_id, topic_id, delivery_id)
);

CREATE INDEX IF NOT EXISTS ix_managed_scope
    ON managed_replicas(customer_id, topic_id);

-- One row per acknowledged erasure operation.  ``watermark`` seals the scope:
-- callbacks with authorization_seq <= watermark can never store again.
CREATE TABLE IF NOT EXISTS erasure_seals (
    operation_id       TEXT PRIMARY KEY,
    customer_id        TEXT NOT NULL,
    topic_id           TEXT NOT NULL,
    receiver_id        TEXT NOT NULL,
    watermark          INTEGER NOT NULL,
    acknowledged_at    REAL NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_seal_scope
    ON erasure_seals(customer_id, topic_id);
"""

# A seal on (customer, topic) is keyed by scope, so the latest/highest
# watermark wins.  Reading the seal from the row makes every later callback
# cheap to reject.


def _connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=5.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


class ReferenceErasureReceiver:
    """Persistent managed-copy store + protocol handling, independent of HTTP."""

    def __init__(self, database_path: str) -> None:
        self._path = database_path
        self._lock = threading.Lock()
        with _connect(self._path) as conn:
            conn.executescript(SCHEMA)

    # ---------------------------------------------------------- callbacks
    def store_callback(self, body: dict[str, Any], now: float) -> dict[str, Any]:
        """Store one managed copy of a topic callback.

        Rejects (and stores nothing) when the callback's authorization sequence
        is already sealed by an erasure request, or when it is malformed.
        Retries of the same ``delivery_id`` are idempotent.
        """
        customer_id = body.get("customer_id")
        topic_id = body.get("topic_id")
        delivery_id = body.get("delivery_id")
        receiver_id = body.get("erasure_receiver_id")
        auth_seq = body.get("authorization_seq")
        if not all(isinstance(x, str) and x for x in (customer_id, topic_id, delivery_id, receiver_id)):
            return {"ok": False, "reason": "not_a_managed_callback"}
        if not isinstance(auth_seq, int):
            return {"ok": False, "reason": "missing_authorization_seq"}
        with self._lock, _connect(self._path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            seal = conn.execute(
                "SELECT watermark FROM erasure_seals WHERE customer_id=? AND topic_id=?",
                (customer_id, topic_id),
            ).fetchone()
            if seal is not None and auth_seq <= int(seal["watermark"]):
                conn.execute("COMMIT")
                # Erased and sealed: a pre-withdrawal or in-flight callback must
                # not recreate the copy even though its request arrived late.
                return {
                    "ok": True,
                    "stored": False,
                    "reason": "sealed",
                    "watermark": int(seal["watermark"]),
                }
            conn.execute(
                """INSERT INTO managed_replicas
                       (customer_id, topic_id, delivery_id, receiver_id,
                        authorization_seq, payload_json, stored_at)
                   VALUES (?,?,?,?,?,?,?)
                   ON CONFLICT(customer_id, topic_id, delivery_id) DO UPDATE SET
                       authorization_seq = MAX(managed_replicas.authorization_seq, excluded.authorization_seq),
                       payload_json = excluded.payload_json""",
                (
                    customer_id,
                    topic_id,
                    delivery_id,
                    receiver_id,
                    auth_seq,
                    json.dumps(body.get("payload"), ensure_ascii=False, sort_keys=True),
                    now,
                ),
            )
            conn.execute("COMMIT")
        return {"ok": True, "stored": True}

    # ----------------------------------------------------------- erasure
    def erase_topic(self, body: dict[str, Any], now: float) -> dict[str, Any]:
        """Handle one erasure request and return the acknowledgement body.

        Acknowledges only after the (customer, topic) scope has no managed
        copies and is sealed at the request's watermark.  Repeating the same
        operation id returns the same acknowledgement without touching other
        scopes.
        """
        operation_id = body.get("operation_id")
        customer_id = body.get("customer_id")
        topic_id = body.get("topic_id")
        receiver_id = body.get("receiver_id")
        watermark = body.get("watermark")
        if not all(isinstance(x, str) and x for x in (operation_id, customer_id, topic_id, receiver_id)):
            return {"acknowledged": False, "reason": "malformed_erasure_request"}
        if not isinstance(watermark, int):
            return {"acknowledged": False, "reason": "missing_watermark"}
        with self._lock, _connect(self._path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                """SELECT * FROM erasure_seals
                   WHERE customer_id=? AND topic_id=? AND operation_id=?""",
                (customer_id, topic_id, operation_id),
            ).fetchone()
            if existing is not None:
                # Idempotent repeat: same operation already acknowledged.
                conn.execute("COMMIT")
                return {
                    "acknowledged": True,
                    "operation_id": operation_id,
                    "customer_id": customer_id,
                    "topic_id": topic_id,
                    "receiver_id": receiver_id,
                    "watermark": int(existing["watermark"]),
                    "deleted": 0,
                    "repeated": True,
                }
            # Scope-limited deletion: never a different customer or topic.
            deleted = conn.execute(
                "DELETE FROM managed_replicas WHERE customer_id=? AND topic_id=?",
                (customer_id, topic_id),
            ).rowcount or 0
            remaining = int(
                conn.execute(
                    "SELECT COUNT(*) AS c FROM managed_replicas WHERE customer_id=? AND topic_id=?",
                    (customer_id, topic_id),
                ).fetchone()["c"]
            )
            if remaining != 0:  # pragma: no cover - defensive
                conn.execute("ROLLBACK")
                return {"acknowledged": False, "reason": "replicas_remain"}

            # Seal the scope.  A re-run for this same (customer, topic) keeps the
            # highest watermark so older, still-in-flight callbacks stay blocked.
            prior = conn.execute(
                "SELECT watermark FROM erasure_seals WHERE customer_id=? AND topic_id=?",
                (customer_id, topic_id),
            ).fetchone()
            effective = max(watermark, int(prior["watermark"]) if prior else watermark)
            if prior is None:
                conn.execute(
                    """INSERT INTO erasure_seals
                           (operation_id, customer_id, topic_id, receiver_id,
                            watermark, acknowledged_at)
                       VALUES (?,?,?,?,?,?)""",
                    (operation_id, customer_id, topic_id, receiver_id, effective, now),
                )
            else:
                # Update the existing scope seal (operation id stays the first
                # acknowledged one for idempotency of that specific repeat).
                conn.execute(
                    """UPDATE erasure_seals
                       SET watermark=?, acknowledged_at=?, receiver_id=?
                       WHERE customer_id=? AND topic_id=?""",
                    (effective, now, receiver_id, customer_id, topic_id),
                )
                conn.execute(
                    """INSERT OR IGNORE INTO erasure_seals
                           (operation_id, customer_id, topic_id, receiver_id,
                            watermark, acknowledged_at)
                       SELECT ?, customer_id, topic_id, receiver_id, watermark, acknowledged_at
                       FROM erasure_seals WHERE customer_id=? AND topic_id=?""",
                    (operation_id, customer_id, topic_id),
                )
            conn.execute("COMMIT")
        return {
            "acknowledged": True,
            "operation_id": operation_id,
            "customer_id": customer_id,
            "topic_id": topic_id,
            "receiver_id": receiver_id,
            "watermark": effective,
            "deleted": deleted,
            "repeated": False,
        }

    # ------------------------------------------------------------- read side
    def list_replicas(self, *, customer_id: str | None = None, topic_id: str | None = None) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if customer_id:
            clauses.append("customer_id=?")
            params.append(customer_id)
        if topic_id:
            clauses.append("topic_id=?")
            params.append(topic_id)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with _connect(self._path) as conn:
            rows = conn.execute(
                f"SELECT * FROM managed_replicas{where} ORDER BY stored_at", params
            ).fetchall()
        return [
            {
                "customer_id": r["customer_id"],
                "topic_id": r["topic_id"],
                "delivery_id": r["delivery_id"],
                "receiver_id": r["receiver_id"],
                "authorization_seq": int(r["authorization_seq"]),
                "payload": json.loads(r["payload_json"]) if r["payload_json"] else None,
                "stored_at": r["stored_at"],
            }
            for r in rows
        ]

    def list_seals(self) -> list[dict[str, Any]]:
        with _connect(self._path) as conn:
            rows = conn.execute("SELECT * FROM erasure_seals ORDER BY acknowledged_at").fetchall()
        return [
            {
                "operation_id": r["operation_id"],
                "customer_id": r["customer_id"],
                "topic_id": r["topic_id"],
                "receiver_id": r["receiver_id"],
                "watermark": int(r["watermark"]),
                "acknowledged_at": r["acknowledged_at"],
            }
            for r in rows
        ]


def build_handler(backend: ReferenceErasureReceiver):
    import time

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args: object) -> None:  # keep demo/test output quiet
            return

        def _read_json(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length", "0") or 0)
            raw = self.rfile.read(length) if length else b"{}"
            try:
                data = json.loads(raw)
            except ValueError:
                return {}
            return data if isinstance(data, dict) else {}

        def _send(self, status: int, body: dict[str, Any]) -> None:
            payload = json.dumps(body).encode("utf-8")
            try:
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def do_POST(self) -> None:  # noqa: N802 (http.server API)
            body = self._read_json()
            if self.path.rstrip("/").endswith("/erase") or body.get("action") == "erase_topic_replicas":
                self._send(200, backend.erase_topic(body, time.time()))
                return
            # Otherwise: a managed topic callback.
            result = backend.store_callback(body, time.time())
            self._send(200, result)

        def do_GET(self) -> None:  # noqa: N802
            # Introspection only; not part of the protocol.
            self._send(
                200,
                {
                    "replicas": backend.list_replicas(),
                    "seals": backend.list_seals(),
                },
            )

    return Handler


class ReferenceErasureReceiverServer:
    """HTTP wrapper around :class:`ReferenceErasureReceiver`."""

    def __init__(self, database_path: str, host: str = "127.0.0.1", port: int = 0) -> None:
        self.backend = ReferenceErasureReceiver(database_path)
        self._host = host
        self._requested_port = port
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def url(self) -> str:
        if self._server is None:
            raise RuntimeError("receiver is not started")
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    @property
    def port(self) -> int:
        assert self._server is not None
        return int(self._server.server_address[1])

    def start(self) -> "ReferenceErasureReceiverServer":
        server = ThreadingHTTPServer((self._host, self._requested_port), build_handler(self.backend))
        server.daemon_threads = True
        self._server = server
        self._thread = threading.Thread(target=server.serve_forever, name="erasure-receiver", daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    def __enter__(self) -> "ReferenceErasureReceiverServer":
        return self.start()

    def __exit__(self, *_exc: object) -> None:
        self.stop()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Reference participating receiver for the external-replica erasure protocol."
    )
    parser.add_argument("--db", default="reference_receiver.db", help="persistent copy/seal database")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8100)
    args = parser.parse_args(argv)
    server = ReferenceErasureReceiverServer(args.db, host=args.host, port=args.port)
    server.start()
    print(f"reference erasure receiver listening on {server.url}  (db={args.db})")
    try:
        while True:
            server._thread.join(timeout=1.0) if server._thread else None
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
