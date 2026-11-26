"""A controllable HTTP receiver used by the demo and tests.

The receiver is a plain ``http.server`` (no framework) so it can be started in
a thread or a separate process.  Behaviour is programmed per request with
:class:`Response` objects, and every received request is recorded — including
duplicates, late requests, and the exact ``X-Webhook-Delivery-Id`` header.

Control rules
-------------
``program(matcher, responses)`` where ``matcher`` is a callable receiving the
request dict and ``responses`` is a list; each request consumes the next
response, with the last one repeating.  ``Response`` can delay N seconds,
return any status code, or "hang" (delay longer than the client timeout).

The receiver deliberately does NOT de-duplicate.  That is the point: the demo
shows the same ``delivery_id`` arriving twice after a crash/retry and proves
the receiver *can* dedupe using it.
"""

from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable

Matcher = Callable[["RecordedRequest"], bool]
Responder = Callable[["RecordedRequest"], "Response"]


@dataclass(frozen=True)
class Response:
    status: int = 200
    delay_seconds: float = 0.0
    body: str = '{"ok":true}'


@dataclass
class RecordedRequest:
    path: str
    headers: dict[str, str]
    body: dict
    received_at: float
    delivery_id: str | None
    event_id: str | None
    flow_id: str | None
    seq: int | None
    target_version: str | None
    response_status: int | None = None

    def summary(self) -> dict:
        return {
            "delivery_id": self.delivery_id,
            "event_id": self.event_id,
            "flow_id": self.flow_id,
            "seq": self.seq,
            "target_version": self.target_version,
            "received_at": round(self.received_at, 4),
            "response_status": self.response_status,
        }


@dataclass
class Rule:
    matcher: Matcher
    responses: list[Response] | Responder
    used: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def next_response(self, request: RecordedRequest) -> Response:
        with self.lock:
            if callable(self.responses):
                return self.responses(request)
            index = min(self.used, len(self.responses) - 1)
            self.used += 1
            return self.responses[index]


class Receiver:
    def __init__(self, host: str = "127.0.0.1", port: int = 0) -> None:
        self._host = host
        self._requested_port = port
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self._rules: list[Rule] = []
        self._rules_lock = threading.Lock()
        self._records: list[RecordedRequest] = []
        self._records_lock = threading.Lock()
        self._default = Response(status=200)
        self._closed = threading.Event()

    # ------------------------------------------------------------------ life
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

    def start(self) -> "Receiver":
        receiver = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_args: object) -> None:  # keep test output quiet
                return

            def do_POST(self) -> None:  # noqa: N802 (http.server API)
                length = int(self.headers.get("Content-Length", "0") or 0)
                raw = self.rfile.read(length) if length else b"{}"
                try:
                    body = json.loads(raw)
                except ValueError:
                    body = {"_raw": raw.decode("utf-8", "replace")}
                record = RecordedRequest(
                    path=self.path,
                    headers={k.lower(): v for k, v in self.headers.items()},
                    body=body,
                    received_at=time.time(),
                    delivery_id=self.headers.get("X-Webhook-Delivery-Id"),
                    event_id=self.headers.get("X-Webhook-Event-Id"),
                    flow_id=self.headers.get("X-Webhook-Flow-Id"),
                    seq=int(self.headers["X-Webhook-Seq"]) if self.headers.get("X-Webhook-Seq") else None,
                    target_version=self.headers.get("X-Webhook-Target-Version"),
                )
                receiver._record(record)
                response = receiver._resolve(record)
                if response.delay_seconds > 0:
                    # Abort promptly when the receiver is shutting down.
                    receiver._closed.wait(timeout=response.delay_seconds)
                record.response_status = response.status
                try:
                    payload = response.body.encode("utf-8")
                    self.send_response(response.status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                except (BrokenPipeError, ConnectionResetError):
                    pass  # client timed out and hung up; that is expected

            def do_GET(self) -> None:  # noqa: N802
                payload = json.dumps({"ok": True}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

        server = ThreadingHTTPServer((self._host, self._requested_port), Handler)
        server.daemon_threads = True
        self._server = server
        self._thread = threading.Thread(target=server.serve_forever, name="receiver", daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self._closed.set()
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    def __enter__(self) -> "Receiver":
        return self.start()

    def __exit__(self, *_exc: object) -> None:
        self.stop()

    # ---------------------------------------------------------------- control
    def program(self, matcher: Matcher | None, responses: list[Response] | Responder) -> None:
        with self._rules_lock:
            self._rules.append(Rule(matcher=matcher or (lambda _r: True), responses=responses))

    def reset(self) -> None:
        with self._rules_lock:
            self._rules.clear()
        with self._records_lock:
            self._records.clear()
        self._default = Response(status=200)

    def set_default(self, response: Response) -> None:
        self._default = response

    def _resolve(self, request: RecordedRequest) -> Response:
        with self._rules_lock:
            rules = list(self._rules)
        for rule in reversed(rules):  # newest rule wins
            try:
                if rule.matcher(request):
                    return rule.next_response(request)
            except Exception:  # pragma: no cover - defensive
                continue
        return self._default

    def _record(self, record: RecordedRequest) -> None:
        with self._records_lock:
            self._records.append(record)

    @property
    def records(self) -> list[RecordedRequest]:
        with self._records_lock:
            return list(self._records)

    def records_for(self, delivery_id: str) -> list[RecordedRequest]:
        return [r for r in self.records if r.delivery_id == delivery_id]

    def wait_for(self, predicate: Callable[[list[RecordedRequest]], bool], *, timeout: float = 5.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate(self.records):
                return True
            time.sleep(0.01)
        return predicate(self.records)


def by_flow(flow_id: str) -> Matcher:
    return lambda r: r.flow_id == flow_id


def by_seq(seq: int) -> Matcher:
    return lambda r: r.seq == seq


def always(*, status: int = 200, delay_seconds: float = 0.0) -> list[Response]:
    return [Response(status=status, delay_seconds=delay_seconds)]
