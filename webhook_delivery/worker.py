"""Standalone delivery worker(s).

Run one or more of these against the same SQLite file::

    python -m webhook_delivery.worker --db webhook.db
    python -m webhook_delivery.worker --db webhook.db   # second process

Each process only advances work for which it holds a valid lease.  Killing a
process mid-request leaves a lease that expires; another process (or the same
one after restart) recovers and retries the delivery.  The retried request
carries the same ``delivery_id``, so receivers can de-duplicate.

Time control
------------
``--backoff 0.05,0.2,0.8`` compresses the production 1s/4s/16s schedule for
demos.  ``--request-timeout`` does the same for HTTP timeouts.  Tests can
instead inject a :class:`~webhook_delivery.config.FakeClock` and a fake sender
through :class:`Worker`, so the *exact* 1/4/16 schedule is verified without
real waiting.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Protocol

import httpx

from .config import Clock, DeliveryPolicy, RealClock, Settings
from .store import ClaimedDelivery, ClaimedErasure, Store
from .util import canonical_json

log = logging.getLogger("webhook_delivery.worker")

# Version marker the receiver echoes when it has durably erased + sealed.
ERASURE_PROTOCOL_VERSION = "1"


@dataclass(frozen=True)
class SendResult:
    outcome: str  # succeeded | failed_http | timeout | transport_error
    http_status: int | None = None
    error: str | None = None


@dataclass(frozen=True)
class ErasureResult:
    acknowledged: bool
    http_status: int | None = None
    error: str | None = None


class Sender(Protocol):
    def send(self, claim: ClaimedDelivery, timeout: float) -> SendResult: ...


class ErasureSender(Protocol):
    def send_erasure(self, claim: ClaimedErasure, timeout: float) -> ErasureResult: ...


class HttpxSender:
    """POSTs the event.  A fresh client per thread keeps worker threads simple."""

    def __init__(self, connect_timeout: float | None = None) -> None:
        self._connect_timeout = connect_timeout
        self._local = threading.local()
        self._clients: list[httpx.Client] = []
        self._clients_lock = threading.Lock()

    def _client(self) -> httpx.Client:
        client = getattr(self._local, "client", None)
        if client is None:
            client = httpx.Client(follow_redirects=False)
            self._local.client = client
            with self._clients_lock:
                self._clients.append(client)
        return client

    def send(self, claim: ClaimedDelivery, timeout: float) -> SendResult:
        payload = {
            "delivery_id": claim.id,
            "event_id": claim.event_id,
            "subscription_id": claim.subscription_id,
            "customer_id": claim.customer_id,
            "flow_id": claim.flow_id,
            "seq": claim.seq,
            "caller_event_id": claim.caller_event_id,
            "payload": json.loads(claim.payload_json),
        }
        headers = {
            "Content-Type": "application/json",
            # The stable idempotency key: identical on every retry of this
            # delivery, different for every other delivery.
            "X-Webhook-Delivery-Id": claim.id,
            "X-Webhook-Event-Id": claim.event_id,
            "X-Webhook-Subscription-Id": claim.subscription_id,
            "X-Webhook-Flow-Id": claim.flow_id,
            "X-Webhook-Seq": str(claim.seq),
            "X-Webhook-Target-Version": str(claim.target_version),
        }
        if claim.erasure_receiver_id is not None and claim.topic_id is not None:
            # Participating receiver: the callback must let it identify the
            # owning customer, the topic, and the original delivery id, so it
            # can keep a manageable replica of exactly this topic.  The
            # registration id also disambiguates several registrations that
            # happen to share a base URL.
            payload["topic_id"] = claim.topic_id
            payload["erasure_receiver_id"] = claim.erasure_receiver_id
            payload["authorization_seq"] = claim.auth_seq
            headers["X-Webhook-Topic-Id"] = claim.topic_id
            headers["X-Webhook-Erasure-Receiver-Id"] = claim.erasure_receiver_id
            headers["X-Webhook-Authorization-Seq"] = str(claim.auth_seq)
        body = canonical_json(payload).encode("utf-8")
        if self._connect_timeout is not None:
            limits = httpx.Timeout(timeout, connect=min(self._connect_timeout, timeout))
        else:
            limits = httpx.Timeout(timeout)
        try:
            response = self._client().post(
                claim.target_url,
                content=body,
                headers=headers,
                timeout=limits,
            )
        except httpx.TimeoutException as exc:
            return SendResult("timeout", None, type(exc).__name__)
        except httpx.TransportError as exc:
            return SendResult("transport_error", None, str(exc) or type(exc).__name__)
        except Exception as exc:  # pragma: no cover - defensive
            return SendResult("transport_error", None, repr(exc))
        # 2xx means success; a redirect is *not* followed and counts as failure
        # so that a misconfigured target does not silently deliver elsewhere.
        if 200 <= response.status_code < 300:
            return SendResult("succeeded", response.status_code)
        return SendResult("failed_http", response.status_code, response.text[:300] or None)

    def send_erasure(self, claim: ClaimedErasure, timeout: float) -> ErasureResult:
        """POST an erasure request.  It carries no event body, content digest or
        excerpt — only the customer, topic and operation identity — and is not
        a redelivery of the original event."""
        payload = {
            "protocol": "webhook-erasure",
            "protocol_version": ERASURE_PROTOCOL_VERSION,
            "operation_id": claim.id,
            "customer_id": claim.customer_id,
            "topic_id": claim.topic_id,
            "receiver_id": claim.receiver_id,
            "watermark": claim.watermark,
            "action": "erase_topic_replicas",
        }
        headers = {
            "Content-Type": "application/json",
            "X-Webhook-Erasure-Protocol": ERASURE_PROTOCOL_VERSION,
            "X-Webhook-Erasure-Operation-Id": claim.id,
            "X-Webhook-Erasure-Customer-Id": claim.customer_id,
            "X-Webhook-Erasure-Topic-Id": claim.topic_id,
            "X-Webhook-Erasure-Receiver-Id": claim.receiver_id,
            "X-Webhook-Erasure-Watermark": str(claim.watermark),
        }
        body = canonical_json(payload).encode("utf-8")
        if self._connect_timeout is not None:
            limits = httpx.Timeout(timeout, connect=min(self._connect_timeout, timeout))
        else:
            limits = httpx.Timeout(timeout)
        try:
            response = self._client().post(
                claim.receiver_url,
                content=body,
                headers=headers,
                timeout=limits,
            )
        except httpx.TimeoutException as exc:
            return ErasureResult(False, None, type(exc).__name__)
        except httpx.TransportError as exc:
            return ErasureResult(False, None, str(exc) or type(exc).__name__)
        except Exception as exc:  # pragma: no cover - defensive
            return ErasureResult(False, None, repr(exc))
        if 200 <= response.status_code < 300:
            # Only an explicit positive acknowledgement of *this* operation id
            # counts.  The receiver returns it in the body; a bare 2xx that
            # does not confirm this operation is not treated as confirmation.
            try:
                data = response.json()
            except ValueError:
                return ErasureResult(
                    False, response.status_code, "2xx without a JSON acknowledgement"
                )
            if isinstance(data, dict) and data.get("acknowledged") is True and str(
                data.get("operation_id")
            ) == claim.id:
                return ErasureResult(True, response.status_code)
            return ErasureResult(
                False,
                response.status_code,
                "2xx did not acknowledge this erasure operation",
            )
        return ErasureResult(False, response.status_code, response.text[:300] or None)

    def close(self) -> None:
        with self._clients_lock:
            clients = list(self._clients)
            self._clients.clear()
        for client in clients:
            try:
                client.close()
            except Exception:  # pragma: no cover - defensive
                pass
        self._local.client = None


class Worker:
    def __init__(
        self,
        *,
        store: Store,
        policy: DeliveryPolicy,
        clock: Clock,
        sender: Sender | None = None,
        concurrency: int = 1,
        poll_seconds: float | None = None,
    ) -> None:
        self.store = store
        self.policy = policy
        self.clock = clock
        self.sender = sender or HttpxSender(policy.settings.http_connect_timeout_seconds)
        self.concurrency = max(1, concurrency)
        self.poll_seconds = policy.settings.worker_poll_seconds if poll_seconds is None else poll_seconds
        self._stop = threading.Event()
        self._inflight: set[Future[None]] = set()
        self._inflight_lock = threading.Lock()
        self._executor: ThreadPoolExecutor | None = None
        self._erasure_sender_obj: ErasureSender | None = None

    # ----------------------------------------------------------- lifecycle
    def stop(self) -> None:
        self._stop.set()

    def _handle_signal(self, signum: int, _frame: object) -> None:
        log.info("received signal %s, finishing in-flight attempts then exiting", signum)
        self.stop()

    def install_signal_handlers(self) -> None:
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    # -------------------------------------------------------------- one unit
    def attempt(self, claim: ClaimedDelivery) -> str:
        # Withdrawal fence: the claim happened earlier (possibly in another
        # process/iteration).  This write transaction is serialised against
        # topic withdrawal; if withdrawal won, no HTTP request may be sent.
        # Stores without the gate (test doubles) keep the old behaviour.
        gate = getattr(self.store, "begin_send", None)
        if callable(gate):
            stamped = gate(claim, now=self.clock.now())
            if stamped is None:
                log.info(
                    "delivery=%s skipped at send gate (topic withdrawn or lease lost)",
                    claim.id,
                )
                return "not_sent"
            claim = stamped
        result = self.sender.send(claim, self.policy.settings.request_timeout_seconds)
        retry_delay = None
        if result.outcome != "succeeded":
            retry_delay = self.policy.delay_after_failure(claim.attempts_count)
        return self.store.finish_attempt(
            claim,
            outcome=result.outcome,
            http_status=result.http_status,
            error=result.error,
            now=self.clock.now(),
            retry_delay=retry_delay,
        )

    def _run_attempt(self, claim: ClaimedDelivery) -> None:
        try:
            outcome = self.attempt(claim)
            log.info(
                "delivery=%s sub=%s flow=%s seq=%s attempt=%s -> %s",
                claim.id,
                claim.subscription_id,
                claim.flow_id,
                claim.seq,
                claim.attempts_count,
                outcome,
            )
        except Exception:  # pragma: no cover - leasing makes retries safe
            log.exception("attempt crashed for delivery=%s", claim.id)

    # ------------------------------------------------- external erasures
    def attempt_erasure(self, claim: ClaimedErasure) -> str:
        # ``_erasure_sender`` reuses an injected sender that can erase, or falls
        # back to the real HTTP sender (so a custom event-only sender still gets
        # working erasures in production).
        result = self._erasure_sender().send_erasure(
            claim, self.policy.settings.request_timeout_seconds
        )
        retry_delay = None
        if not result.acknowledged:
            retry_delay = self.policy.delay_after_failure(claim.attempts_count)
        return self.store.finish_erasure(
            claim,
            acknowledged=result.acknowledged,
            http_status=result.http_status,
            error=result.error,
            now=self.clock.now(),
            retry_delay=retry_delay,
        )

    def _erasure_sender(self) -> ErasureSender:
        # Reuse the injected sender when it can send erasures (test doubles,
        # custom transports); otherwise fall back to the real HTTP sender.
        if callable(getattr(self.sender, "send_erasure", None)):
            return self.sender  # type: ignore[return-value]
        sender = self._erasure_sender_obj
        if sender is None:
            sender = HttpxSender(self.policy.settings.http_connect_timeout_seconds)
            self._erasure_sender_obj = sender
        return sender

    def _run_erasure(self, claim: ClaimedErasure) -> None:
        try:
            outcome = self.attempt_erasure(claim)
            log.info(
                "erasure=%s receiver=%s topic=%s attempt=%s -> %s",
                claim.id,
                claim.receiver_id,
                claim.topic_id,
                claim.attempts_count,
                outcome,
            )
        except Exception:  # pragma: no cover - leasing makes retries safe
            log.exception("erasure attempt crashed for %s", claim.id)

    def recover_erasures(self) -> int:
        recover = getattr(self.store, "recover_expired_erasure_leases", None)
        if not callable(recover):
            return 0
        recovered = recover(now=self.clock.now())
        if recovered:
            log.warning("recovered %d erasure lease(s) that expired mid-attempt", recovered)
        return recovered

    def run_erasures_once(self, *, limit: int | None = None) -> int:
        """Claim and synchronously process one batch of erasure requests.

        Erasures are a separate work queue from deliveries: one receiver being
        slow, failing or timing out never blocks another receiver's erasure or
        any unrelated topic's delivery.
        """
        self.recover_erasures()
        claimer = getattr(self.store, "claim_due_erasures", None)
        if not callable(claimer):
            return 0
        claims = claimer(
            now=self.clock.now(),
            lease_seconds=self.policy.settings.lease_seconds,
            limit=limit if limit is not None else self.concurrency,
        )
        for claim in claims:
            self._run_erasure(claim)
        return len(claims)

    # ------------------------------------------------------------- pumping
    def recover(self) -> int:
        recovered = self.store.recover_expired_leases(now=self.clock.now())
        if recovered:
            log.warning("recovered %d delivery lease(s) that expired mid-attempt", recovered)
        return recovered

    @property
    def inflight_count(self) -> int:
        with self._inflight_lock:
            return len(self._inflight)

    def run_once(self, *, max_claims: int | None = None) -> int:
        """Claim and synchronously process one batch.  Returns claims made."""
        self.recover()
        self.recover_erasures()
        # Erasure requests are processed independently of delivery claims, so a
        # full delivery queue (or a sender with no free slot) never starves
        # them, and a slow erasure never blocks a delivery.
        self.run_erasures_once(limit=max_claims)
        free = self.concurrency - self.inflight_count
        if free <= 0:
            return 0
        limit = free if max_claims is None else min(free, max_claims)
        claims = self.store.claim_due(
            now=self.clock.now(),
            lease_seconds=self.policy.settings.lease_seconds,
            limit=limit,
        )
        for claim in claims:
            self._run_attempt(claim)
        return len(claims)

    def run_until_idle(self, *, max_iterations: int = 1000) -> None:
        """Synchronous, deterministic pump.  Useful in tests with a fake clock."""
        for _ in range(max_iterations):
            if self.run_once() == 0:
                if (
                    self.store.next_wakeup(now=self.clock.now()) is None
                    and getattr(self.store, "next_erasure_wakeup", lambda **_k: None)(
                        now=self.clock.now()
                    )
                    is None
                ):
                    return
                break
            if self._stop.is_set():
                return

    def run_forever(self) -> None:
        self._executor = ThreadPoolExecutor(max_workers=self.concurrency, thread_name_prefix="delivery")
        try:
            while not self._stop.is_set():
                self.recover()
                self.recover_erasures()
                free = self.concurrency - self.inflight_count
                if free > 0:
                    claims = self.store.claim_due(
                        now=self.clock.now(),
                        lease_seconds=self.policy.settings.lease_seconds,
                        limit=free,
                    )
                    for claim in claims:
                        future = self._executor.submit(self._run_attempt, claim)
                        with self._inflight_lock:
                            self._inflight.add(future)
                        future.add_done_callback(self._forget)
                # Erasures use their own claim path and are run synchronously in
                # this loop, so they proceed even while every delivery slot is
                # busy.
                self.run_erasures_once()
                self._prune_finished()
                self._sleep_until_next_event()
        finally:
            # Graceful stop: let in-flight attempts report; anything that does
            # not finish before the lease expires is recovered by a peer.
            self._stop.set()
            if self._executor is not None:
                self._executor.shutdown(wait=True, cancel_futures=False)
                self._executor = None
            close = getattr(self.sender, "close", None)
            if callable(close):
                close()

    def _forget(self, future: Future[None]) -> None:
        with self._inflight_lock:
            self._inflight.discard(future)

    def _prune_finished(self) -> None:
        with self._inflight_lock:
            for future in [f for f in self._inflight if f.done()]:
                self._inflight.discard(future)

    def _sleep_until_next_event(self) -> None:
        if self.inflight_count >= self.concurrency:
            # All slots busy: poll for a completion promptly.
            self.clock.sleep(min(self.poll_seconds, 0.05))
            return
        now = self.clock.now()
        wake = self.store.next_wakeup(now=now)
        erasure_wake = getattr(self.store, "next_erasure_wakeup", None)
        if callable(erasure_wake):
            ew = erasure_wake(now=now)
            wake = ew if wake is None else (wake if ew is None else min(wake, ew))
        if wake is None:
            self.clock.sleep(self.poll_seconds)
            return
        wait = min(max(0.0, wake - now), self.poll_seconds)
        if wait > 0:
            self.clock.sleep(wait)


def _parse_backoff(text: str) -> tuple[float, ...]:
    parts = [p.strip() for p in text.split(",") if p.strip()]
    if not parts:
        raise argparse.ArgumentTypeError("backoff must be a comma-separated list of seconds")
    return tuple(float(p) for p in parts)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a webhook delivery worker process.")
    parser.add_argument("--db", default="webhook_delivery.db", help="SQLite database path")
    parser.add_argument(
        "--backoff",
        type=_parse_backoff,
        default=(1.0, 4.0, 16.0),
        help="comma-separated waits after failed attempts (default: 1,4,16)",
    )
    parser.add_argument("--request-timeout", type=float, default=5.0)
    parser.add_argument("--lease", type=float, default=30.0)
    parser.add_argument("--poll", type=float, default=0.2)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--once", action="store_true", help="drain ready work and exit")
    parser.add_argument("--max-attempts", type=int, default=4)
    parser.add_argument("--verbose", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    settings = Settings(
        database_path=args.db,
        backoff_seconds=args.backoff,
        request_timeout_seconds=args.request_timeout,
        lease_seconds=args.lease,
        worker_poll_seconds=args.poll,
        max_attempts=args.max_attempts,
    )
    store = Store(settings)
    store.initialize()
    worker = Worker(
        store=store,
        policy=DeliveryPolicy(settings),
        clock=RealClock(),
        concurrency=args.concurrency,
    )
    if args.once:
        worker.run_until_idle()
        return 0
    worker.install_signal_handlers()
    log.info("worker started db=%s concurrency=%s backoff=%s", args.db, args.concurrency, args.backoff)
    worker.run_forever()
    log.info("worker stopped")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
