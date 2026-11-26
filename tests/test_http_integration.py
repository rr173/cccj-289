"""Real-socket integration tests: actual HTTP, actual timeouts, actual threads.

These are slower than the fake-clock tests but still finish in seconds because
the back-off schedule and timeouts are compressed for the test.
"""

from __future__ import annotations

import time

import pytest

from webhook_delivery.config import DeliveryPolicy, RealClock, Settings
from webhook_delivery.receiver import Receiver, Response
from webhook_delivery.store import Store
from webhook_delivery.worker import Worker


@pytest.fixture()
def http_settings(tmp_path) -> Settings:
    return Settings(
        database_path=str(tmp_path / "http.db"),
        backoff_seconds=(0.02, 0.02, 0.02),
        request_timeout_seconds=0.3,
        lease_seconds=5.0,
        worker_poll_seconds=0.01,
        max_attempts=3,
    )


@pytest.fixture()
def http_store(http_settings) -> Store:
    store = Store(http_settings)
    store.initialize()
    return store


def wait_for_status(store: Store, delivery_id: str, wanted: set[str], timeout: float = 10.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        detail = store.get_delivery(delivery_id)
        if detail["status"] in wanted:
            return detail
        time.sleep(0.01)
    raise AssertionError(f"stuck in {store.get_delivery(delivery_id)['status']}")


def drive(store, settings, worker: Worker, delivery_id: str, *statuses: str, timeout: float = 10.0) -> dict:
    """Pump the worker until the delivery reaches one of *statuses*."""
    wanted = set(statuses)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        worker.run_once()
        detail = store.get_delivery(delivery_id)
        if detail["status"] in wanted:
            return detail
        time.sleep(0.01)
    raise AssertionError(f"stuck in {store.get_delivery(delivery_id)['status']}")


def make_delivery(store: Store, customer: dict, target_url: str, *, flow="f", caller="e"):
    sub = store.create_subscription(customer_id=customer["id"], target_url=target_url, now=time.time())
    ev = store.ingest_event(
        customer_id=customer["id"], flow_id=flow, caller_event_id=caller, payload={"a": 1}, now=time.time()
    )
    dlv = [d["delivery_id"] for d in ev["deliveries"] if d["subscription_id"] == sub["id"]][0]
    return sub, dlv


def test_real_http_retry_then_success(http_store, http_settings):
    recv = Receiver().start()
    try:
        recv.program(None, [Response(status=503), Response(status=200)])
        customer = http_store.create_customer(name="c", now=time.time())
        sub, dlv = make_delivery(http_store, customer, recv.url)
        worker = Worker(
            store=http_store,
            policy=DeliveryPolicy(http_settings),
            clock=RealClock(),
            concurrency=1,
            poll_seconds=0.01,
        )
        detail = drive(http_store, http_settings, worker, dlv, "succeeded")
        assert [a["outcome"] for a in detail["attempts"]] == ["failed_http", "succeeded"]
        assert [a["http_status"] for a in detail["attempts"]] == [503, 200]
        assert len(recv.records_for(dlv)) == 2
        assert {r.delivery_id for r in recv.records_for(dlv)} == {dlv}
        # Request carries the stable headers the receiver de-dupes on.
        record = recv.records[0]
        assert record.headers["x-webhook-delivery-id"] == dlv
        assert record.headers["x-webhook-flow-id"] == "f"
        assert record.headers["x-webhook-seq"] == "1"
        assert record.body["payload"] == {"a": 1}
        worker.sender.close()
    finally:
        recv.stop()


def test_real_http_timeout_is_retried(http_store, http_settings):
    recv = Receiver().start()
    try:
        recv.program(None, [Response(status=200, delay_seconds=0.9), Response(status=200)])
        customer = http_store.create_customer(name="c", now=time.time())
        sub, dlv = make_delivery(http_store, customer, recv.url)
        worker = Worker(
            store=http_store,
            policy=DeliveryPolicy(http_settings),
            clock=RealClock(),
            concurrency=1,
            poll_seconds=0.01,
        )
        detail = drive(http_store, http_settings, worker, dlv, "succeeded", timeout=15)
        assert detail["attempts"][0]["outcome"] == "timeout"
        assert detail["attempts"][1]["outcome"] == "succeeded"
        worker.sender.close()
    finally:
        recv.stop()


def test_connection_refused_is_retried_then_parked(http_store, http_settings):
    # Bind and close a port so nothing listens there.
    import socket

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    customer = http_store.create_customer(name="c", now=time.time())
    sub, dlv = make_delivery(http_store, customer, f"http://127.0.0.1:{port}/hook")
    worker = Worker(
        store=http_store,
        policy=DeliveryPolicy(http_settings),
        clock=RealClock(),
        concurrency=1,
        poll_seconds=0.01,
    )
    detail = drive(http_store, http_settings, worker, dlv, "needs_attention", timeout=15)
    assert len(detail["attempts"]) == 3
    assert all(a["outcome"] == "transport_error" for a in detail["attempts"])
    worker.sender.close()


def test_redirect_is_not_success(http_store, http_settings):
    recv = Receiver().start()
    try:
        recv.program(None, [Response(status=302), Response(status=200)])
        customer = http_store.create_customer(name="c", now=time.time())
        sub, dlv = make_delivery(http_store, customer, recv.url)
        worker = Worker(
            store=http_store,
            policy=DeliveryPolicy(http_settings),
            clock=RealClock(),
            concurrency=1,
            poll_seconds=0.01,
        )
        detail = drive(http_store, http_settings, worker, dlv, "succeeded")
        assert detail["attempts"][0]["outcome"] == "failed_http"
        assert detail["attempts"][0]["http_status"] == 302
        worker.sender.close()
    finally:
        recv.stop()


def test_two_worker_threads_only_one_sends(http_store, http_settings):
    """Two Worker objects (as two processes) polling the same DB."""
    recv = Receiver().start()
    try:
        recv.program(None, [Response(status=200, delay_seconds=0.2)])
        customer = http_store.create_customer(name="c", now=time.time())
        sub, dlv = make_delivery(http_store, customer, recv.url)
        worker_a = Worker(
            store=http_store, policy=DeliveryPolicy(http_settings), clock=RealClock(), concurrency=1, poll_seconds=0.01
        )
        other_store = Store(http_settings)
        other_store.initialize()
        worker_b = Worker(
            store=other_store, policy=DeliveryPolicy(http_settings), clock=RealClock(), concurrency=1, poll_seconds=0.01
        )
        # Interleave both workers; the second must find nothing to claim while
        # the lease is live, so exactly one HTTP request is sent.
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            worker_a.run_once()
            worker_b.run_once()
            if http_store.get_delivery(dlv)["status"] == "succeeded":
                break
            time.sleep(0.01)
        detail = http_store.get_delivery(dlv)
        assert detail["status"] == "succeeded"
        assert len(recv.records_for(dlv)) == 1
        assert len(detail["attempts"]) == 1
        worker_a.sender.close()
        worker_b.sender.close()
    finally:
        recv.stop()
