from __future__ import annotations

import pytest

from webhook_delivery.config import FakeClock, Settings
from webhook_delivery.store import Store
from tests.helpers import ScriptedSender


@pytest.fixture()
def settings(tmp_path) -> Settings:
    return Settings(
        database_path=str(tmp_path / "test.db"),
        backoff_seconds=(1.0, 4.0, 16.0),
        request_timeout_seconds=1.0,
        lease_seconds=30.0,
        worker_poll_seconds=0.01,
        max_attempts=4,
    )


@pytest.fixture()
def store(settings) -> Store:
    store = Store(settings)
    store.initialize()
    return store


@pytest.fixture()
def clock() -> FakeClock:
    return FakeClock(start=1_000_000.0)


@pytest.fixture()
def customer(store, clock) -> dict:
    return store.create_customer(name="Acme", now=clock.now())


@pytest.fixture()
def make_subscription(store, clock):
    def _make(customer_id: str, target_url: str = "http://receiver.test/hook") -> dict:
        return store.create_subscription(customer_id=customer_id, target_url=target_url, now=clock.now())

    return _make


@pytest.fixture()
def subscribe(customer, make_subscription):
    return make_subscription(customer["id"])


@pytest.fixture()
def sender(clock) -> ScriptedSender:
    sender = ScriptedSender()
    sender.clock = clock
    return sender


@pytest.fixture()
def sender_factory(clock):
    """Returns ``name -> ScriptedSender`` so one test can run several workers."""
    senders: dict[str, ScriptedSender] = {}

    def _make(name: str) -> ScriptedSender:
        if name in senders:
            return senders[name]
        sender = ScriptedSender()
        sender.label = name
        sender.clock = clock
        senders[name] = sender
        return sender

    _make.senders = senders  # type: ignore[attr-defined]
    return _make
