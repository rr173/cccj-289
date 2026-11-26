"""Typed configuration with explicit, injectable time control.

Every wall-clock concern in the service goes through a :class:`Clock`.  The
default implementation reads the real clock; tests and the demo can install a
:class:`FakeClock` so that retry back-offs (1s/4s/16s) and lease expiry take
milliseconds instead of minutes.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field


class Clock:
    """Monotonic wall clock.  ``now()`` is always seconds since some epoch."""

    def now(self) -> float:
        return time.time()

    def sleep(self, seconds: float) -> None:
        if seconds > 0:
            time.sleep(seconds)


class FakeClock(Clock):
    """A clock that only advances when told to.

    ``advance`` can optionally sleep in tiny real increments so that background
    worker threads observe the new time promptly (useful for cooperative tests).
    The demo uses this to compress 1+4+16 seconds of delay into milliseconds.
    """

    def __init__(self, start: float = 1_700_000_000.0) -> None:
        self._now = float(start)
        self._lock = threading.Lock()

    def now(self) -> float:
        with self._lock:
            return self._now

    def advance(self, seconds: float) -> None:
        with self._lock:
            self._now += float(seconds)

    def sleep(self, seconds: float) -> None:
        # A fake clock never blocks a real thread for back-off; the caller is
        # expected to advance the clock instead.  Explicitly ignore.
        return None


class RealClock(Clock):
    pass


@dataclass(frozen=True)
class Settings:
    """Runtime knobs.  All production defaults are overridable for the demo.

    ``backoff_seconds`` are the waits *after* failed attempts: the first retry
    happens 1s after failure #1, the second 4s after failure #2, the third 16s
    after failure #3.  After the fourth failed attempt the delivery parks in
    ``needs_attention`` for an administrator.
    """

    database_path: str = "webhook_delivery.db"
    backoff_seconds: tuple[float, ...] = (1.0, 4.0, 16.0)
    request_timeout_seconds: float = 5.0
    lease_seconds: float = 60.0
    worker_poll_seconds: float = 0.2
    http_connect_timeout_seconds: float = 3.0
    # Rejected attempts (HTTP non-2xx, timeout, transport error) before a
    # delivery needs an administrator.  With the default 1/4/16 back-off this
    # means attempts at t=0, t=1, t=5 and t=21, then parking.
    max_attempts: int = 4

    def __post_init__(self) -> None:
        if not self.backoff_seconds:
            raise ValueError("backoff_seconds must not be empty")
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.request_timeout_seconds <= 0:
            raise ValueError("request_timeout_seconds must be > 0")
        if self.lease_seconds <= 0:
            raise ValueError("lease_seconds must be > 0")


@dataclass
class DeliveryPolicy:
    """Per-run holder shared between scheduler and worker loop."""

    settings: Settings = field(default_factory=Settings)

    def delay_after_failure(self, failed_attempt_number: int) -> float | None:
        """Return the wait after failure N, or ``None`` when exhausted.

        ``failed_attempt_number`` is 1-based: the first failed attempt waits
        ``backoff_seconds[0]``.  Attempts at or beyond ``max_attempts`` have no
        further automatic retry and require an administrator.
        """
        if failed_attempt_number >= self.settings.max_attempts:
            return None
        index = min(failed_attempt_number - 1, len(self.settings.backoff_seconds) - 1)
        return float(self.settings.backoff_seconds[index])
