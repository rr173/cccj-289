"""Shared test doubles."""

from __future__ import annotations

from webhook_delivery.worker import SendResult


class ScriptedSender:
    """Returns pre-programmed results per delivery, then a default."""

    def __init__(self) -> None:
        self.script: dict[str, list[SendResult]] = {}
        self.results: dict[str, SendResult] = {}
        self.default = SendResult("succeeded", 200)
        self.sent: list[tuple[str, int, float, str, str]] = []  # delivery, attempt, time, url, outcome
        self.clock = None
        self.label = "sender"
        self.on_send = None

    def send(self, claim, timeout: float) -> SendResult:
        script = self.script.get(claim.id)
        if script:
            result = script.pop(0)
        elif claim.id in self.results:
            result = self.results[claim.id]
        else:
            result = self.default
        self.sent.append(
            (
                claim.id,
                claim.attempts_count,
                self.clock.now() if self.clock else 0.0,
                claim.target_url,
                result.outcome,
            )
        )
        if self.on_send is not None:
            self.on_send(claim, result)
        return result
