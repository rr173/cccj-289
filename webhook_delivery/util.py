"""Small shared helpers: IDs, canonical JSON, URL validation and time formatting."""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from urllib.parse import urlsplit


class DomainError(Exception):
    """An expected business-rule failure, mapped to an HTTP error by the API."""

    def __init__(self, message: str, *, status_code: int = 409, code: str = "conflict") -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.code = code


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def canonical_json(value: object) -> str:
    """Stable serialization used both for storage and content hashing."""
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def content_digest(canonical: str) -> str:
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def validate_target_url(url: str) -> str:
    if not isinstance(url, str) or not url.strip():
        raise DomainError("target_url must be a non-empty URL", status_code=400, code="invalid_target_url")
    candidate = url.strip()
    parts = urlsplit(candidate)
    if parts.scheme.lower() not in {"http", "https"} or not parts.hostname:
        raise DomainError(
            "target_url must be an absolute http(s) URL",
            status_code=400,
            code="invalid_target_url",
        )
    # Credentials in target URLs are almost always a configuration mistake and
    # would be echoed in the attempt log; reject them.
    if parts.username is not None or parts.password is not None:
        raise DomainError(
            "target_url must not contain user credentials",
            status_code=400,
            code="invalid_target_url",
        )
    return candidate


def iso_time(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def require_text(value: str, field: str, *, max_length: int = 200) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DomainError(f"{field} must be a non-empty string", status_code=422, code="invalid_request")
    value = value.strip()
    if len(value) > max_length:
        raise DomainError(
            f"{field} must be at most {max_length} characters",
            status_code=422,
            code="invalid_request",
        )
    return value
