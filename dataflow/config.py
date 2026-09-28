"""SDK configuration: DATAFLOW_* environment plus an explicit configure()."""

from __future__ import annotations

import logging
import os
import socket
from dataclasses import dataclass, field
from typing import List, Optional

log = logging.getLogger("dataflow")


@dataclass
class Settings:
    api_key: str = ""
    endpoint: str = "api.huginnlabs.com:9090"
    service_name: str = ""
    encryption_key: Optional[str] = None
    sensitive_paths: List[str] = field(default_factory=list)
    sample_ratio: float = 1.0
    buffer_size: int = 10000
    max_body_bytes: int = 4096
    insecure: bool = False
    disabled: bool = False


def _load_env() -> Settings:
    def f(name: str, default: float) -> float:
        try:
            return float(os.environ.get(name, "") or default)
        except ValueError:
            return default

    def i(name: str, default: int) -> int:
        try:
            return int(os.environ.get(name, "") or default)
        except ValueError:
            return default

    def b(name: str) -> bool:
        return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")

    csv = [p.strip() for p in os.environ.get("DATAFLOW_SENSITIVE_PATHS", "").split(",") if p.strip()]
    return Settings(
        api_key=os.environ.get("DATAFLOW_API_KEY", ""),
        endpoint=os.environ.get("DATAFLOW_ENDPOINT") or "api.huginnlabs.com:9090",
        service_name=os.environ.get("DATAFLOW_SERVICE_NAME") or socket.gethostname() or "unknown-service",
        encryption_key=os.environ.get("DATAFLOW_ENCRYPTION_KEY") or None,
        sensitive_paths=csv,
        sample_ratio=f("DATAFLOW_SAMPLE_RATIO", 1.0),
        buffer_size=i("DATAFLOW_BUFFER_SIZE", 10000),
        max_body_bytes=i("DATAFLOW_MAX_BODY_BYTES", 4096),
        insecure=b("DATAFLOW_INSECURE"),
        disabled=b("DATAFLOW_DISABLED"),
    )


_settings = _load_env()


def settings() -> Settings:
    """The active configuration (safe to read anywhere)."""
    return _settings


def configure(
    *,
    api_key: Optional[str] = None,
    endpoint: Optional[str] = None,
    service_name: Optional[str] = None,
    encryption_key: Optional[str] = None,
    sensitive_paths: Optional[List[str]] = None,
    sample_ratio: Optional[float] = None,
    buffer_size: Optional[int] = None,
    max_body_bytes: Optional[int] = None,
    insecure: Optional[bool] = None,
) -> None:
    """Override configuration and (re)start the delivery pipeline. Only the
    first call starts the background sender."""
    global _settings
    s = Settings(**vars(_settings))
    if api_key is not None:
        s.api_key = api_key
    if endpoint is not None:
        s.endpoint = endpoint
    if service_name is not None:
        s.service_name = service_name
    if encryption_key is not None:
        s.encryption_key = encryption_key
    if sensitive_paths is not None:
        s.sensitive_paths = list(sensitive_paths)
    if sample_ratio is not None:
        s.sample_ratio = sample_ratio
    if buffer_size is not None:
        s.buffer_size = buffer_size
    if max_body_bytes is not None:
        s.max_body_bytes = max_body_bytes
    if insecure is not None:
        s.insecure = insecure
    _settings = s

    # Deferred import to avoid a cycle (client imports config).
    from .client import ensure_started

    ensure_started()


def enabled() -> bool:
    s = _settings
    return bool(s.api_key) and bool(s.endpoint) and not s.disabled
