"""Host/process descriptor stamped onto root HTTP spans."""

from __future__ import annotations

import os
import platform
import socket
import threading
import time

__version__ = "0.3.0"

_lock = threading.Lock()
_attrs: list[tuple[str, str]] | None = None
_started = time.time()


def agent_attrs() -> list[tuple[str, str]]:
    global _attrs
    with _lock:
        if _attrs is not None:
            return _attrs
        attrs: list[tuple[str, str]] = []

        def add(k: str, v: str) -> None:
            if v:
                attrs.append((k, v))

        add("agent.os", f"{platform.system().lower()}/{platform.machine().lower()}")
        add("agent.runtime", f"python {platform.python_version()}")
        add("agent.sdk", f"python-sdk/{__version__}")
        try:
            add("agent.cpu", str(os.cpu_count() or 1))
        except Exception:
            pass
        add("agent.pid", str(os.getpid()))
        add("agent.started", str(int(_started * 1000)))
        add("agent.host", socket.gethostname())
        add("agent.env", os.environ.get("DATAFLOW_ENV", ""))
        add("agent.app_version", os.environ.get("DATAFLOW_APP_VERSION", ""))
        _attrs = attrs
        return attrs
