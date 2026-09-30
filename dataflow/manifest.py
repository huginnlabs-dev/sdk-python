"""Service manifest: one best-effort HTTP POST at startup describing this
service (framework, runtime, dependency inventory from the environment).
The server turns it into the project's service catalog. Failures are silent
— tracing never depends on the manifest reaching the server. Mirrors the
Go SDK's manifest.go wire-for-wire."""

from __future__ import annotations

import importlib.metadata
import json
import os
import platform
import threading
import urllib.request
from typing import List, Optional

from .agent import __version__ as SDK_VERSION
from .config import settings

# Well-known framework dists, checked in priority order; the first match
# wins. Everything else reports as "".
KNOWN_FRAMEWORKS = ("flask", "django", "fastapi", "starlette", "aiohttp", "sanic", "tornado")

# maxManifestDeps caps the reported dependency list; the server enforces the
# same limit.
MAX_DEPS = 500


def build_manifest(service_name: str, sdk_version: str) -> dict:
    """Derive the startup manifest. Pure: reads only the installed
    distributions, platform info and DATAFLOW_APP_VERSION."""
    deps: List[dict] = []
    installed: set = set()
    try:
        for dist in importlib.metadata.distributions():
            meta = dist.metadata
            name = (meta.get("Name") or "").strip()
            if not name:
                continue
            installed.add(name.lower())
            deps.append({"name": name, "version": meta.get("Version") or ""})
    except Exception:  # noqa: BLE001 - inventory is best-effort
        deps = []
        installed = set()
    deps.sort(key=lambda d: d["name"])
    del deps[MAX_DEPS:]

    framework = ""
    for candidate in KNOWN_FRAMEWORKS:
        if candidate in installed:
            framework = candidate
            break

    return {
        "service_name": service_name,
        "language": "python",
        "sdk_version": sdk_version,
        "runtime_version": platform.python_version(),
        "framework": framework,
        "os_arch": f"{platform.system().lower()}/{platform.machine().lower()}",
        "app_version": os.environ.get("DATAFLOW_APP_VERSION", ""),
        "dependencies": deps,
    }


def resolve_http_base(endpoint: str) -> Optional[str]:
    """Resolve the HTTP API base for manifest reporting: an explicit
    DATAFLOW_HTTP_URL wins (needed when the gRPC DATAFLOW_ENDPOINT is a bare
    host:port); URL-form endpoints map directly; a bare gRPC endpoint with
    no override has no derivable HTTP base and reporting is skipped."""
    override = os.environ.get("DATAFLOW_HTTP_URL", "").strip()
    if override:
        return override.rstrip("/")
    endpoint = (endpoint or "").strip()
    if endpoint.startswith(("http://", "https://")):
        return endpoint.rstrip("/")
    return None


def send_manifest() -> None:
    """Report the service manifest once per process. Best-effort: runs on
    its own daemon thread with a short timeout and swallows every failure,
    so startup and tracing are never delayed or disturbed."""
    s = settings()
    base = resolve_http_base(s.endpoint)
    if base is None or not s.api_key:
        return
    api_key = s.api_key

    def _post() -> None:
        try:
            body = json.dumps(build_manifest(s.service_name, SDK_VERSION)).encode("utf-8")
            req = urllib.request.Request(
                base + "/api/v1/manifest",
                data=body,
                headers={"Content-Type": "application/json", "X-Api-Key": api_key},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                resp.read()
        except Exception:  # noqa: BLE001 - manifest reporting is best-effort
            pass

    threading.Thread(target=_post, name="dataflow-manifest", daemon=True).start()
