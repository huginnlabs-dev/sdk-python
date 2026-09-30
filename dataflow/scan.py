"""Static route scanner: ``python -m dataflow.scan``.

Parses the ``*.py`` files under ``--dir`` with the stdlib ``ast`` module —
the scanned code is never imported or executed — and extracts HTTP endpoint
declarations for Flask, FastAPI, Starlette and aiohttp, then posts them to
the Dataflow service catalog (``POST {base}/api/v1/catalog``). Django
URLconfs are intentionally not supported (``urls.py`` is its own
mini-language; see the README).

Exit codes: 0 = ok (posted, printed, or nothing to post), 1 = scan error
(bad ``--dir``), 2 = catalog POST failed (missing API key, network or
non-2xx response).
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, List, Optional, Sequence, Tuple

from .manifest import resolve_http_base

CATALOG_PATH = "/api/v1/catalog"
MAX_ROUTES = 1000  # server-enforced limit; mirrored client-side
HTTP_TIMEOUT = 10.0

# Directories never descended into, on top of every dotted directory
# (.git, .venv, .mypy_cache, ...).
SKIP_DIRS = frozenset(
    {"venv", ".venv", "node_modules", "__pycache__", "site-packages"}
)

# Decorator verbs: @app.get("/p") style (Flask/FastAPI/aiohttp @routes.get —
# the object name is irrelevant, only the attribute matters).
DECORATOR_VERBS = frozenset({"get", "post", "put", "delete", "patch", "head", "options"})

# aiohttp router.add_get("/p", handler) style.
ADD_PREFIX = "add_"


@dataclass(frozen=True)
class Route:
    """One extracted endpoint; source_file is relative to the scan root."""

    method: str
    path: str
    handler: str
    source_file: str


@dataclass
class ScanResult:
    routes: List[Route] = field(default_factory=list)
    files_scanned: int = 0
    parse_errors: int = 0


# -- extraction -------------------------------------------------------------


def _literal_str(node: ast.expr) -> Optional[str]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _handler_name(node: ast.expr) -> Optional[str]:
    """The endpoint reference of a Starlette Route() / aiohttp add_* call:
    a bare name, an attribute (``handlers.show`` -> ``show``) or a handler
    name string. Anything else is not something we can name."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return _literal_str(node)


def _literal_methods(node: ast.expr) -> Optional[List[str]]:
    """``["GET", "POST"]`` -> uppercased verbs. Anything else (a variable, a
    call, a list with a non-string element) -> None: unknown, don't guess."""
    if not isinstance(node, ast.List):
        return None
    out: List[str] = []
    for elt in node.elts:
        s = _literal_str(elt)
        if s is None:
            return None
        out.append(s.upper())
    return out


def _routes_from_decorator(dec: ast.expr) -> List[Tuple[str, str]]:
    """(method, path) pairs from one decorator, or [] when it is not a route
    declaration or its path is not a literal string."""
    if not isinstance(dec, ast.Call) or not isinstance(dec.func, ast.Attribute):
        return []
    attr = dec.func.attr
    if attr == "route" or attr == "api_route":  # Flask / FastAPI
        if not dec.args:
            return []
        path = _literal_str(dec.args[0])
        if path is None:
            return []
        methods: Optional[List[str]] = None
        has_methods_kw = False
        for kw in dec.keywords:
            if kw.arg == "methods":
                has_methods_kw = True
                methods = _literal_methods(kw.value)
                break
        if methods is None:
            if has_methods_kw:
                return []  # methods= present but not a literal list
            methods = ["GET"]  # Flask default when omitted
        return [(m, path) for m in methods]
    if attr in DECORATOR_VERBS:
        if not dec.args:
            return []
        path = _literal_str(dec.args[0])
        if path is None:
            return []
        return [(attr.upper(), path)]
    return []


def _routes_from_call(node: ast.Call) -> List[Tuple[str, str, str]]:
    """aiohttp ``router.add_get(...)`` and Starlette ``Route(...)`` calls."""
    func = node.func
    if isinstance(func, ast.Attribute):
        if not func.attr.startswith(ADD_PREFIX) or len(node.args) < 2:
            return []
        verb = func.attr[len(ADD_PREFIX) :]
        if verb not in DECORATOR_VERBS:
            return []
        path = _literal_str(node.args[0])
        handler = _handler_name(node.args[1])
        if path is None or handler is None:
            return []
        return [(verb.upper(), path, handler)]
    if isinstance(func, ast.Name) and func.id == "Route" and len(node.args) >= 2:
        # Starlette: Route("/path", endpoint, methods=["GET"]); methods
        # defaults to GET.
        path = _literal_str(node.args[0])
        handler = _handler_name(node.args[1])
        if path is None or handler is None:
            return []
        methods: Optional[List[str]] = ["GET"]
        for kw in node.keywords:
            if kw.arg == "methods":
                methods = _literal_methods(kw.value)
                break
        if methods is None:
            return []
        return [(m, path, handler) for m in methods]
    return []


def extract_routes(source: str) -> List[Tuple[str, str, str]]:
    """(method, path, handler) triples declared in one Python source string.

    Handles decorated functions (``@app.get`` / ``@app.route`` /
    ``@bp.route`` / ``@router.get`` — any decorator-object name — and
    ``@routes.get``) plus constructor-style declarations (Starlette
    ``Route(...)`` and aiohttp ``router.add_get/post/...``).
    """
    tree = ast.parse(source)
    out: List[Tuple[str, str, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for dec in node.decorator_list:
                for method, path in _routes_from_decorator(dec):
                    out.append((method, path, node.name))
        elif isinstance(node, ast.Call):
            out.extend(_routes_from_call(node))
    return out


def iter_python_files(root: Path) -> Iterator[Path]:
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(
            d for d in dirnames if d not in SKIP_DIRS and not d.startswith(".")
        )
        for name in sorted(filenames):
            if not name.endswith(".py"):
                continue
            if name.startswith("test_") or name.endswith("_test"):
                continue
            yield Path(dirpath) / name


def scan_directory(root: Path) -> ScanResult:
    """Extract every route under root; routes are sorted by source file for
    deterministic output. Files that fail to parse are counted and skipped,
    never fatal — scanning foreign source must stay best-effort."""
    result = ScanResult()
    for path in iter_python_files(root):
        result.files_scanned += 1
        try:
            triples = extract_routes(path.read_text(encoding="utf-8"))
        except (OSError, SyntaxError, ValueError):
            result.parse_errors += 1
            continue
        source_file = path.relative_to(root).as_posix()
        for method, rpath, handler in triples:
            result.routes.append(
                Route(method=method, path=rpath, handler=handler, source_file=source_file)
            )
    result.routes.sort(key=lambda r: (r.source_file, r.path, r.method, r.handler))
    seen = set()
    unique: List[Route] = []
    for route in result.routes:
        key = (route.method, route.path, route.handler, route.source_file)
        if key not in seen:
            seen.add(key)
            unique.append(route)
    result.routes = unique
    return result


# -- wire format ------------------------------------------------------------


def build_body(service_name: str, routes: Sequence[Route]) -> dict:
    return {
        "service_name": service_name,
        "routes": [
            {
                "method": r.method,
                "path": r.path,
                "handler": r.handler,
                "source_file": r.source_file,
            }
            for r in routes
        ],
    }


def post_catalog(base: str, api_key: str, body: dict, timeout: float = HTTP_TIMEOUT) -> None:
    """POST the catalog to the server; raises on any failure."""
    req = urllib.request.Request(
        base + CATALOG_PATH,
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json", "X-Api-Key": api_key},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
        resp.read()
        if not 200 <= resp.status < 300:
            raise RuntimeError(f"unexpected status {resp.status}")


def resolve_base_url(url_flag: str = "", endpoint: str = "") -> Optional[str]:
    """Base URL precedence: ``--url`` > DATAFLOW_HTTP_URL > URL-form
    DATAFLOW_ENDPOINT. A bare host:port endpoint with no override has no
    derivable HTTP base -> None (mirrors manifest.resolve_http_base)."""
    flag = (url_flag or "").strip()
    if flag:
        return flag.rstrip("/")
    return resolve_http_base(endpoint)


# -- CLI --------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m dataflow.scan",
        description=(
            "Static route scanner: extract HTTP endpoints from Python source "
            "(Flask, FastAPI, Starlette, aiohttp) and post them to the "
            "Dataflow service catalog."
        ),
    )
    parser.add_argument("--dir", default=".", help="directory to scan (default: .)")
    parser.add_argument(
        "--service",
        default="",
        help="service name (default: DATAFLOW_SERVICE_NAME or the directory basename)",
    )
    parser.add_argument(
        "--url",
        default="",
        help="Dataflow HTTP base URL (default: DATAFLOW_HTTP_URL, then URL-form DATAFLOW_ENDPOINT)",
    )
    parser.add_argument(
        "--api-key", default="", help="API key (default: DATAFLOW_API_KEY)"
    )
    parser.add_argument(
        "--print",
        dest="print_json",
        action="store_true",
        help="print the catalog JSON to stdout instead of posting",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)

    root = Path(args.dir).resolve()
    if not root.is_dir():
        print(f"dataflow.scan: not a directory: {args.dir}", file=sys.stderr)
        return 1

    service = (
        args.service
        or os.environ.get("DATAFLOW_SERVICE_NAME", "").strip()
        or root.name
    )

    result = scan_directory(root)
    if result.parse_errors:
        print(
            f"dataflow.scan: warning: {result.parse_errors} file(s) could not be parsed and were skipped",
            file=sys.stderr,
        )
    print(
        f"dataflow.scan: {len(result.routes)} route(s) across "
        f"{result.files_scanned} file(s) under {args.dir}",
        file=sys.stderr,
    )

    routes = result.routes
    if len(routes) > MAX_ROUTES:
        print(
            f"dataflow.scan: warning: truncating to first {MAX_ROUTES} routes (server limit)",
            file=sys.stderr,
        )
        routes = routes[:MAX_ROUTES]
    body = build_body(service, routes)

    if args.print_json:
        json.dump(body, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return 0

    if not routes:
        print("dataflow.scan: no routes found; nothing to post", file=sys.stderr)
        return 0

    base = resolve_base_url(args.url, os.environ.get("DATAFLOW_ENDPOINT", ""))
    if base is None:
        print(
            "dataflow.scan: no HTTP endpoint derived from DATAFLOW_ENDPOINT "
            "(bare host:port has no HTTP base). Re-run with --url or set "
            "DATAFLOW_HTTP_URL to post; results were NOT posted.",
            file=sys.stderr,
        )
        return 0

    api_key = args.api_key or os.environ.get("DATAFLOW_API_KEY", "")
    if not api_key:
        print(
            "dataflow.scan: no API key (--api-key or DATAFLOW_API_KEY); cannot post the catalog",
            file=sys.stderr,
        )
        return 2

    try:
        post_catalog(base, api_key, body)
    except (urllib.error.HTTPError, urllib.error.URLError, OSError, RuntimeError) as exc:
        print(f"dataflow.scan: POST {base}{CATALOG_PATH} failed: {exc}", file=sys.stderr)
        return 2

    print(
        f"dataflow.scan: posted {len(routes)} route(s) to {base}{CATALOG_PATH} (service: {service})",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
