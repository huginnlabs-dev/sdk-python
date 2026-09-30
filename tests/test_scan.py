"""Tests for the static route scanner (python -m dataflow.scan)."""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from dataflow.scan import (
    Route,
    build_body,
    extract_routes,
    main,
    post_catalog,
    resolve_base_url,
    scan_directory,
)

# -- fixture sources --------------------------------------------------------

FLASK_APP = '''
from flask import Flask, Blueprint

app = Flask(__name__)
bp = Blueprint("billing", __name__)


@app.get("/healthz")
def health():
    return "ok"


@app.route("/orders/<int:order_id>", methods=["GET", "PATCH"])
def get_order(order_id):
    return "order"


@app.route("/quiet")
def quiet():
    return "quiet"


@bp.post("/billing/invoices")
def create_invoice():
    return "created", 201


@app.get(DYNAMIC_PATH)  # non-literal path: skipped
def dynamic():
    return "dyn"


@app.route(ROUTE_NAME_VAR, methods=["POST"])  # non-literal path: skipped
def variable():
    return "var"


@app.route("/unknown-methods", methods=METHODS_VAR)  # non-literal methods: skipped
def unknown_methods():
    return "m"
'''

FASTAPI_APP = '''
from fastapi import APIRouter, FastAPI

app = FastAPI()
router = APIRouter(prefix="/v1")


@app.post("/v1/orders/{id}/cancel")
def cancel_order(id: int):
    return {}


@router.get("/orders/{id}")
def read_order(id: int):
    return {}


@router.delete("/orders/{id}")
def delete_order(id: int):
    return {}


@app.put(f"/orders/{computed}/items")  # f-string path: skipped
def computed_items():
    return {}
'''

STARLETTE_APP = '''
from starlette.applications import Starlette
from starlette.routing import Route


async def homepage(request):
    return {}


async def users(request):
    return {}


routes = [
    Route("/", homepage),
    Route("/users", users, methods=["POST", "PUT"]),
    Route(VARIABLE_PATH, homepage),  # non-literal path: skipped
]

app = Starlette(routes=routes)
'''

AIOHTTP_APP = '''
from aiohttp import web

routes = web.RouteTableDef()


@routes.get("/hooks")
async def hooks(request):
    return web.Response()


async def ping(request):
    return web.json_response({})


async def submit(request):
    return web.json_response({})


def make_app():
    app = web.Application()
    app.router.add_get("/ping", ping)
    app.router.add_post("/submit", submit)
    app.router.add_delete("/things/{id}", remove_thing)
    return app
'''

EXPECTED_ROUTES = {
    # flask (app/api.py)
    ("GET", "/healthz", "health", "app/api.py"),
    ("GET", "/orders/<int:order_id>", "get_order", "app/api.py"),
    ("PATCH", "/orders/<int:order_id>", "get_order", "app/api.py"),
    ("GET", "/quiet", "quiet", "app/api.py"),
    ("POST", "/billing/invoices", "create_invoice", "app/api.py"),
    # fastapi (app/fastapi_app.py) — paths kept exactly as written;
    # APIRouter(prefix=...) is not resolved by static extraction
    ("POST", "/v1/orders/{id}/cancel", "cancel_order", "app/fastapi_app.py"),
    ("GET", "/orders/{id}", "read_order", "app/fastapi_app.py"),
    ("DELETE", "/orders/{id}", "delete_order", "app/fastapi_app.py"),
    # starlette (app/starlette_app.py)
    ("GET", "/", "homepage", "app/starlette_app.py"),
    ("POST", "/users", "users", "app/starlette_app.py"),
    ("PUT", "/users", "users", "app/starlette_app.py"),
    # aiohttp (app/aiohttp_app.py)
    ("GET", "/hooks", "hooks", "app/aiohttp_app.py"),
    ("GET", "/ping", "ping", "app/aiohttp_app.py"),
    ("POST", "/submit", "submit", "app/aiohttp_app.py"),
    ("DELETE", "/things/{id}", "remove_thing", "app/aiohttp_app.py"),
}

SCANNED_FILES = 5  # app/*.py (4) + broken.py; test_*/venv/.hidden excluded
PARSE_ERRORS = 1


def build_tree(tmp_path: Path) -> None:
    (tmp_path / "app").mkdir()
    (tmp_path / "app" / "api.py").write_text(FLASK_APP, encoding="utf-8")
    (tmp_path / "app" / "fastapi_app.py").write_text(FASTAPI_APP, encoding="utf-8")
    (tmp_path / "app" / "starlette_app.py").write_text(STARLETTE_APP, encoding="utf-8")
    (tmp_path / "app" / "aiohttp_app.py").write_text(AIOHTTP_APP, encoding="utf-8")
    # never scanned: test files, vendored code, hidden dirs
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_app.py").write_text(
        '@app.get("/from-a-test")\ndef t(): ...\n', encoding="utf-8"
    )
    (tmp_path / "venv" / "lib").mkdir(parents=True)
    (tmp_path / "venv" / "lib" / "sample.py").write_text(
        '@app.get("/from-venv")\ndef v(): ...\n', encoding="utf-8"
    )
    (tmp_path / ".hidden").mkdir()
    (tmp_path / ".hidden" / "secret.py").write_text(
        '@app.get("/from-hidden")\ndef h(): ...\n', encoding="utf-8"
    )
    # unparseable file: counted, skipped, never fatal
    (tmp_path / "broken.py").write_text("def broken(:\n", encoding="utf-8")


# -- extraction -------------------------------------------------------------


def test_scan_directory_extracts_all_frameworks(tmp_path):
    build_tree(tmp_path)
    result = scan_directory(tmp_path)
    got = {(r.method, r.path, r.handler, r.source_file) for r in result.routes}
    assert got == EXPECTED_ROUTES
    assert len(result.routes) == len(EXPECTED_ROUTES)  # no duplicates
    assert result.files_scanned == SCANNED_FILES
    assert result.parse_errors == PARSE_ERRORS


def test_source_file_is_posix_relative(tmp_path):
    (tmp_path / "pkg").mkdir()
    (tmp_path / "pkg" / "views.py").write_text(FLASK_APP, encoding="utf-8")
    result = scan_directory(tmp_path)
    assert {r.source_file for r in result.routes} == {"pkg/views.py"}


@pytest.mark.parametrize(
    "decorator, expected",
    [
        ('@app.route("/x", methods=["GET", "POST"])', [("GET", "/x"), ("POST", "/x")]),
        ('@app.route("/x", methods=["post"])', [("POST", "/x")]),  # case normalized
        ('@app.route("/x")', [("GET", "/x")]),  # flask default
        ('@app.get("/x")', [("GET", "/x")]),
        ('@app.post("/x")', [("POST", "/x")]),
        ('@bp.delete("/x")', [("DELETE", "/x")]),
        ('@router.patch("/x")', [("PATCH", "/x")]),
        ('@routes.put("/x")', [("PUT", "/x")]),
        ('@app.get(DYNAMIC)', []),  # non-literal path
        ('@app.get(f"/x/{fid}")', []),  # f-string path
        ('@app.route("/x", methods=METHODS)', []),  # non-literal methods
        ("@app.get", []),  # bare decorator, no call
        ('@app.websocket("/ws")', []),  # not an HTTP verb
    ],
)
def test_extract_routes_decorators(decorator, expected):
    source = f"def handler(): ...\n{decorator}\ndef handler(): ...\n"
    assert extract_routes(source) == [(m, p, "handler") for m, p in expected]


def test_extract_routes_sync_and_async_handlers():
    source = '''
@app.get("/sync")
def sync(): ...

@app.get("/async")
async def asyn(): ...
'''
    assert sorted(extract_routes(source)) == [
        ("GET", "/async", "asyn"),
        ("GET", "/sync", "sync"),
    ]


def test_extract_routes_starlette():
    source = '''
routes = [
    Route("/", homepage),
    Route("/users", user_views, methods=["POST", "PUT"]),
    Route("/admin", handlers.admin, name="admin"),
    Route(VARIABLE, homepage),
    Route("/nonescal", obj.as_view()),
    Route("/badmethods", home, methods=EXTRA_METHODS),
]
'''
    assert sorted(extract_routes(source)) == [
        ("GET", "/", "homepage"),
        ("GET", "/admin", "admin"),
        ("POST", "/users", "user_views"),
        ("PUT", "/users", "user_views"),
    ]


def test_extract_routes_aiohttp_router():
    source = '''
def make_app():
    router.add_get("/ping", ping)
    router.add_post("/submit", submit)
    router.add_delete("/things/{id}", remove_thing)
    router.add_get(VAR, handler)
    router.add_get("/nohandler")
    router.add_view("/view", view)
    router.add_route("/x", handler)
'''
    assert sorted(extract_routes(source)) == [
        ("DELETE", "/things/{id}", "remove_thing"),
        ("GET", "/ping", "ping"),
        ("POST", "/submit", "submit"),
    ]


def test_build_body_shape():
    body = build_body(
        "orders-api", [Route("GET", "/orders/{id}", "get_order", "app/api.py")]
    )
    assert body == {
        "service_name": "orders-api",
        "routes": [
            {
                "method": "GET",
                "path": "/orders/{id}",
                "handler": "get_order",
                "source_file": "app/api.py",
            }
        ],
    }


# -- base URL precedence ----------------------------------------------------


def test_url_flag_wins_over_env(monkeypatch):
    monkeypatch.setenv("DATAFLOW_HTTP_URL", "http://env-override:1")
    assert resolve_base_url("http://flag:2/", "http://endpoint:3") == "http://flag:2"


def test_env_http_url_wins_over_url_form_endpoint(monkeypatch):
    monkeypatch.setenv("DATAFLOW_HTTP_URL", "http://env-override:1")
    assert resolve_base_url("", "http://endpoint:3") == "http://env-override:1"


def test_url_form_endpoint_used_without_override(monkeypatch):
    monkeypatch.delenv("DATAFLOW_HTTP_URL", raising=False)
    assert resolve_base_url("", "https://endpoint:3/") == "https://endpoint:3"


def test_bare_host_port_endpoint_has_no_base(monkeypatch):
    monkeypatch.delenv("DATAFLOW_HTTP_URL", raising=False)
    assert resolve_base_url("", "api.huginnlabs.com:9090") is None


def test_no_endpoint_at_all(monkeypatch):
    monkeypatch.delenv("DATAFLOW_HTTP_URL", raising=False)
    assert resolve_base_url("", "") is None


# -- catalog posting --------------------------------------------------------


class _CatalogHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        payload = self.rfile.read(length)
        self.server.captured.append(
            {
                "path": self.path,
                "headers": {k.lower(): v for k, v in self.headers.items()},
                "body": json.loads(payload.decode("utf-8")) if payload else None,
            }
        )
        self.send_response(self.server.status)
        self.end_headers()

    def log_message(self, *args):
        pass


@pytest.fixture
def catalog_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _CatalogHandler)
    server.captured = []
    server.status = 200
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()


def test_post_catalog_sends_json_and_api_key(catalog_server):
    body = build_body("svc", [Route("GET", "/healthz", "health", "app/api.py")])
    base = f"http://127.0.0.1:{catalog_server.server_address[1]}"
    post_catalog(base, "sekrit-1", body)
    captured = catalog_server.captured[0]
    assert captured["path"] == "/api/v1/catalog"
    assert captured["headers"]["x-api-key"] == "sekrit-1"
    assert captured["headers"]["content-type"] == "application/json"
    assert captured["body"] == body


def test_main_posts_catalog_to_server(tmp_path, catalog_server):
    build_tree(tmp_path)
    base = f"http://127.0.0.1:{catalog_server.server_address[1]}"
    rc = main(
        ["--dir", str(tmp_path), "--service", "orders-api", "--url", base, "--api-key", "k-123"]
    )
    assert rc == 0
    captured = catalog_server.captured[0]
    assert captured["path"] == "/api/v1/catalog"
    assert captured["headers"]["x-api-key"] == "k-123"
    body = captured["body"]
    assert body["service_name"] == "orders-api"
    got = {
        (r["method"], r["path"], r["handler"], r["source_file"]) for r in body["routes"]
    }
    assert got == EXPECTED_ROUTES


def test_main_print_flag_prints_instead_of_posting(tmp_path, catalog_server, capsys):
    build_tree(tmp_path)
    rc = main(["--dir", str(tmp_path), "--service", "svc", "--print"])
    assert rc == 0
    assert catalog_server.captured == []
    body = json.loads(capsys.readouterr().out)
    assert body["service_name"] == "svc"
    assert len(body["routes"]) == len(EXPECTED_ROUTES)


def test_main_summary_on_stderr(tmp_path, capsys):
    build_tree(tmp_path)
    main(["--dir", str(tmp_path), "--print"])
    err = capsys.readouterr().err
    assert f"{len(EXPECTED_ROUTES)} route(s) across {SCANNED_FILES} file(s)" in err
    assert "1 file(s) could not be parsed" in err


def test_main_default_service_is_dir_basename(tmp_path, capsys):
    build_tree(tmp_path)
    main(["--dir", str(tmp_path), "--print"])
    body = json.loads(capsys.readouterr().out)
    assert body["service_name"] == Path(str(tmp_path)).resolve().name


def test_main_service_from_env(tmp_path, monkeypatch, capsys):
    build_tree(tmp_path)
    monkeypatch.setenv("DATAFLOW_SERVICE_NAME", "env-svc")
    main(["--dir", str(tmp_path), "--print"])
    body = json.loads(capsys.readouterr().out)
    assert body["service_name"] == "env-svc"


def test_main_missing_dir_exit_1(tmp_path):
    assert main(["--dir", str(tmp_path / "nope")]) == 1


def test_main_no_base_skips_posting(tmp_path, capsys):
    build_tree(tmp_path)
    # conftest leaves DATAFLOW_ENDPOINT at its bare host:port default and
    # DATAFLOW_HTTP_URL unset -> the scan succeeds but nothing is posted.
    rc = main(["--dir", str(tmp_path)])
    assert rc == 0
    err = capsys.readouterr().err
    assert "no HTTP endpoint" in err
    assert "NOT posted" in err


def test_main_missing_api_key_exit_2(tmp_path, catalog_server):
    build_tree(tmp_path)
    base = f"http://127.0.0.1:{catalog_server.server_address[1]}"
    rc = main(["--dir", str(tmp_path), "--url", base])
    assert rc == 2
    assert catalog_server.captured == []


def test_main_api_key_from_env(tmp_path, monkeypatch, catalog_server):
    build_tree(tmp_path)
    monkeypatch.setenv("DATAFLOW_API_KEY", "env-key")
    base = f"http://127.0.0.1:{catalog_server.server_address[1]}"
    rc = main(["--dir", str(tmp_path), "--url", base])
    assert rc == 0
    assert catalog_server.captured[0]["headers"]["x-api-key"] == "env-key"


def test_main_post_failure_exit_2(tmp_path, catalog_server):
    build_tree(tmp_path)
    catalog_server.status = 500
    base = f"http://127.0.0.1:{catalog_server.server_address[1]}"
    rc = main(["--dir", str(tmp_path), "--url", base, "--api-key", "k"])
    assert rc == 2
    assert len(catalog_server.captured) == 1


def test_main_empty_dir_nothing_to_post(tmp_path, catalog_server, capsys):
    rc = main(["--dir", str(tmp_path)])
    assert rc == 0
    assert catalog_server.captured == []
    assert "no routes found" in capsys.readouterr().err


def test_main_truncates_to_server_limit(tmp_path, capsys):
    lines = ["from flask import Flask", "app = Flask(__name__)"]
    for i in range(1001):
        lines.append(f"@app.get('/r{i}')\ndef r{i}(): ...")
    (tmp_path / "many.py").write_text("\n".join(lines), encoding="utf-8")
    rc = main(["--dir", str(tmp_path), "--print"])
    assert rc == 0
    captured = capsys.readouterr()
    body = json.loads(captured.out)
    assert len(body["routes"]) == 1000
    assert "truncating" in captured.err
