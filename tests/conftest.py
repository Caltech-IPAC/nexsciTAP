"""Shared pytest fixtures.

Spins up a per-session HTTP server backed by SQLite fixtures and the
production CGI shim, so tests can exercise the full HTTP→CGI→TAP→DB pipeline.

Layout under the session tmpdir:

    <tmp>/
        tap_schema.db        — populated TAP_SCHEMA
        test_data.db         — data_l0, data_l1, test_access, test_users
        TAP.conf             — substituted from fixtures/TAP.conf.template
        workdir/             — TAP_WORKDIR (request artifacts)
        cgi-bin/
            TAP/
                nph-tap.py   — copy of fixtures/nph-tap.py
"""
from __future__ import annotations

import http.server
import os
import shutil
import socket
import sys
import threading
import time
from pathlib import Path

import pytest

TESTS_DIR = Path(__file__).parent
FIXTURES = TESTS_DIR / "fixtures"
STUBS = FIXTURES / "stubs"
sys.path.insert(0, str(FIXTURES))
sys.path.insert(0, str(TESTS_DIR))
sys.path.insert(0, str(STUBS))

import build_test_db  # noqa: E402


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def fixture_root(tmp_path_factory) -> Path:
    """Build all the per-session artifacts and return the tmpdir root."""
    root = tmp_path_factory.mktemp("nexscitap-ci")

    build_test_db.build_all(str(root))

    (root / "workdir").mkdir()
    cgi_dir = root / "cgi-bin" / "TAP"
    cgi_dir.mkdir(parents=True)
    shutil.copy(FIXTURES / "nph-tap.py", cgi_dir / "nph-tap.py")
    (cgi_dir / "nph-tap.py").chmod(0o755)

    return root


@pytest.fixture(scope="session")
def tap_conf(fixture_root: Path) -> Path:
    """Materialize TAP.conf with absolute paths substituted in."""
    port = _free_port()
    template = (FIXTURES / "TAP.conf.template").read_text()
    conf = template.format(
        TEST_WORKDIR=str(fixture_root / "workdir"),
        TEST_HTTP_URL=f"http://127.0.0.1:{port}",
        TEST_DB_PATH=str(fixture_root / "test_data.db"),
        TEST_TAP_SCHEMA=str(fixture_root / "tap_schema.db"),
    )
    conf_path = fixture_root / "TAP.conf"
    conf_path.write_text(conf)
    # Stash the port so tap_server can reuse it
    (fixture_root / ".port").write_text(str(port))
    return conf_path


class _NphCGIHandler(http.server.BaseHTTPRequestHandler):
    """Minimal nph-CGI runner.

    The TAP CGI script writes its own HTTP status line (it's an `nph-` /
    non-parsed-headers script in Apache parlance — see TAP/tap.py:755, 2538,
    etc.). Python's stdlib CGIHTTPRequestHandler is not nph-aware: it always
    prepends a `HTTP/1.1 200 Script output follows` status line, so the
    script's status line ends up looking like a header to the client.

    This handler runs the CGI subprocess and forwards its raw stdout to the
    socket without modification — exactly what an Apache nph- script does.
    """

    nph_script_path = ""  # set by the tap_server fixture before serve_forever

    def do_GET(self):
        self._run_cgi()

    def do_POST(self):
        self._run_cgi()

    def _run_cgi(self):
        # Parse PATH_INFO and QUERY_STRING from self.path.
        from urllib.parse import urlsplit

        parts = urlsplit(self.path)
        # Two URL patterns are supported, both mapping to nph-tap.py:
        #   /cgi-bin/TAP/nph-tap.py/<sync|async>   — direct CGI URL
        #   /TAP/<sync|async>                      — short form pyNEID uses,
        #                                            mimicking the Apache
        #                                            rewrite KOA/NEID/NEA
        #                                            run in production.
        path = parts.path
        marker = "/nph-tap.py"
        if marker in path:
            path_info = path.split(marker, 1)[1]
        elif path.startswith("/TAP/"):
            path_info = path[len("/TAP"):]  # keep leading slash
        else:
            path_info = ""

        # Read request body (if any) for the subprocess stdin.
        content_length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(content_length) if content_length else b""

        env = os.environ.copy()
        env.update({
            "GATEWAY_INTERFACE": "CGI/1.1",
            "SERVER_PROTOCOL": "HTTP/1.1",
            "SERVER_SOFTWARE": "test-nph-cgi/1.0",
            "REQUEST_METHOD": self.command,
            "QUERY_STRING": parts.query,
            "PATH_INFO": path_info,
            "SCRIPT_NAME": "/cgi-bin/TAP/nph-tap.py",
            "REMOTE_ADDR": self.client_address[0],
            "CONTENT_TYPE": self.headers.get("Content-Type", ""),
            "CONTENT_LENGTH": str(content_length),
            "HTTP_HOST": self.headers.get("Host", ""),
        })

        import subprocess
        proc = subprocess.Popen(
            [sys.executable, self.server.nph_script_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )
        stdout, stderr = proc.communicate(input=body)

        # Forward raw subprocess stdout to the socket. The script is
        # responsible for emitting a valid HTTP status line + headers.
        try:
            self.wfile.write(stdout)
            self.wfile.flush()
        except BrokenPipeError:
            pass

    # Quieter logging during test runs.
    def log_message(self, format, *args):
        return


@pytest.fixture(scope="session")
def tap_server(fixture_root: Path, tap_conf: Path):
    """Start http.server with the CGI handler. Yields the base URL."""
    port = int((fixture_root / ".port").read_text())

    os.environ["TAP_CONF"] = str(tap_conf)
    os.environ["TAP_PORT"] = str(port)

    # CGIHTTPRequestHandler runs each request as a subprocess; it inherits
    # the parent's environment but not the in-process sys.path. Add the
    # repo root to PYTHONPATH so `from TAP import tap` works in the CGI
    # subprocess even if the package isn't pip-installed (CI installs the
    # package; local pytest may not).
    repo_root = TESTS_DIR.parent
    paths = [str(repo_root), str(STUBS)]
    existing = os.environ.get("PYTHONPATH", "")
    if existing:
        paths.append(existing)
    os.environ["PYTHONPATH"] = os.pathsep.join(paths)

    original_cwd = os.getcwd()
    os.chdir(fixture_root)

    httpd = http.server.HTTPServer(("127.0.0.1", port), _NphCGIHandler)
    httpd.allow_reuse_address = True
    httpd.nph_script_path = str(fixture_root / "cgi-bin" / "TAP" / "nph-tap.py")
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()

    # Brief wait for the server to come up
    for _ in range(50):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                break
        except OSError:
            time.sleep(0.05)

    base_url = f"http://127.0.0.1:{port}"
    try:
        yield base_url
    finally:
        httpd.shutdown()
        httpd.server_close()
        os.chdir(original_cwd)


