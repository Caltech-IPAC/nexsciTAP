"""Wire-level checks for the VOSI /capabilities and /availability endpoints.

These are nph- (non-parsed-headers) CGI endpoints: the script is responsible
for emitting the entire HTTP response itself, status line included. Anything
short of a well-formed `HTTP/1.1 200 OK\\r\\n` start-line followed by CRLF
headers and a blank CRLF line is rejected as an upstream protocol error by
nginx and Cloudflare, so these tests assert on raw bytes rather than going
through a forgiving client library.
"""
from __future__ import annotations

import os
import socket
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlsplit

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
FIXTURES = Path(__file__).parent / "fixtures"
STUBS = FIXTURES / "stubs"


def _raw_get(server: str, path: str) -> bytes:
    """Issue a GET and return the unparsed response bytes off the socket."""
    parts = urlsplit(server)
    with socket.create_connection((parts.hostname, parts.port), timeout=30) as sock:
        request = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {parts.hostname}:{parts.port}\r\n"
            "Connection: close\r\n"
            "\r\n"
        )
        sock.sendall(request.encode("ascii"))
        chunks = []
        while True:
            chunk = sock.recv(65536)
            if not chunk:
                break
            chunks.append(chunk)
    return b"".join(chunks)


VOSI_PATHS = ["/TAP/capabilities", "/TAP/availability"]


@pytest.mark.parametrize("path", VOSI_PATHS)
def test_vosi_status_line_is_crlf_terminated(tap_server: str, path: str):
    """The response opens with a CRLF-terminated 200 status line."""
    raw = _raw_get(tap_server, path)
    assert raw.startswith(b"HTTP/1.1 200 OK\r\n"), (
        f"{path} must begin with a CRLF-terminated status line; "
        f"got {raw[:60]!r}"
    )


@pytest.mark.parametrize("path", VOSI_PATHS)
def test_vosi_headers_are_crlf_terminated(tap_server: str, path: str):
    """Every header line uses CRLF, and CRLFCRLF separates headers from body."""
    raw = _raw_get(tap_server, path)
    assert b"\r\n\r\n" in raw, f"{path} has no CRLF blank line ending the headers"

    head = raw.split(b"\r\n\r\n", 1)[0]
    for line in head.split(b"\r\n"):
        assert b"\n" not in line, (
            f"{path} header line {line!r} contains a bare LF"
        )
        assert b"\r" not in line, (
            f"{path} header line {line!r} contains a bare CR"
        )


@pytest.mark.parametrize("path", VOSI_PATHS)
def test_vosi_declares_xml_content_type(tap_server: str, path: str):
    """A Content-type header is present and announces XML."""
    raw = _raw_get(tap_server, path)
    head = raw.split(b"\r\n\r\n", 1)[0].lower()
    assert b"content-type: text/xml" in head, (
        f"{path} is missing an XML Content-type header; got {head!r}"
    )


@pytest.mark.parametrize(
    "path,root_element",
    [
        ("/TAP/capabilities", b"<vosi:capabilities"),
        ("/TAP/availability", b"<vosi:availability"),
    ],
)
def test_vosi_body_still_intact(tap_server: str, path: str, root_element: bytes):
    """Adding headers must not disturb the XML payload itself."""
    raw = _raw_get(tap_server, path)
    body = raw.split(b"\r\n\r\n", 1)[1]
    assert body.lstrip().startswith(b'<?xml version="1.0" encoding="UTF-8"?>')
    assert root_element in body


def _run_cgi_with_workdir(tmp_path, fixture_root, path_info: str) -> bytes:
    """Run the CGI directly against a *pristine* TAP_WORKDIR.

    The session `tap_server` fixture shares one workdir across the whole
    suite, so by the time these tests run `<workdir>/TAP` has already been
    created by earlier sync requests. That hides the fresh-deployment case,
    which is exactly where the workspace-resolution bug bit. Point the CGI at
    an empty workdir so the no-workspace path is exercised deterministically.
    """
    workdir = tmp_path / "workdir"
    workdir.mkdir()
    conf = tmp_path / "TAP.conf"
    conf.write_text(
        (FIXTURES / "TAP.conf.template").read_text().format(
            TEST_WORKDIR=str(workdir),
            TEST_HTTP_URL="http://127.0.0.1:8099",
            TEST_DB_PATH=str(fixture_root / "test_data.db"),
            TEST_TAP_SCHEMA=str(fixture_root / "tap_schema.db"),
        )
    )

    env = os.environ.copy()
    env.update({
        "TAP_CONF": str(conf),
        "PATH_INFO": path_info,
        "REQUEST_METHOD": "GET",
        "QUERY_STRING": "",
        "PYTHONPATH": os.pathsep.join([str(REPO_ROOT), str(STUBS)]),
    })
    proc = subprocess.run(
        [sys.executable, str(fixture_root / "cgi-bin" / "TAP" / "nph-tap.py")],
        env=env, capture_output=True, timeout=60,
    )
    assert (workdir / "TAP").exists() is False, (
        "the VOSI endpoints must not create a workspace"
    )
    return proc.stdout


@pytest.mark.parametrize("path", VOSI_PATHS)
def test_vosi_works_without_existing_workspace(tmp_path, fixture_root, path):
    """A fresh deployment, where <workdir>/TAP does not yet exist, still works.

    These endpoints previously fell through to the "retrieve workspace from
    jobid" branch with an empty jobid, resolving to <workdir>/TAP and
    returning a 500 whenever that directory was absent.
    """
    raw = _run_cgi_with_workdir(tmp_path, fixture_root, path.replace("/TAP", "", 1))
    assert raw.startswith(b"HTTP/1.1 200 OK\r\n"), (
        f"{path} on a pristine workdir must return a CRLF-terminated 200; "
        f"got {raw[:120]!r}"
    )
    assert b"500" not in raw.split(b"\r\n", 1)[0]
