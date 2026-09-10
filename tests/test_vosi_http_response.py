"""Wire-level checks for the VOSI /capabilities and /availability endpoints.

These are nph- (non-parsed-headers) CGI endpoints: the script is responsible
for emitting the entire HTTP response itself, status line included. Anything
short of a well-formed `HTTP/1.1 200 OK\\r\\n` start-line followed by CRLF
headers and a blank CRLF line is rejected as an upstream protocol error by
nginx and Cloudflare, so these tests assert on raw bytes rather than going
through a forgiving client library.
"""
from __future__ import annotations

import socket
from urllib.parse import urlsplit

import pytest


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
