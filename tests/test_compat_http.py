"""Compat mode end to end on the SQLite fixture: a 1.x TAP.conf gives NEA's
responses (legacy_tap_server); the 3.x config is unchanged (tap_server)."""
from __future__ import annotations

from _helpers import raw_get

VOSI_HEAD = (b"HTTP/1.1 200 OK\r\nContent-type: application/xml\r\n"
             b"Connection: close\r\n\r\n")
XML_DECL = b'<?xml version="1.0" encoding="UTF-8"?>\n'


def test_vosi_documents_carry_neas_headers(legacy_tap_server):
    for ep in ("availability", "capabilities"):
        raw = raw_get(legacy_tap_server, "/cgi-bin/TAP/nph-tap.py/" + ep)
        assert raw.startswith(VOSI_HEAD + XML_DECL), raw[:200]
        assert raw.endswith(b"</vosi:" + ep.encode() + b">\n\r\n"), raw[-80:]


def test_vosi_documents_unchanged_in_default_mode(tap_server):
    for ep in ("availability", "capabilities"):
        raw = raw_get(tap_server, "/cgi-bin/TAP/nph-tap.py/" + ep)
        assert raw.startswith(XML_DECL), raw[:200]
