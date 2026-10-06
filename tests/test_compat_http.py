"""Compat mode end to end on the SQLite fixture: a 1.x TAP.conf gives NEA's
responses (legacy_tap_server); the 3.x config is unchanged (tap_server)."""
from __future__ import annotations

import requests
from _helpers import raw_get, tap_sync_url

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


DENIED = "Table 'not_a_table' is not available for querying."


def _sync(server, query, fmt="csv"):
    return requests.post(tap_sync_url(server), data={"query": query, "format": fmt},
                         timeout=60)


def test_unknown_table_is_neas_400_votable(legacy_tap_server):
    r = _sync(legacy_tap_server, "select * from not_a_table")
    assert r.status_code == 400
    assert r.headers["Content-Type"] == "application/xml"
    assert '<INFO name="QUERY_STATUS" value="ERROR">\n' + DENIED in r.text


def test_bad_adql_is_a_votable_error(legacy_tap_server):
    r = _sync(legacy_tap_server, "selec x frm y")
    assert r.status_code == 400
    assert r.headers["Content-Type"] == "application/xml"
    assert r.text.startswith('<?xml version="1.0" encoding="UTF-8"?>\n<VOTABLE')


def test_unknown_table_unchanged_in_default_mode(tap_server):
    r = _sync(tap_server, "select * from not_a_table")
    assert r.status_code == 403
    assert r.headers["Content-Type"] == "text/plain"
