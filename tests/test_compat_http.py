"""Compat mode end to end on the SQLite fixture: a 1.x TAP.conf gives NEA's
responses (legacy_tap_server); the 3.x config is unchanged (tap_server)."""
from __future__ import annotations

import time
import xml.etree.ElementTree as ET

import requests
from _helpers import raw_get, tap_async_url, tap_sync_url

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


NONCHAR = "select obsjd, l0propint from data_l0"


def test_votable_is_neas_form(legacy_tap_server):
    r = _sync(legacy_tap_server, NONCHAR, fmt="votable")
    assert r.status_code == 200, r.text[:300]
    assert r.headers["Content-Type"] == "application/xml"
    assert "<DESCRIPTION>" not in r.text
    assert '<FIELD ID="obsjd" datatype="double" name="obsjd"/>' in r.text


def test_votable_unchanged_in_default_mode(tap_server):
    r = _sync(tap_server, NONCHAR, fmt="votable")
    assert r.status_code == 200, r.text[:300]
    assert r.headers["Content-Type"] == "text/xml"
    assert "Julian date of observation." in r.text


UWS = "{http://www.ivoa.net/xml/UWS/v1.0}"


def _completed_job(server):
    """Submit, run and wait; returns (the 303 answering RUN, status URL).

    The 303 to the initial submit (job left PENDING) is written by a different
    code path and carries no Content-Type in either mode; the one answering
    PHASE=RUN is what ``async_submit_headers`` controls.
    """
    sub = requests.post(tap_async_url(server),
                        data={"query": "select obsjd from data_l0", "format": "csv"},
                        allow_redirects=False, timeout=60)
    assert sub.status_code == 303, sub.text[:300]
    url = sub.headers["Location"]
    run = requests.post(url + "/phase", data={"PHASE": "RUN"},
                        allow_redirects=False, timeout=60)
    assert run.status_code == 303, run.text[:300]
    deadline = time.time() + 30
    while time.time() < deadline:
        if requests.get(url + "/phase", timeout=30).text.strip() in ("COMPLETED", "ERROR"):
            break
        time.sleep(0.2)
    return run, url


def test_uws_is_neas_form(legacy_tap_server):
    run, url = _completed_job(legacy_tap_server)
    assert "Content-Type" not in run.headers
    assert "Content-Length" not in run.headers
    assert "Connection" not in run.headers
    r = requests.get(url, timeout=30)
    assert r.headers["Content-Type"] == "application/xml"
    job = ET.fromstring(r.text)
    assert job.find(UWS + "phase").text == "COMPLETED"
    assert job.find(UWS + "executionDuration") is not None
    assert job.find(UWS + "executionduration") is None
    assert not job.find(UWS + "destruction").text.endswith("Z")
    dur = requests.get(url + "/executionduration", timeout=30)
    assert dur.status_code == 200 and dur.text.strip().isdigit(), dur.text[:200]


def test_uws_unchanged_in_default_mode(tap_server):
    run, url = _completed_job(tap_server)
    assert run.headers["Content-Type"] == "text/plain"
    assert int(run.headers["Content-Length"]) == len(run.content)
    assert run.headers["Connection"] == "close"
    r = requests.get(url, timeout=30)
    assert r.headers["Content-Type"] == "text/xml"
    job = ET.fromstring(r.text)
    assert job.find(UWS + "executionduration") is not None
    assert job.find(UWS + "destruction").text.endswith("Z")


def test_tables_is_neas_layout(legacy_tap_server):
    r = requests.get(legacy_tap_server + "/cgi-bin/TAP/nph-tap.py/tables", timeout=60)
    assert r.status_code == 200, r.text[:300]


def test_tables_unchanged_in_default_mode(tap_server):
    r = requests.get(tap_server + "/cgi-bin/TAP/nph-tap.py/tables", timeout=60)
    assert r.status_code == 200, r.text[:300]
    assert "<flag>principal</flag>" not in r.text
