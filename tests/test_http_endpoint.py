"""Smoke tests for the CI HTTP fixture.

These don't probe security boundaries (see test_security_e2e.py in the next
PR). They just confirm the fixture itself works: the CGI launches, the SQLite
DBs are wired in, and a few representative queries make it through the full
HTTP→CGI→TAP→SQLite→IPAC pipeline.
"""
from __future__ import annotations

import requests
from _helpers import row_count_from_ipac, tap_sync_url


def test_server_starts(tap_server: str):
    """The fixture comes up and the CGI returns something for a trivial GET."""
    r = requests.get(tap_server)
    # Root path is not a CGI script — we expect a 200 with the directory
    # listing or a 404; either confirms the server itself is alive.
    assert r.status_code in (200, 404)


def test_sync_select_l0(tap_server: str):
    """A trivial select against data_l0 returns 6 rows in IPAC format."""
    r = requests.post(
        tap_sync_url(tap_server),
        data={
            "query": "select l0filename, program from data_l0",
            "format": "ipac",
        },
    )
    assert r.status_code == 200, r.text[:500]
    assert row_count_from_ipac(r.text) == 6


def test_sync_select_l1(tap_server: str):
    """data_l1 has 10 rows total (3 public + 3 expired + 4 embargoed)."""
    r = requests.post(
        tap_sync_url(tap_server),
        data={
            "query": "select l1filename from data_l1",
            "format": "ipac",
            # propflag=0 → tapQuery path → no filter → all rows
            "propflag": "0",
        },
    )
    assert r.status_code == 200, r.text[:500]
    assert row_count_from_ipac(r.text) == 10


def test_sync_get_method(tap_server: str):
    """Sync TAP must also accept GET (not just POST)."""
    r = requests.get(
        tap_sync_url(tap_server),
        params={
            "query": "select l0filename from data_l0",
            "format": "ipac",
        },
    )
    assert r.status_code == 200, r.text[:500]
    assert row_count_from_ipac(r.text) == 6


def test_tap_schema_query(tap_server: str):
    """tap_schema queries take a separate code path; confirm it works."""
    r = requests.post(
        tap_sync_url(tap_server),
        data={
            "query": "select table_name from tap_schema.tables",
            "format": "ipac",
        },
    )
    assert r.status_code == 200, r.text[:500]
    # Our fixture registers 4 user tables + 5 TAP_SCHEMA self-tables = 9.
    assert row_count_from_ipac(r.text) >= 9


def test_csv_format(tap_server: str):
    """The CSV serializer works."""
    r = requests.post(
        tap_sync_url(tap_server),
        data={
            "query": "select l0filename from data_l0",
            "format": "csv",
        },
    )
    assert r.status_code == 200, r.text[:500]
    lines = [L for L in r.text.splitlines() if L.strip()]
    # 1 header line + 6 data lines
    assert len(lines) == 7
