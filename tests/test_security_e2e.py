"""End-to-end security regression tests.

Each test is named after the specific regression or security boundary it
guards. The intent is that any future change that re-opens one of these
holes turns the corresponding test red at PR time, making the regression
unmissable in code review.

Actual TAP status-code behavior in v3.0.0:
- 403 only for `TableValidationError`. The validator raises this for
  *system-catalog* references (e.g., sqlite_master, dba_users), and
  the table whitelist raises it for tables not in TAP_SCHEMA.tables.
- 400 for everything else, including DML rejection, dangerous-function
  rejection, semicolon-chained statements, and ordinary query errors.
  This is inconsistent — DML/dangerous-func rejections are security
  policy decisions and should arguably also be 403 — but the existing
  code raises plain Exception for those, not TableValidationError.
  Tests match the actual behavior; harmonizing the codes is a follow-up.
- 500 for startup failures.
"""
from __future__ import annotations

import pytest
import requests
from _helpers import row_count_from_ipac, tap_sync_url

# ---------------------------------------------------------------- helpers

def _post(server: str, query: str, **params) -> requests.Response:
    """POST a sync TAP query."""
    data = {"query": query, "format": "ipac"}
    data.update(params)
    return requests.post(tap_sync_url(server), data=data)


def _count(server: str, query: str, **params) -> int:
    """Run a query and return the row count from the IPAC response.

    Asserts a 200 before counting — caller should expect success.
    """
    r = _post(server, query, **params)
    assert r.status_code == 200, f"expected 200 got {r.status_code}: {r.text[:300]}"
    return row_count_from_ipac(r.text)


# ---------------------------------------------------------------- table whitelist

def test_table_whitelist_rejects_system_catalog(tap_server: str):
    """SQL injection class: system-catalog reads must be blocked.

    Regression marker: cd3d963 (initial table whitelist).
    """
    r = _post(tap_server, "select * from sqlite_master")
    assert r.status_code == 403, r.text[:300]


def test_table_whitelist_rejects_unregistered_table(tap_server: str):
    """Tables not in TAP_SCHEMA.tables are 403, not 400 or 200."""
    r = _post(tap_server, "select * from not_in_tap_schema_at_all")
    assert r.status_code == 403, r.text[:300]


def test_table_whitelist_allows_propfilter_internal_table_in_subquery(tap_server: str):
    """The propfilter's access-control table is reachable from subqueries.

    Regression markers: bdbd2d7, de1941f. The fix excluded the configured
    ACCESS_TBL / USERS_TBL from validation so the prop check could still
    reference them in `WHERE program IN (SELECT program FROM test_access ...)`.
    """
    q = (
        "select l1filename from data_l1 "
        "where program in (select program from test_access where userid='') "
        "or l1propint = 0"
    )
    r = _post(tap_server, q, propflag="0")
    assert r.status_code == 200, r.text[:300]


def test_order_by_not_treated_as_table(tap_server: str):
    """ORDER BY columns must not be misidentified as table names.

    Regression marker: 1404e15. sqlparse tokenizes 'ORDER BY' as a single
    keyword; the table extractor was treating columns after it as tables
    until that fix.
    """
    r = _post(tap_server, "select l0filename from data_l0 order by obsdate")
    assert r.status_code == 200, r.text[:300]
    assert row_count_from_ipac(r.text) == 6


def test_unknown_statement_does_not_skip_validation(tap_server: str):
    """Oracle-dialect queries using ROWNUM must still be validated.

    Regression marker: 867278c. sqlparse returns get_type() == 'UNKNOWN'
    for ROWNUM-bearing queries; an earlier extractor skipped UNKNOWN
    statements entirely, letting them through validation. The fix
    extracts tables from UNKNOWN statements too.
    """
    r = _post(
        tap_server,
        "select * from not_in_tap_schema where rownum < 10",
    )
    assert r.status_code == 403, r.text[:300]


# ---------------------------------------------------------------- propflag

def test_propflag_zero_honored_for_simple_l0(tap_server: str):
    """L0 with propflag=0 must return all rows — the UI's L0 metadata
    listing path. data_l0 has 6 rows total (all public, propint=0)."""
    n = _count(tap_server, "select l0filename from data_l0", propflag="0")
    assert n == 6


@pytest.mark.skip(
    reason="Cannot exercise the propflag guard with the SQLite fixture. "
           "configparam.py only reads PROPFILTER/ACCESS_TBL/USERS_TBL "
           "inside its `if self.dbms == 'oracle'` branch (~lines 255-345), "
           "so on a SQLite config those keys are silently dropped and "
           "self.config.propfilter ends up '' — neither the NEID nor the "
           "KOA guard fires. This is a separate bug worth fixing (the "
           "guard semantics should not be Oracle-only). End-to-end propflag "
           "coverage will land with the deferred Oracle/Postgres/MySQL CI "
           "matrix work."
)
def test_propflag_bypass_routes_l1_through_propfilter(tap_server: str):
    """L1 with propflag=0 must route through propfilter, not bare tapQuery.

    Regression markers: PR #16 + PR #17. The guard in tap.py:1612-1638
    forces propflag=1 when datalevel is eng/l1/l2. Cannot be exercised
    against the SQLite fixture (see skip reason)."""
    # If/when configparam.py is fixed to read PROPFILTER for non-Oracle
    # configs, this test will run and assert:
    #   - propflag=0 status_code != 200 (because propfilter then errors
    #     on add_months — fixing THAT is a separate concern)
    #   - propflag=0 status_code == propflag=1 status_code
    raise AssertionError("Skipped — see decorator")


def test_propflag_whitelist_l0_complex_case_does_not_misfire(tap_server: str):
    """The UI's L0 download-link query shape (CASE WHEN with a subquery
    FROM nested in the SELECT list) with propflag=0 must succeed and
    return rows.

    Regression marker: PR #16 introduced a bug where the propflag guard
    used `datalevel != 'l0'`, which evaluates True for the empty
    datalevel that __getDatalevel__ returns when the table-name parser
    picks the subquery's table first. That bug routed the L0 query
    through propfilter, which then constructed malformed prop-check SQL.

    PR #17 switched to an explicit `datalevel in ('eng','l1','l2')`
    whitelist so the misfire stopped happening — when datalevel='', the
    whitelist doesn't match, and the L0 query stays on the tapQuery path.

    Query structure is the regression trigger: a CASE WHEN containing a
    `SELECT ... FROM test_access` subquery in the SELECT list. The
    table-name parser yields `test_access` first; __getDatalevel__ on
    'test_access' returns ''. Production used `add_months(obsdate,
    l0propint)` in the CASE condition; SQLite doesn't have add_months,
    so this test uses a comparison the table-name parser still treats
    identically.
    """
    q = (
        "select (case when program in "
        "(SELECT program from test_access where userid = '') "
        "then l0filename else 'redacted' end) as fn, "
        "l0filename, program from data_l0"
    )
    r = _post(tap_server, q, propflag="0")
    assert r.status_code == 200, r.text[:300]
    assert row_count_from_ipac(r.text) == 6


# ---------------------------------------------------------------- statement validator

def test_statement_validator_rejects_dml(tap_server: str):
    """INSERT/UPDATE/DELETE/DROP must all be rejected (status 400).

    `validate_statement` raises plain Exception (not TableValidationError)
    for these, so the response is 400 rather than 403. Arguably they
    should also be 403 — security policy rejection rather than malformed
    query — but harmonizing the codes is a separate follow-up.
    """
    for dml in (
        "insert into data_l0 (l0filename) values ('x')",
        "update data_l0 set l0filename = 'x'",
        "delete from data_l0",
        "drop table data_l0",
    ):
        r = _post(tap_server, dml)
        assert r.status_code == 400, f"{dml!r} -> {r.status_code}: {r.text[:200]}"
        assert "not permitted" in r.text or "rejected" in r.text, r.text[:300]


def test_statement_validator_rejects_oracle_dangerous_functions(tap_server: str):
    """utl_http, dbms_sql, sys_context etc. must be rejected (status 400).

    Same 400-vs-403 inconsistency as DML rejection — see that test.
    """
    for sql in (
        "select utl_http.request('http://evil/x') from data_l0",
        "select dbms_sql.parse(1) from data_l0",
        "select sys_context('USERENV','IP_ADDRESS') from data_l0",
    ):
        r = _post(tap_server, sql)
        assert r.status_code == 400, f"{sql!r} -> {r.status_code}: {r.text[:200]}"
        assert "not permitted" in r.text, r.text[:300]


def test_statement_validator_rejects_semicolon_chained(tap_server: str):
    """Semicolon-chained statements (classic SQL injection) are rejected.

    Same 400-vs-403 inconsistency as DML rejection.
    """
    r = _post(
        tap_server,
        "select l0filename from data_l0; drop table data_l0",
    )
    assert r.status_code == 400, r.text[:300]
    assert "semicolon" in r.text.lower(), r.text[:300]


# ---------------------------------------------------------------- HTTP status codes

def test_http_status_code_400_for_query_error(tap_server: str):
    """A query referencing a missing column on a valid table → 400."""
    r = _post(tap_server, "select column_that_does_not_exist from data_l0")
    # The column doesn't exist; SQLite/TAP returns this as a query error,
    # which is 400 (not 403, since the table itself is valid).
    assert r.status_code == 400, r.text[:300]


# ---------------------------------------------------------------- error message safety

def test_table_reject_error_message_is_safe(tap_server: str):
    """Error responses for rejected tables must not leak internal paths
    or expose stack-trace internals.

    Regression marker: 4fa46ff (generic startup error messages) +
    c202285 (don't leak raw Oracle errors). This test is a sanity check
    that the error response for a table rejection stays clean.
    """
    r = _post(tap_server, "select * from sqlite_master")
    body = r.text
    # No bare file paths (full /tmp/, /var/, /Users/ etc.)
    forbidden_substrings = [
        "Traceback",
        "Password=",
        "/tmp/tap_",
        "site-packages",
    ]
    for s in forbidden_substrings:
        assert s not in body, f"Leaked {s!r} in error response:\n{body[:500]}"
