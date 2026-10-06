"""Regression coverage for proprietary row filtering and credential handling."""
from __future__ import annotations

import logging
import re
import sqlite3
import sys
from types import SimpleNamespace

import pytest

from TAP import propfilter, vositables
from TAP.configparam import configParam
from TAP.propfilter import propFilter
from TAP.tap import Tap
from TAP.tapquery import tapQuery
from TAP.taputil import tapUtil

ORACLE_CONFIG = """[WEB]
TAP_WORKDIR={work}/
TAP_WORKURL=/workspace/
HTTP_URL=http://localhost
HTTP_PORT=80
CGI_PGM=/TAP
[DBMS]
DBMS=oracle
ServerName=testdb
UserID=test
Password=secret
COOKIENAME=tapcookie
PROPFILTER=neid
FILEID=filename
ACCESSID=program
RACOL=ra
DECCOL=dec
[SPTIND]
MODE=HTM
LEVEL=20
XCOL=x
YCOL=y
ZCOL=z
COLNAME=htm20
ENCODING=BASE10
"""


@pytest.mark.parametrize("query, where", [
    ("select * from neidl2", ""),
    ("SELECT COUNT(*) FROM neidl2 WHERE obstype = 'Sci' OR obstype = 'Cal'",
     "WHERE obstype = 'Sci' OR obstype = 'Cal'"),
    ("select program, count(*) from neidl2 where (x = 1 or x = 2) group by program",
     "where (x = 1 or x = 2)"),
])
def test_filterable_accepts_single_table_queries(query, where):
    assert propfilter._filterable(query, where)


@pytest.mark.parametrize("query, where", [
    ("select * from neidl2 join neidl1 on neidl2.x = neidl1.x", ""),
    ("select * from neidl2\nLEFT\tJOIN\nneidl1 on neidl2.x = neidl1.x", ""),
    ("select * from neidl2, neidl1", ""),
    ("select (select x from neidl1) from neidl2", ""),
    ("select * from neidl2 where x in (select x from neidl1)",
     "where x in (select x from neidl1)"),
    ("select * from neidl2 union all select * from neidl1", ""),
    ("select * from neidl2 where (x = 1", "where (x = 1"),
    ("select * from neidl2 where x = 1)(", "where x = 1)("),
    ("select * from neidl2; select * from neidl1", ""),
    ("select * from neidl2 where x = 1 -- trailing comment", "where x = 1 -- trailing comment"),
    ("select * from neidl2 /* comment */", ""),
    ("select * from neidl2 where x = 'select'", "where x = 'select'"),
    ("select * from neidl2 a", ""),
    ("select * from neidl2 connect by prior x = x", ""),
])
def test_filterable_refuses_queries_the_filter_cannot_rewrite(query, where):
    assert not propfilter._filterable(query, where)


class _OracleSQLiteCursor:
    """Execute the real filter SQL locally, adapting only Oracle temp-table syntax."""

    def __init__(self, conn):
        self.cursor = conn.cursor()

    def execute(self, sql, params=None):
        sql = sql.replace("create global temporary table", "create temporary table")
        sql = sql.replace(" on commit preserve rows", "")
        sql = re.sub(r"^(insert into \w+)\((select .*)\)$", r"\1 \2", sql)
        return self.cursor.execute(sql) if params is None else self.cursor.execute(sql, params)

    def parse(self, sql):
        self.cursor.execute("explain " + sql)

    def __getattr__(self, name):
        return getattr(self.cursor, name)


class _OracleSQLiteConnection:
    def __init__(self, conn):
        self.conn = conn

    def cursor(self):
        return _OracleSQLiteCursor(self.conn)


@pytest.mark.parametrize("archive, level", [
    ("koa", ""), ("neid", "l0"), ("neid", "l1"), ("neid", "l2"), ("neid", "eng"),
])
@pytest.mark.parametrize("authenticated", [False, True])
@pytest.mark.parametrize("where, public_ids", [
    ("where obstype = 'Sci'", ["public-sci"]),
    ("WHERE obstype = 'Sci' OR obstype = 'Sci'", ["public-sci"]),
    ("where obstype = 'Sci' OR 1 = 0", ["public-sci"]),
    ("where 1 = 0 OR obstype = 'Sci'", ["public-sci"]),
    ("where (obstype = 'Sci' OR obstype = 'Cal')", ["public-cal", "public-sci"]),
    ("where obstype = 'Sci' OR obstype = 'Cal'", ["public-cal", "public-sci"]),
    ("", ["public-cal", "public-sci"]),
])
def test_allowed_files_apply_access_rules_to_every_or_branch(tmp_path, archive, level,
                                                           authenticated, where, public_ids):
    with sqlite3.connect(":memory:") as conn:
        # Zero-month intervals let SQLite evaluate the date comparison unchanged.
        conn.create_function("add_months", 2, lambda date, months: date if months == 0 else None)
        conn.execute("create table observations (filename text, obstype text, program text, "
                     "date_obs text, obsdate text, propint int, l0propint int, l1propint int, l2propint int)")
        conn.executemany("insert into observations values (?, ?, ?, ?, ?, 0, 0, 0, 0)", [
            ("public-sci", "Sci", "public", "2000-01-01", "2000-01-01"),
            ("public-cal", "Cal", "public", "2000-01-01", "2000-01-01"),
            ("owned-sci", "Sci", "owned", "9999-01-01", "9999-01-01"),
            ("private-sci", "Sci", "other", "9999-01-01", "9999-01-01"),
        ])
        conn.execute("create table allowed_programs (program text)")
        conn.execute("insert into allowed_programs values ('owned')")
        pf = propFilter.__new__(propFilter)
        pf.conn = _OracleSQLiteConnection(conn)
        pf.dbms, pf.propfilter, pf.datalevel = "oracle", archive, level
        pf.userid = "authorized-user" if authenticated else ""
        pf.arraysize = 1000
        pf.userworkdir = str(tmp_path)
        pf.__createTmpFileiddb__("allowed_files", "filename", "filename_allowed",
                                "observations", where, "program", "allowed_programs")
        ids = [row[0] for row in conn.execute("select filename_allowed from allowed_files order by 1")]
        expected = sorted(public_ids + (["owned-sci"] if authenticated else []))
        assert ids == expected


class _RecordingCursor:
    description = (("password",),)
    arraysize = 1000
    rowcount = 1

    def __init__(self):
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))

    def fetchmany(self):
        return [("test-credential",)]


@pytest.mark.parametrize("dbms, placeholder", [("oracle", ":userid"), ("pgsql", "%(userid)s")])
@pytest.mark.parametrize("archive, column", [("koa", "passwd"), ("neid", "password")])
def test_cookie_user_lookup_binds_userid(dbms, placeholder, archive, column):
    cursor = _RecordingCursor()
    cursor.description = [(column,)]
    pf = propFilter.__new__(propFilter)
    pf.dbms = dbms
    pf.conn = SimpleNamespace(cursor=lambda: cursor)
    pf.__validateUser__("tapcookie", "tapcookie=o'brien|test-credential", archive, "users")
    assert pf.status == "ok"
    assert cursor.calls == [(f"select {column} from users where userid = {placeholder}",
                             {"userid": "o'brien"})]


@pytest.mark.parametrize("dbms, placeholder", [("oracle", ":userid"), ("pgsql", "%(userid)s")])
def test_access_list_binds_userid(dbms, placeholder, monkeypatch):
    cursor = _RecordingCursor()
    pf = propFilter.__new__(propFilter)
    pf.dbms = dbms
    pf.conn = SimpleNamespace(cursor=lambda: cursor)
    monkeypatch.setattr(pf, "__dropDbtbl__", lambda _: None)
    pf.__createTmpAccessiddb__("allowed_programs", "o'brien", "program", "access_list")
    insert = [call for call in cursor.calls if call[0].startswith("insert ")]
    assert insert == [(("insert into allowed_programs(select lower(program) as program "
                       f"from access_list where userid = {placeholder})"), {"userid": "o'brien"})]


def test_debug_authentication_does_not_log_credentials(caplog):
    cursor = _RecordingCursor()
    pf = propFilter.__new__(propFilter)
    pf.dbms, pf.debug = "oracle", 1
    pf.conn = SimpleNamespace(cursor=lambda: cursor)
    with caplog.at_level(logging.DEBUG):
        pf.__validateUser__("tapcookie", "tapcookie=o'brien|test-credential", "neid", "users")
    assert pf.status == "ok"
    assert "test-credential" not in caplog.text


def test_debug_configuration_does_not_log_database_password(tmp_path, caplog):
    conf = tmp_path / "TAP.conf"
    conf.write_text(ORACLE_CONFIG.format(work=tmp_path).replace("Password=secret", "Password=db-secret-marker"))
    with caplog.at_level(logging.DEBUG):
        cp = configParam(str(conf), debug=1)
    assert cp.connectInfo["password"] == "db-secret-marker"
    assert "db-secret-marker" not in caplog.text


@pytest.mark.parametrize("caller", ["query", "utility"])
def test_database_helpers_do_not_log_passwords(caller, tmp_path, monkeypatch, caplog):
    def unavailable_connection(*args, **kwargs):
        raise RuntimeError("test database unavailable")

    monkeypatch.setitem(sys.modules, "cx_Oracle", SimpleNamespace(connect=unavailable_connection))
    monkeypatch.setattr(sys, "tracebacklimit", getattr(sys, "tracebacklimit", 1000), raising=False)
    conf = tmp_path / "TAP.conf"
    conf.write_text(ORACLE_CONFIG.format(work=tmp_path).replace("Password=secret", "Password=helper-secret-marker"))
    cp = configParam(str(conf))
    with caplog.at_level(logging.DEBUG), pytest.raises(Exception, match="connect"):
        if caller == "query":
            tapQuery(query="select * from neidl2", workdir=str(tmp_path), connectInfo=cp.connectInfo, debug=1)
        else:
            tapUtil(configpath=str(conf), debug=1)
    assert "helper-secret-marker" not in caplog.text


@pytest.mark.parametrize("operator_debug, request_debug, expected", [
    (None, True, 0), ("0", True, 0), ("1", False, 1),
])
def test_request_cannot_enable_debug(monkeypatch, tmp_path, capsys, operator_debug, request_debug, expected):
    if operator_debug is None:
        monkeypatch.delenv("TAP_DEBUG", raising=False)
    else:
        monkeypatch.setenv("TAP_DEBUG", operator_debug)
    monkeypatch.setenv("PATH_INFO", "/sync")
    monkeypatch.delenv("TAP_CONF", raising=False)
    app = Tap.__new__(Tap)
    app.form = {"debug": SimpleNamespace(value="1")} if request_debug else {}
    app.debug, app.param, app.infomsg, app.dbtable = 0, {}, "", ""
    app.debugfname = str(tmp_path / "tap.debug")
    with pytest.raises(SystemExit):
        app.__run__()
    capsys.readouterr()
    assert app.debug == expected


def test_operator_debug_does_not_log_cookies_or_tokens(monkeypatch, tmp_path, caplog, capsys):
    monkeypatch.setenv("TAP_DEBUG", "1")
    monkeypatch.setenv("PATH_INFO", "/sync")
    monkeypatch.setenv("HTTP_COOKIE", "tapcookie=user|cookie-secret-marker")
    monkeypatch.delenv("TAP_CONF", raising=False)
    app = Tap.__new__(Tap)
    app.form = {"token": SimpleNamespace(value="tapcookie=user|token-secret-marker")}
    app.debug, app.param, app.infomsg, app.dbtable = 1, {}, "", ""
    app.debugfname = str(tmp_path / "tap.debug")
    with caplog.at_level(logging.DEBUG), pytest.raises(SystemExit):
        app.__run__()
    capsys.readouterr()
    assert "cookie-secret-marker" not in caplog.text
    assert "token-secret-marker" not in caplog.text


@pytest.mark.parametrize("query", [
    "select a.program from data_l1 a join data_l0 b on a.program = b.program",
    "select program from data_l1 where program in (select program from data_l0)",
    "select a.program from data_l1 a, data_l0 b",
    "select a.program from data_l0 a join data_l1 b on a.program = b.program",
    "select program from data_l0 where program in (select program from data_l1)",
    "select table_name from tap_schema.tables where exists (select program from data_l1)",
])
def test_unsupported_proprietary_query_returns_400(tmp_path, query, monkeypatch, capsys):
    def unavailable_connection(*args, **kwargs):
        raise RuntimeError("a rejected query must not connect to the database")

    monkeypatch.setitem(sys.modules, "cx_Oracle", SimpleNamespace(connect=unavailable_connection))
    tap_conf = tmp_path / "TAP.conf"
    tap_conf.write_text(ORACLE_CONFIG.format(work=tmp_path))
    monkeypatch.setenv("TAP_CONF", str(tap_conf))
    monkeypatch.setenv("PATH_INFO", "/sync")
    monkeypatch.delenv("TAP_DEBUG", raising=False)
    monkeypatch.delenv("HTTP_COOKIE", raising=False)
    app = Tap.__new__(Tap)
    app.form = {key: SimpleNamespace(value=value) for key, value in
                {"query": query, "format": "votable"}.items()}
    app.debug, app.propflag, app.param, app.statdict = 0, -1, {}, {}
    with pytest.raises(SystemExit):
        app.__init__()
    output = capsys.readouterr().out
    assert output.startswith("HTTP/1.1 400"), output[:500]
    assert "proprietary data" in output
    assert 'value="ERROR"' in output


def test_filter_rejects_unsupported_query_before_creating_temporary_tables(tmp_path, monkeypatch):
    config = tmp_path / "TAP.conf"
    config.write_text(ORACLE_CONFIG.format(work=tmp_path))
    info = configParam(str(config)).connectInfo
    config.unlink()
    with sqlite3.connect(":memory:") as conn:
        conn.execute("attach database ':memory:' as TAP_SCHEMA")
        conn.execute("create table TAP_SCHEMA.tables (table_name text)")
        conn.executemany("insert into TAP_SCHEMA.tables values (?)", [("data_l0",), ("data_l1",)])
        for table in ("data_l0", "data_l1"):
            conn.execute("create table " + table + " (program text)")
        bridge = _OracleSQLiteConnection(conn)
        monkeypatch.setitem(sys.modules, "cx_Oracle", SimpleNamespace(connect=lambda *args: bridge))
        with pytest.raises(Exception, match="proprietary data"):
            propFilter(connectInfo=info, workdir=str(tmp_path), propfilter="neid",
                       query="select a.program from data_l1 a join data_l0 b on a.program = b.program",
                       fileid="filename", accessid="program")
        assert not conn.execute("select name from sqlite_temp_master").fetchall()
    assert list(tmp_path.iterdir()) == []


def test_vosi_debug_does_not_log_database_password(monkeypatch, tmp_path, caplog):
    def unavailable_connection(*args, **kwargs):
        raise RuntimeError("test database unavailable")

    monkeypatch.setitem(sys.modules, "cx_Oracle", SimpleNamespace(connect=unavailable_connection))
    with caplog.at_level(logging.DEBUG), pytest.raises(Exception, match="Required connectInfo"):
        vositables.vosiTables(dbms="oracle", dbserver="test", userid="test",
                          password="vosi-secret-marker", debug=1,
                          outpath=str(tmp_path / "tables.xml"))
    assert "vosi-secret-marker" not in caplog.text
