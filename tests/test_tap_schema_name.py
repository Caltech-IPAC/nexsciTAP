"""TAP_SCHEMA's name in SQL: the schema for Oracle, the ATTACH name for SQLite.

configparam always passes tap_schema_file='TAP_SCHEMA.db' (SQLite's attach
name).  Using it for Oracle produced 'FROM TAP_SCHEMA.db.tables' and
ORA-03048 on every query (found with nea-box, 2026-10-05).
"""
import contextlib

from TAP.datadictionary import dataDictionary
from TAP.tablevalidator import TableValidator


class _Cursor:
    def __init__(self, log):
        self.log = log
        self.description = [('column_name',), ('datatype',), ('description',), ('unit',)]
        self.arraysize = 100

    def execute(self, sql, *params):
        self.log.append(sql)

    def fetchall(self):
        return [('ps',)]

    def fetchmany(self, *a):
        return []

    def close(self):
        pass


class _Conn:
    def __init__(self):
        self.log = []

    def cursor(self):
        return _Cursor(self.log)


def info(dbms, file_name):
    return {'dbms': dbms, 'tap_schema': 'TAP_SCHEMA', 'tap_schema_file': file_name,
            'tables_table': 'tables', 'columns_table': 'columns'}


def test_validator_uses_schema_name_for_oracle():
    conn = _Conn()
    TableValidator(conn, info('oracle', 'TAP_SCHEMA.db'))
    assert conn.log == ['SELECT table_name FROM TAP_SCHEMA.tables']


def test_validator_uses_attach_name_for_sqlite():
    conn = _Conn()
    TableValidator(conn, info('sqlite3', 'TS'))
    assert conn.log == ['SELECT table_name FROM TS.tables']


def test_data_dictionary_uses_schema_name_for_oracle():
    conn = _Conn()
    with contextlib.suppress(Exception):     # the fake has no rows; only the SQL matters
        dataDictionary(conn, 'ps', info('oracle', 'TAP_SCHEMA.db'))
    assert conn.log and conn.log[0].startswith('select * from TAP_SCHEMA.columns ')


def test_data_dictionary_uses_attach_name_for_sqlite():
    conn = _Conn()
    with contextlib.suppress(Exception):
        dataDictionary(conn, 'ps', info('sqlite3', 'TS'))
    assert conn.log and conn.log[0].startswith('select * from TS.columns ')
