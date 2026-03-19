import os
import sqlite3
import tempfile
import unittest

from TAP.tablevalidator import TableValidator, TableValidationError


def _make_db(table_names, schema_name='TAP_SCHEMA', tables_table='tables'):
    """Create SQLite DBs with TAP_SCHEMA attached, mimicking real setup.

    Returns (conn, tap_schema_path) — caller is responsible for cleanup.
    """
    fd, tap_schema_path = tempfile.mkstemp(suffix='.db')
    os.close(fd)

    schema_conn = sqlite3.connect(tap_schema_path)
    schema_conn.execute('CREATE TABLE ' + tables_table + ' (table_name TEXT)')
    for name in table_names:
        schema_conn.execute(
            'INSERT INTO ' + tables_table + ' VALUES (?)', (name,))
    schema_conn.commit()
    schema_conn.close()

    conn = sqlite3.connect(':memory:')
    conn.execute(
        'ATTACH DATABASE ? AS ' + schema_name, (tap_schema_path,))

    return conn, tap_schema_path


class TestTableValidator(unittest.TestCase):

    def setUp(self):
        self.conn, self._tap_schema_path = _make_db([
            'ps',
            'pscomppars',
            'stellarhosts',
            'TAP_SCHEMA.tables',
            'TAP_SCHEMA.columns',
            'TAP_SCHEMA.schemas',
            'cumulative',
        ])
        self.connectInfo = {
            'tap_schema': 'TAP_SCHEMA',
            'tables_table': 'tables',
        }

    def tearDown(self):
        self.conn.close()
        if os.path.exists(self._tap_schema_path):
            os.unlink(self._tap_schema_path)

    def test_exact_match(self):
        v = TableValidator(self.conn, connectInfo=self.connectInfo)
        v.validate(['ps'])  # should not raise

    def test_default_connectInfo(self):
        """Works with no connectInfo (uses TAP_SCHEMA.tables default)."""
        v = TableValidator(self.conn)
        v.validate(['ps'])

    def test_case_insensitive(self):
        v = TableValidator(self.conn, connectInfo=self.connectInfo)
        v.validate(['PS'])
        v.validate(['Ps'])
        v.validate(['TAP_SCHEMA.Tables'])

    def test_schema_prefix_in_whitelist_bare_in_query(self):
        """TAP_SCHEMA.columns is whitelisted; query says just 'columns'."""
        v = TableValidator(self.conn, connectInfo=self.connectInfo)
        v.validate(['columns'])

    def test_bare_in_whitelist_unknown_schema_in_query(self):
        """'ps' is whitelisted bare; 'public.ps' has unknown schema — rejected."""
        v = TableValidator(self.conn, connectInfo=self.connectInfo)
        # Assert TableValidationError specifically so a revert to generic Exception
        # would be caught — tap.py relies on this type for 403 vs 400 distinction.
        with self.assertRaises(TableValidationError):
            v.validate(['public.ps'])

    def test_known_schema_bare_table_in_query(self):
        """'ps' is whitelisted bare; 'tap_schema.ps' uses known schema — allowed."""
        v = TableValidator(self.conn, connectInfo=self.connectInfo)
        v.validate(['tap_schema.ps'])

    def test_disallowed_table_raises(self):
        v = TableValidator(self.conn, connectInfo=self.connectInfo)
        # Assert TableValidationError specifically so a revert to generic Exception
        # would be caught — tap.py relies on this type for 403 vs 400 distinction.
        with self.assertRaises(TableValidationError) as ctx:
            v.validate(['pg_catalog.pg_tables'])
        self.assertIn('not available', str(ctx.exception))

    def test_multi_table_all_valid(self):
        v = TableValidator(self.conn, connectInfo=self.connectInfo)
        v.validate(['ps', 'pscomppars', 'stellarhosts'])

    def test_multi_table_one_invalid(self):
        v = TableValidator(self.conn, connectInfo=self.connectInfo)
        # Assert TableValidationError specifically so a revert to generic Exception
        # would be caught — tap.py relies on this type for 403 vs 400 distinction.
        with self.assertRaises(TableValidationError) as ctx:
            v.validate(['ps', 'information_schema.tables', 'stellarhosts'])
        self.assertIn('information_schema.tables', str(ctx.exception))

    def test_empty_table_list_raises(self):
        v = TableValidator(self.conn, connectInfo=self.connectInfo)
        # Empty list is a protocol-level error, not a table access rejection —
        # generic Exception is correct here (not TableValidationError).
        with self.assertRaises(Exception):
            v.validate([])

    def test_system_catalog_blocked(self):
        v = TableValidator(self.conn, connectInfo=self.connectInfo)
        for bad_table in [
            'ALL_TABLES',
            'DBA_USERS',
            'V$SESSION',
            'information_schema.tables',
            'pg_catalog.pg_class',
            'EXOFOP.FILES',
        ]:
            # Assert TableValidationError specifically so a revert to generic Exception
            # would be caught — tap.py relies on this type for 403 vs 400 distinction.
            with self.assertRaises(TableValidationError, msg=f'{bad_table} should be blocked'):
                v.validate([bad_table])


if __name__ == '__main__':
    unittest.main()
