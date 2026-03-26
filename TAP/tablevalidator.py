# Copyright (c) 2020, Caltech IPAC.
# This code is released with a BSD 3-clause license. License information is at
#   https://github.com/Caltech-IPAC/nexsciTAP/blob/master/LICENSE


import logging


class TableValidationError(Exception):
    """
    Raised when an ADQL query references a table that is not registered
    in TAP_SCHEMA. Distinct from a generic Exception so callers can
    return HTTP 403 (access denied) rather than HTTP 400 (bad request).
    """
    pass


class TableValidator:
    """
    Validates that table names in an ADQL query are registered in
    the TAP_SCHEMA tables table, preventing access to unauthorized
    database objects.
    """

    def __init__(self, conn, connectInfo=None, debug=0):

        self.conn = conn
        self.debug = debug

        self.tap_schema = 'TAP_SCHEMA'
        self.tables_table = 'tables'

        if connectInfo is not None:
            if 'tap_schema' in connectInfo:
                self.tap_schema = connectInfo['tap_schema']
            if 'tables_table' in connectInfo:
                self.tables_table = connectInfo['tables_table']

        self.allowed_tables = set()
        self.allowed_bare = set()
        self.allowed_schemas = set()

        self._load_allowed_tables()

    def _load_allowed_tables(self):

        cursor = self.conn.cursor()

        tables_ref = self.tap_schema + '.' + self.tables_table

        cursor.execute('SELECT table_name FROM ' + tables_ref)
        rows = cursor.fetchall()
        cursor.close()

        for row in rows:
            full_name = row[0].strip().lower()
            self.allowed_tables.add(full_name)

            if '.' in full_name:
                schema, bare = full_name.split('.', 1)
                self.allowed_bare.add(bare)
                self.allowed_schemas.add(schema)
            else:
                self.allowed_bare.add(full_name)

        if self.debug:
            logging.debug('')
            logging.debug(
                f'TableValidator: loaded {len(self.allowed_tables)} '
                f'allowed tables: {self.allowed_tables}')

    def validate(self, table_names):

        if not table_names:
            # Empty list means extraction failed — reject rather than pass through.
            raise Exception('No table names found in query. Unable to verify table access.')

        for tname in table_names:
            tname_lower = tname.strip().lower()

            # Exact match against full table names (e.g. "tap_schema.columns")
            if tname_lower in self.allowed_tables:
                continue

            if '.' in tname_lower:
                schema, bare = tname_lower.split('.', 1)

                # Schema-qualified query table: only match if the schema
                # is one we know about AND the bare name is allowed.
                # This prevents "information_schema.tables" from matching
                # just because "tables" is a bare name in TAP_SCHEMA.
                if schema in self.allowed_schemas and \
                        bare in self.allowed_bare:
                    continue
            else:
                # Unqualified query table: bare-name match is fine
                # (e.g. query says "columns", whitelist has
                # "tap_schema.columns")
                if tname_lower in self.allowed_bare:
                    continue

            raise TableValidationError(
                f'Table \'{tname}\' is not available for querying. '
                f'Use TAP_SCHEMA.tables to see available tables.')

        if self.debug:
            logging.debug('')
            logging.debug(
                f'TableValidator: all tables validated: {table_names}')
