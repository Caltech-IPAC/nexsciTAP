# Copyright (c) 2020, Caltech IPAC.
# This code is released with a BSD 3-clause license. License information is at
#   https://github.com/Caltech-IPAC/nexsciTAP/blob/master/LICENSE


import logging
import re


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

    # DML/DDL keywords that must never appear in a TAP query.
    # TAP is a read-only protocol (IVOA TAP 1.1).  The ADQL translator
    # already rejects non-SELECT at the parser level, but this check
    # runs before ADQL translation as an additional layer of defense.
    _FORBIDDEN_RE = re.compile(
        r'\b('
        r'INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|'
        r'EXEC|EXECUTE|GRANT|REVOKE|COMMIT|ROLLBACK|MERGE|'
        r'CALL|DECLARE|SET\b\s|BEGIN|'
        r'INTO\s+OUTFILE|LOAD\s+DATA'
        r')\b',
        re.IGNORECASE
    )

    # Oracle-specific functions that should never be called through TAP.
    # These are dangerous even in a SELECT context: UTL_HTTP enables SSRF,
    # UTL_FILE enables filesystem access, DBMS_SQL enables dynamic SQL.
    _DANGEROUS_FUNCTIONS_RE = re.compile(
        r'\b('
        r'UTL_HTTP|UTL_TCP|UTL_SMTP|UTL_FILE|UTL_INADDR|'
        r'HTTPURITYPE|DBMS_LDAP|DBMS_SQL|DBMS_SCHEDULER|'
        r'DBMS_JOB|DBMS_METADATA|DBMS_XMLGEN|'
        r'SYS_CONTEXT|XMLTYPE'
        r')\b',
        re.IGNORECASE
    )

    # Oracle system catalog views and tables that should never be
    # referenced in a TAP query.  The TableValidator.validate() method
    # already blocks these (they are not in TAP_SCHEMA), but this is
    # an additional layer that catches them before any database
    # connection is made.  Added after an external vulnerability report
    # used ALL_USERS to enumerate Oracle accounts via UNION ALL bypass.
    _SYSTEM_OBJECTS_RE = re.compile(
        r'\b('
        r'ALL_USERS|DBA_USERS|ALL_TAB_PRIVS|ALL_TAB_COLUMNS|'
        r'ALL_TABLES|ALL_OBJECTS|ALL_SOURCE|ALL_VIEWS|'
        r'DBA_TAB_PRIVS|DBA_SYS_PRIVS|DBA_ROLE_PRIVS|'
        r'DBA_TABLES|DBA_OBJECTS|DBA_SOURCE|DBA_VIEWS|'
        r'USER_TABLES|USER_TAB_PRIVS|USER_SYS_PRIVS|'
        r'USER_OBJECTS|USER_SOURCE|USER_ROLE_PRIVS|'
        r'V\$SESSION|V\$INSTANCE|V\$DATABASE|V\$PARAMETER|'
        r'INFORMATION_SCHEMA|PG_CATALOG|PG_TABLES|PG_ROLES'
        r')\b',
        re.IGNORECASE
    )

    @staticmethod
    def validate_statement(query, debug=False):
        """Reject queries containing DML, DDL, or dangerous Oracle functions.

        TAP is a read-only protocol.  This method blocks statement types
        and function calls that should never appear in a TAP query,
        regardless of whether the database user has the privileges to
        execute them.  This is defense-in-depth: even if database
        privileges are misconfigured, the application layer rejects
        the query before it reaches the DBMS.

        Raises Exception if the query contains forbidden content.
        """

        # Semicolons indicate statement stacking.  TAP accepts a single
        # statement per request.  Reject before any other check so that
        # chained statements cannot bypass keyword scanning.
        if ';' in query:
            if debug:
                logging.debug(
                    'TableValidator: rejected semicolon (statement stacking)')
            raise Exception(
                'Query rejected: semicolons are not permitted. '
                'TAP accepts a single statement per request.')

        forbidden = TableValidator._FORBIDDEN_RE.search(query)
        if forbidden:
            keyword = forbidden.group(1).upper()
            if debug:
                logging.debug(
                    f'TableValidator: rejected forbidden keyword: {keyword}')
            raise Exception(
                f'Query rejected: \'{keyword}\' statements are not '
                f'permitted. TAP only supports SELECT queries.')

        dangerous = TableValidator._DANGEROUS_FUNCTIONS_RE.search(query)
        if dangerous:
            func = dangerous.group(1).upper()
            if debug:
                logging.debug(
                    f'TableValidator: rejected dangerous function: {func}')
            raise Exception(
                f'Query rejected: \'{func}\' is not permitted in '
                f'TAP queries.')

        system_obj = TableValidator._SYSTEM_OBJECTS_RE.search(query)
        if system_obj:
            obj = system_obj.group(1).upper()
            if debug:
                logging.debug(
                    f'TableValidator: rejected system catalog '
                    f'reference: {obj}')
            raise Exception(
                f'Query rejected: \'{obj}\' is a system catalog '
                f'and is not available for querying. '
                f'Use TAP_SCHEMA.tables to see available tables.')
