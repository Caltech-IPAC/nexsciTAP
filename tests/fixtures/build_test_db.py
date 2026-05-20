"""Build the SQLite test fixtures: tap_schema.db and test_data.db.

Used by the pytest `test_db` session fixture in tests/conftest.py.  Can also
be run standalone for local inspection:

    python tests/fixtures/build_test_db.py /tmp/fixture-out
"""
from __future__ import annotations

import os
import sqlite3
import sys
from datetime import datetime, timedelta


# ---------------------------------------------------------------- TAP_SCHEMA

TAP_SCHEMA_DDL = [
    """
    CREATE TABLE IF NOT EXISTS schemas (
        schema_name  TEXT NOT NULL,
        utype        TEXT,
        description  TEXT,
        schema_index INTEGER,
        PRIMARY KEY (schema_name)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tables (
        schema_name  TEXT NOT NULL,
        table_name   TEXT NOT NULL,
        table_type   TEXT,
        utype        TEXT,
        description  TEXT,
        table_index  INTEGER,
        PRIMARY KEY (table_name)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS columns (
        table_name   TEXT NOT NULL,
        column_name  TEXT NOT NULL,
        datatype     TEXT,
        arraysize    TEXT,
        size         INTEGER,
        description  TEXT,
        utype        TEXT,
        unit         TEXT,
        ucd          TEXT,
        indexed      INTEGER,
        principal    INTEGER,
        std          INTEGER,
        column_index INTEGER,
        PRIMARY KEY (table_name, column_name)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS keys (
        key_id        TEXT NOT NULL,
        from_table    TEXT,
        target_table  TEXT,
        description   TEXT,
        utype         TEXT,
        PRIMARY KEY (key_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS key_columns (
        key_id        TEXT NOT NULL,
        from_column   TEXT,
        target_column TEXT,
        PRIMARY KEY (key_id, from_column)
    )
    """,
]

SCHEMAS = [
    ("test",       "Test data for nexsciTAP CI",          1),
    ("TAP_SCHEMA", "Standard TAP schema description.",    2),
]

# Only data_l0 and data_l1 are reachable as query targets; test_access and
# test_users are propfilter-internal tables that the validator allows in
# subqueries by way of the ACCESS_TBL/USERS_TBL config entries.
TABLES = [
    # (schema, table, description)
    ("test",       "data_l0",  "Public-data test table (propint=0)."),
    ("test",       "data_l1",  "Proprietary test table (mix of embargoed and released)."),
    ("test",       "test_access", "Propfilter access table — internal."),
    ("test",       "test_users",  "Propfilter users table — internal."),
    ("TAP_SCHEMA", "schemas",     "TAP_SCHEMA.schemas."),
    ("TAP_SCHEMA", "tables",      "TAP_SCHEMA.tables."),
    ("TAP_SCHEMA", "columns",     "TAP_SCHEMA.columns."),
    ("TAP_SCHEMA", "keys",        "TAP_SCHEMA.keys."),
    ("TAP_SCHEMA", "key_columns", "TAP_SCHEMA.key_columns."),
]

# Columns for data_l0 and data_l1 — shape mirrors NEID's neidl0/neidl1 enough
# to drive the propfilter code paths and the UI's CASE-WHEN download-link
# query pattern. Keep small; only the columns used by tests need to exist.
DATA_L0_COLUMNS = [
    ("data_l0", "obsdate",    "char", "DATE",   "Observation date."),
    ("data_l0", "obsjd",      "double", None,   "Julian date of observation."),
    ("data_l0", "program",    "char", None,     "Program ID."),
    ("data_l0", "piname",     "char", None,     "PI name."),
    ("data_l0", "obsmode",    "char", None,     "Observation mode."),
    ("data_l0", "obstype",    "char", None,     "Sci or Cal."),
    ("data_l0", "l0propint",  "int",  "months", "Proprietary interval (months)."),
    ("data_l0", "l0filename", "char", None,     "Filename."),
    ("data_l0", "l0filehand", "char", None,     "File handle."),
    ("data_l0", "l0filepath", "char", None,     "File path."),
    ("data_l0", "object",     "char", None,     "Object name."),
    ("data_l0", "qobject",    "char", None,     "Resolved object."),
    ("data_l0", "qra",        "char", "deg",    "Query RA."),
    ("data_l0", "qdec",       "char", "deg",    "Query Dec."),
    ("data_l0", "ra",         "double", "deg",  "RA."),
    ("data_l0", "dec",        "double", "deg",  "Dec."),
]
DATA_L1_COLUMNS = [(("data_l1",) + c[1:]) for c in DATA_L0_COLUMNS]
# Rename the l0-prefixed columns to l1-prefixed for data_l1
DATA_L1_COLUMNS = [
    (t, name.replace("l0", "l1") if name.startswith("l0") else name, *rest)
    for (t, name, *rest) in DATA_L1_COLUMNS
]

# Minimal column metadata for the propfilter-internal and TAP_SCHEMA tables
# — TableValidator looks at table names not column names, but the data
# dictionary code path queries `select * from tap_schema.columns where
# table_name = ?` so the rows must exist.
ACCESS_COLUMNS = [
    ("test_access", "userid",  "char", None, "User ID."),
    ("test_access", "program", "char", None, "Program ID accessible to userid."),
]
USERS_COLUMNS = [
    ("test_users", "userid",   "char", None, "User ID."),
    ("test_users", "cookie",   "char", None, "Session cookie."),
]
SELF_COLUMNS = [
    ("schemas", "schema_name", "char", None, ""),
    ("tables",  "schema_name", "char", None, ""),
    ("tables",  "table_name",  "char", None, ""),
    ("columns", "table_name",  "char", None, ""),
    ("columns", "column_name", "char", None, ""),
    ("columns", "datatype",    "char", None, ""),
    ("columns", "unit",        "char", None, ""),
    ("columns", "description", "char", None, ""),
]


def build_tap_schema(path: str) -> None:
    """Populate tap_schema.db at *path*."""
    if os.path.exists(path):
        os.remove(path)
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    for ddl in TAP_SCHEMA_DDL:
        cur.execute(ddl)
    cur.executemany(
        "INSERT INTO schemas (schema_name, description, schema_index) VALUES (?, ?, ?)",
        SCHEMAS,
    )
    # TableValidator expects table_name values to be schema-qualified
    # (e.g. "TAP_SCHEMA.tables", not just "tables") so it can populate its
    # allowed_schemas set from the dotted prefix. See tablevalidator.py:50-60.
    qualified_tables = [
        (schema, f"{schema}.{table}", description)
        for (schema, table, description) in TABLES
    ]
    cur.executemany(
        "INSERT INTO tables (schema_name, table_name, description) VALUES (?, ?, ?)",
        qualified_tables,
    )
    all_cols = DATA_L0_COLUMNS + DATA_L1_COLUMNS + ACCESS_COLUMNS + USERS_COLUMNS + SELF_COLUMNS
    cur.executemany(
        """INSERT INTO columns (table_name, column_name, datatype, unit, description)
           VALUES (?, ?, ?, ?, ?)""",
        all_cols,
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------- data tables

def build_test_data(path: str) -> None:
    """Populate test_data.db with the data_l0, data_l1, test_access, test_users tables."""
    if os.path.exists(path):
        os.remove(path)
    conn = sqlite3.connect(path)
    cur = conn.cursor()

    # data_l0 — public (l0propint = 0). 6 rows across 2 programs.
    cur.execute("""
        CREATE TABLE data_l0 (
            obsdate     TEXT,
            obsjd       REAL,
            program     TEXT,
            piname      TEXT,
            obsmode     TEXT,
            obstype     TEXT,
            l0propint   INTEGER,
            l0filename  TEXT,
            l0filehand  TEXT,
            l0filepath  TEXT,
            object      TEXT,
            qobject     TEXT,
            qra         TEXT,
            qdec        TEXT,
            ra          REAL,
            dec         REAL
        )
    """)

    now = datetime.now()
    l0_rows = []
    for i in range(6):
        d = now - timedelta(days=10 + i)
        prog = "TEST-A" if i % 2 == 0 else "TEST-B"
        l0_rows.append((
            d.strftime("%Y-%m-%d %H:%M:%S"),     # obsdate
            2460000.0 + i,                        # obsjd
            prog,                                 # program
            "Test PI",                            # piname
            "hr",                                 # obsmode
            "Cal" if i < 2 else "Sci",            # obstype
            0,                                    # l0propint — PUBLIC
            f"testL0_2026{i:04d}.fits",           # l0filename
            f"/handle/{i}",                       # l0filehand
            f"/data/L0/testL0_2026{i:04d}.fits",  # l0filepath
            f"OBJ-{i}",                           # object
            f"OBJ-{i}",                           # qobject
            "10.0",                               # qra
            "20.0",                               # qdec
            10.0,                                 # ra
            20.0,                                 # dec
        ))
    cur.executemany(
        "INSERT INTO data_l0 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", l0_rows
    )

    # data_l1 — proprietary mix. Critical fixture invariant: at least some
    # rows must be STILL EMBARGOED (today < add_months(obsdate, l1propint))
    # so propfilter has something to filter out. Without this, propflag=0
    # vs propflag=1 returns identical counts and the security test can't
    # detect a regression.
    cur.execute("""
        CREATE TABLE data_l1 (
            obsdate     TEXT,
            obsjd       REAL,
            program     TEXT,
            piname      TEXT,
            obsmode     TEXT,
            obstype     TEXT,
            l1propint   INTEGER,
            l1filename  TEXT,
            l1filehand  TEXT,
            l1filepath  TEXT,
            object      TEXT,
            qobject     TEXT,
            qra         TEXT,
            qdec        TEXT,
            ra          REAL,
            dec         REAL
        )
    """)
    l1_rows = []
    # 3 public rows (propint=0)
    for i in range(3):
        d = now - timedelta(days=30 + i)
        l1_rows.append((
            d.strftime("%Y-%m-%d %H:%M:%S"), 2460500.0 + i,
            "TEST-PUBLIC", "PI Public", "hr", "Sci", 0,
            f"testL1_public_{i:02d}.fits", f"/h/p/{i}", f"/data/L1/p{i}.fits",
            "STAR", "STAR", "10.0", "20.0", 10.0, 20.0,
        ))
    # 3 expired-embargo rows (propint=12 months, obsdate > 2 years ago)
    for i in range(3):
        d = now - timedelta(days=900 + i)
        l1_rows.append((
            d.strftime("%Y-%m-%d %H:%M:%S"), 2459500.0 + i,
            "TEST-EXPIRED", "PI Expired", "hr", "Sci", 12,
            f"testL1_expired_{i:02d}.fits", f"/h/e/{i}", f"/data/L1/e{i}.fits",
            "STAR", "STAR", "10.0", "20.0", 10.0, 20.0,
        ))
    # 4 STILL-EMBARGOED rows (propint=24 months, obsdate < 6 months ago)
    for i in range(4):
        d = now - timedelta(days=60 + i * 10)
        l1_rows.append((
            d.strftime("%Y-%m-%d %H:%M:%S"), 2461000.0 + i,
            "TEST-PROPRIETARY", "PI Proprietary", "hr", "Sci", 24,
            f"testL1_embargoed_{i:02d}.fits", f"/h/x/{i}", f"/data/L1/x{i}.fits",
            "STAR", "STAR", "10.0", "20.0", 10.0, 20.0,
        ))
    cur.executemany(
        "INSERT INTO data_l1 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", l1_rows
    )

    # test_access — propfilter access table. Maps userid -> program.
    cur.execute("""
        CREATE TABLE test_access (
            userid  TEXT,
            program TEXT
        )
    """)
    cur.executemany(
        "INSERT INTO test_access VALUES (?, ?)",
        [
            ("testuser", "TEST-PROPRIETARY"),
        ],
    )

    # test_users — propfilter users table. Holds session token mapping.
    cur.execute("""
        CREATE TABLE test_users (
            userid TEXT,
            cookie TEXT
        )
    """)
    cur.executemany(
        "INSERT INTO test_users VALUES (?, ?)",
        [
            ("testuser", "fake-test-cookie"),
        ],
    )

    conn.commit()
    conn.close()


def build_all(out_dir: str) -> dict:
    """Build both fixtures under *out_dir*. Returns {'tap_schema': path, 'test_data': path}."""
    os.makedirs(out_dir, exist_ok=True)
    tap_schema_path = os.path.join(out_dir, "tap_schema.db")
    test_data_path = os.path.join(out_dir, "test_data.db")
    build_tap_schema(tap_schema_path)
    build_test_data(test_data_path)
    return {"tap_schema": tap_schema_path, "test_data": test_data_path}


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "."
    paths = build_all(out)
    for k, v in paths.items():
        print(f"{k}: {v}")
