"""Inputs for the writerecs C extension, shared by the golden-output tests.

Each case is a list of calls to ``writerecs.writerecs`` that together write
one result file, the way ``writeresult.py`` does: one call with ``ishdr=1``,
optionally more calls with ``ishdr=0``, the last with ``istail=1``.

The cases use only ordinary values (no quotes, no XML or JSON special
characters, no integers beyond 32 bits, short names), so their expected
output files in ``fixtures/writerecs_golden/`` were produced by the v3.1.0
module and pin its behaviour: a rewrite of the module must reproduce them
byte for byte.

Not collected by pytest (no ``test_`` prefix).
"""
from __future__ import annotations

FORMATS = ("ipac", "votable", "csv", "tsv", "json")


def _dd(cols):
    """Build the 7-row ddlist from (name, type, dbtype, fmt, units, desc, width)."""
    names, types, dbtypes, fmts, units, descs, widths = (list(x) for x in zip(*cols))
    return [names, types, dbtypes, fmts, units, descs, widths]


MIXED_COLS = [
    ("ObjName", "char", "varchar(12)", "12s", "", "Object name", 12),
    ("ra", "double", "double", "14.6f", "deg", "Right ascension", 14),
    ("Dec", "double", "double", "14.6f", "deg", "", 14),
    ("mag", "float", "real", "10.3e", "", "", 10),
    ("n", "int", "integer", "8d", "", "", 8),
    ("lid", "long", "long", "22d", "counts", "Long id", 22),
    ("obsdate", "date", "date", "14s", "", "Observation date", 14),
    ("ts", "char", "timestamp", "30s", "", "", 30),
]

MIXED_ROWS = [
    ["M31", 10.684708, 41.26875, 3.44, 7, 2147483647,
     "2020-01-02", "2020-01-02 03:04:05"],
    ["NGC 1", -0.5, -89.999999, 1.0e-5, -12, -2147483648,
     "1999-12-31", "1999-12-31 23:59:59"],
    [None, None, None, None, None, None, None, None],
    # int value in a double column, non-string in a char column
    [b"bytes", 3, 0.0, 2, 0, 0, None, ""],
]

SINGLE_INT_COLS = [("count", "int", "integer", "22d", "", "", 22)]


def cases():
    """Return {case_name: (fmt, [call_args, ...])}."""
    out = {}
    for fmt in FORMATS:
        dd = _dd(MIXED_COLS)
        out[f"mixed_{fmt}"] = (fmt, [
            (fmt, dd, MIXED_ROWS, 1, 0, 0, 1),
        ])
        out[f"chunked_{fmt}"] = (fmt, [
            (fmt, dd, MIXED_ROWS[:2], 1, 0, 0, 0),
            (fmt, dd, MIXED_ROWS[2:], 0, 0, 0, 1),
        ])
        out[f"overflow_{fmt}"] = (fmt, [
            (fmt, dd, MIXED_ROWS[:1], 1, 0, 1, 1),
        ])
        out[f"empty_{fmt}"] = (fmt, [
            (fmt, dd, [], 1, 0, 0, 1),
        ])
        sdd = _dd(SINGLE_INT_COLS)
        out[f"count_{fmt}"] = (fmt, [
            (fmt, sdd, [[123456]], 1, 0, 0, 1),
        ])
    return out


def run_case(writerecs, path, calls):
    """Run one case's calls against *writerecs*, writing to *path*."""
    for fmt, dd, rows, ishdr, coldesc, overflow, istail in calls:
        writerecs.writerecs(str(path), fmt, dd, rows,
                            ishdr, coldesc, overflow, istail)
