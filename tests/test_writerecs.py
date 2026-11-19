"""Tests for the writerecs C extension (TAP/writerecsmodule.c).

The golden tests pin the module's output for ordinary values to what v3.1.0
wrote.  The others cover defects reported against v3.1.0: fixed-size buffers
overrun by long names and values, 64-bit integers truncated to 32 bits, an
invalid VOTable datatype for 64-bit columns, unescaped quotes in CSV, and
values that broke the VOTable and JSON documents.

Writes that could overrun memory run in a child process, so a crash shows up
as a test failure instead of taking pytest down with it.
"""
from __future__ import annotations

import csv
import io
import json
import pickle
import subprocess
import sys
import warnings
from pathlib import Path

import pytest
import writerecs_cases as wc

from TAP import writerecs

GOLDEN = Path(__file__).parent / "fixtures" / "writerecs_golden"

BIG = -529002177281003050          # needs 64 bits
HUGE = 2 ** 70                     # does not fit in 64 bits


def _dd(cols):
    return wc._dd(cols)


def _write(tmp_path, fmt, dd, rows, name="out"):
    path = tmp_path / f"{name}.{fmt}"
    writerecs.writerecs(str(path), fmt, dd, rows, 1, 0, 0, 1)
    return path.read_bytes().decode("utf-8")      # no newline translation


def _write_in_child(tmp_path, fmt, dd, rows):
    """Run writerecs in a child process; return (returncode, stderr, text)."""
    out = tmp_path / f"child.{fmt}"
    args = tmp_path / f"child_{fmt}.pkl"
    args.write_bytes(pickle.dumps((str(out), fmt, dd, rows)))
    code = (
        "import pickle, sys\n"
        "from TAP import writerecs\n"
        "out, fmt, dd, rows = pickle.load(open(sys.argv[1], 'rb'))\n"
        "writerecs.writerecs(out, fmt, dd, rows, 1, 0, 0, 1)\n"
    )
    proc = subprocess.run([sys.executable, "-c", code, str(args)],
                          capture_output=True, text=True, timeout=60)
    text = out.read_bytes().decode("utf-8") if out.exists() else ""
    return proc.returncode, proc.stderr, text


def _votable(text):
    from astropy.io.votable import parse_single_table
    with warnings.catch_warnings():
        warnings.simplefilter("error")      # any VOTable spec warning fails
        return parse_single_table(io.BytesIO(text.encode("utf-8")))


# ---------------------------------------------------------------------------
# Output for ordinary values is unchanged from v3.1.0
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("name", sorted(wc.cases()))
def test_golden_output(tmp_path, name):
    fmt, calls = wc.cases()[name]
    path = tmp_path / f"{name}.{fmt}"
    wc.run_case(writerecs, path, calls)
    assert path.read_bytes() == (GOLDEN / f"{name}.{fmt}").read_bytes()


@pytest.mark.parametrize("row, message", [
    (["toolong", 1, 1.0],
     "Column 0 [s], row 0: Formatted value [toolong](4s) -> 8 characters,"
     "  overflows column width [4 char]."),
    (["ab", 123456, 1.0],
     "Column 1 [n], row 0: Formatted value [123456](4d) -> 7 characters,"
     "  overflows column width [4 char]."),
    (["ab", 1, 12345.0],
     "Column 2 [d], row 0: Formatted value [12345.0](4.1f) -> 8 characters,"
     "  overflows column width [4 char]."),
])
def test_ipac_width_overflow_message(tmp_path, row, message):
    dd = [["s", "n", "d"], ["char", "int", "double"],
          ["varchar", "integer", "double"], ["4s", "4d", "4.1f"],
          ["", "", ""], ["", "", ""], [4, 4, 4]]
    with pytest.raises(Exception) as exc:
        _write(tmp_path, "ipac", dd, [row])
    assert str(exc.value) == message


# ---------------------------------------------------------------------------
# Item 1: long names, units, descriptions and values
# ---------------------------------------------------------------------------

LONG_NAME = "count(distinct w1mpro+w2mpro+ra)" + "_x" * 141        # 314 chars
LONG_VALUE = "v" * 4990 + "0123456789"                             # 5000 chars
LONG_UNIT = "u" * 300
LONG_DESC = "d" * 3000


@pytest.mark.parametrize("fmt", wc.FORMATS)
def test_long_names_and_values_do_not_overrun(tmp_path, fmt):
    assert len(LONG_NAME) == 314
    width = 5000
    dd = _dd([
        (LONG_NAME, "char", "varchar", f"{width}s", LONG_UNIT, LONG_DESC, width),
        ("n", "long", "long", "22d", LONG_UNIT, LONG_DESC, 22),
        ("x", "double", "double", "14.6f", "", LONG_DESC, 14),
    ])
    rows = [[LONG_VALUE, 1, 2.5], [LONG_VALUE[::-1], None, None]]
    rc, err, text = _write_in_child(tmp_path, fmt, dd, rows)
    assert rc == 0, err
    assert LONG_NAME.lower() in text
    assert LONG_VALUE in text and LONG_VALUE[::-1] in text
    if fmt == "votable":
        assert LONG_UNIT in text and LONG_DESC in text
        table = _votable(text)
        assert table.array[LONG_NAME.lower()][0] == LONG_VALUE


@pytest.mark.parametrize("fmt", wc.FORMATS)
def test_long_dd_type_and_format_strings(tmp_path, fmt):
    # dbtype and an over-long dd format are copied too; neither may overrun.
    dd = _dd([("c", "char", "varchar(" + "9" * 200 + ")", "12s", "", "", 12)])
    rc, err, _ = _write_in_child(tmp_path, fmt, dd, [["abc"]])
    assert rc == 0, err


# ---------------------------------------------------------------------------
# Item 2: 64-bit integers
# ---------------------------------------------------------------------------

INT_TYPES = ("int", "long", "longlong", "integer")


@pytest.mark.parametrize("coltype", INT_TYPES)
@pytest.mark.parametrize("fmt", ["ipac", "csv", "tsv", "json"])
def test_int64_values_are_not_truncated(tmp_path, fmt, coltype):
    dd = _dd([("id", coltype, "bigint", "24d", "", "", 24)])
    text = _write(tmp_path, fmt, dd, [[BIG], [2 ** 63 - 1], [-(2 ** 63)]])
    for v in (BIG, 2 ** 63 - 1, -(2 ** 63)):
        assert str(v) in text
    assert "-1849560618" not in text


@pytest.mark.parametrize("coltype", INT_TYPES)
def test_int64_votable_round_trip(tmp_path, coltype):
    dd = _dd([("id", coltype, "bigint", "24d", "", "", 24)])
    text = _write(tmp_path, "votable", dd, [[BIG]])
    assert str(BIG) in text
    if coltype in ("long", "longlong"):
        assert _votable(text).array["id"][0] == BIG


@pytest.mark.parametrize("fmt", wc.FORMATS)
def test_int_beyond_64_bits_written_exactly(tmp_path, fmt):
    dd = _dd([("id", "long", "numeric", "24d", "", "", 24)])
    text = _write(tmp_path, fmt, dd, [[HUGE]])
    assert str(HUGE) in text


def test_ipac_int_format_flags_kept(tmp_path):
    # A dd integer format with flags keeps them now that values are 64-bit.
    dd = _dd([("id", "long", "bigint", "+24ld", "", "", 24)])
    text = _write(tmp_path, "ipac", dd, [[BIG], [5]])
    assert f" {BIG:<+24d} " in text
    assert f" {5:<+24d} " in text


def test_large_int_in_double_column(tmp_path):
    dd = _dd([("x", "double", "double", "24.1f", "", "", 24)])
    text = _write(tmp_path, "csv", dd, [[2 ** 40 + 1]])
    assert "1099511627777.0" in text


# ---------------------------------------------------------------------------
# Item 3: VOTable datatypes
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("coltype, fmt, expected", [
    ("longlong", "24ld", "long"), ("integer", "22d", "int"),
    ("int", "22d", "int"), ("long", "22d", "long"),
    ("double", "14.6f", "double"), ("float", "14.6f", "float"),
])
def test_votable_datatype_names(tmp_path, coltype, fmt, expected):
    dd = _dd([("n", coltype, "x", fmt, "", "", 22)])
    text = _write(tmp_path, "votable", dd, [])
    assert f'datatype="{expected}"' in text
    _votable(text)


def test_votable_count_star_longlong_parses(tmp_path):
    dd = _dd([("n", "longlong", "longlong", "24ld", "", "", 24)])
    table = _votable(_write(tmp_path, "votable", dd, [[59000000]]))
    assert table.array["n"][0] == 59000000


# ---------------------------------------------------------------------------
# Item 4 and related: escaping in CSV, VOTable and JSON
# ---------------------------------------------------------------------------

AWKWARD = 'J 20mag/sq." isophotal, <a&b> ]]> \\ tab\tnl\ncr\r ctl\x01 é'


def test_csv_quotes_are_doubled(tmp_path):
    dd = _dd([("desc", "char", "varchar", "80s", "", "", 80),
              ('a,"b"', "int", "integer", "8d", "", "", 8)])
    text = _write(tmp_path, "csv", dd, [[AWKWARD, 1]])
    rows = list(csv.reader(io.StringIO(text, newline="")))
    assert rows[0] == ["desc", 'a,"b"']
    assert rows[1] == [AWKWARD, "1"]


def test_votable_special_characters(tmp_path):
    name = 'a"<b>&\'c'
    dd = _dd([(name, "char", "varchar", "80s", 'u"<&>', "desc ]]> end", 80)])
    text = _write(tmp_path, "votable", dd, [[AWKWARD]])
    table = _votable(text)
    field = table.fields[0]
    assert field.name == name
    assert field.unit is not None
    assert field.description.strip() == "desc ]]> end"
    # XML normalizes CR to LF; XML cannot carry U+0001 at all.
    assert table.array[name][0] == (
        AWKWARD.replace("\r", "\n").replace("\x01", "\ufffd"))


def test_json_special_characters(tmp_path):
    name = 'a"b\\c'
    dd = _dd([(name, "char", "varchar", "80s", "", "", 80)])
    text = _write(tmp_path, "json", dd, [[AWKWARD]])
    assert json.loads(text) == [{name: AWKWARD}]
