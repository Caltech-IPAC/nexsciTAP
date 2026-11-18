"""Regression tests for v3.1.0 defects outside the C writer.

- tapquery.py's command-line main() passed the spatial index level to the
  ADQL translator as a string, so every CONTAINS query failed with
  "an integer is required".
- tap.py read UWS job status documents with BeautifulSoup's HTML builder,
  and beautifulsoup4 >= 4.11 logged an XMLParsedAsHTMLWarning (seven lines
  on stderr) every time a client asked about a job.
"""
from __future__ import annotations

import sys
import warnings

import pytest

UWS_DOC = """<?xml version="1.0" encoding="UTF-8"?>
<uws:job xmlns:uws="http://www.ivoa.net/xml/UWS/v1.0">
  <uws:jobId>abc123</uws:jobId>
  <uws:phase>COMPLETED</uws:phase>
  <uws:parameters>
    <uws:parameter id="query">SELECT 1</uws:parameter>
    <uws:parameter id="format">votable</uws:parameter>
  </uws:parameters>
</uws:job>
"""


def test_uws_parse_does_not_warn():
    from TAP import tap

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        soup = tap._parse_uws(UWS_DOC)

    assert [w for w in caught if "XMLParsedAsHTML" in w.category.__name__] == []

    # The tag look-ups tap.py makes still work.
    params = soup.find("uws:parameters")
    assert params is not None
    ids = [p.get("id") for p in params.find_all("uws:parameter")]
    assert ids == ["query", "format"]


def test_uws_parse_leaves_warning_filters_alone():
    from TAP import tap

    before = list(warnings.filters)
    tap._parse_uws(UWS_DOC)
    assert warnings.filters == before


def test_tapquery_main_passes_integer_level(tap_conf, monkeypatch):
    from TAP import tapquery

    seen = {}

    class FakeADQL:
        def __init__(self, **kwargs):
            seen.update(kwargs)
            raise RuntimeError("stop after construction")

    monkeypatch.setattr(tapquery, "ADQL", FakeADQL)
    monkeypatch.setattr(sys, "argv", [
        "tapquery.py", "--configpath", str(tap_conf),
        "--sql", "SELECT ra FROM data_l0 WHERE "
                 "CONTAINS(POINT('ICRS', ra, dec), CIRCLE('ICRS', 10, 10, 1))=1",
    ])

    with pytest.raises(SystemExit):
        tapquery.main()

    assert seen["level"] == 20
    assert isinstance(seen["level"], int)


# ---------------------------------------------------------------------------
# A result whose row count is a multiple of the fetch size
# ---------------------------------------------------------------------------
#
# writeresult.py writes each fetch of `arraysize` rows in its own writerecs
# call and closes the document (istail) on a short fetch.  When the last
# fetch was full, the next one is empty and the loop used to stop without
# closing it: VOTable without </VOTABLE>, JSON without ']'.

class _PgCursor:
    """DB-API cursor stand-in that reports PostgreSQL type OIDs."""

    def __init__(self, rows):
        self.description = [("a", 23, None, None, None, None, None),
                            ("b", 1043, None, None, None, None, None)]
        self._rows = list(rows)
        self.arraysize = 1

    def fetchmany(self, n):
        out, self._rows = self._rows[:n], self._rows[n:]
        return out


@pytest.mark.parametrize("nrows", [0, 2, 3, 5, 6, 9])
@pytest.mark.parametrize("fmt", ["votable", "json", "csv", "ipac"])
def test_result_closed_at_any_row_count(tmp_path, fmt, nrows):
    import csv
    import io
    import json

    from astropy.io.votable import parse_single_table

    from TAP.writeresult import writeResult

    out = tmp_path / "out"
    rows = [(i, f"s{i}") for i in range(nrows)]
    writeResult(_PgCursor(rows), str(tmp_path), None, format=fmt,
                dbms="pgsql", arraysize=3, maxrec=-1, filename=str(out))
    text = out.read_bytes().decode("utf-8")

    if fmt == "votable":
        table = parse_single_table(io.BytesIO(text.encode()))
        assert len(table.array) == nrows
    elif fmt == "json":
        assert json.loads(text) == [{"a": i, "b": f"s{i}"} for i in range(nrows)]
    elif fmt == "csv":
        assert len(list(csv.reader(io.StringIO(text)))) == nrows + 1
    else:
        assert len(text.splitlines()) == nrows + 4
