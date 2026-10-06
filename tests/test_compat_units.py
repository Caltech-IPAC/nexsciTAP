"""compat.py's pure functions, active and inactive."""
import datetime
import io

from TAP import compat


def test_error_document_escapes_markup_not_quotes():
    doc = compat.error_document(compat.ALL, "a < b & 'c'", '400')
    assert doc.startswith('HTTP/1.1 400 ERROR\r\nContent-type: application/xml\r\n\r\n'
                          '<?xml version="1.0" encoding="UTF-8"?>\n')
    assert "\na &lt; b &amp; 'c'\n</INFO>\n" in doc
    assert doc.endswith('</INFO>\n</RESOURCE>\n</VOTABLE>\n')


def test_error_document_inactive_is_none():
    assert compat.error_document(compat.NONE, 'x', '400') is None


def test_table_denied_status():
    assert compat.table_denied_status(compat.ALL) == '400'
    assert compat.table_denied_status(compat.NONE) == '403'


def test_votable_content_type():
    assert compat.votable_content_type(compat.ALL) == 'application/xml'
    assert compat.votable_content_type(compat.NONE) == 'text/xml'


def test_field_description_blanks_only_non_char_columns():
    assert compat.field_description(compat.ALL, 'Julian date', 'double') == ''
    assert compat.field_description(compat.ALL, 'Program ID', 'char') == 'Program ID'
    assert compat.field_description(compat.NONE, 'Julian date', 'double') == 'Julian date'


def test_uws_functions():
    assert compat.duration_element(compat.ALL) == 'executionDuration'
    assert compat.duration_element(compat.NONE) == 'executionduration'
    assert compat.uws_key(compat.ALL, 'executionduration') == 'executionDuration'
    assert compat.uws_key(compat.ALL, 'phase') == 'phase'
    assert compat.destruction_suffix(compat.ALL) == ''
    assert compat.destruction_suffix(compat.NONE) == 'Z'
    assert compat.uws_content_type(compat.ALL) == 'application/xml'
    assert compat.uws_content_type(compat.NONE) == 'text/xml'
    assert compat.async_submit_content_type(compat.ALL) == ''
    assert compat.async_submit_content_type(compat.NONE) == 'Content-Type: text/plain\r\n'


def test_vosi_tables_class():
    from TAP import compat_vositables, vositables
    assert compat.vosi_tables_class(compat.ALL) is compat_vositables.vosiTables
    assert compat.vosi_tables_class(compat.NONE) is vositables.vosiTables


def test_warning_once_per_day(tmp_path):
    out = io.StringIO()
    day1, day2 = datetime.date(2026, 10, 6), datetime.date(2026, 10, 7)
    assert compat.warn_once_per_day(compat.ALL, str(tmp_path), day1, out) is True
    assert compat.warn_once_per_day(compat.ALL, str(tmp_path), day1, out) is False
    assert compat.warn_once_per_day(compat.ALL, str(tmp_path), day2, out) is True
    lines = out.getvalue().splitlines()
    assert len(lines) == 2
    assert lines[0].startswith('nexsciTAP: compatibility mode active (nea-errors, ')
    assert lines[0].endswith('see MIGRATING.md')
    assert (tmp_path / 'TAP' / '.compat-warned-2026-10-06').exists()


def test_no_warning_without_compat(tmp_path):
    out = io.StringIO()
    assert compat.warn_once_per_day(compat.NONE, str(tmp_path), None, out) is False
    assert out.getvalue() == '' and not (tmp_path / 'TAP').exists()


def test_warning_never_fails_a_request(tmp_path):
    blocker = tmp_path / 'file'          # a file where the work dir should be
    blocker.write_text('x')
    out = io.StringIO()
    assert compat.warn_once_per_day(compat.ALL, str(blocker), None, out) is False
    assert compat.warn_once_per_day(compat.ALL, None, None, out) is False
