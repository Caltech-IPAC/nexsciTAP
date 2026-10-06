"""compat.py's pure functions, active and inactive."""
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
