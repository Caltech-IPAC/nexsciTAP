"""Compat mode's config side: COMPAT names and the 1.x TAP.conf translator."""
import pytest

from TAP import compat
from TAP.configparam import configParam

LEGACY_ORACLE = """[webserver]
DBMS=oracle
TAP_WORKDIR={work}/
TAP_WORKURL=/workspace/
HTTP_PORT=80
COOKIENAME=tapcookie
CGI_PGM=/TAP
RACOL=ra
DECCOL=dec
# Spatial indexing settings
ADQL_MODE=HTM
ADQL_LEVEL=20
ADQL_XCOL=x
ADQL_YCOL=y
ADQL_ZCOL=z
ADQL_COLNAME=htm20
ADQL_ENCODING=BASE10
HTTP_URL=https://exoplanetarchive.ipac.caltech.edu
[oracle]
ServerName=tapdb
UserID=exo_tap
Password=secret
"""

CURRENT_ORACLE = """[WEB]
TAP_WORKDIR={work}/
TAP_WORKURL=/workspace/
HTTP_URL=https://exoplanetarchive.ipac.caltech.edu
HTTP_PORT=80
CGI_PGM=/TAP
{compat}
[DBMS]
DBMS=oracle
ServerName=tapdb
UserID=exo_tap
Password=secret
COOKIENAME=tapcookie
RACOL=ra
DECCOL=dec
[SPTIND]
MODE=HTM
LEVEL=20
XCOL=x
YCOL=y
ZCOL=z
COLNAME=htm20
ENCODING=BASE10
"""


def write(tmp_path, name, text, **fmt):
    p = tmp_path / name
    p.write_text(text.format(work=tmp_path, **fmt))
    return str(p)


def state(cp):
    """Everything configParam derived, minus where it came from."""
    out = {k: v for k, v in vars(cp).items() if k not in ('configpath', 'compat', 'connectInfo')}
    out['connectInfo'] = {k: v for k, v in cp.connectInfo.items() if k != 'compat'}
    return out


def test_parse_names_accepts_str_and_list():
    assert compat.parse_names(None) == compat.NONE
    assert compat.parse_names('nea-errors') == {'nea-errors'}
    assert compat.parse_names(['nea-errors', ' nea-uws ']) == {'nea-errors', 'nea-uws'}
    assert compat.parse_names(['', '']) == compat.NONE


def test_unknown_compat_name_is_an_error(tmp_path):
    with pytest.raises(compat.CompatConfigError, match='nea-eror'):
        compat.parse_names(['nea-eror'])
    path = write(tmp_path, 'TAP.conf', CURRENT_ORACLE, compat='COMPAT = nea-eror')
    with pytest.raises(compat.CompatConfigError):
        configParam(path)


def test_legacy_oracle_reads_like_its_3x_equivalent(tmp_path):
    legacy = configParam(write(tmp_path, 'legacy.conf', LEGACY_ORACLE))
    current = configParam(write(tmp_path, 'current.conf', CURRENT_ORACLE, compat=''))
    assert state(legacy) == state(current)
    assert legacy.compat == compat.ALL
    assert legacy.connectInfo['compat'] == compat.ALL
    assert current.compat == compat.NONE
    assert current.connectInfo['compat'] == compat.NONE


def test_explicit_compat_list(tmp_path):
    cp = configParam(write(tmp_path, 'TAP.conf', CURRENT_ORACLE,
                           compat='COMPAT = nea-errors, nea-uws'))
    assert cp.compat == {'nea-errors', 'nea-uws'}


def test_adql_keys_after_db_section_are_found(tmp_path):
    # configobj files keys that follow [oracle] under [oracle].
    text = LEGACY_ORACLE.replace('ADQL_MODE=HTM\n', '').replace(
        'Password=secret\n', 'Password=secret\nADQL_MODE=HTM\n')
    conf = compat.translate_legacy(__import__('configobj').ConfigObj(
        write(tmp_path, 'TAP.conf', text)))
    assert conf['SPTIND']['MODE'] == 'HTM'
    assert 'ADQL_MODE' not in conf['DBMS']


def test_legacy_sqlite_gets_an_attach_name(tmp_path):
    text = '[webserver]\nDBMS=sqlite3\nTAP_WORKDIR={work}/\n[sqlite3]\nDB=/x.db\nTAP_SCHEMA=/ts.db\n'
    conf = compat.translate_legacy(__import__('configobj').ConfigObj(
        write(tmp_path, 'TAP.conf', text)))
    assert dict(conf['DBMS']) == {'DBMS': 'sqlite3', 'DB': '/x.db', 'TAP_SCHEMA': '/ts.db',
                                  'TAP_SCHEMA_FILE': 'TAP_SCHEMA'}


def test_legacy_without_its_db_section_is_an_error(tmp_path):
    text = '[webserver]\nDBMS=oracle\nTAP_WORKDIR={work}/\n'
    with pytest.raises(compat.CompatConfigError, match=r'no \[oracle\] section'):
        compat.translate_legacy(__import__('configobj').ConfigObj(
            write(tmp_path, 'TAP.conf', text)))


def test_compat_outside_web_is_an_error(tmp_path):
    text = CURRENT_ORACLE.replace('Password=secret\n', 'Password=secret\nCOMPAT = nea-uws\n')
    with pytest.raises(compat.CompatConfigError, match=r'\[DBMS\]'):
        configParam(write(tmp_path, 'TAP.conf', text, compat=''))


def test_compat_in_legacy_webserver_is_an_error(tmp_path):
    text = LEGACY_ORACLE.replace('DBMS=oracle\n', 'DBMS=oracle\nCOMPAT=nea-uws\n', 1)
    with pytest.raises(compat.CompatConfigError, match=r'\[webserver\]'):
        configParam(write(tmp_path, 'TAP.conf', text))
