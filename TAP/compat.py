"""NEA compatibility mode: NEA's pre-3.x responses on the shared code line.

Compat mode is on when TAP.conf uses the 1.x layout ([webserver], no [WEB]),
which implies every behavior below, or when [WEB] lists COMPAT names.  Each
function returns what the default code does unless its behavior is active,
so call sites hold no conditionals, and retiring compat means deleting this
module and compat_vositables.py.  See MIGRATING.md for the plan.
"""
import configobj

NAMES = ('nea-vosi-headers', 'nea-errors', 'nea-tables', 'nea-votable', 'nea-uws')
ALL = frozenset(NAMES)
NONE = frozenset()

# 1.x [webserver] keys that 3.x reads from [WEB].
_WEB_KEYS = ('TAP_WORKDIR', 'TAP_WORKURL', 'HTTP_URL', 'HTTP_PORT', 'CGI_PGM',
             'ArraySize', 'INFOMSG')
# 1.x [webserver] keys that 3.x reads from the connection section.
_DB_KEYS_FROM_WEB = ('COOKIENAME', 'RACOL', 'DECCOL')
# 1.x ADQL_* keys and their 3.x [SPTIND] names.
_SPTIND_KEYS = {'ADQL_MODE': 'MODE', 'ADQL_LEVEL': 'LEVEL', 'ADQL_XCOL': 'XCOL',
                'ADQL_YCOL': 'YCOL', 'ADQL_ZCOL': 'ZCOL',
                'ADQL_COLNAME': 'COLNAME', 'ADQL_ENCODING': 'ENCODING'}


class CompatConfigError(Exception):
    pass


def parse_names(value):
    """A COMPAT value (configobj gives str or list) -> frozenset of names."""
    if value is None:
        return NONE
    items = [value] if isinstance(value, str) else list(value)
    names = frozenset(v.strip() for v in items if v.strip())
    unknown = sorted(names - ALL)
    if unknown:
        raise CompatConfigError('unknown COMPAT name(s) %s; valid names: %s'
                                % (', '.join(unknown), ', '.join(NAMES)))
    return names


def _find(conf, key):
    """A 1.x key wherever configobj filed it: [webserver] first, then any section.

    Keys written after a section header belong to that section, so ADQL_*
    lines placed after [oracle] land in [oracle].
    """
    if key in conf['webserver']:
        return conf['webserver'][key]
    for name in conf.sections:
        if key in conf[name]:
            return conf[name][key]
    return None


def translate_legacy(conf):
    """A 1.x TAP.conf ([webserver], [<dbms>], ADQL_*) as an equivalent 3.x one."""
    web = conf['webserver']
    dbms = web.get('DBMS', '')
    if dbms not in conf:
        raise CompatConfigError('1.x TAP.conf names DBMS=%s but has no [%s] section'
                                % (dbms, dbms))
    new = configobj.ConfigObj()
    new['WEB'] = {k: web[k] for k in _WEB_KEYS if k in web}
    db = {'DBMS': dbms}
    db.update({k: v for k, v in conf[dbms].items() if not k.startswith('ADQL_')})
    for k in _DB_KEYS_FROM_WEB:
        if k in web:
            db[k] = web[k]
    if dbms == 'sqlite3':
        db.setdefault('TAP_SCHEMA_FILE', 'TAP_SCHEMA')   # 3.x's SQLite ATTACH name
    new['DBMS'] = db
    sptind = {}
    for old, current in _SPTIND_KEYS.items():
        value = _find(conf, old)
        if value is not None:
            sptind[current] = value
    new['SPTIND'] = sptind
    return new


def load_config(conf):
    """(ConfigObj as read) -> (ConfigObj in 3.x layout, active compat names)."""
    if 'WEB' not in conf and 'webserver' in conf:
        return translate_legacy(conf), ALL
    web = conf['WEB'] if 'WEB' in conf else {}
    return conf, parse_names(web.get('COMPAT'))


# --- nea-vosi-headers -------------------------------------------------------

def vosi_head(active):
    """Status line and headers NEA's nph- CGI writes before a VOSI document."""
    if 'nea-vosi-headers' not in active:
        return ''
    return ('HTTP/1.1 200 OK\r\nContent-type: application/xml\r\n'
            'Connection: close\r\n\r\n')


def vosi_tail(active):
    """NEA ends its VOSI documents with one more CRLF."""
    return '\r\n' if 'nea-vosi-headers' in active else ''
