"""NEA compatibility mode: NEA's pre-3.x responses on the shared code line.

Compat mode is on when TAP.conf uses the 1.x layout ([webserver], no [WEB]),
which implies every behavior below, or when [WEB] lists COMPAT names.  Each
function returns what the default code does unless its behavior is active,
so call sites hold no conditionals, and retiring compat means deleting this
module and compat_vositables.py.  See MIGRATING.md for the plan.
"""
import datetime
import html
import os
import sys

import configobj

NAMES = ('nea-vosi-headers', 'nea-errors', 'nea-tables', 'nea-votable', 'nea-uws')
ALL = frozenset(NAMES)
NONE = frozenset()

# 1.x [webserver] keys that 3.x reads from [WEB].
_WEB_KEYS = ('TAP_WORKDIR', 'TAP_WORKURL', 'HTTP_URL', 'HTTP_PORT', 'CGI_PGM',
             'ArraySize', 'INFOMSG')
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
    # Every other [webserver] key goes to the connection section, where 3.x
    # reads COOKIENAME, RACOL, DECCOL and the proprietary-filter keys
    # (PROPFILTER, ACCESS_TBL, USERS_TBL, FILEID, ACCESSID).  Nothing is
    # dropped: a lost PROPFILTER would serve proprietary rows to anyone.
    for k, v in web.items():
        if k not in _WEB_KEYS and k != 'DBMS' and not k.startswith('ADQL_'):
            db[k] = v
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
    for name in conf.sections:
        if name != 'WEB' and 'COMPAT' in conf[name]:
            raise CompatConfigError('COMPAT belongs in [WEB], found in [%s]' % name)
    if 'WEB' not in conf and 'webserver' in conf:
        return translate_legacy(conf), ALL
    web = conf['WEB'] if 'WEB' in conf else {}
    return conf, parse_names(web.get('COMPAT'))


# --- nea-vosi-headers -------------------------------------------------------

def vosi_head(active):
    """Status line and headers written before a VOSI document.

    nph- CGI: the script emits the whole HTTP response.  The status line and
    each header end in CRLF, and a bare CRLF closes the header block; nginx
    and Cloudflare reject the response otherwise.  NEA also sends
    Connection: close.
    """
    head = 'HTTP/1.1 200 OK\r\nContent-type: application/xml\r\n'
    if 'nea-vosi-headers' in active:
        head += 'Connection: close\r\n'
    return head + '\r\n'


def vosi_tail(active):
    """NEA ends its VOSI documents with one more CRLF."""
    return '\r\n' if 'nea-vosi-headers' in active else ''


# --- nea-errors -------------------------------------------------------------

def table_denied_status(active):
    """Status for a table outside TAP_SCHEMA: NEA answers 400, the shared line 403."""
    return '400' if 'nea-errors' in active else '403'


def error_document(active, errmsg, errcode):
    """NEA's error response: a VOTable error document whatever the format.

    Returns the whole response, or None when nea-errors is inactive.  Markup
    is escaped so the document stays well-formed; quotes are not, matching
    NEA's bytes for ordinary messages.
    """
    if 'nea-errors' not in active:
        return None
    return ('HTTP/1.1 %s ERROR\r\n' % errcode
            + 'Content-type: application/xml\r\n\r\n'
            + '<?xml version="1.0" encoding="UTF-8"?>\n'
            + '<VOTABLE version="1.4" xmlns="http://www.ivoa.net/xml/VOTable/v1.3">\n'
            + '<RESOURCE type="results">\n'
            + '<INFO name="QUERY_STATUS" value="ERROR">\n'
            + html.escape(str(errmsg), quote=False) + '\n'
            + '</INFO>\n'
            + '</RESOURCE>\n'
            + '</VOTABLE>\n')


# --- nea-votable ------------------------------------------------------------

def votable_content_type(active):
    """Content type for VOTable results: NEA sends application/xml."""
    return 'application/xml' if 'nea-votable' in active else 'text/xml'


def field_description(active, desc, coltype):
    """NEA's VOTables carry no DESCRIPTION for non-char columns.

    The C writer prints the plain <FIELD .../> form when a non-char column's
    description is empty (writerecsmodule.c, non-char branch), so blanking it
    here reproduces NEA's FIELDs without touching the C API.  Char columns
    already match.
    """
    if 'nea-votable' in active and coltype != 'char':
        return ''
    return desc


# --- nea-uws ----------------------------------------------------------------

def duration_element(active):
    """UWS names it executionDuration; the shared line writes it lowercase."""
    return 'executionDuration' if 'nea-uws' in active else 'executionduration'


def uws_key(job, key):
    """Element of a parsed status.xml that answers a job sub-resource key.

    The duration is spelled as the document spells it (see duration_value).
    """
    if key == 'executionduration' and 'uws:executionDuration' in job:
        return 'executionDuration'
    return key


def uws_url_key(active, key):
    """The job sub-resource key from the URL, as the code compares it.

    NEA's /async/<id>/executionDuration (UWS spelling) is accepted next to the
    lowercase form pyvo uses; the shared line knows only the lowercase one.
    """
    if 'nea-uws' in active and key == 'executionDuration':
        return 'executionduration'
    return key


def duration_value(job):
    """The duration in a parsed status.xml, whichever spelling wrote it.

    A job written under one mode may be read under the other (jobs live up
    to four days), so reading tolerates both; writing uses duration_element.
    """
    return job.get('uws:executionDuration', job.get('uws:executionduration'))


def destruction_suffix(active):
    """NEA writes the destruction time without a trailing 'Z'."""
    return '' if 'nea-uws' in active else 'Z'


def uws_content_type(active):
    """Content type of the job document: NEA sends application/xml."""
    return 'application/xml' if 'nea-uws' in active else 'text/xml'


def async_submit_headers(active, body):
    """Framing headers of the 303 that starts a job (PHASE=RUN).

    NEA sends none: the response ends when the CGI exits, because the
    forked worker holds no stdout.  Otherwise: Content-Type, the length of
    `body` in bytes, and Connection: close.
    """
    if 'nea-uws' in active:
        return ''
    return ('Content-Type: text/plain\r\nContent-Length: %d\r\n'
            'Connection: close\r\n' % len(body.encode('utf-8')))


# --- nea-tables -------------------------------------------------------------

def vosi_tables_class(active):
    """The /tables writer: NEA's (prod's vositables.py, verbatim) or the shared one."""
    if 'nea-tables' in active:
        from TAP.compat_vositables import vosiTables
    else:
        from TAP.vositables import vosiTables
    return vosiTables


# --- deprecation ------------------------------------------------------------

def warn_once_per_day(active, workdir, today=None, stream=None):
    """While compat is active, one stderr line (Apache's error_log) per day.

    A stamp file <TAP_WORKDIR>/TAP/.compat-warned-<date>, created exclusively,
    marks the day, so concurrent requests warn once.  Any failure is
    swallowed: the warning must never fail a request.
    """
    if not active:
        return False
    try:
        day = (today or datetime.date.today()).isoformat()
        stamp = os.path.join(workdir, 'TAP', '.compat-warned-' + day)
        os.makedirs(os.path.dirname(stamp), exist_ok=True)
        with open(stamp, 'x'):
            pass
        (stream or sys.stderr).write(
            'nexsciTAP: compatibility mode active (%s); see MIGRATING.md\n'
            % ', '.join(sorted(active)))
        return True
    except Exception:
        return False
