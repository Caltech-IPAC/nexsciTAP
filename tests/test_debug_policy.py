"""Debug policy regression tests.

Covers:
- configParam.debug_flag attribute (absent / 0 / 1 / non-canonical values)
- 2x2 matrix: TAP_DEBUG env var x config DEBUG key
  - app.debug value after config loads
  - whether a debug file is created and what it contains
- Bootstrap buffer: pre-config failures flush to stderr (web server error_log) when TAP_DEBUG=1
- No debug file when TAP_DEBUG is unset, even on startup failure
- Forward fence: form['debug'] field is not wired to debug activation
- 1.x [webserver] DEBUG key translation via compat._WEB_KEYS (test_compat_config.py)
"""
from __future__ import annotations

import logging
from pathlib import Path
from types import SimpleNamespace

import pytest

from TAP.configparam import configParam
from TAP.tap import Tap

# ---------------------------------------------------------------------------
# Minimal inline config — fake DB paths; configParam only checks they're set,
# not that the files exist.
# ---------------------------------------------------------------------------

_CONF_TEMPLATE = """\
[WEB]
TAP_WORKDIR={workdir}
TAP_WORKURL=/workspace
HTTP_URL=http://localhost
HTTP_PORT=80
CGI_PGM=TAP
{debug_line}
[DBMS]
DBMS=sqlite3
DB={workdir}/fake.db
TAP_SCHEMA={workdir}/fake_schema.db
TAP_SCHEMA_FILE=TAP_SCHEMA
ACCESS_TBL=test_access
USERS_TBL=test_users
FILEID=l0filename
ACCESSID=program
PROPFILTER=neid
RACOL=ra
DECCOL=dec
[SPTIND]
MODE=HTM
LEVEL=20
XCOL=x
YCOL=y
ZCOL=z
COLNAME=sptind
ENCODING=BASE10
"""


def _write_conf(tmp_path: Path, debug_value: str | None) -> Path:
    """Write a minimal TAP.conf. debug_value: '0', '1', or None (key absent)."""
    debug_line = f"DEBUG = {debug_value}" if debug_value is not None else ""
    text = _CONF_TEMPLATE.format(workdir=str(tmp_path), debug_line=debug_line)
    conf = tmp_path / "TAP.conf"
    conf.write_text(text)
    return conf


class _Form(dict):
    """Stand-in for cgi.FieldStorage."""

    @property
    def list(self):
        return list(self.values())




# ---------------------------------------------------------------------------
# configParam unit tests
# ---------------------------------------------------------------------------

class TestConfigParamDebugFlag:

    def test_absent(self, tmp_path):
        """Missing DEBUG key → debug_flag defaults to 0."""
        cp = configParam(str(_write_conf(tmp_path, None)))
        assert cp.debug_flag == 0

    def test_zero(self, tmp_path):
        """DEBUG = 0 → debug_flag is 0."""
        cp = configParam(str(_write_conf(tmp_path, "0")))
        assert cp.debug_flag == 0

    def test_one(self, tmp_path):
        """DEBUG = 1 → debug_flag is 1."""
        cp = configParam(str(_write_conf(tmp_path, "1")))
        assert cp.debug_flag == 1

    @pytest.mark.parametrize("bad", ["true", "yes", "on", "True", "YES"])
    def test_non_canonical_stays_off(self, tmp_path, bad):
        """Non-'1' values are treated as 0 — strict match only."""
        cp = configParam(str(_write_conf(tmp_path, bad)))
        assert cp.debug_flag == 0, f"DEBUG={bad!r} should not enable debug"


# ---------------------------------------------------------------------------
# Tap integration: 2x2 matrix (TAP_DEBUG env var) x (config DEBUG key)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("env_debug,conf_debug,want_debug,want_file", [
    # env unset, config off  → debug off, no file
    (None, "0",  0, False),
    # env unset, config on   → debug on, file created
    (None, "1",  1, True),
    # env on,    config off  → config wins: debug off, no file created
    ("1",  "0",  0, False),
    # env on,    config on   → debug on, file created
    ("1",  "1",  1, True),
])
def test_debug_policy_matrix(
    monkeypatch, tmp_path, capsys,
    env_debug, conf_debug, want_debug, want_file,
):
    """Config DEBUG key is authoritative after load; TAP_DEBUG is bootstrap-only.

    The critical case is (TAP_DEBUG=1, DEBUG=0): even if the env var is set,
    no debug file should be created once the config confirms debug is off.
    """
    if env_debug is None:
        monkeypatch.delenv("TAP_DEBUG", raising=False)
    else:
        monkeypatch.setenv("TAP_DEBUG", env_debug)

    conf = _write_conf(tmp_path, conf_debug)
    monkeypatch.setenv("TAP_CONF", str(conf))
    monkeypatch.setenv("PATH_INFO", "/sync")

    debug_file = tmp_path / "tap.debug"

    app = Tap.__new__(Tap)
    app.form = _Form({})
    app.debug = 0
    app.param = {}
    app.statdict = {}   # Tap.statdict is class-level; must be instance attr to avoid cross-test mutation
    app.infomsg = ""
    app.dbtable = ""
    app.debugfname = str(debug_file)

    # __run__ loads config then eventually fails (no query / empty DB) → SystemExit
    with pytest.raises(SystemExit):
        app.__run__()
    capsys.readouterr()

    assert app.debug == want_debug, (
        f"TAP_DEBUG={env_debug!r} + config DEBUG={conf_debug!r}: "
        f"expected app.debug={want_debug}, got {app.debug}"
    )
    assert debug_file.exists() == want_file, (
        f"TAP_DEBUG={env_debug!r} + config DEBUG={conf_debug!r}: "
        f"debug file {'should' if want_file else 'should NOT'} exist"
    )
    if want_file:
        content = debug_file.read_text()
        if env_debug == "1":
            # Bootstrap records must be replayed into the file (not just the file opened).
            # Catches: setTarget dropped, or close() called before setTarget.
            # Note: buf.flush() alone is NOT required — MemoryHandler.close() flushes
            # automatically (flushOnClose=True). The real guard is setTarget().
            assert "Enter Tap.init()" in content, (
                f"TAP_DEBUG=1 + DEBUG=1: bootstrap records must be replayed; "
                f"file content: {content[:200]!r}"
            )
        else:
            # Config-only debug: at minimum the 'debug active' marker is logged.
            # If setLevel(DEBUG) were dropped from _open_debug_log, file would
            # be empty and this fails.
            assert "debug active" in content, (
                f"TAP_DEBUG unset + DEBUG=1: 'debug active' must appear in log; "
                f"file content: {content[:200]!r}"
            )
    # Checked here, before clean_root_logger fixture teardown (handlers still attached).
    # Guard against double-handler bug: each debug-enabled request must attach
    # exactly one FileHandler. had_bootstrap must be captured BEFORE _end_bootstrap
    # or TAP_DEBUG=1 + DEBUG=1 attaches two and every record is written twice.
    debug_handlers = [
        h for h in logging.root.handlers
        if isinstance(h, logging.FileHandler) and str(debug_file) in h.baseFilename
    ]
    want_handler_count = 1 if want_file else 0   # 0 or 1; > 1 = double-handler bug
    assert len(debug_handlers) == want_handler_count, (
        f"TAP_DEBUG={env_debug!r} + config DEBUG={conf_debug!r}: "
        f"expected exactly {want_handler_count} FileHandler(s) for debugfname, "
        f"got {len(debug_handlers)} — catches both missing-handler and double-handler bugs"
    )


def test_request_field_has_no_debug_wiring(monkeypatch, tmp_path, capsys):
    """Forward fence: form['debug'] is not wired to debug activation.

    No code path in __run__ reads form['debug'] to enable debug — DEBUG is
    sourced only from configParam ([WEB] section) and TAP_DEBUG env var.
    This test is a forward fence: it catches accidental form-field-to-debug
    wiring added AFTER `self.debug = self.config.debug_flag` in Tap.__run__.
    Pre-config wiring is covered by test_propfilter_security.py::test_request_cannot_enable_debug.
    """
    conf = _write_conf(tmp_path, "0")
    monkeypatch.delenv("TAP_DEBUG", raising=False)
    monkeypatch.setenv("TAP_CONF", str(conf))
    monkeypatch.setenv("PATH_INFO", "/sync")

    debug_file = tmp_path / "tap.debug"

    app = Tap.__new__(Tap)
    app.form = _Form({"debug": SimpleNamespace(value="1")})
    app.debug = 0
    app.param = {}
    app.statdict = {}
    app.infomsg = ""
    app.dbtable = ""
    app.debugfname = str(debug_file)

    with pytest.raises(SystemExit):
        app.__run__()
    capsys.readouterr()

    assert app.debug == 0, "form field 'debug=1' does not currently enable debug"
    assert not debug_file.exists(), "no debug file should be created from form field alone"


@pytest.mark.parametrize("tap_conf_missing,want_text", [
    # TAP_CONF env var unset — classic "can't find config" case
    (True,  "TAP_CONF"),
    # TAP_CONF points at a file that doesn't exist — config read failure
    (False, "Cannot find config file"),
])
def test_bootstrap_buffer_flushed_to_stderr_on_pre_config_exit(
    monkeypatch, tmp_path, capsys,
    tap_conf_missing, want_text,
):
    """TAP_DEBUG=1 + pre-config failure → bootstrap records written to stderr only.

    Regression for: pre-config exits must NOT write the per-archive debug file —
    that file is exclusively controlled by the archive's TAP.conf [WEB] DEBUG key.
    Bootstrap records go to the web server error log (stderr) instead, so
    TAP_DEBUG remains a server-admin diagnostic tool that never bypasses
    per-archive debug policy.
    """
    monkeypatch.setenv("TAP_DEBUG", "1")
    monkeypatch.setenv("PATH_INFO", "/sync")

    if tap_conf_missing:
        monkeypatch.delenv("TAP_CONF", raising=False)
    else:
        monkeypatch.setenv("TAP_CONF", str(tmp_path / "nonexistent.conf"))

    debug_file = tmp_path / "tap.debug"

    app = Tap.__new__(Tap)
    app.form = _Form({})
    app.param = {}
    app.statdict = {}
    app.infomsg = ""
    app.dbtable = ""
    app.debugfname = str(debug_file)

    with pytest.raises(SystemExit):
        app.__init__()
    stderr = capsys.readouterr().err

    assert not debug_file.exists(), (
        f"per-archive debug file must NOT be created on pre-config failure "
        f"(tap_conf_missing={tap_conf_missing}); TAP.conf controls the file, not TAP_DEBUG"
    )
    assert "Enter Tap.init()" in stderr, (
        "bootstrap records must appear on stderr (web server error log)"
    )
    assert want_text in stderr, f"expected {want_text!r} in stderr output"


def test_no_debug_file_when_tap_debug_unset_and_conf_missing(
    monkeypatch, tmp_path, capsys,
):
    """TAP_DEBUG unset + TAP_CONF missing → no debug file created."""
    monkeypatch.delenv("TAP_DEBUG", raising=False)
    monkeypatch.delenv("TAP_CONF", raising=False)
    monkeypatch.setenv("PATH_INFO", "/sync")

    debug_file = tmp_path / "tap.debug"

    app = Tap.__new__(Tap)
    app.form = _Form({})
    app.param = {}
    app.statdict = {}
    app.infomsg = ""
    app.dbtable = ""
    app.debugfname = str(debug_file)   # must be set so the assertion checks the actual write target
    with pytest.raises(SystemExit):
        app.__init__()
    capsys.readouterr()

    assert not debug_file.exists(), "no debug file should be created when TAP_DEBUG is unset"


def test_executesql_rejects_debug_kwarg():
    """__executeSql__ has no **kwargs; passing debug= now raises TypeError.

    Guards against restoring the vestigial **kwargs removed when the debug=1
    kwarg cleanup left no consumer.  Python raises TypeError before the method
    body runs, so no live DB cursor is required.
    """
    from TAP.tapquery import tapQuery

    tq = tapQuery.__new__(tapQuery)
    tq.debug = 0
    with pytest.raises(TypeError, match="unexpected keyword argument"):
        tq.__executeSql__(None, "SELECT 1", debug=1)
