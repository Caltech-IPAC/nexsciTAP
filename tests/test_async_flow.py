"""End-to-end coverage of the UWS async flow.

An async TAP job takes three round trips, the same sequence pyvo and
pyNEID drive:

1. POST to ``/async`` with the query. The server creates the job in
   PENDING and answers 303 with ``Location: <statusurl>``.
2. POST ``PHASE=RUN`` to ``<statusurl>/phase``. The server starts the
   job and answers 303 to the same status URL.
3. GET ``<statusurl>`` until the phase reaches COMPLETED, then read the
   result named in the status document.

None of this could be covered before the detach fix. ``tap.py`` ended
every async response by SIGKILLing ``os.getppid()`` — under this fixture
that is the process running the test suite, and in production it is the
web server child serving the request. Step 2 therefore either hung (the
CGI never closed the stdout pipe the server was reading) or took the
test runner down with it.
"""
from __future__ import annotations

import time
import xml.etree.ElementTree as ET
from pathlib import Path

import requests
from _helpers import tap_async_url

UWS = {"uws": "http://www.ivoa.net/xml/UWS/v1.0",
       "xlink": "http://www.w3.org/1999/xlink"}

QUERY = "select * from data_l0"


def _submit(server: str) -> str:
    """Round trip 1. Returns the job's status URL."""
    resp = requests.post(
        tap_async_url(server),
        data={"query": QUERY, "format": "ipac"},
        allow_redirects=False,
        timeout=30,
    )
    assert resp.status_code == 303, resp.text[:500]
    statusurl = resp.headers["Location"]
    assert statusurl, "303 carried no Location header"
    return statusurl


def _status(statusurl: str) -> ET.Element:
    resp = requests.get(statusurl, timeout=30)
    assert resp.status_code == 200, resp.text[:500]
    return ET.fromstring(resp.text)


def _phase(statusurl: str) -> str:
    return _status(statusurl).find("uws:phase", UWS).text


def test_async_submit_creates_pending_job(tap_server):
    statusurl = _submit(tap_server)
    assert _phase(statusurl) == "PENDING"


def test_async_run_starts_the_job(tap_server):
    """Round trip 2 answers promptly and does not take the server with it."""
    statusurl = _submit(tap_server)

    resp = requests.post(
        f"{statusurl}/phase",
        data={"PHASE": "RUN"},
        allow_redirects=False,
        timeout=30,
    )

    assert resp.status_code == 303, resp.text[:500]
    assert resp.headers["Location"] == statusurl


def test_async_run_response_is_self_delimiting(tap_server):
    """The 303 must be framed by its headers, not by the connection dying.

    A reverse proxy in front of the CGI needs a Content-Length (or a
    chunked body) to relay the response. The old code emitted a body
    with neither and relied on the connection being destroyed to mark
    the end of the message.
    """
    statusurl = _submit(tap_server)

    resp = requests.post(
        f"{statusurl}/phase",
        data={"PHASE": "RUN"},
        allow_redirects=False,
        timeout=30,
    )

    assert "Content-Length" in resp.headers
    assert int(resp.headers["Content-Length"]) == len(resp.content)


def test_async_job_runs_to_completion(tap_server, fixture_root: Path):
    statusurl = _submit(tap_server)
    requests.post(
        f"{statusurl}/phase",
        data={"PHASE": "RUN"},
        allow_redirects=False,
        timeout=30,
    )

    deadline = time.time() + 30
    phase = ""
    while time.time() < deadline:
        phase = _phase(statusurl)
        if phase in ("COMPLETED", "ERROR", "ABORTED"):
            break
        time.sleep(0.2)

    assert phase == "COMPLETED", f"job ended in phase {phase}"

    job = _status(statusurl)
    href = job.find("uws:results/uws:result", UWS).get(
        "{http://www.w3.org/1999/xlink}href")
    assert href

    # The fixture server does not serve TAP_WORKURL (Apache does that in
    # production), so read the result out of the workspace on disk.
    # tap.py puts each job under <TAP_WORKDIR>/TAP/<workspace>.
    workspace = job.find("uws:jobId", UWS).text
    results = list((fixture_root / "workdir" / "TAP" / workspace).glob("*"))
    assert results, f"no result artifacts in workspace {workspace}"
    written = [p for p in results if p.name != "status.xml" and p.stat().st_size]
    assert written, f"job completed but wrote no result file: {results}"


def _post_phase(statusurl: str, phase: str):
    return requests.post(
        f"{statusurl}/phase",
        data={"PHASE": phase},
        allow_redirects=False,
        timeout=30,
    )


def _results(fixture_root: Path, workspace: str) -> list:
    workdir = fixture_root / "workdir" / "TAP" / workspace
    return [p for p in workdir.glob("*") if p.name != "status.xml"]


def test_async_abort_is_terminal(tap_server, fixture_root: Path):
    """ABORT must end the job, not run it.

    Every phase shares one response path at the end of the async block,
    and that path used to fall through into the query. So a POST of
    PHASE=ABORT wrote ABORTED, answered the client, and then ran the
    job anyway, overwriting the phase the client had just been told.
    """
    statusurl = _submit(tap_server)
    assert _phase(statusurl) == "PENDING"

    resp = _post_phase(statusurl, "ABORT")
    assert resp.status_code == 303, resp.text[:500]

    job = _status(statusurl)
    assert job.find("uws:phase", UWS).text == "ABORTED"

    workspace = job.find("uws:jobId", UWS).text
    time.sleep(1.0)
    assert _phase(statusurl) == "ABORTED", "the aborted job kept running"
    assert not _results(fixture_root, workspace), \
        "the aborted job wrote a result"


def test_async_unknown_phase_is_terminal(tap_server, fixture_root: Path):
    """An unrecognized phase reports ERROR and runs nothing."""
    statusurl = _submit(tap_server)

    resp = _post_phase(statusurl, "SPIN")
    assert resp.status_code == 303, resp.text[:500]

    job = _status(statusurl)
    assert job.find("uws:phase", UWS).text == "ERROR"

    workspace = job.find("uws:jobId", UWS).text
    time.sleep(1.0)
    assert _phase(statusurl) == "ERROR", "the rejected job kept running"
    assert not _results(fixture_root, workspace), \
        "the rejected job wrote a result"


def test_async_detach_does_not_signal_the_web_server():
    """Regression guard against the SIGKILL-the-parent detach.

    Asserted on the source rather than on behavior on purpose: the
    behavioral assertion is "the process running this suite is still
    alive", which a suite that has been SIGKILLed cannot make.
    """
    import tokenize

    from TAP import tap

    # Comments and strings are stripped before the check so the code can
    # still explain in prose why it no longer signals its parent.
    with open(tap.__file__, "rb") as fp:
        src = "".join(
            tok.string
            for tok in tokenize.tokenize(fp.readline)
            if tok.type not in (tokenize.COMMENT, tokenize.STRING))

    assert "getppid" not in src, (
        "tap.py signals its parent process. Under CGI the parent is the "
        "web server child serving the request; killing it drops the "
        "response and, behind a proxy, poisons the upstream connection."
    )
