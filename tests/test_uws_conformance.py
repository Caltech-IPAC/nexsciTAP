"""UWS behaviors taplint checks (E-UWS-DENO, -HTDE, -JDED, -JDDE) and what
deleting a job needs to be safe."""
from __future__ import annotations

import http.client
import os
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import urlsplit

import pytest
import requests
from _helpers import tap_async_url

from TAP import compat

UWS = "{http://www.ivoa.net/xml/UWS/v1.0}"
QUERY = "SELECT TOP 2 table_name FROM TAP_SCHEMA.tables"


def _submit(server):
    r = requests.post(tap_async_url(server), data={"query": QUERY, "lang": "ADQL", "format": "csv"},
                      allow_redirects=False, timeout=60)
    assert r.status_code == 303, r.text
    return r.headers["Location"]


def _job(url):
    r = requests.get(url, timeout=60)
    assert r.status_code == 200, r.text
    return ET.fromstring(r.content)


def _text(url, key):
    r = requests.get(f"{url}/{key}", timeout=60)
    assert r.status_code == 200, r.text
    return r.text.strip()


def _jobs_dir(fixture_root: Path) -> Path:
    return fixture_root / "workdir" / "TAP"


@pytest.mark.parametrize("phase", ["PENDING", "COMPLETED"])
def test_job_document_agrees_with_its_resources(tap_server, phase):
    """JDED, JDDE: the job document and /executionduration, /destruction match."""
    url = _submit(tap_server)
    if phase == "COMPLETED":
        requests.post(url + "/phase", data={"PHASE": "RUN"}, allow_redirects=False, timeout=60)
        for _ in range(60):
            if _text(url, "phase") == "COMPLETED":
                break
    job = _job(url)
    duration = job.find(UWS + "executionDuration")
    assert duration is not None, "UWS names the element executionDuration"
    assert (duration.text or "") == _text(url, "executionduration")
    assert (job.find(UWS + "destruction").text or "") == _text(url, "destruction")


@pytest.mark.parametrize("how", ["POST ACTION=DELETE", "HTTP DELETE"])
def test_deleted_job_is_gone(tap_server, fixture_root, how):
    """DENO, HTDE: deleting answers 303 to the job list; the job is then 404."""
    url = _submit(tap_server)
    jobid = url.rstrip("/").rsplit("/", 1)[1]
    assert (_jobs_dir(fixture_root) / jobid).is_dir()
    if how == "HTTP DELETE":
        r = requests.delete(url, allow_redirects=False, timeout=60)
    else:
        r = requests.post(url, data={"ACTION": "DELETE"}, allow_redirects=False, timeout=60)
    assert r.status_code == 303, r.text
    assert r.headers["Location"].rstrip("/").endswith("/async")
    assert requests.get(url, timeout=60).status_code == 404
    assert not (_jobs_dir(fixture_root) / jobid).exists()


def test_completed_job_can_be_deleted(tap_server):
    url = _submit(tap_server)
    requests.post(url + "/phase", data={"PHASE": "RUN"}, allow_redirects=False, timeout=60)
    for _ in range(60):
        if _text(url, "phase") == "COMPLETED":
            break
    r = requests.delete(url, allow_redirects=False, timeout=60)
    assert r.status_code == 303, r.text
    assert requests.get(url, timeout=60).status_code == 404


def test_unknown_job_is_404(tap_server):
    url = tap_async_url(tap_server) + "/tap_doesnotexist"
    assert requests.get(url, timeout=60).status_code == 404
    assert requests.delete(url, allow_redirects=False, timeout=60).status_code == 404


def _raw_status(server, method, path, body=b""):
    """Send path as is: requests would drop '.' and '..' segments first."""
    parts = urlsplit(server)
    conn = http.client.HTTPConnection(parts.hostname, parts.port, timeout=60)
    headers = {"Content-Type": "application/x-www-form-urlencoded"} if body else {}
    conn.request(method, path, body=body, headers=headers)
    status = conn.getresponse().status
    conn.close()
    return status


@pytest.mark.parametrize("jobid", ["..", ".", "TAP", "tap_x.y", "notajob"])
def test_job_ids_outside_the_job_namespace_are_404(tap_server, fixture_root, jobid):
    """A job id names a directory under <workdir>/TAP; nothing else may be reached."""
    before = sorted(os.listdir(fixture_root / "workdir"))
    path = urlsplit(tap_async_url(tap_server)).path + "/" + jobid
    assert _raw_status(tap_server, "DELETE", path) == 404
    assert _raw_status(tap_server, "POST", path, b"ACTION=DELETE") == 404
    assert _raw_status(tap_server, "GET", path) == 404
    assert sorted(os.listdir(fixture_root / "workdir")) == before
    assert _jobs_dir(fixture_root).is_dir()


def test_job_list_does_not_create_a_job(tap_server, fixture_root):
    """Clients follow the delete redirect with a GET on the job list."""
    before = set(os.listdir(_jobs_dir(fixture_root)))
    r = requests.get(tap_async_url(tap_server), allow_redirects=False, timeout=60)
    assert r.status_code == 200, r.text
    assert ET.fromstring(r.content).tag == UWS + "jobs"
    assert set(os.listdir(_jobs_dir(fixture_root))) == before


@pytest.mark.parametrize("httpurl, cgipgm", [
    ("https://host.org", "/TAP"), ("https://host.org/", "TAP"), ("https://host.org", "TAP"),
    ("https://host.org/", "/TAP/"),
])
def test_job_urls_have_one_slash_between_parts(httpurl, cgipgm):
    assert compat.service_url(compat.NONE, httpurl, cgipgm) == "https://host.org/TAP"


def test_nea_keeps_its_job_url_form():
    # NEA's job URLs carry the double slash today (HTTP_URL + '/' + CGI_PGM=/TAP).
    assert compat.service_url(compat.ALL, "https://host.org", "/TAP") == "https://host.org//TAP"


def test_delete_works_in_compat_mode(legacy_tap_server, fixture_root):
    url = _submit(legacy_tap_server)
    r = requests.delete(url, allow_redirects=False, timeout=60)
    assert r.status_code == 303, r.text
    assert requests.get(url, timeout=60).status_code == 404
