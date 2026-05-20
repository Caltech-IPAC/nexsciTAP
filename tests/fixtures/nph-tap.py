#!/usr/bin/env python
"""CGI entry point for the CI test fixture.

Matches the production CGI shim documented at Sphinx/install.rst.  The pytest
`tap_server` fixture in tests/conftest.py serves this file via
http.server.CGIHTTPRequestHandler with TAP_CONF set to the fixture's TAP.conf.
"""
from TAP import tap

tap.Tap()
