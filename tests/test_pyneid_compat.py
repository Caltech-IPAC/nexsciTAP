"""pyNEID compatibility regression tests.

Still skipped, but for a different reason than before.

pyNEID's ``query_adql`` always uses the async TAP endpoint, which works
in three round trips:

1. POST to /TAP/async with the ADQL query -> server returns 303 See Other
   with a Location: <statusurl> header.
2. POST to <statusurl>/phase with PHASE=RUN -> server starts the job and
   returns another 303 to the same status URL.
3. GET the status URL repeatedly until phase=COMPLETED, then GET the
   result URL embedded in the status XML.

This module used to say the fixture handler could not carry that
sequence, and recorded that "pyNEID hung at step 2 with no observable
error in the TAP debug log". The hang was not the fixture. Step 2 ended
by SIGKILLing os.getppid(), which under the fixture is the test runner
and in production is the web server child serving the request, so the
client was left waiting on a pipe nobody would close. That is fixed, and
the sequence above now has direct coverage in tests/test_async_flow.py.

What is still missing here is pyNEID itself: it is not a test dependency
and is not installed in CI, so there is nothing to import. Adding it
means weighing a heavier test dependency (and its own network defaults)
against coverage that tests/test_async_flow.py already provides at the
HTTP level. Until that call is made, this module stays skipped.

To wire it up later: add pyNEID to requirements-test.txt, write the
round-trip test against the fixture server, and drop the skip below.
"""
from __future__ import annotations

import pytest

pytest.skip(
    "pyNEID is not a test dependency; the async flow it drives is covered "
    "directly in tests/test_async_flow.py. See module docstring.",
    allow_module_level=True,
)
