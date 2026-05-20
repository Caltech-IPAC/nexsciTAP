"""Shared test utility functions. Not auto-discovered by pytest (no `test_` prefix)."""
from __future__ import annotations


def tap_sync_url(server: str) -> str:
    """The TAP sync endpoint URL relative to *server*."""
    return f"{server}/cgi-bin/TAP/nph-tap.py/sync"


def tap_async_url(server: str) -> str:
    """The TAP async endpoint URL relative to *server*."""
    return f"{server}/cgi-bin/TAP/nph-tap.py/async"


def row_count_from_ipac(body: str) -> int:
    """Count data rows in an IPAC ASCII table body.

    IPAC format has 4 header lines (names, types, units, nulls) followed by
    data rows. Empty trailing lines are ignored.
    """
    lines = [L for L in body.splitlines() if L.strip()]
    return max(0, len(lines) - 4)
