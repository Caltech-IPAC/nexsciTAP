"""Minimal stub of the spatial_index package for CI use.

The real `spatial_index` package on PyPI only ships wheels for Python ≤ 3.8,
so the CI matrix (3.9 / 3.10 / 3.11 / 3.12) can't install it.  TAP imports
the package unconditionally for four module-level constants — that's all
the surface we exercise in tests, since no test query uses spatial
geometry functions.

If/when a test starts exercising spatial queries (POINT, CIRCLE, CONTAINS,
DISTANCE), this stub will need to grow or the real package needs to be
installed.

Tracking the upstream issue: spatial-index 1.1.0 on PyPI lacks a source
distribution and modern-Python wheels.
"""


class SpatialIndex:
    HTM = 1
    HPX = 2
    BASE4 = 10
    BASE10 = 16
