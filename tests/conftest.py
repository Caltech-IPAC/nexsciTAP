"""Test setup for non-spatial security regressions on the 3.0.1 release."""
import importlib.util
import sys
from types import ModuleType

if importlib.util.find_spec("spatial_index") is None:
    # These tests use no spatial queries. Supply only the constants needed by
    # TAP/ADQL imports when the deployment's native spatial package is absent.
    spatial = ModuleType("spatial_index")
    spatial.SpatialIndex = type("SpatialIndex", (), {
        "HTM": 1, "HPX": 2, "BASE4": 10, "BASE10": 16,
    })
    sys.modules["spatial_index"] = spatial
