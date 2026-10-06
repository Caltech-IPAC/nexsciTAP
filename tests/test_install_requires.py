"""setup.py declares every package the TAP code needs at run time.

CI installs nexsciTAP with --no-deps and gets its runtime packages from
requirements-test.txt, so a package missing from install_requires passes
CI and only fails on a real `pip install`.  Missing lxml did exactly that:
sync queries worked and every async job returned 500.
"""
from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

# Import name -> distribution name, where they differ.
DIST_NAME = {'bs4': 'beautifulsoup4'}

# Database drivers are imported only for the DBMS a deployment configures.
OPTIONAL = {'cx_Oracle', 'psycopg2', 'mysql'}

# Not imported, but named as a parser: BeautifulSoup(data, 'lxml').
NAMED_ONLY = {'lxml'}


def _install_requires() -> set[str]:
    tree = ast.parse((REPO_ROOT / 'setup.py').read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.keyword) and node.arg == 'install_requires':
            reqs = ast.literal_eval(node.value)
            return {re.split(r'[\s;\[<>=!~]', r, maxsplit=1)[0] for r in reqs}
    raise AssertionError('setup.py has no install_requires')


def _third_party_imports() -> set[str]:
    found = set()
    for path in (REPO_ROOT / 'TAP').glob('*.py'):
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0:
                names = [node.module or '']
            else:
                continue
            for name in names:
                top = name.split('.')[0]
                if top != 'TAP' and top not in sys.stdlib_module_names:
                    found.add(top)
    return found


@pytest.mark.skipif(sys.version_info < (3, 10), reason='needs sys.stdlib_module_names')
def test_every_runtime_import_is_declared():
    needed = {DIST_NAME.get(m, m) for m in _third_party_imports() - OPTIONAL} | NAMED_ONLY
    missing = needed - _install_requires()
    assert not missing, f'add to install_requires in setup.py: {sorted(missing)}'
