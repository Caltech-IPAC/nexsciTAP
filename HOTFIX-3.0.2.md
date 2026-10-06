# nexsciTAP 3.0.2 security hotfix

This branch starts at the `v3.0.1` release commit
`6cf63cb8c282fb9fa7ca3800de85197b7b9e1734`. Its production changes are the
proprietary-access security fix, removal of credential logging, and the
3.0.2 version bump. It keeps the 3.0.1 configuration format and dependency
declarations. It does not include the 3.1 compatibility or async changes.

## Behavior changes

- The complete user WHERE condition is grouped before the access restriction
  is added, so the restriction applies to every OR branch.
- Queries involving protected tables must use a single table without aliases,
  joins, subqueries, set operations, or SQL comments. Unsupported queries
  return HTTP 400. The guard is conservative and can also reject keywords or
  parentheses inside string literals.
- Every referenced table is checked when choosing the protected-query path,
  including protected tables inside a query whose outer table is public.
- Cookie user IDs are bound as parameters in the user lookup and access-list
  query. Debug logging no longer dumps cookies, tokens, or database passwords.
- Operators enable request debugging with `TAP_DEBUG=1` in the server
  environment; a request's `debug` parameter no longer enables it.

## Deploy from the private advisory fork

Use the reviewed commit on `security/propfilter-access-3.0.2` in
`Caltech-IPAC/nexsciTAP-ghsa-p9v9-r3q6-gxg6`. The separate
`security/propfilter-access` branch contains the 3.1 candidate.

Build or install with the Python environment used by the TAP CGI/service.
For a checked-out copy of the reviewed commit, the installation command is:

```sh
/path/to/service/venv/bin/python -m pip install --no-deps --no-build-isolation --upgrade .
```

The same command accepts the `nexscitap-3.0.2.tar.gz` source package in place
of `.`. The existing environment supplies its installed database driver,
ADQL, spatial_index, runtime dependencies, and packaging tools. Building from
source compiles the existing C extension for the deployment's Python and
platform; use that deployment's normal build process if it distributes wheels.

Verify the imported package with the service's Python:

```sh
/path/to/service/venv/bin/python -c 'import importlib.metadata, TAP.propfilter; print(importlib.metadata.version("nexsciTAP")); print(TAP.propfilter.__file__)'
```

The version must be `3.0.2` and the path must identify the service's installed
package. Reload persistent service workers through the team's normal deployment
procedure. Keep the existing TAP configuration.

## Validation

Local tests cover synthetic public and embargoed rows for KOA and NEID,
anonymous and authorized access, OR grouping, rejected query shapes, mixed
public/protected table routing, bound user IDs with apostrophes, and debug
credential handling. The complete release-branch suite contains 144 passing
tests, with no skips. The non-spatial test setup supplies only SpatialIndex
constants when the native spatial package is unavailable.

Run the service's Oracle replica and frontend smoke checks before production
rollout. For an anonymous NEID check, these predicates must give the same count:

```sql
SELECT COUNT(*) AS n FROM neidl2 WHERE obstype = 'Sci'
SELECT COUNT(*) AS n FROM neidl2 WHERE obstype = 'Sci' OR obstype = 'Sci'
SELECT COUNT(*) AS n FROM neidl2 WHERE (obstype = 'Sci' OR obstype = 'Sci')
```

Repeat with an authorized account and confirm its intended proprietary access.
Check the team's usual KOA queries and any UI queries that use table aliases,
joins, or subqueries against protected tables, since those now return HTTP 400.
The live Oracle and NEA production comparisons have not been run as part of
the local validation.
