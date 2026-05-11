# NEID propflag bypass fix — design

## Background

NEID exposes Level-0 (L0) metadata to all users so the UI can build per-file
download links. The actual file downloads are handled by a separate process
that enforces proprietary access on its own. Level-1 (L1), Level-2 (L2), and
Engineering (ENG) metadata, however, contain proprietary information that must
only be returned for rows the requesting user has access to.

The UI implements this distinction by passing a `propflag` query parameter:

- `propflag=0` for L0 tables — disables the proprietary filter, returns all rows
- `propflag=1` for L1/L2/ENG tables — forces the proprietary filter

`propflag` is also exposed as an HTTP request parameter, which is read at
`TAP/tap.py:220-221` without validation. The server-side default-selection
block at `TAP/tap.py:1574-1581` only runs when `propflag` is unset
(`propflag == -1`), so a client that explicitly passes `propflag=0` bypasses
the policy and disables the prop filter on any table — including NEID
L1/L2/ENG, which exposes proprietary metadata.

The download-side risk is contained (file downloads go through a separate
process with its own access controls), but the metadata exposure is a real
issue and must be closed before the security/table-validation merge to
`develop` reaches production.

## Goal

Server-side enforcement: a client cannot disable the prop filter on NEID
L1/L2/ENG tables, regardless of what `propflag` they send.

## Scope

In scope:

- NEID configurations only (KOA already forces `propflag=1` via the default
  block).
- L1/L2/ENG tables (L0 must continue to honor `propflag=0`).

Out of scope:

- KOA hardening (existing default already forces `propflag=1`; no known UI
  path sends `propflag=0`).
- Removing `propflag` from the HTTP API surface (cleaner architecturally but
  bigger blast radius for command-line callers; not justified for this fix).
- `tap_schema` handling — the existing override at `TAP/tap.py:1587-1590`
  already forces `propflag=0` for `tap_schema` queries and must continue to
  run after the new guard.

## Approach

Add a narrow server-side override in `TAP/tap.py`, immediately after the
existing default-selection block (after line 1581, before the `tap_schema`
override at line 1587):

```python
# Ignore client-supplied propflag=0 for NEID L1/L2/ENG —
# only L0 metadata is permitted to bypass the prop filter.
if (self.config.propfilter.lower() == 'neid'
        and self.datalevel != 'l0'
        and self.propflag == 0):
    if self.debug:
        logging.debug('NEID non-L0 table: ignoring client propflag=0, '
                      'forcing propflag=1')
    self.propflag = 1
```

The guard runs after the `propflag == -1` default block, so it sees either
the client-supplied value or the server default. In either case, NEID
non-L0 + `propflag=0` becomes `propflag=1`. It runs before the `tap_schema`
override so that override still has the final word for `tap_schema` queries.

## Behavior matrix

| Config | datalevel | client `propflag` | Before fix | After fix |
|--------|-----------|-------------------|------------|-----------|
| NEID   | l0        | (unset)           | 0          | 0         |
| NEID   | l0        | 0                 | 0          | 0         |
| NEID   | l0        | 1                 | 1          | 1         |
| NEID   | l1/l2/eng | (unset)           | 1          | 1         |
| NEID   | l1/l2/eng | 0                 | **0**      | **1**     |
| NEID   | l1/l2/eng | 1                 | 1          | 1         |
| KOA    | any       | (unset)           | 1          | 1         |
| KOA    | any       | 0                 | 0          | 0         |
| KOA    | any       | 1                 | 1          | 1         |
| any    | (any, `tap_schema` table) | any | 0 | 0 |

Only one row changes — NEID non-L0 with explicit `propflag=0` — exactly the
exposed surface.

## Testing

Three behavioral checks against a NEID-configured server:

1. Query a `neid_l1` table with `propflag=0` and a token that has access to
   only a subset of rows → returns only the accessible subset (was: all
   rows).
2. Query a `neid_l0` table with `propflag=0` → still returns all rows
   (regression check — the L0 exposure feature must continue to work).
3. Query a `neid_l1` table without `propflag` → unchanged (already
   prop-filtered via the `-1` default).

A unit-level test that drives the `propflag` decision logic on a mocked
`config.propfilter` and `datalevel` would be cleaner than running the
full HTTP path, but the decision is currently inline in a 1300-line
`Tap.run()`-style method, so extracting it for testing is out of scope
for this fix. The three integration checks above are sufficient.

## Rollout

Single PR against `develop`, targeting merge before the
security/table-validation branch ships. No data migration, no config
change, no API change.
