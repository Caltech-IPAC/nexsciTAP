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

Server-side enforcement: a client cannot disable the prop filter on KOA
tables or NEID L1/L2/ENG tables, regardless of what `propflag` they send.

## Scope

In scope:

- KOA configurations — block `propflag=0` always.
- NEID configurations — block `propflag=0` on ENG, L1, L2 tables.

Out of scope:

- Removing `propflag` from the HTTP API surface (cleaner architecturally but
  bigger blast radius for command-line callers; not justified for this fix).
  See "Why not just remove propflag" below.
- `tap_schema` handling — the existing override at `TAP/tap.py:1587-1590`
  already forces `propflag=0` for `tap_schema` queries and must continue to
  run after the new guard.
- Fixing the root-cause `dbtable` extraction bug (see "Known limitation"
  below) — separate concern affecting more than this fix.

## Approach

Add two server-side overrides in `TAP/tap.py`, immediately after the
existing default-selection block (after line 1581, before the `tap_schema`
override at line 1587):

```python
if (self.config.propfilter.lower() == 'koa'
        and self.propflag == 0):
    self.propflag = 1

if (self.config.propfilter.lower() == 'neid'
        and self.propflag == 0
        and self.datalevel in ('eng', 'l1', 'l2')):
    self.propflag = 1
```

The guards run after the `propflag == -1` default block, so they see either
the client-supplied value or the server default. They run before the
`tap_schema` override so that override still has the final word for
`tap_schema` queries.

### Why an explicit whitelist (eng/l1/l2), not a blacklist (!= l0)

`TAP/tap.py:__getDatalevel__` (line 2826) determines `datalevel` by
substring-matching the *first* table extracted from the query against
`['l0','l1','l2','eng']`. The "first table" comes from
`TableNames.extract_tables()[0]`, which recurses into subqueries
inner-first — so for a query like:

```sql
SELECT (CASE WHEN (program IN (SELECT program FROM neid_access ...))
             OR current_date > add_months(obsdate, l0propint)
        THEN ... END) AS filename, ...
FROM neidl0 WHERE ...
```

`dbtable` becomes `neid_access` (the subquery FROM), not `neidl0`, and
`__getDatalevel__` returns `''` because none of `l0/l1/l2/eng` appear in
`neid_access`.

A blacklist check `datalevel != 'l0'` evaluates True for `datalevel=''` and
would incorrectly force `propflag=1` on an L0 query, routing it through
`propFilter`, which then constructs its prop-check SQL using
`dbtable='neid_access'` — garbage out. (This was the cause of the
"Referenced column ... not found in FROM clause!" error during testing.)

A whitelist check `datalevel in ('eng', 'l1', 'l2')` evaluates False for
`datalevel=''`, so the guard does not fire — L0 queries with such
subqueries continue working unchanged.

### Why not just remove `propflag`

Removing `propflag` from the HTTP API and deriving it entirely server-side
does not solve the underlying parsing problem. The server's existing
default-selection block at lines 1574-1581 also depends on `datalevel`, so
when `datalevel=''` for a complex L1/L2/ENG query, the default block falls
through to `propflag=0` and the prop check is skipped just the same.

The robust fix is to make `dbtable` extraction return the outer-FROM table
rather than the first FROM encountered — see "Known limitation".

### Known limitation

The whitelist guard is correct for queries where `__getDatalevel__` returns
a valid datalevel. It is **not effective** for L1/L2/ENG queries where the
parser returns `datalevel=''` due to a subquery FROM appearing before the
outer FROM. In that case the server cannot tell from the table name alone
whether the query targets L0 or proprietary data, so the guard does not
fire and the client-supplied `propflag=0` is honored.

This is a deeper bug in `TAP/tablenames.py`/`TAP/tap.py:__getDatalevel__`
that affects more than just `propflag` handling — for example,
`propFilter` itself uses `dbtable` when building prop-check SQL. A
follow-up should add a `TableNames.extract_main_table()` method that
returns the table from the *outermost* FROM and use it everywhere
`tables[0]` is currently consumed.

For the current fix this gap is mitigated, not closed, by:

- The UI continuing to send `propflag=1` for L1/L2/ENG (so the bypass
  requires a hand-crafted client request).
- The download path enforcing propriety independently of TAP.

## Behavior matrix

| Config | datalevel | client `propflag` | Before fix | After fix |
|--------|-----------|-------------------|------------|-----------|
| NEID   | l0        | (unset)           | 0          | 0         |
| NEID   | l0        | 0                 | 0          | 0         |
| NEID   | l0        | 1                 | 1          | 1         |
| NEID   | l1/l2/eng | (unset)           | 1          | 1         |
| NEID   | l1/l2/eng | 0                 | **0**      | **1**     |
| NEID   | l1/l2/eng | 1                 | 1          | 1         |
| NEID   | `''` (parse failure) | 0      | 0          | 0         |
| KOA    | any       | (unset)           | 1          | 1         |
| KOA    | any       | **0**             | **0**      | **1**     |
| KOA    | any       | 1                 | 1          | 1         |
| any    | (any, `tap_schema` table) | any | 0 | 0 |

Two rows change — NEID non-L0 with explicit `propflag=0`, and KOA with
explicit `propflag=0`. The `datalevel=''` row remains at `0` and is
covered by the limitation noted above.

## Testing

Behavioral checks against a NEID-configured server:

1. Query a `neid_l1` table with `propflag=0` → returns only rows the token
   has access to (was: all rows).
2. Query a `neid_l0` table with `propflag=0` (the simple form) → still
   returns all rows (L0 metadata exposure preserved).
3. Query a `neid_l0` table with `propflag=0` and a CASE WHEN containing a
   subquery FROM (the UI's actual download-link query) → still returns
   all rows (regression check — this was broken by the earlier blacklist
   approach).
4. Query a `neid_l1` table without `propflag` → unchanged (already
   prop-filtered via the `-1` default).

KOA checks against a KOA-configured server:

5. Query a KOA table with `propflag=0` → forced through propFilter (was:
   bypassed).
6. Query a KOA table without `propflag` → unchanged.

A unit-level test that drives the `propflag` decision logic on a mocked
`config.propfilter` and `datalevel` would be cleaner than running the
full HTTP path, but the decision is currently inline in a 1300-line
`Tap.run()`-style method, so extracting it for testing is out of scope
for this fix. The three integration checks above are sufficient.

## Rollout

Single PR against `develop`, targeting merge before the
security/table-validation branch ships. No data migration, no config
change, no API change.
