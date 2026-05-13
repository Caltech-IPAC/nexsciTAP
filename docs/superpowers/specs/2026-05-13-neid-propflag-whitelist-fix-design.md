# NEID propflag whitelist fix (follow-up to PR #16) — design

## Background

PR #16 (commit `9306c9a`) added a server-side override that forced
`propflag=1` when:

```python
config.propfilter.lower() == 'neid'
    and datalevel != 'l0'
    and propflag == 0
```

The intent was: "if the client sends `propflag=0` for a non-L0 NEID query,
ignore it and force the prop filter on."

In testing against a real UI query, this guard misfired on a **legitimate
L0 query**. The UI's L0 download-link query has the shape:

```sql
SELECT (CASE WHEN (program IN (SELECT program FROM neid_access ...))
             OR current_date > add_months(obsdate, l0propint)
        THEN '<a href=...>' || l0filename || '</a>' ELSE l0filename END)
       AS filename, ...
FROM neidl0
WHERE ...
```

The subquery in the CASE expression has its own `FROM neid_access`.
`TableNames.extract_tables()` (`TAP/tablenames.py`) walks tokens
recursively and returns subquery tables first:

```
tables = ['neid_access', 'neidl0']
dbtable = tables[0] = 'neid_access'
```

`Tap.__getDatalevel__()` then substring-matches `dbtable` against
`['l0', 'l1', 'l2', 'eng']`. None of those appear in `neid_access`, so
`datalevel = ''`.

With `datalevel = ''`, the guard's `datalevel != 'l0'` clause evaluates
True, so the guard fired and forced `propflag = 1`. The L0 query was
then routed through `propFilter`, which constructed prop-check SQL using
`dbtable = 'neid_access'` and produced an invalid query. The user-visible
symptom was a parse error in the response (originally misattributed to a
DuckDB layer in the UI; the actual source is propFilter's malformed SQL).

The bypass-blocking goal of PR #16 still applies. What needs to change is
how `datalevel` is interpreted when it is empty.

## Goal

1. Preserve the bypass-blocking behavior of PR #16 for NEID L1/L2/ENG.
2. Stop misfiring on NEID L0 queries where the parser returns `datalevel=''`.
3. Extend the same bypass-blocking treatment to KOA.

## Approach

Replace the single blacklist guard with two explicit whitelist guards.

```python
if (self.config.propfilter.lower() == 'koa'
        and self.propflag == 0):
    self.propflag = 1

if (self.config.propfilter.lower() == 'neid'
        and self.propflag == 0
        and self.datalevel in ('eng', 'l1', 'l2')):
    self.propflag = 1
```

- The KOA guard runs unconditionally on KOA configs — defense-in-depth.
  Existing default-selection already produces `propflag=1` for KOA when
  the client doesn't supply one; this guard closes the explicit-`0` case.
- The NEID guard uses an explicit whitelist of known proprietary
  datalevels. When `datalevel = ''` (parser couldn't determine it), the
  guard does not fire and the query proceeds as it would have before
  PR #16 — which is the right behavior for L0 + complex CASE.

## Behavior matrix (cumulative — before PR #16, after PR #16, after this fix)

| Config | datalevel | client `propflag` | Pre-#16 | After #16 | After this fix |
|--------|-----------|-------------------|---------|-----------|----------------|
| NEID   | l0        | (unset)           | 0       | 0         | 0              |
| NEID   | l0        | 0                 | 0       | 0         | 0              |
| NEID   | l0        | 1                 | 1       | 1         | 1              |
| NEID   | `''` (parse miss on L0) | 0  | 0       | **1 ✗**   | **0 ✓**         |
| NEID   | l1/l2/eng | (unset)           | 1       | 1         | 1              |
| NEID   | l1/l2/eng | 0                 | 0       | **1 ✓**   | **1 ✓**         |
| NEID   | l1/l2/eng | 1                 | 1       | 1         | 1              |
| KOA    | any       | (unset)           | 1       | 1         | 1              |
| KOA    | any       | 0                 | 0       | 0         | **1 ✓**         |
| KOA    | any       | 1                 | 1       | 1         | 1              |
| any    | (any, `tap_schema` table) | any | 0   | 0         | 0              |

✓ = intended fix. ✗ = regression introduced by PR #16, now corrected.

## Known limitation

`datalevel = ''` can also happen on a complex **L1/L2/ENG** query where
the parser picks a subquery table first. In that case the whitelist
guard does not fire, and a client-supplied `propflag = 0` would still
bypass the filter.

This is the deeper bug: `dbtable = tables[0]` is just the first table
the parser yields, which depends on traversal order rather than the
outermost FROM clause. It affects more than `propflag` handling — for
example, `propFilter` itself uses `dbtable` when building prop-check SQL
(visible in this PR's regression: a wrong `dbtable` led to invalid SQL).

Out of scope for this fix. A follow-up should:

- Add `TableNames.extract_main_table()` that returns the table from the
  outermost FROM, ignoring subqueries.
- Use it everywhere `tables[0]` is currently consumed
  (`TAP/tapquery.py`, `TAP/propfilter.py`, `TAP/tap.py:__getDatalevel__`).
- Additionally, consider failing safe when `__getDatalevel__` cannot
  determine a level on a propfilter-enabled config: treat empty
  `datalevel` as if it were a prop-required level so the filter is
  applied by default rather than skipped.

For the current fix the gap is mitigated, not closed, by:

- The UI continues to send `propflag = 1` for L1/L2/ENG, so a bypass
  requires a hand-crafted client request.
- The download path enforces propriety independently of TAP.

## Why not just remove `propflag` from the HTTP API

Considered and rejected. The server's existing default-selection block
at `tap.py:1612-1619` also depends on `datalevel`. When `datalevel = ''`
the default block falls through to `propflag = 0`. So even without a
client-supplied `propflag`, a complex L1/L2/ENG query whose parser
returns `datalevel = ''` would silently skip the prop check. Removing
the client surface doesn't fix the underlying parsing bug.

The robust fix is the dbtable extraction work described under "Known
limitation."

## Testing

NEID:

1. Run the UI's L0 download-link query (CASE with subquery FROM
   `neid_access`, outer FROM `neidl0`) with `propflag=0`. Expect: returns
   all rows, no error. (This is the regression PR #16 introduced.)
2. Run an L1 query with `propflag=0`. Expect: rows filtered by token.
3. Run an L1 query with no `propflag`. Expect: rows filtered by token
   (unchanged).
4. Run an L0 simple query with `propflag=0`. Expect: returns all rows
   (unchanged).

KOA:

5. Run any KOA query with `propflag=0`. Expect: rows filtered by token.
   (This was a defense-in-depth gap before this fix.)
6. Run any KOA query with no `propflag`. Expect: rows filtered by token
   (unchanged).
