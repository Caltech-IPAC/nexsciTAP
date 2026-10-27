# Migrating from the 1.x TAP.conf layout and NEA compatibility mode

nexsciTAP 3.1 runs deployments that still use the 1.x `TAP.conf` layout
(`[webserver]`, a section named by `DBMS=`, top-level `ADQL_*` keys).  A 1.x
config is translated to the 3.x layout when it's read, and it turns on
**compatibility mode**: the responses that deployment sent under nexsciTAP
1.2.  Compatibility mode is temporary.  While it's on, the service writes one
line a day to stderr (the web server's error log):

    nexsciTAP: compatibility mode active (...); see MIGRATING.md

## Compatibility behaviors

| Name | With it (1.2 behavior) | Without it (3.x behavior) |
|---|---|---|
| `nea-vosi-headers` | `/availability` and `/capabilities` also send `Connection: close`, and end with an extra CRLF | status line and `Content-type` only |
| `nea-errors` | errors are VOTable documents; a table not in TAP_SCHEMA returns 400 | plain text; 403 |
| `nea-tables` | `/tables` uses VODataService `<flag>` elements | per-column `<principal>`, `<indexed>`, `<std>`, `<column_index>`, `<arraysize>` |
| `nea-votable` | VOTable results are `application/xml`, non-char FIELDs carry no DESCRIPTION (units are still written) | `text/xml`, with descriptions on every FIELD |
| `nea-uws` | job documents are `application/xml`, no `Z` on `destruction`; the 303 that starts a job (`PHASE=RUN`) carries no `Content-Type`, `Content-Length` or `Connection` lines, because the response ends when the CGI exits; `/async/<id>/executionDuration` (camelCase) is accepted as well as `/executionduration`; job URLs join `HTTP_URL` and `CGI_PGM` with `/` as written, so `CGI_PGM=/TAP` gives `//TAP` | `text/xml`, `Z`; the 303 carries `Content-Type: text/plain`, `Content-Length` and `Connection: close`; only `/executionduration` is a valid key; job URLs have one `/` between `HTTP_URL` and `CGI_PGM` |

## Moving to the 3.x layout

1. Translate the config, keeping every behavior: in `[WEB]`, list them all.

   | 1.x | 3.x |
   |---|---|
   | `[webserver]` `TAP_WORKDIR`, `TAP_WORKURL`, `HTTP_URL`, `HTTP_PORT`, `CGI_PGM`, `ArraySize`, `INFOMSG` | `[WEB]`, same keys |
   | `[webserver]` `DBMS` | `[DBMS]` `DBMS` |
   | the `[oracle]` / `[sqlite3]` section's keys | `[DBMS]`, same keys |
   | `[webserver]` `COOKIENAME`, `RACOL`, `DECCOL` | `[DBMS]`, same keys |
   | `ADQL_MODE`, `ADQL_LEVEL`, `ADQL_XCOL`, `ADQL_YCOL`, `ADQL_ZCOL`, `ADQL_COLNAME`, `ADQL_ENCODING` | `[SPTIND]` `MODE`, `LEVEL`, `XCOL`, `YCOL`, `ZCOL`, `COLNAME`, `ENCODING` |

   ```ini
   [WEB]
   COMPAT = nea-vosi-headers, nea-errors, nea-tables, nea-votable, nea-uws
   ```
   Responses don't change.

2. Remove names from `COMPAT` one at a time, each as an announced change for
   that service's users.
3. When `COMPAT` is empty, remove the key.

## Known differences from 1.2

Compatibility mode reproduces 1.2's responses, with these exceptions:

- Errors raised before `TAP.conf` is read (a missing or broken `TAP.conf`, a
  malformed request path) use the 3.x error format, since the mode isn't known
  yet.
- The async job document escapes quotes in the query text as `&#x27;`; it
  parses to the same XML.
- Both modes write the job document's duration as `executionDuration` (the
  UWS name) and read either spelling, so jobs written by an earlier release
  or under the other mode read correctly.
- Both modes follow UWS where 1.2 did not: a job can be deleted
  (`POST ACTION=DELETE` or HTTP `DELETE`, answered with 303 to the job list),
  an unknown job id is 404, `GET` on the job list returns an empty
  `<uws:jobs>` instead of creating a job, and an unset job value (such as
  `/destruction` before the job runs) is empty text instead of `None`.

`COMPAT` is read only from `[WEB]`; placing it in any other section, including
a 1.x `[webserver]`, is a configuration error.

## Release plan

- **3.1.0**: compatibility mode, the 1.x reader, this guide.  The default
  job document now spells the duration `executionDuration`, as UWS
  requires, which retires that part of `nea-uws`.
- **3.x**: where the shared behavior is to change (for example VOTable
  errors, which IVOA standards require), the release notes say so before
  any default changes.
- **4.0.0**: compatibility mode, the `COMPAT` key and the 1.x reader are
  removed; a 1.x `TAP.conf` then fails at startup with a pointer here.
