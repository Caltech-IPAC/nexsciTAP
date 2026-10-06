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
| `nea-vosi-headers` | `/availability` and `/capabilities` send a status line and headers | the document only |
| `nea-errors` | errors are VOTable documents; a table not in TAP_SCHEMA returns 400 | plain text; 403 |
| `nea-tables` | `/tables` uses VODataService `<flag>` elements | per-column `<principal>`, `<indexed>`, `<std>`, `<column_index>`, `<arraysize>` |
| `nea-votable` | VOTable results are `application/xml`, non-char FIELDs carry no DESCRIPTION | `text/xml`, with descriptions and units |
| `nea-uws` | job documents use `executionDuration`, `application/xml`, no `Z` on `destruction` | `executionduration`, `text/xml`, `Z` |

## Moving to the 3.x layout

1. Translate the config, keeping every behavior: in `[WEB]`, list them all.

   | 1.x | 3.x |
   |---|---|
   | `[webserver]` `TAP_WORKDIR`, `TAP_WORKURL`, `HTTP_URL`, `HTTP_PORT`, `CGI_PGM` | `[WEB]`, same keys |
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

## Release plan

- **3.1.0**: compatibility mode, the 1.x reader, this guide.
- **3.x**: where the shared behavior is to change (for example
  `executionDuration` casing and VOTable errors, which IVOA standards
  require), the release notes say so before any default changes.
- **4.0.0**: compatibility mode, the `COMPAT` key and the 1.x reader are
  removed; a 1.x `TAP.conf` then fails at startup with a pointer here.
