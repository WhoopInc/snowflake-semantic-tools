# Diagnostic-code guards

These tests hold the engine's diagnostic codes to the 1.0 error catalog. They are ordinary tests:
`pytest tests/` runs them, and so does CI.

| Test | Guard |
|------|-------|
| `test_catalog_parity.py` | Every live catalog code is registered with the catalog's severity, non-demotable flag, and message template; no retired code is registered; every registered code is in the catalog. |
| `test_codes_emitted.py` | Every registered code is named by some package module outside `domain/diagnostics/specs/`, and every code the package names is registered. |
| `test_codes_tested.py` | Every registered code has a `test_<code>_fires` and a `test_<code>_silent` test somewhere under `tests/`, such as `test_sst_cfg003_fires`. |
| `test_guard_machinery.py` | The ratchet, the scans, and the catalog projection each fail on the shape they exist for. |

## `catalog.json`

The 1.0 error catalog's declaration rows: for each code, its area, number, severity (`RETIRED` for
a burned number), whether it is non-demotable, its title, its message template, and where its
condition is first observable (`precheck`). It is the source of truth for what a code means and how
severe it is; when the engine disagrees, the engine is what changes, or the disagreement is listed
in the allowlist below.

It is generated, never edited by hand. When the catalog changes, extract its declaration rows to a
JSON list and regenerate:

```bash
poetry run python -m tests.helpers.code_guards path/to/extracted-catalog.json
```

The projection keeps only the columns above, sorted by code. The catalog's rationale columns cite
design documents that do not ship with this repository, so they are not committed, and the
projection refuses any row whose kept columns would carry a planning identifier.

## Allowlists

`allowlist/` lists, for each guard, the codes that fail it today and why: `catalog_divergence.json`,
`unemitted.json`, and `untested.json`, each a JSON object of code to reason. Every allowlist only
shrinks. A guard fails on a gap that is not listed, on a listed reason that no longer describes the
gap, and on a listed code that now conforms; the failure names the entry to remove or update.
Never add an entry to make a guard pass: fix the code instead.

## Writing a fires/silent pair

Copy `cfg/test_sst_cfg003.py`. Put the pair in `tests/codes/<area>/test_<code>.py`, where `<area>`
is the three letters after `SST-` in lowercase. `_fires` builds the smallest input that reports the
code and pins its severity, message, and subject; `_silent` builds the nearest legitimate input and
shows the code stays quiet. A code whose condition is only observable at run time fires against a
fake or recorded Snowflake session from `tests/helpers/`. Then remove the code from
`allowlist/untested.json`.
