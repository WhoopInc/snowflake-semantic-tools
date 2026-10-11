# Diagnostic-code guards

These tests hold every code the engine registers to two promises: something raises it, and a
test shows it firing and staying silent. They are ordinary tests: `pytest tests/` runs them, and
so does CI.

| Test | Guard |
|------|-------|
| `test_codes_emitted.py` | Every registered code is named, as a string constant, by some package module outside `domain/diagnostics/specs/`, and every code the package names is registered. A code mentioned only inside longer text, such as a docstring, does not count. |
| `test_codes_tested.py` | Every registered code has a `test_<code>_fires` and a `test_<code>_silent` test somewhere under `tests/` outside `fixtures/` and `golden/`, such as `test_sst_cfg003_fires`. |
| `test_guard_machinery.py` | The ratchet and the scans each fail on the shape they exist for. |

## Where a code's contract lives

The registry is the source of truth. Each code's `spec(...)` entry in
[`domain/diagnostics/specs/`](../../snowflake_semantic_tools/domain/diagnostics/specs/) declares its
severity, whether a setting may demote it, its title, message template, suggestion, when it is
raised, and where its condition can first be detected (`precheck=`, `local` unless set). A retired
number is listed in `specs/retired.py`, and `TOMBSTONES` in `specs/aliases.py` says why it was
retired and which code took its condition over; the registry refuses to register it again
(SST-REG017).

`sst docs` renders all of it into [`docs/reference/error-codes.md`](../../docs/reference/error-codes.md)
and [`docs/reference/coverage.md`](../../docs/reference/coverage.md), and CI fails on
`sst docs --check` when either is stale, so a change to a code's contract shows up in review as a
diff of those pages.

## Allowlists

`allowlist/` lists, for each guard, the codes it tolerates and why: `unemitted.json` and
`untested.json`, each a JSON object of code to reason. Both are empty (`{}`): every registered code
is emitted and has its fires/silent pair. Every allowlist only shrinks, so they stay empty. A guard
fails on a gap that is not listed, on a listed reason that no longer describes the gap, and on a
listed code that now conforms; the failure names the entry to remove or update. Never add an entry
to make a guard pass: fix the code instead.

## Writing a fires/silent pair

Copy `cfg/test_sst_cfg003.py`. Put the pair in `tests/codes/<area>/test_<code>.py`, where `<area>`
is the three letters after `SST-` in lowercase. `_fires` builds the smallest input that reports the
code and pins its severity, message, and subject; `_silent` builds the nearest legitimate input and
shows the code stays quiet. A code whose condition is only observable at run time fires against a
fake or recorded Snowflake session from `tests/helpers/`. A new code lands with its pair in the
same change: the allowlists are empty, so `test_codes_tested.py` fails without it.
