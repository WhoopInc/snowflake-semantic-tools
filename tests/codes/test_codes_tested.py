"""Guard: every registered code has a test that makes it fire and one that keeps it silent.

The pair is named for the code -- `test_sst_cfg010_fires` and `test_sst_cfg010_silent`, optionally
followed by `_<what it shows>` -- and may live anywhere under `tests/`; `tests/codes/<area>/` holds
the ones written for this guard. A code whose condition is only observable at run time is made to
fire against a fake or recorded Snowflake session. Today's untested codes are listed in
`allowlist/untested.json`, which only shrinks.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY
from tests.helpers.code_guards import code_tests, load_allowlist, ratchet, untested


def test_every_registered_code_has_a_fires_and_a_silent_test() -> None:
    found = untested(ERROR_REGISTRY, code_tests())
    problems = ratchet(found, load_allowlist("untested"), "untested")
    assert not problems, "\n".join(problems)


def test_every_code_test_names_a_registered_code() -> None:
    unknown = sorted(set(code_tests()) - set(ERROR_REGISTRY))
    assert not unknown, f"fires/silent tests name codes that are not registered: {unknown}"
