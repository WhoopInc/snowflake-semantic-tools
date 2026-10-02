"""Guard: every registered code is emitted somewhere, and every code the package names is registered.

A code is emitted when some package module outside the registry names it as a string constant --
`D("SST-...")`, or a constant a table or conditional hands to `D`. A code registered but named
nowhere else can never be reported; one named but not registered becomes SST-INT900 at run time.
Today's unemitted codes are listed in `allowlist/unemitted.json`, which only shrinks.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY
from tests.helpers.code_guards import code_references, load_allowlist, ratchet, unemitted


def test_every_registered_code_is_referenced_outside_the_registry() -> None:
    found = unemitted(ERROR_REGISTRY, code_references())
    problems = ratchet(found, load_allowlist("unemitted"), "unemitted")
    assert not problems, "\n".join(problems)


def test_every_code_the_package_names_is_registered() -> None:
    unknown = {code: where for code, where in code_references().items() if code not in ERROR_REGISTRY}
    assert not unknown, f"codes named in the package but not registered (a typo, or a missing spec): {unknown}"
