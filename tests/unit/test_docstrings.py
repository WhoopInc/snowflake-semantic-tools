"""Package code carries the docstrings CONTRIBUTING.md asks for; known gaps may only shrink.

The rules live in tests/helpers/docstring_rules.py and are described in "Docstrings and
comments" in CONTRIBUTING.md. tests/unit/ratchets/docstrings.txt lists today's gaps: an
entry may be removed, never added, so new code is documented as it is written and old code
as it is touched. Regenerate after a deliberate move: `python -m tests.helpers.ratchet`.
"""

from __future__ import annotations

import ast

import pytest

from tests.helpers import docstring_rules, ratchet


def test_docstrings_hold_or_shrink() -> None:
    found = ratchet.problems("docstrings", docstring_rules.violations())
    assert not found, "\n".join(found) + f"\nAfter a deliberate move, regenerate: {ratchet.REGENERATE}"


def _violations(source: str) -> set[str]:
    tree = ast.parse(source)
    index = docstring_rules.ClassIndex(node for node in ast.walk(tree) if isinstance(node, ast.ClassDef))
    return set(docstring_rules.module_violations("m.py", tree, index, registered={"SST-VAL102"}))


BITES = [
    pytest.param("x = 1\n", "docstring-module m.py", id="module"),
    pytest.param('"""M."""\nclass Public:\n    x = 1\n', "docstring-class m.py::Public", id="class"),
    pytest.param('"""M."""\ndef public():\n    return 1\n', "docstring-function m.py::public", id="function"),
    pytest.param(
        '"""M."""\nfrom typing import Protocol\n\n\nclass Port(Protocol):\n    """P."""\n\n'
        "    def read(self) -> int: ...\n",
        "docstring-function m.py::Port.read",
        id="protocol-method",
    ),
    pytest.param('"""M."""\ndef _long():\n' + "    x = 1\n" * 24, "docstring-private m.py::_long", id="long-private"),
    pytest.param(
        '"""M."""\ndef _branchy(a):\n' + "".join(f"    if a == {i}:\n        return {i}\n" for i in range(9)),
        "docstring-private m.py::_branchy",
        id="complex-private",
    ),
    pytest.param('"""No period"""\n', "docstring-summary m.py", id="summary-period"),
    pytest.param('"""First line.\nSecond line."""\n', "docstring-summary m.py", id="summary-blank-line"),
    pytest.param('"""' + "x" * 100 + '."""\n', "docstring-summary m.py", id="summary-length"),
    pytest.param(
        '"""M."""\ndef f():\n    """Check.\n\n    Diagnostics:\n        SST-VAL999: not registered.\n    """\n',
        "docstring-codes m.py::f",
        id="unregistered-code",
    ),
]


@pytest.mark.parametrize(("source", "key"), BITES)
def test_each_docstring_rule_bites(source: str, key: str) -> None:
    assert key in _violations(source)


def test_documented_overrides_dunders_and_nested_helpers_are_exempt() -> None:
    source = '''"""M."""


class Base:
    """B."""

    def read(self) -> int:
        """Read one value."""
        return 1


class Child(Base):
    """C."""

    def __init__(self) -> None:
        self.value = 1

    def read(self) -> int:
        return 2


def outer() -> int:
    """O."""

    def helper() -> int:
        return 1

    return helper()


def _short() -> int:
    return 1


def checked() -> None:
    """Check.

    Diagnostics:
        SST-VAL102: a registered code.
    """
'''
    assert _violations(source) == set()


def test_documented_codes_stop_at_the_next_section() -> None:
    docstring = "Check.\n\nDiagnostics:\n    SST-VAL102: one.\n    continued text.\n\nReturns:\n    SST-VAL999 is prose."
    assert docstring_rules.documented_codes(docstring) == ["SST-VAL102"]
