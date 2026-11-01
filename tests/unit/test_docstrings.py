"""Package code carries the docstrings CONTRIBUTING.md asks for, with no exceptions.

The rules live in tests/helpers/docstring_rules.py and are described in "Docstrings and
comments" in CONTRIBUTING.md. The package meets every rule, so the gate allows nothing:
new code is documented as it is written.
"""

from __future__ import annotations

import ast

import pytest

from tests.helpers import docstring_rules


def test_package_code_meets_every_docstring_rule() -> None:
    found = sorted(docstring_rules.violations())
    assert not found, "docstring rules broken (see CONTRIBUTING.md, Docstrings and comments):\n" + "\n".join(found)


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
    docstring = (
        "Check.\n\nDiagnostics:\n    SST-VAL102: one.\n    continued text.\n\nReturns:\n    SST-VAL999 is prose."
    )
    assert docstring_rules.documented_codes(docstring) == ["SST-VAL102"]


def _model_violations(source: str, path: str = "snowflake_semantic_tools/domain/model/m.py") -> set[str]:
    tree = ast.parse(source)
    index = docstring_rules.ClassIndex(node for node in ast.walk(tree) if isinstance(node, ast.ClassDef))
    return set(docstring_rules.module_violations(path, tree, index))


_MODEL = '''"""M."""
from dataclasses import dataclass
from typing import ClassVar


@dataclass(frozen=True)
class Row:
    """A row.

    Authored keys: `authored_schema.{table}`.

    Attributes:
        name, kind: What it is.
        {listed}
    """

    name: str
    kind: str
    origin: str
    LIMIT: ClassVar[int] = 3
'''


def test_a_model_dataclass_lists_each_field_it_declares_and_cites_only_tables_that_exist() -> None:
    complete = _MODEL.format(table="AGENT", listed="origin: Where it starts.")
    assert _model_violations(complete) == set()
    # An entry's continuation line names nothing; a `ClassVar` is no field.
    unlisted = _MODEL.format(table="AGENT", listed="other: Not a field.\n            origin: indented deeper.")
    assert _model_violations(unlisted) == {"docstring-fields snowflake_semantic_tools/domain/model/m.py::Row"}
    unknown = _MODEL.format(table="NO_SUCH_TABLE", listed="origin: Where it starts.")
    assert _model_violations(unknown) == {"docstring-authored snowflake_semantic_tools/domain/model/m.py::Row"}
    # Outside the model and API packages a dataclass documents only what its fields do not say.
    assert _model_violations(unlisted, "snowflake_semantic_tools/app/m.py") == set()
