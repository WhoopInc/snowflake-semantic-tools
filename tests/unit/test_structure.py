"""Package code stays inside its size and complexity budgets; known excesses may only shrink.

The budgets live in tests/helpers/structure_rules.py. tests/unit/ratchets/structure.txt lists
the code that exceeds them today, with the measured value: an entry may improve or be
removed, never grow, and a new excess fails here. Splitting a module moves its keys, so
regenerate the list after a deliberate move (`python -m tests.helpers.ratchet`) and check
that the diff only shrinks.
"""

from __future__ import annotations

import ast

import pytest

from tests.helpers import ratchet, structure_rules


def test_structure_budgets_hold_or_shrink() -> None:
    found = ratchet.problems("structure", structure_rules.violations())
    assert not found, "\n".join(found) + f"\nAfter a deliberate move, regenerate: {ratchet.REGENERATE}"


def _branches(count: int) -> str:
    return "".join(f"    if a == {index}:\n        return {index}\n" for index in range(count))


# (rule, module source over the limit, expected key, the same shape exactly at the limit)
BITES = [
    pytest.param("x = 1\n" * 801, "module-lines m.py", "x = 1\n" * 800, id="module-lines"),
    pytest.param(
        "def f():\n" + "    x = 1\n" * 100,
        "function-lines m.py::f",
        "def f():\n" + "    x = 1\n" * 99,
        id="function-lines",
    ),
    pytest.param(
        "def f(a):\n" + _branches(25) + "    return a\n",
        "function-complexity m.py::f",
        "def f(a):\n" + _branches(24) + "    return a\n",
        id="function-complexity",
    ),
    pytest.param(
        "def f():\n    def g():\n" + "        x = 1\n" * 25 + "    return g\n",
        "nested-function-lines m.py::f.g",
        "def f():\n    def g():\n" + "        x = 1\n" * 24 + "    return g\n",
        id="nested-function-lines",
    ),
]


@pytest.mark.parametrize(("over", "key", "at_limit"), BITES)
def test_each_structure_rule_bites_just_past_its_limit(over: str, key: str, at_limit: str) -> None:
    rule = key.split(" ")[0]
    assert key in dict(structure_rules.module_violations("m.py", ast.parse(over), over))
    assert not any(
        found.startswith(rule) for found, _ in structure_rules.module_violations("m.py", ast.parse(at_limit), at_limit)
    )


def test_complexity_counts_boolean_operands_and_comprehension_clauses() -> None:
    source = "def f(a, b, c):\n    return [x for x in a if x and b or c]\n"
    function = ast.parse(source).body[0]
    assert isinstance(function, ast.FunctionDef)
    # 1 + comprehension (1) + its `if` (1) + `or` (1) + `and` (1)
    assert structure_rules.complexity(function) == 5


def test_a_ratchet_reports_new_worse_and_fixed_entries(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setattr(ratchet, "RATCHETS", tmp_path)
    ratchet.write("demo", {"function-lines a.py::f": 120, "docstring-class a.py::C": None})
    assert ratchet.problems("demo", {"function-lines a.py::f": 120, "docstring-class a.py::C": None}) == []
    assert ratchet.problems("demo", {"function-lines a.py::f": 110, "docstring-class a.py::C": None}) == []
    found = ratchet.problems("demo", {"function-lines a.py::f": 130, "docstring-class a.py::D": None})
    assert found == [
        "new violation: docstring-class a.py::D",
        "worse than allowed: function-lines a.py::f is 130, allowlist says 120",
        "fixed or moved, remove from tests/unit/ratchets/demo.txt: docstring-class a.py::C",
    ]
