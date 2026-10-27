"""Every import is absolute, and tests share code only through tests.helpers, with no exceptions.

The rules live in tests/helpers/import_rules.py and are listed under "Code Style" in
CONTRIBUTING.md. import-linter resolves a relative import to its full name before it checks
the rings, so these rules keep the code searchable; they do not change what the rings allow.
"""

from __future__ import annotations

import ast

import pytest

from tests.helpers import import_rules
from tests.helpers.code_metrics import python_modules


def test_every_import_is_absolute_and_tests_share_code_through_helpers() -> None:
    found = import_rules.violations()
    assert not found, "import rules broken (see CONTRIBUTING.md, Code Style):\n" + "\n".join(found)


def test_the_rules_read_the_package_and_the_tests_but_not_test_data() -> None:
    paths = [path for path, _, _ in python_modules()]
    assert "snowflake_semantic_tools/cli/main.py" in paths
    assert "tests/unit/test_import_style.py" in paths
    assert "tests/helpers/run_recorded_plan.py" in paths
    assert not [path for path in paths if path.startswith(("tests/fixtures/", "tests/golden/"))]


# (module path, source, the one key it must produce)
BITES = [
    pytest.param("pkg/m.py", "from .dbt import DbtCatalog\n", "relative-import pkg/m.py:1", id="sibling"),
    pytest.param("pkg/m.py", "from . import main\n", "relative-import pkg/m.py:1", id="own-package"),
    pytest.param("pkg/m.py", "from ..model.dbt import DbtCatalog\n", "relative-import pkg/m.py:1", id="parent"),
    pytest.param("pkg/m.py", "def f():\n    from .x import y\n", "relative-import pkg/m.py:2", id="in-a-function"),
    pytest.param(
        "pkg/m.py",
        "from typing import TYPE_CHECKING\n\nif TYPE_CHECKING:\n    from .x import Y\n",
        "relative-import pkg/m.py:4",
        id="type-checking",
    ),
    pytest.param(
        "tests/unit/test_m.py",
        "from .helpers import common\n",
        "relative-import tests/unit/test_m.py:1",
        id="in-a-test",
    ),
    pytest.param(
        "tests/unit/test_m.py",
        "from tests.unit.app.test_eval_run import status_result\n",
        "test-support-import tests/unit/test_m.py:1",
        id="from-a-test-module",
    ),
    pytest.param(
        "tests/contract/test_m.py",
        "import tests.unit.app.conftest\n",
        "test-support-import tests/contract/test_m.py:1",
        id="a-conftest",
    ),
]


@pytest.mark.parametrize(("path", "source", "key"), BITES)
def test_each_import_rule_bites(path: str, source: str, key: str) -> None:
    assert list(import_rules.module_violations(path, ast.parse(source))) == [key]


ALLOWED = [
    pytest.param(
        "snowflake_semantic_tools/m.py",
        "from snowflake_semantic_tools.domain.model.dbt import DbtCatalog\n",
        id="full-dotted-path",
    ),
    pytest.param("snowflake_semantic_tools/m.py", "import snowflake_semantic_tools.cli.main as entry\n", id="module"),
    pytest.param("snowflake_semantic_tools/m.py", "from __future__ import annotations\n", id="future"),
    pytest.param("tests/unit/test_m.py", "from tests.helpers.app_ports import FixedClock\n", id="helpers-module"),
    pytest.param("tests/unit/test_m.py", "from tests.helpers import import_rules\n", id="helpers-package"),
    pytest.param("tests/helpers/run_m.py", "from snowflake_fake import FakeSnowflake\n", id="script-sibling"),
]


@pytest.mark.parametrize(("path", "source"), ALLOWED)
def test_absolute_imports_and_helpers_imports_pass(path: str, source: str) -> None:
    assert not list(import_rules.module_violations(path, ast.parse(source)))
