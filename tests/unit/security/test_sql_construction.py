"""Guard: outside `domain/sql`, no package code builds SQL from strings.

Every statement is composed by `sql()` from a static template and `Sql` parts, so the scans
here refuse what would bypass that: an f-string, `%` format, `.format()`, `+`, or `join` whose
constant text reads as SQL; a call to the `Sql` constructor or its sealing function; and a
`sql()` or `join()` from the package whose template or separator is not a string constant.
Each scan is probed with a source that must fail it, so a scan that stopped looking fails too.
"""

from __future__ import annotations

import ast
import re
from collections.abc import Iterator

import pytest

from tests.helpers.code_metrics import package_modules

SQL_PACKAGE = "snowflake_semantic_tools/domain/sql/"
_KEYWORDS = (
    *("CREATE", "ALTER", "DROP", "SELECT", "INSERT", "DELETE", "MERGE", "GRANT", "REVOKE", "USE", "PUT", "GET"),
    *("EXPLAIN", "SHOW", "DESCRIBE", "COMMENT", "CALL", "EXECUTE", "UPDATE", "TRUNCATE", "COPY", "LIST", "REMOVE"),
    *("UNDROP", "WITH", "FROM", "WHERE", "SET"),
)
_KEYWORD = re.compile(r"\b(" + "|".join(_KEYWORDS) + r")\b")
# Constant text reads as SQL when it opens with a keyword or holds two of them: prose such as
# "another writer won the MERGE" names one keyword mid-sentence, SQL like " FROM " starts with one.
_OPENS_WITH_KEYWORD = re.compile(r"^\W*(" + "|".join(_KEYWORDS) + r")\b")


def _constants(node: ast.AST) -> list[str]:
    return [item.value for item in ast.walk(node) if isinstance(item, ast.Constant) and isinstance(item.value, str)]


def _reads_as_sql(texts: list[str]) -> bool:
    found = {match.group(1) for text in texts for match in _KEYWORD.finditer(text)}
    return len(found) >= 2 or any(_OPENS_WITH_KEYWORD.match(text) for text in texts)


def _string_side(node: ast.AST) -> bool:
    return isinstance(node, ast.JoinedStr) or (isinstance(node, ast.Constant) and isinstance(node.value, str))


def string_built_sql(tree: ast.AST) -> Iterator[tuple[int, str]]:
    """Each place a string operation builds text that reads as SQL, as (line, what)."""
    for node in ast.walk(tree):
        if isinstance(node, ast.JoinedStr):
            parts = [value.value for value in node.values if isinstance(value, ast.Constant)]
            if _reads_as_sql([str(part) for part in parts]):
                yield node.lineno, "f-string"
        elif isinstance(node, ast.BinOp) and isinstance(node.op, ast.Mod) and _string_side(node.left):
            if _reads_as_sql(_constants(node.left)):
                yield node.lineno, "% format"
        elif isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            if (_string_side(node.left) or _string_side(node.right)) and _reads_as_sql(_constants(node)):
                yield node.lineno, "string +"
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in ("format", "join")
            and isinstance(node.func.value, ast.Constant)
            and isinstance(node.func.value.value, str)
            and _reads_as_sql([node.func.value.value, *_constants(node)])
        ):
            yield node.lineno, f"str.{node.func.attr}"


def _imported_names(tree: ast.Module) -> dict[str, str]:
    """What each name imported from the SQL package is bound to, by bound name."""
    names: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and (node.module or "").startswith("snowflake_semantic_tools.domain.sql"):
            for alias in node.names:
                names[alias.asname or alias.name] = alias.name
    return names


def unsealed_construction(tree: ast.Module) -> Iterator[tuple[int, str]]:
    """Each call that builds `Sql` other than through its constructors, or with a non-constant template."""
    imported = _imported_names(tree)
    for bound, name in imported.items():
        if bound != name and name in ("sql", "join", "Sql", "_seal"):
            yield 0, f"{name} imported as {bound}"
        if name.startswith("_"):
            yield 0, f"private {name} imported"
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.id if isinstance(func, ast.Name) else func.attr if isinstance(func, ast.Attribute) else ""
        if name in ("Sql", "_seal"):
            yield node.lineno, f"{name}() called"
        elif isinstance(func, ast.Name) and imported.get(func.id) in ("sql", "join"):
            first = node.args[0] if node.args else None
            if not (isinstance(first, ast.Constant) and isinstance(first.value, str)):
                yield node.lineno, f"{imported[func.id]}() with a template that is not a string constant"


def _outside_sql_package() -> Iterator[tuple[str, ast.Module]]:
    for path, tree, _ in package_modules():
        if not path.startswith(SQL_PACKAGE):
            yield path, tree


def test_the_scans_see_the_whole_package() -> None:
    paths = [path for path, _ in _outside_sql_package()]
    assert len(paths) > 200
    assert "snowflake_semantic_tools/domain/render/semantic_view.py" in paths
    assert not any(path.startswith(SQL_PACKAGE) for path in paths)


def test_no_package_code_builds_sql_from_strings() -> None:
    found = [f"{path}:{line}: {what}" for path, tree in _outside_sql_package() for line, what in string_built_sql(tree)]
    assert not found, "build these statements with sql():\n" + "\n".join(found)


def test_sql_is_built_only_through_its_constructors_and_constant_templates() -> None:
    found = [
        f"{path}:{line}: {what}" for path, tree in _outside_sql_package() for line, what in unsealed_construction(tree)
    ]
    assert not found, "\n".join(found)


@pytest.mark.parametrize(
    ("source", "what"),
    [
        ('f"DROP TABLE {name}"', "f-string"),
        ('f"{a} FROM {b}"', "f-string"),
        ('"SELECT " + column', "string +"),
        ('prefix + " WHERE x"', "string +"),
        ('"DELETE FROM %s" % table', "% format"),
        ('"UPDATE {} SET x = 1".format(table)', "str.format"),
        ('" FROM ".join(parts)', "str.join"),
        ('", ".join(["SELECT 1", "SELECT 2"])', "str.join"),
    ],
)
def test_the_string_scan_bites(source: str, what: str) -> None:
    assert [found for _, found in string_built_sql(ast.parse(source))] == [what]


@pytest.mark.parametrize(
    "source",
    [
        'f"row {name} carries VERSION {version}, another writer won the MERGE"',
        'f"stage {name} is absent after creation"',
        '"select " + name',
        '", ".join(names)',
        'f"{table}.{column}"',
    ],
)
def test_the_string_scan_leaves_prose_and_names_alone(source: str) -> None:
    assert list(string_built_sql(ast.parse(source))) == []


@pytest.mark.parametrize(
    ("source", "what"),
    [
        ("from snowflake_semantic_tools.domain.sql import Sql\nSql('DROP TABLE x')", "Sql() called"),
        ("from snowflake_semantic_tools.domain.sql.core import _seal\n_seal(text)", "_seal() called"),
        ("from snowflake_semantic_tools.domain.sql import sql\nsql(template)", "sql() with a template"),
        ("from snowflake_semantic_tools.domain.sql import sql\nsql(f'SELECT {x}')", "sql() with a template"),
        ("from snowflake_semantic_tools.domain.sql import join\njoin(sep, parts)", "join() with a template"),
        ("from snowflake_semantic_tools.domain.sql import sql as build", "sql imported as build"),
    ],
)
def test_the_construction_scan_bites(source: str, what: str) -> None:
    assert any(found.startswith(what) for _, found in unsealed_construction(ast.parse(source)))


def test_the_construction_scan_accepts_constant_templates() -> None:
    source = "from snowflake_semantic_tools.domain.sql import join, sql\nsql('SELECT {x}', x=y)\njoin(', ', parts)"
    assert list(unsealed_construction(ast.parse(source))) == []
