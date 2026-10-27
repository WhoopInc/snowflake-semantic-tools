"""Every file the package writes is written by `adapters.paths`, so no write can bypass its checks.

`adapters.paths` refuses a write outside its root, or through a symbolic link, and writes
atomically. A call anywhere else in the package that could create, replace, move or remove a
file -- `open` for writing, `Path.write_text`, `os.replace`, `shutil.copy`, a temporary file --
fails here, so a new write path has to go through the helper.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

import snowflake_semantic_tools

PACKAGE = Path(snowflake_semantic_tools.__file__).parent
HELPER = PACKAGE / "adapters" / "paths.py"

# Module functions that create, replace, move or remove a file or folder.
_FUNCTIONS = {
    "os": {"open", "fdopen", "replace", "rename", "renames", "link", "symlink", "mkdir", "makedirs", "write"}
    | {"truncate", "ftruncate", "mkfifo", "mknod"},
    "shutil": {"copy", "copy2", "copyfile", "copytree", "copymode", "copystat", "move", "rmtree"},
    "tempfile": {"mkstemp", "mkdtemp", "NamedTemporaryFile", "TemporaryFile", "SpooledTemporaryFile"}
    | {"TemporaryDirectory"},
}
# `Path` methods that write; `replace` is one only with a single argument, since `str.replace`
# always takes two.
_METHODS = {"write_text", "write_bytes", "touch", "mkdir", "symlink_to", "hardlink_to", "rename", "link_to"}
_WRITE_MODES = frozenset("wax+")


def _mode(call: ast.Call, position: int) -> ast.expr | None:
    for keyword in call.keywords:
        if keyword.arg == "mode":
            return keyword.value
    return call.args[position] if len(call.args) > position else None


def _writes(mode: ast.expr | None) -> bool:
    """A mode opens for writing when it says so, or when it is not a literal SST can read."""
    if mode is None:
        return False
    if isinstance(mode, ast.Constant) and isinstance(mode.value, str):
        return bool(_WRITE_MODES & set(mode.value))
    return True


def _aliases(tree: ast.Module) -> dict[str, str]:
    """Map each name a module binds to `os`, `shutil` or `tempfile` to that module."""
    found: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.update({alias.asname or alias.name: alias.name for alias in node.names if alias.name in _FUNCTIONS})
    return found


def write_calls(source: str) -> list[str]:
    """Return `line: call` for each call in `source` that may write a file, outside the helper."""
    tree = ast.parse(source)
    aliases = _aliases(tree)
    found: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module in _FUNCTIONS:
            banned = [alias.name for alias in node.names if alias.name in _FUNCTIONS[node.module]]
            found.extend(f"{node.lineno}: from {node.module} import {name}" for name in banned)
        if not isinstance(node, ast.Call):
            continue
        function = node.func
        if isinstance(function, ast.Name) and function.id == "open" and _writes(_mode(node, 1)):
            found.append(f"{node.lineno}: open(..., mode)")
        if not isinstance(function, ast.Attribute):
            continue
        owner = function.value.id if isinstance(function.value, ast.Name) else None
        if owner in aliases and function.attr in _FUNCTIONS[aliases[owner]]:
            found.append(f"{node.lineno}: {aliases[owner]}.{function.attr}")
        elif function.attr in _METHODS:
            found.append(f"{node.lineno}: .{function.attr}()")
        elif function.attr == "replace" and len(node.args) == 1 and not node.keywords:
            found.append(f"{node.lineno}: .replace(target)")
        elif function.attr == "open" and owner not in aliases and _writes(_mode(node, 0)):
            found.append(f"{node.lineno}: .open(mode)")
    return found


def test_no_module_but_the_write_helper_writes_a_file() -> None:
    found = [
        f"{path.relative_to(PACKAGE.parent).as_posix()}:{call}"
        for path in sorted(PACKAGE.rglob("*.py"))
        if path != HELPER
        for call in write_calls(path.read_text(encoding="utf-8"))
    ]
    assert not found, "write a file through snowflake_semantic_tools.adapters.paths:\n" + "\n".join(found)


@pytest.mark.parametrize(
    "source",
    [
        "open(path, 'w')",
        "open(path, mode='ab')",
        "open(path, 'x')",
        "open(path, 'r+')",
        "open(path, chosen)",
        "path.open('a')",
        "path.open(mode='w')",
        "path.write_text('x')",
        "path.write_bytes(b'x')",
        "path.touch()",
        "path.mkdir(parents=True)",
        "path.rename(other)",
        "path.replace(other)",
        "path.symlink_to(other)",
        "import os\nos.replace(a, b)",
        "import os\nos.open(a, flags)",
        "import os as system\nsystem.rename(a, b)",
        "import shutil\nshutil.copyfile(a, b)",
        "import shutil\nshutil.move(a, b)",
        "import shutil\nshutil.rmtree(a)",
        "import tempfile\ntempfile.mkstemp()",
        "import tempfile\ntempfile.NamedTemporaryFile()",
        "from os import replace",
        "from shutil import copy",
    ],
)
def test_the_scan_finds_each_way_to_write(source: str) -> None:
    assert write_calls(source)


@pytest.mark.parametrize(
    "source",
    [
        "open(path)",
        "open(path, 'rb')",
        "path.open()",
        "path.open('r', encoding='utf-8')",
        "path.read_text()",
        "text.replace('a', 'b')",
        "value.replace(tzinfo=None)",
        "import os\nos.path.join(a, b)",
        "import os\nos.environ.get('X')",
        "from os import path",
    ],
)
def test_the_scan_leaves_reads_alone(source: str) -> None:
    assert write_calls(source) == []
