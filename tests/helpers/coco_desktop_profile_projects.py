"""A dbt project with one Snowflake target, for the tests of how a profile is read."""

from __future__ import annotations

from pathlib import Path


def target_project(root: Path, fields: str = "", *, adapter: str = "snowflake") -> Path:
    """Write a project whose profile `test` has the one target `x`, with `fields` added to it."""
    (root / "dbt_project.yml").write_text("profile: test\n", encoding="utf-8")
    lines = "".join(f"      {line}\n" for line in fields.splitlines())
    (root / "profiles.yml").write_text(
        f"test:\n  target: x\n  outputs:\n    x:\n      type: {adapter}\n      database: DB\n      schema: S\n{lines}",
        encoding="utf-8",
    )
    return root


def default_target_project(root: Path, target: str) -> Path:
    """Write `target_project`'s project with its profile's `target:` written as `target`, and a target `y`."""
    target_project(root, "")
    path = root / "profiles.yml"
    text = path.read_text(encoding="utf-8").replace("  target: x\n", f"  target: {target}\n", 1)
    path.write_text(text + "    y:\n      type: snowflake\n      database: DB\n      schema: S\n", encoding="utf-8")
    return root
