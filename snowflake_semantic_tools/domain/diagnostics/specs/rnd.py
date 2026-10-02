"""Rendering codes (RND): a resolved artifact the renderer cannot turn into output."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Severity, spec

TITLE: str = "Rendering"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-RND012",
        Severity.ERROR,
        "Unknown agent tool type",
        "agent '{artifact}': tool type '{found}' is unknown to the renderer",
        "use a supported type, or extend the allowlist",
    ),
    spec(
        "SST-RND031",
        Severity.WARNING,
        "Skill body is empty",
        "skill '{artifact}': SKILL.md has no instructions after its frontmatter",
        "add the instructions the skill carries",
    ),
)
