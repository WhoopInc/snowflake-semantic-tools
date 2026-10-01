"""Internal codes (INT), which report a defect in SST itself, and rendering codes (RND).

`D` emits SST-INT900 for an unregistered code and SST-INT901 for missing template
context, so both must stay registered. Each prefix keeps its own section title in
`SUBSYSTEMS`; `TITLE` is the title of INT.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.diagnostic.core import ErrorSpec, Severity, spec

TITLE: str = "Internal"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-INT900",
        Severity.ERROR,
        "Unregistered diagnostic code",
        "unregistered code {value}",
        "report this as a bug",
        demotable=False,
    ),
    spec(
        "SST-INT901",
        Severity.ERROR,
        "Missing diagnostic context",
        "{value} template needs {placeholder}, which was not supplied",
        "report this as a bug",
        demotable=False,
    ),
    spec(
        "SST-INT902",
        Severity.ERROR,
        "Domain invariant violated",
        "domain invariant violated: {detail}",
        "report this as a bug",
        demotable=False,
    ),
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
