"""The project policy every command result is held to: severity overrides and the strict notices.

`diagnostics.severity_overrides` changes the severity a code reports at, and so the outcome of a
command whose exit follows its diagnostics. The first validate, plan, or apply of a project that
declares `validation.strict: true` with no baseline says, once, how many warnings now block. Both
read the run's resolved configuration, the one every other reader sees.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.diagnostics.policy import SeverityPolicy, apply_overrides
from snowflake_semantic_tools.domain.model.config_schema import config_block, config_bool
from snowflake_semantic_tools.domain.ports.project import ProjectConfig
from snowflake_semantic_tools.domain.validate.config import severity_overrides

# The commands whose result a strict project's warnings now block.
STRICT_COMMANDS = frozenset(("validate", "plan", "apply"))


@dataclass(frozen=True, slots=True)
class PolicyResult:
    """A command's diagnostics once the project's policy holds them.

    Attributes:
        diagnostics: The diagnostics, overrides applied, after the strict notice when it is given.
        blocks: For a result whose exit follows its diagnostics, whether it now blocks; None when
            the policy changes nothing about its exit.
        notice_given: Whether the strict-adoption notice is in `diagnostics`, so it is not given again.
    """

    diagnostics: DiagnosticBag
    blocks: bool | None
    notice_given: bool


def severity_policy(tree: Mapping[str, Any], *, strict: bool = False) -> SeverityPolicy:
    """Return the severity policy a resolved configuration declares, with strict mode as given."""
    return SeverityPolicy(severity_overrides(tree), strict)


def hold_to_policy(
    command: str,
    diagnostics: DiagnosticBag,
    config: ProjectConfig,
    *,
    gated: bool,
    promoted: int,
    baselined: bool,
    notice_due: bool,
) -> PolicyResult:
    """Apply the project's overrides to a command's diagnostics, and give the strict notice when due.

    Args:
        gated: Whether the command's exit follows its diagnostics, so an override can change it.
        promoted: How many warnings strict mode made errors, which the notice reports.
        baselined: Whether the run read a baseline; the notice is for a project without one.
        notice_due: Whether the notice has not yet been given for this project.

    Diagnostics:
        SST-CFG037: the project declares `validation.strict: true` and has no baseline; once.
    """
    overrides = severity_overrides(config.tree)
    held = apply_overrides(diagnostics, overrides)
    blocks = held.has_errors if gated and overrides else None
    strict = config_bool(config_block(config.tree.get("validation")).get("strict"))
    if not (command in STRICT_COMMANDS and strict and not baselined and notice_due):
        return PolicyResult(held, blocks, notice_given=False)
    notice = D("SST-CFG037", origin=Origin(config.file), subject="config:validation.strict", count=promoted)
    return PolicyResult(DiagnosticBag((notice, *held)), blocks, notice_given=True)


def strict_disagreement(config: ProjectConfig, strict: bool | None) -> tuple[Diagnostic, ...]:
    """Report a `--strict` or `--no-strict` flag that contradicts `validation.strict`; the flag wins.

    Diagnostics:
        SST-CFG034: the flag and the config key are both set and disagree.
    """
    configured = config_bool(config_block(config.tree.get("validation")).get("strict"))
    if strict is None or configured is None or strict == configured:
        return ()
    return (
        D(
            "SST-CFG034",
            origin=Origin(config.file),
            subject="config:validation.strict",
            flag=str(strict).lower(),
            config=str(configured).lower(),
        ),
    )
