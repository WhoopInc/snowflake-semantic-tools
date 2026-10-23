"""Typed readers for the configuration settings the commands consult themselves.

Everything else in the file reaches the use cases through `ProjectInputs`. Every reader here
reads the run's resolved configuration, the one value `adapters.resolved_config` produces per
invocation, so a target conditional means the same thing to each. A setting that has both a
flag and a config key resolves here: a flag that is given wins, else the key, else the default.
"""

from __future__ import annotations

from collections.abc import Mapping

from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.resolved_config import resolved_config
from snowflake_semantic_tools.app import policy
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Severity, resolve_severities
from snowflake_semantic_tools.domain.diagnostics.policy import apply_overrides
from snowflake_semantic_tools.domain.model.config_schema import (
    CONFIG_KEYS,
    DEFAULT_THREADS,
    config_block,
    config_bool,
    config_int,
    configured_dir,
    stated,
)
from snowflake_semantic_tools.domain.model.lifecycle import DRIFT_CHECK_FULL, DRIFT_CHECKS


def project_config(paths: ProjectPaths) -> dict[str, object]:
    """Return the run's resolved configuration as a plain tree."""
    return dict(resolved_config(paths).tree)


def semantic_models_dir(paths: ProjectPaths) -> str:
    """Return `project.semantic_models_dir`, or `semantic_models` when it is not set."""
    return configured_dir(project_config(paths), "semantic_models_dir", "semantic_models")


def threads_setting(paths: ProjectPaths, threads: int | None, *, default: int = DEFAULT_THREADS) -> int:
    """Return how many sessions a command reads on at once: the flag, else the config, else `default`.

    That is `--threads`, which already stands for `$SST_THREADS` when the flag is not given, else
    `generation.threads`, else `default`. `plan`, `validate` and apply's plan take
    `DEFAULT_THREADS`, the default apply applies with; `--threads 1` reads serially. `test`
    and `eval` pass 1.
    """
    if threads is not None:
        return threads
    return _generation_threads(project_config(paths)) or default


def apply_parallelism(paths: ProjectPaths, threads: int | None = None) -> int:
    """Return how many changes of one wave apply runs at once.

    `--threads` (else `$SST_THREADS`), else `generation.threads`, else `skills.+threads`, else
    `DEFAULT_THREADS`.
    """
    if threads is not None:
        return threads
    tree = project_config(paths)
    skills = _bounded("skills.+threads", config_block(tree.get("skills")).get("+threads"))
    return _generation_threads(tree) or skills or DEFAULT_THREADS


def _generation_threads(tree: dict[str, object]) -> int | None:
    return _bounded("generation.threads", config_block(tree.get("generation")).get("threads"))


def _bounded(key: str, value: object) -> int | None:
    """Return an integer setting within its key's bounds; None when it is unset or outside them.

    A value outside the bounds is the configuration's SST-CFG008, which compile reports where it
    is written, so the command does not build a session pool from it first.
    """
    number = config_int(value)
    spec = CONFIG_KEYS[key]
    if number is None or (spec.minimum is not None and number < spec.minimum):
        return None
    return None if spec.maximum is not None and number > spec.maximum else number


def drift_check_setting(paths: ProjectPaths) -> str:
    """Return `plan.drift_check`: which live properties plan compares, `full` when unset."""
    value = config_block(project_config(paths).get("plan")).get("drift_check")
    return value if isinstance(value, str) and value in DRIFT_CHECKS else DRIFT_CHECK_FULL


def apply_fail_fast(paths: ProjectPaths, flag: bool | None) -> bool:
    """Return whether apply stops at its first failure: the flag pair when given, else `apply.fail_fast`."""
    if flag is not None:
        return flag
    return config_bool(config_block(project_config(paths).get("apply")).get("fail_fast")) or False


def ask_transcripts(paths: ProjectPaths, flag: bool | None) -> bool:
    """Return whether `sst ask` keeps a transcript: the flag pair when given, else `ask.transcripts`."""
    if flag is not None:
        return flag
    return config_bool(config_block(project_config(paths).get("ask")).get("transcripts")) or False


def ask_theme(paths: ProjectPaths) -> str | None:
    """Return the theme `ask.theme` names; None when it is unset, so the conversation's default applies."""
    value = config_block(project_config(paths).get("ask")).get("theme")
    return value if isinstance(value, str) and value else None


def validation_settings(paths: ProjectPaths, *, strict: bool | None, connected: bool | None) -> tuple[bool, bool]:
    """Return whether to validate strictly, and against Snowflake: each flag given, else `validation:`.

    An unstated `snowflake_syntax_check` checks against Snowflake, its default, unless the
    SST-CFG031 that reports it blocks the run: connecting could then only fail first and hide it.
    """
    validation = config_block(project_config(paths).get("validation"))
    configured_strict = config_bool(validation.get("strict"))
    effective_strict = strict if strict is not None else bool(configured_strict)
    if connected is not None:
        return effective_strict, connected
    if stated(validation, "snowflake_syntax_check"):
        return effective_strict, config_bool(validation.get("snowflake_syntax_check")) is not False
    unstated = apply_overrides(DiagnosticBag((D("SST-CFG031"),)), severity_overrides_setting(paths))
    reported, _ = resolve_severities(unstated, strict=effective_strict)
    return effective_strict, not any(item.blocks for item in reported)


def strict_disagreement(paths: ProjectPaths, strict: bool | None) -> tuple[Diagnostic, ...]:
    """Report a `--strict` or `--no-strict` flag that contradicts `validation.strict`; the flag wins.

    Diagnostics:
        SST-CFG034: as `app.policy.strict_disagreement` reports it.
    """
    return policy.strict_disagreement(resolved_config(paths), strict)


def severity_overrides_setting(paths: ProjectPaths) -> Mapping[str, Severity]:
    """Return the severity each code `diagnostics.severity_overrides` overrides reports at."""
    return policy.severity_policy(resolved_config(paths).tree).overrides
