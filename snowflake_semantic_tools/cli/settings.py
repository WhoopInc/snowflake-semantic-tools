"""Typed readers for the configuration settings the commands consult themselves.

Everything else in the file reaches the use cases through `ProjectInputs`. A setting that has
both a flag and a config key resolves here: a flag that is given wins, else the key, else the
default.
"""

from __future__ import annotations

from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.yaml.config import load_project_config
from snowflake_semantic_tools.cli.wiring.project import project_inputs
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.config_schema import config_block, config_bool, config_int, configured_dir


def project_config(paths: ProjectPaths) -> dict[str, object]:
    """Return the project's configuration file as a plain tree."""
    return dict(load_project_config(paths).tree)


def semantic_models_dir(paths: ProjectPaths) -> str:
    """Return `project.semantic_models_dir`, or `semantic_models` when it is not set."""
    return configured_dir(project_config(paths), "semantic_models_dir", "semantic_models")


def threads_setting(paths: ProjectPaths, threads: int | None) -> int:
    """Return how many sessions a command reads on at once: `--threads`, else `generation.threads`, else 1.

    `--threads` already stands for `$SST_THREADS` when the flag is not given.
    """
    if threads is not None:
        return threads
    return _generation_threads(project_config(paths)) or 1


def apply_parallelism(paths: ProjectPaths, threads: int | None = None) -> int:
    """Return how many changes of one wave apply runs at once.

    `--threads` (else `$SST_THREADS`), else `generation.threads`, else `skills.+threads`, else 4.
    """
    if threads is not None:
        return threads
    tree = project_config(paths)
    return _generation_threads(tree) or config_int(config_block(tree.get("skills")).get("+threads")) or 4


def _generation_threads(tree: dict[str, object]) -> int | None:
    return config_int(config_block(tree.get("generation")).get("threads"))


def apply_fail_fast(paths: ProjectPaths, flag: bool | None) -> bool:
    """Return whether apply stops at its first failure: the flag pair when given, else `apply.fail_fast`."""
    if flag is not None:
        return flag
    return config_bool(config_block(project_config(paths).get("apply")).get("fail_fast")) or False


def validation_settings(paths: ProjectPaths, *, strict: bool | None, connected: bool | None) -> tuple[bool, bool]:
    """Return whether to validate strictly, and against Snowflake: each flag given, else `validation:`."""
    return project_inputs(paths, None, None).validation_defaults().resolve(strict, connected)


def strict_disagreement(paths: ProjectPaths, strict: bool | None) -> tuple[Diagnostic, ...]:
    """Report a `--strict` or `--no-strict` flag that contradicts `validation.strict`; the flag wins.

    Diagnostics:
        SST-CFG034: the flag and the config key are both set and disagree.
    """
    configured = config_bool(config_block(project_config(paths).get("validation")).get("strict"))
    if strict is None or configured is None or strict == configured:
        return ()
    return (
        D(
            "SST-CFG034",
            origin=Origin(paths.config_name),
            subject="config:validation.strict",
            flag=str(strict).lower(),
            config=str(configured).lower(),
        ),
    )
