"""Pure builders for an eval-model comparison: the candidate specifications, and the statements.

A comparison evaluates orchestration models on real committed versions of a published agent or
CoWork profile, so Snowsight ties each evaluation run to a version. It commits, in order: a hold
copy of the released specification, which closes the live version; one version per candidate
model, whose specification is the released one with `models.orchestration` alone changed, each
aliased `EVAL_<MODEL>`; and a restore copy, from which a live version is opened again. Nothing
here moves the default or the released alias: only `sst apply` does
(docs/design/snowflake-behavior.md).

Each version is staged under `<folder>/eval/<run id>/<slug>/` of the agent's specification stage,
where `<slug>` is the candidate's alias in lowercase without its prefix, or `restore`; the hold
commits the live version and stages nothing.
"""

from __future__ import annotations

import json
import re

from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import EVAL_ALIAS_PREFIX, eval_model_alias
from snowflake_semantic_tools.domain.render.agent import render_spec_json
from snowflake_semantic_tools.domain.sql import Sql, ident, literal, qname, sql, stage_path

# The label of the copy of the released specification that closes the live version.
HOLD_LABEL = "hold"
# The label, and the stage directory, of the copy a live version is opened from again.
RESTORE_LABEL = "restore"
_SYSTEM_VERSION = re.compile(r"VERSION\$[1-9]\d*", re.IGNORECASE)


def with_orchestration_model(payload: str, model: str) -> str:
    """Return a specification payload with `models.orchestration` set to `model`, nothing else changed.

    The payload is re-serialized as `render_spec_json` writes every agent's, so keys keep their
    order and every other value its text.

    Raises:
        ValueError: the payload is not a JSON object, or its `models` is not one.
    """
    document = json.loads(payload)
    if not isinstance(document, dict) or not isinstance(document.get("models", {}), dict):
        raise ValueError("an agent specification is a JSON object whose `models` is an object")
    document["models"] = {**document.get("models", {}), "orchestration": model}
    return render_spec_json(document)


def model_slug(model: str) -> str:
    """Name a model as its stage directory and run names spell it: its alias, lowercased, unprefixed.

    Example:
        `claude-opus-4-8` gives `claude_opus_4_8`.
    """
    return eval_model_alias(model).removeprefix(EVAL_ALIAS_PREFIX).lower()


def compare_run_name(subject: str, run_id: str, model: str, number: int) -> str:
    """Name one evaluation run of a comparison: subject, run id, model and run number, uppercased.

    Example:
        `compare_run_name("jaffle_analyst", "c7f2a19e", "claude-sonnet-4-6", 1)` gives
        `JAFFLE_ANALYST_CMP_C7F2A19E_CLAUDE_SONNET_4_6_1`.
    """
    plain = eval_model_alias(subject).removeprefix(EVAL_ALIAS_PREFIX)
    return f"{plain}_CMP_{run_id.upper()}_{model_slug(model).upper()}_{number}"


def version_directory(folder: str, run_id: str, slug: str) -> str:
    """Return the directory below the stage one comparison version is staged in, ending in `/`."""
    return f"{folder}/eval/{run_id}/{slug}/"


def staged_spec_path(stage: QualifiedName, folder: str, run_id: str, slug: str) -> str:
    """Return the stage path one comparison version's specification is uploaded to.

    Snowflake reads a staged specification from `agent_spec.yaml` in the directory ADD VERSION
    names, as an agent's own versions are staged (docs/design/snowflake-behavior.md).
    """
    return f"@{stage.sql}/{version_directory(folder, run_id, slug)}agent_spec.yaml"


def hold_statement(agent: QualifiedName, comment: str) -> Sql:
    """Commit the live version, which holds the released specification, as the comparison's hold copy.

    Example:
        ALTER AGENT DB.S.SALES COMMIT COMMENT = 'git:abc sst-spec:... sst-eval-model:hold run:c7f2'
    """
    return sql("ALTER AGENT {agent} COMMIT COMMENT = {comment}", agent=qname(agent), comment=literal(comment))


def add_version_statement(agent: QualifiedName, stage: QualifiedName, directory: str, comment: str) -> Sql:
    """Commit a version from a staged specification directory, as a comparison's candidate or restore.

    Raises:
        ValueError: as `stage_path` raises for a directory it cannot write unquoted.

    Example:
        ALTER AGENT DB.S.SALES
          ADD VERSION FROM @DB.S.AGENT_SPECS/sales/eval/c7f2/claude_opus_4_8/
          COMMENT = 'git:abc sst-spec:... sst-eval-model:claude-opus-4-8 run:c7f2'
    """
    return sql(
        "ALTER AGENT {agent}\n  ADD VERSION FROM {directory}\n  COMMENT = {comment}",
        agent=qname(agent),
        directory=stage_path(stage, directory),
        comment=literal(comment),
    )


def unset_alias_statement(agent: QualifiedName, version: str) -> Sql:
    """Take the alias off a committed version, which an alias must leave before another takes it.

    Raises:
        ValueError: `version` is not a system version name such as `VERSION$3`.
    """
    return sql(
        "ALTER AGENT {agent} MODIFY VERSION {version} UNSET ALIAS", agent=qname(agent), version=_version(version)
    )


def set_alias_statement(agent: QualifiedName, version: str, alias: str) -> Sql:
    """Set a comparison's alias on the committed version it evaluates a candidate on.

    Raises:
        ValueError: `version` is not a system version name, or `alias` is not a plain identifier.
    """
    return sql(
        "ALTER AGENT {agent} MODIFY VERSION {version} SET ALIAS = {alias}",
        agent=qname(agent),
        version=_version(version),
        alias=ident(Identifier.parse(alias)),
    )


def open_live_statement(agent: QualifiedName) -> Sql:
    """Open a live version from the newest committed one, the restore copy.

    Snowflake opens a live version from no named version, only `FROM LAST`
    (docs/design/snowflake-behavior.md).
    """
    return sql("ALTER AGENT {agent} ADD LIVE VERSION FROM LAST", agent=qname(agent))


def _version(name: str) -> Sql:
    if _SYSTEM_VERSION.fullmatch(name) is None:
        raise ValueError(f"{name!r} is not a committed version's system name")
    return ident(Identifier.parse(name.upper()))
