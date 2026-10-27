"""The rules an eval's plan and publication apply, which read nothing from Snowflake.

They say what an eval mints and under which dataset version, which custom metrics changed
definition under their own name, what provenance a dataset version's METADATA records, and
whether a config stage declares the file format evals read.
"""

from __future__ import annotations

import json
import re
from dataclasses import replace
from hashlib import sha256

from snowflake_semantic_tools.app.compile.evals import METRIC_COMPONENT_PREFIX
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag
from snowflake_semantic_tools.domain.model.eval import EVAL_MINT_NEVER
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import (
    Action,
    CompositeObservation,
    CompositePlan,
    RenderedArtifact,
)
from snowflake_semantic_tools.domain.sql import sql
from snowflake_semantic_tools.domain.state import AppliedEntry

EVAL_STAGE_SQL = sql(
    "TYPE='CSV' FIELD_DELIMITER=NONE RECORD_DELIMITER='\\n' SKIP_HEADER=0 "
    "FIELD_OPTIONALLY_ENCLOSED_BY=NONE ESCAPE_UNENCLOSED_FIELD=NONE"
)
EVAL_STAGE_FILE_FORMAT = str(EVAL_STAGE_SQL)


def edited_metrics(artifact: RenderedArtifact, state_entry: AppliedEntry | None) -> tuple[Diagnostic, ...]:
    """Report each custom metric the eval renders under a name state recorded with another definition.

    A custom metric has no Snowflake version: its name is the score's column and chart, so an
    edited prompt published under the same name would read as a continuous trend across a change
    of judge. A metric versions by taking a new name.

    Diagnostics:
        SST-VAL744: the metric's definition digest differs from the one state recorded for its name.
    """
    if state_entry is None:
        return ()
    recorded = dict(state_entry.component_fingerprints)
    edited = (
        key.removeprefix(METRIC_COMPONENT_PREFIX)
        for key, digest in artifact.component_fingerprints
        if key.startswith(METRIC_COMPONENT_PREFIX) and recorded.get(key, digest) != digest
    )
    return tuple(D("SST-VAL744", subject=f"eval_metric:{name}", artifact=name) for name in edited)


def dataset_version(artifact: RenderedArtifact) -> str | None:
    """Name the version SST adds to an eval's dataset; None for an eval that does not mint one."""
    return dict(artifact.component_fingerprints).get("dataset_version")


def with_provenance(artifact: RenderedArtifact, plan: CompositePlan) -> CompositePlan:
    """Warn on a plan that adds a dataset version whose METADATA carries no commit (SST-VAL714).

    A version is added when the plan creates the eval, or updates one whose dataset or version is
    missing. Its METADATA is the only provenance the dataset carries, since the eval API cannot
    select a version.
    """
    adds_version = plan.action is Action.CREATE or (
        plan.action is Action.UPDATE and dict(plan.observation.details).get("dataset_version") != "present"
    )
    metadata = artifact.version_metadata
    if not adds_version or metadata is None or "git_sha" in json.loads(metadata):
        return plan
    warning = D("SST-VAL714", subject=artifact.key, artifact=artifact.key)
    return replace(plan, diagnostics=DiagnosticBag((*plan.diagnostics, warning)))


def provenance(metadata: str) -> tuple[object, object, object] | None:
    """Return the agent, source table and questions' digest a version's METADATA records.

    None when the METADATA is not a JSON object.
    """
    try:
        document = json.loads(metadata)
    except ValueError:
        return None
    if not isinstance(document, dict):
        return None
    return document.get("agent"), document.get("source_table"), document.get("dataset_fingerprint")


def mints(artifact: RenderedArtifact) -> bool:
    """Report whether an eval artifact's apply creates its source table and dataset (`dataset.mint`)."""
    return dict(artifact.component_fingerprints).get("mint") != EVAL_MINT_NEVER


def eval_stage_format_matches(stage_format: str | None) -> bool:
    """Report whether a stage's declared file format is the one evals read, compared key by key."""
    return _normalize_file_format(stage_format) == _normalize_file_format(EVAL_STAGE_FILE_FORMAT)


def format_problem(stage: QualifiedName, stage_format: str | None) -> Diagnostic | None:
    """Return SST-APL028 when the config stage declares another file format than evals read."""
    if eval_stage_format_matches(stage_format):
        return None
    return D("SST-APL028", value=stage.sql, found=stage_format or "absent", expected=EVAL_STAGE_FILE_FORMAT)


def _normalize_file_format(value: str | None) -> str:
    if value is None:
        return ""
    pairs = re.findall(r"([A-Z_]+)\s*=\s*(?:'([^']*)'|([^\s]+))", value.upper())
    normalized = {key: quoted if quoted != "" else raw for key, quoted, raw in pairs}
    return " ".join(f"{key}={normalized[key]}" for key in sorted(normalized))


def observation_identity(value: CompositeObservation) -> tuple[object, ...]:
    """Return what an eval's observation is compared by, between its plan and its apply."""
    return (
        tuple((item.object_type, item.qualified_name.folded, item.exists) for item in value.resources),
        value.stage_exists,
        _normalize_file_format(value.stage_file_format),
        value.config_path,
        value.config_exists,
        value.config_size,
        value.config_md5,
    )


def safe_stage_segment(value: str) -> str:
    """Return a name as a stage path segment: as written when safe, else made safe and suffixed."""
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", value)
    if safe == value and safe not in {"", ".", ".."}:
        return safe
    digest = sha256(value.encode("utf-8")).hexdigest()[:8]
    return f"{safe or 'agent'}_{digest}"
