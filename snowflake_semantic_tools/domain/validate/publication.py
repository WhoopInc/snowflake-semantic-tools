"""Guard what SST is about to publish for skills, plugins, profiles and evals.

Each condition here is one SST's own compile and publishers rule out: a version is named by
its content, every extension lands in the one catalog schema, every PUT targets a directory,
a profile row points at and hashes every tree it ships, the publisher sends neither a grant
nor raw text, every channel reports its own outcome, and a dataset version's METADATA and
COMMENT hold identifiers and digests only. Compile, plan and apply call these on what they
are about to publish, so a change that broke one of those rules is reported, not published.
Each is pure and returns diagnostics; none raises.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.artifact_key import split_artifact_key
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import CompositeObservation, RenderedArtifact
from snowflake_semantic_tools.domain.model.skill.model import ALIAS_HEX_CHARACTERS
from snowflake_semantic_tools.domain.render.eval import dataset_version_comment
from snowflake_semantic_tools.domain.sql import Sql

# The keys SST writes into a dataset version's METADATA; any other is text SST never wrote.
DATASET_METADATA_KEYS = frozenset(("agent", "dataset_fingerprint", "git_sha", "source_table"))
# Shapes of personal or regulated data, each with the wording a diagnostic names it by.
_SENSITIVE = (
    ("an email address", re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}")),
    ("a US social security number", re.compile(r"\b\d{3}-\d{2}-\d{4}\b")),
    ("a payment card number", re.compile(r"\b(?:\d[ -]?){12,18}\d\b")),
)
# The channel each multi-channel artifact type publishes through.
_CHANNELS: Mapping[str, str] = {"skill": "catalog", "plugin": "catalog", "profile": "stage"}
_GRANT = re.compile(r"\s*GRANT\s", re.IGNORECASE)
_ROLE_TYPE = re.compile(r"\bTO\s+(?:DATABASE\s+)?ROLE\s", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class MintedVersion:
    """One extension version a compile mints: where, under which name, and for which content.

    Attributes:
        artifact: The skill or plugin name the version publishes.
        digest: The hex digest of the bundle the version holds.
    """

    key: str
    artifact: str
    target: QualifiedName
    alias: str
    digest: str


def version_diagnostics(version: MintedVersion, prefix: str) -> tuple[Diagnostic, ...]:
    """Report a version whose name is not the prefix and the leading digits of its content digest.

    Diagnostics:
        SST-VAL803: the alias is not `prefix` and 12 uppercased hex characters of the digest.
    """
    if version.alias == f"{prefix}{version.digest[:ALIAS_HEX_CHARACTERS].upper()}":
        return ()
    return (D("SST-VAL803", subject=version.key, artifact=version.artifact, value=version.alias),)


def version_collision_diagnostics(versions: Iterable[MintedVersion]) -> tuple[Diagnostic, ...]:
    """Report each version whose name an earlier one at the same extension took for other content.

    Two deploys minting one name for different files would race for it, and the loser's
    pinned references would read the winner's files.

    Diagnostics:
        SST-VAL825: an extension's version name stands for two bundle digests.
    """
    claimed: dict[tuple[tuple[str, str, str], str], str] = {}
    found: list[Diagnostic] = []
    for version in versions:
        digest = claimed.setdefault((version.target.folded, version.alias.casefold()), version.digest)
        if digest != version.digest:
            found.append(D("SST-VAL825", subject=version.key, artifact=version.artifact, value=version.alias))
    return tuple(found)


def schema_diagnostics(version: MintedVersion, catalog: tuple[str, str]) -> tuple[Diagnostic, ...]:
    """Warn when a version would land outside the catalog channel's one database and schema.

    Args:
        catalog: The channel's database and schema, folded as `QualifiedName.folded` folds them.

    Diagnostics:
        SST-VAL806: the target's database and schema are not the catalog channel's.
    """
    target = version.target
    if target.folded[:2] == catalog:
        return ()
    schema = f"{target.database.sql}.{target.schema.sql}"
    return (D("SST-VAL806", subject=version.key, artifact=version.artifact, value=schema),)


def put_target_diagnostics(key: str, artifact: str, targets: Iterable[str]) -> tuple[Diagnostic, ...]:
    """Report each upload location that is not a directory, which a file name would run on from.

    Diagnostics:
        SST-VAL820: a location does not end in `/`.
    """
    return tuple(
        D("SST-VAL820", subject=key, artifact=artifact, path=target) for target in targets if not target.endswith("/")
    )


def registry_pointer_diagnostics(
    key: str, artifact: str, uploads: Iterable[str], pointers: Sequence[str]
) -> tuple[Diagnostic, ...]:
    """Report each tree a profile uploads that no pointer in its registry row reaches.

    Args:
        uploads: Each tree's stage location, `@<stage>/<prefix>`.
        pointers: Every stage location the row's columns point Desktop at.

    Diagnostics:
        SST-VAL822: no pointer starts with the tree's location.
    """
    return tuple(
        D("SST-VAL822", subject=key, artifact=artifact, path=upload)
        for upload in uploads
        if not any(pointer.startswith(upload) for pointer in pointers)
    )


def version_coverage_diagnostics(
    key: str, artifact: str, prefixes: Iterable[str], hashed: str
) -> tuple[Diagnostic, ...]:
    """Report each tree a profile ships whose digest-named prefix is absent from what VERSION hashes.

    Args:
        hashed: The text VERSION is the digest of.

    Diagnostics:
        SST-VAL823: a tree's prefix does not occur in `hashed`.
    """
    return tuple(
        D("SST-VAL823", subject=key, artifact=artifact, value=prefix) for prefix in prefixes if prefix not in hashed
    )


def statement_diagnostic(key: str, statement: object, *, certification_pending: bool) -> Diagnostic | None:
    """Report a statement a publisher must not send: raw text, or a grant out of order or untyped.

    Args:
        certification_pending: The publish certifies the artifact and has not yet succeeded.

    Diagnostics:
        SST-VAL827: the statement is not `Sql`, so nothing it interpolates was validated.
        SST-VAL826: a GRANT, while certification is pending or naming no role type.
    """
    artifact = split_artifact_key(key)[1]
    if not isinstance(statement, Sql):
        return D("SST-VAL827", subject=key, artifact=artifact, value=str(statement)[:80])
    text = str(statement)
    if _GRANT.match(text) and (certification_pending or not _ROLE_TYPE.search(text)):
        return D("SST-VAL826", subject=key, artifact=artifact)
    return None


def channel_outcome_diagnostics(
    changes: Iterable[tuple[str, str]], outcome_keys: Sequence[str]
) -> tuple[Diagnostic, ...]:
    """Report each channel change apply reported no outcome, or several outcomes, for.

    Either way the channel's state would not show in the run's result, which could then
    read as a success while the channels disagree.

    Args:
        changes: Each planned change, as its key and artifact type; types without a channel
            are ignored.

    Diagnostics:
        SST-VAL828: a skill, plugin or profile change has no outcome, or more than one.
    """
    found: list[Diagnostic] = []
    for key, artifact_type in changes:
        channel = _CHANNELS.get(artifact_type)
        count = outcome_keys.count(key)
        if channel is None or count == 1:
            continue
        detail = f"the {channel} channel reported {count or 'no'} outcome{'s' if count != 1 else ''}"
        found.append(D("SST-VAL828", subject=key, artifact=split_artifact_key(key)[1], detail=detail))
    return tuple(found)


def dataset_metadata_diagnostics(key: str, *, metadata: str, comment: str) -> tuple[Diagnostic, ...]:
    """Report personal or regulated data, or a field SST never writes, in a dataset version's properties.

    Diagnostics:
        SST-VAL715: METADATA or COMMENT holds an email address, a social security or card
            number, or METADATA is not an object of the keys `DATASET_METADATA_KEYS` names.
    """
    found = [
        D("SST-VAL715", subject=key, artifact=key, field=field, detail=detail)
        for field, text in (("METADATA", metadata), ("COMMENT", comment))
        for detail, pattern in _SENSITIVE
        if pattern.search(text)
    ]
    unknown = _unknown_metadata(metadata)
    if unknown is not None:
        found.append(D("SST-VAL715", subject=key, artifact=key, field="METADATA", detail=unknown))
    return tuple(found)


def run_config_diagnostics(key: str, config_yaml: str, *, dataset_exists: bool) -> tuple[Diagnostic, ...]:
    """Report a run config that declares the dataset it runs against when that dataset already exists.

    SST creates the dataset itself, so the config holds `evaluation:` and `metrics:` only; a
    `dataset:` block would ask the run to create the dataset again and fail every run.

    Diagnostics:
        SST-VAL716: the config has a top-level `dataset:` key and the dataset exists.
    """
    if not dataset_exists or not any(line.startswith("dataset:") for line in config_yaml.splitlines()):
        return ()
    return (D("SST-VAL716", subject=key, artifact=key),)


def eval_publication_diagnostics(
    artifact: RenderedArtifact, observation: CompositeObservation
) -> tuple[Diagnostic, ...]:
    """Report what an eval would publish that its run config or dataset version must never carry.

    Reads the run config, the minted version's name from the artifact's components and its
    METADATA from the artifact, and whether the dataset exists from what plan observed.

    Diagnostics:
        SST-VAL716: as `run_config_diagnostics` reports it.
        SST-VAL715: as `dataset_metadata_diagnostics` reports it.
    """
    dataset_exists = any(item.exists for item in observation.resources if item.object_type.upper() == "DATASET")
    found = list(run_config_diagnostics(artifact.key, artifact.ddl, dataset_exists=dataset_exists))
    components = dict(artifact.component_fingerprints)
    version = components.get("dataset_version")
    metadata = artifact.version_metadata
    if version is not None and metadata is not None:
        comment = dataset_version_comment(version)
        found.extend(dataset_metadata_diagnostics(artifact.key, metadata=metadata, comment=comment))
    return tuple(found)


def _unknown_metadata(metadata: str) -> str | None:
    """Say what in METADATA SST never writes; None when it is an object of SST's keys only."""
    try:
        document = json.loads(metadata)
    except ValueError:
        return "text that is not a JSON object"
    if not isinstance(document, dict):
        return "text that is not a JSON object"
    unknown = sorted(set(document) - DATASET_METADATA_KEYS)
    return f"a field SST never writes ('{unknown[0]}')" if unknown else None
