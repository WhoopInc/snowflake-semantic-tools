"""Assign each loaded semantic-model file to the registered types whose root keys it holds.

Ownership is decided after load, by content: a file's location is only discovery's hint. A
file that holds keys and none of them a registered type's is reported, so it is never ignored
without a word; an empty file is the load phase's to report.
"""

from __future__ import annotations

from collections.abc import Mapping

from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.yaml.discover import discover_yaml
from snowflake_semantic_tools.adapters.yaml.documents import RawDocuments, load_documents
from snowflake_semantic_tools.adapters.yaml.parse import parse_yaml_bytes
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.config_schema import configured_dir
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY, Registry


def owner_root_keys(registry: Registry = SEMANTIC_REGISTRY) -> dict[str, str]:
    """Map each root key a semantic-model file may hold to the type that owns it."""
    owners = {member.root_key: name for name, member in registry.members.items()}
    owners.update({artifact.root_key: name for name, artifact in registry.artifacts.items() if artifact.root_key})
    return owners


def assign_owners(
    documents: RawDocuments, registry: Registry = SEMANTIC_REGISTRY
) -> tuple[DiagnosticBag, DiagnosticBag]:
    """Return the problems with each document's ownership, and the assignment of every owned one.

    Returns:
        The SST-DIS008 for each document that holds keys and no registered root key; then one
        SST-DIS200 per owned document, naming its owners in root-key order. Both in document order.
    """
    owners = owner_root_keys(registry)
    problems: list[Diagnostic] = []
    assigned: list[Diagnostic] = []
    for document in documents.documents:
        types = list(dict.fromkeys(owners[key] for key in document.root_keys if key in owners))
        origin = Origin(document.path)
        if types:
            assigned.append(D("SST-DIS200", origin=origin, path=document.path, type=", ".join(types)))
        elif document.root_keys:
            problems.append(D("SST-DIS008", origin=origin, path=document.path))
    return DiagnosticBag(problems), DiagnosticBag(assigned)


def ownership_report(files: ProjectPaths, config: Mapping[str, object]) -> DiagnosticBag:
    """Discover and load the project's semantic-model files again, and say which type owns each.

    Empty for a project with no configuration file or no semantic-models directory. What
    `sst validate --show-info` adds.

    Args:
        config: The run's resolved configuration.
    """
    if files.config_file is None:
        return DiagnosticBag()
    project_dir = files.project_dir
    semantic_models_dir = configured_dir(config, "semantic_models_dir", "semantic_models")
    if not (project_dir / semantic_models_dir).is_dir():
        return DiagnosticBag()
    documents = load_documents(discover_yaml(project_dir, semantic_models_dir, config=config), parse_yaml_bytes)
    return assign_owners(documents)[1]
