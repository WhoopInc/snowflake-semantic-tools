"""What `sst import` produces for one live object, before anything is written.

Import reads a live object and maps it, here and in its sibling modules, to the files that
would publish it: YAML documents the adapter emits, text and bytes written as they are, and the
dbt model columns a semantic view's dimensions and facts belong to. Nothing in this package reads
Snowflake or the filesystem; the use case in `app.importing` hands it what was read.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from snowflake_semantic_tools.domain.diagnostics import Diagnostic
from snowflake_semantic_tools.domain.model.identifier import QualifiedName

# The types `sst import` reads, in the order a run over several imports them: what an agent
# names comes before the agent, so a reference resolves to the file imported for it.
IMPORTABLE_TYPES: tuple[str, ...] = (
    "semantic_view",
    "tool",
    "skill",
    "plugin",
    "agent",
    "cowork_profile",
    "eval",
)

# The Snowflake object each importable type lists under, as SHOW spells it.
LISTED_AS: Mapping[str, str] = MappingProxyType(
    {
        "semantic_view": "SEMANTIC VIEW",
        "agent": "AGENT",
        "cowork_profile": "AGENT",
        "skill": "CORTEX EXTENSION",
        "plugin": "CORTEX EXTENSION",
        "tool": "",
        "eval": "TABLE",
    }
)


@dataclass(frozen=True, slots=True)
class ImportedFile:
    """One file an import writes, project-relative, holding exactly one of its three forms.

    Attributes:
        path: Where it goes, with `/` separators, relative to the project.
        document: A YAML document, which the adapter emits in `sst format`'s canonical form.
        text: Text written as it is, such as an instruction file.
        data: Bytes copied as they are, such as a skill bundle's file.
    """

    path: str
    document: Mapping[str, object] | None = None
    text: str | None = None
    data: bytes | None = None


@dataclass(frozen=True, slots=True)
class ColumnMeta:
    """What an import sets on one dbt model column, where the project sets none.

    Attributes:
        column: The column's name, as the model's YAML spells it.
        meta: Each `config.meta.sst` key with its live value, in the order enrich writes them.
        description: The column's dbt `description`; None to leave it alone.
    """

    column: str
    meta: tuple[tuple[str, object], ...] = ()
    description: str | None = None


@dataclass(frozen=True, slots=True)
class ModelEdit:
    """What an import sets on one dbt model, in its YAML file, where the project sets none.

    Attributes:
        model: The dbt model's name.
        patch_file: The model's YAML file, relative to the project; a new file when it has none.
        section: The top-level key the model is under: `models`, `snapshots` or `seeds`.
        meta: Each model-level `config.meta.sst` key with its live value.
        description: The model's dbt `description`; None to leave it alone.
        columns: Each column it sets something on.
    """

    model: str
    patch_file: str
    section: str = "models"
    meta: tuple[tuple[str, object], ...] = ()
    description: str | None = None
    columns: tuple[ColumnMeta, ...] = ()


@dataclass(frozen=True, slots=True)
class Dependency:
    """An object an imported one names that the project does not compile, which can be imported too.

    Attributes:
        artifact_type: The type to import it as.
        name: The live object; a stage path for a skill read from a stage.
    """

    artifact_type: str
    name: str


@dataclass(frozen=True, slots=True)
class ImportedObject:
    """One live object as the project files that would publish it, and what mapping it reported.

    Attributes:
        artifact_type: The SST type it is imported as.
        live_name: The object read.
        artifact_name: The name its files give it.
        files: Each file to write; empty when the object is not imported.
        model_edits: What a semantic view's tables, dimensions and facts set on dbt models.
        dependencies: What it names that the project does not compile.
        diagnostics: What mapping it reported; an error means it is not imported.
        read_from: What was read, when it is not `live_name` itself, such as a stage path.
    """

    artifact_type: str
    live_name: QualifiedName
    artifact_name: str
    files: tuple[ImportedFile, ...] = ()
    model_edits: tuple[ModelEdit, ...] = ()
    dependencies: tuple[Dependency, ...] = ()
    diagnostics: tuple[Diagnostic, ...] = ()
    read_from: str = ""

    @property
    def shown(self) -> str:
        """What was read, as a person names it: the stage path a skill was read from, else the object."""
        return self.read_from or self.live_name.sql

    @property
    def artifact_key(self) -> str:
        """The `<type>:<name>` key the imported files compile to."""
        return f"{self.artifact_type}:{self.artifact_name}"

    @property
    def refused(self) -> bool:
        """Whether an error stops the object from being imported."""
        return any(item.blocks for item in self.diagnostics)


def artifact_name(name: QualifiedName) -> str:
    """Return the name an imported object's files give it: casefolded when unquoted, else as created."""
    return name.name.value if name.name.quoted else name.name.value.casefold()
