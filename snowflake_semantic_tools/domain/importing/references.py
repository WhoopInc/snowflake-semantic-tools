"""What an import maps live names back to: the project's artifacts and dbt models on one target.

A live object names others by their physical names: an agent's tool names
`DB.SCHEMA.SALES`, a semantic view's table `DB.MARTS.ORDERS`. The project names them by what
publishes them: `{{ semantic_view('sales') }}`, `{{ ref('orders') }}`. `ProjectIndex` holds
both directions for the target the import reads, as a compile of the project and its dbt
manifest give them, so each live name is looked up once, casefolded as Snowflake resolves it.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from snowflake_semantic_tools.domain.model.dbt import DbtModel
from snowflake_semantic_tools.domain.model.identifier import QualifiedName

Folded = tuple[str, str, str]


@dataclass(frozen=True, slots=True)
class Placed:
    """One compiled artifact of the project: where it publishes, and the folder its file is in.

    Attributes:
        artifact_type: Its registry type.
        name: Its artifact name.
        target: The object it publishes on the target.
        folder: Its file's folder below the type's directory, one segment per directory.
        source: Its file, relative to the project.
    """

    artifact_type: str
    name: str
    target: QualifiedName
    folder: tuple[str, ...]
    source: str = ""


@dataclass(frozen=True, slots=True)
class ReferencedTool:
    """A tool member that references an object someone else owns, with its relation on the target."""

    name: str
    type: str
    relation: QualifiedName


@dataclass(frozen=True, slots=True)
class ProjectIndex:
    """The project's names on one target, by the physical name each publishes or references.

    Attributes:
        target_name: The target the names are for.
        placed: Every compiled artifact.
        models: Each dbt model's name, by its relation.
        tools: Each referencing tool member, by its relation.
        skill_names: Every skill the project publishes, casefolded, plugin members included.
        dbt_models: Each dbt model, seed and snapshot of the project, by name.
        sources: Each dbt source table, as `(source, table)`, by its relation.
        members: The names of the semantic members the project declares, casefolded, by kind:
            `metric`, `relationship`, `filter`, `verified_query`, `custom_instruction`.
        view_models: The dbt models each compiled view's tables are, by the view's name.
        instruction_sets: Each compiled view's rendered `(AI_SQL_GENERATION,
            AI_QUESTION_CATEGORIZATION)` text, and the custom instruction blocks it lists for them.
    """

    target_name: str
    placed: tuple[Placed, ...] = ()
    models: Mapping[Folded, str] = field(default_factory=lambda: MappingProxyType({}))
    tools: Mapping[Folded, ReferencedTool] = field(default_factory=lambda: MappingProxyType({}))
    skill_names: frozenset[str] = frozenset()
    dbt_models: Mapping[str, DbtModel] = field(default_factory=lambda: MappingProxyType({}))
    sources: Mapping[Folded, tuple[str, str]] = field(default_factory=lambda: MappingProxyType({}))
    members: Mapping[str, frozenset[str]] = field(default_factory=lambda: MappingProxyType({}))
    view_models: Mapping[str, frozenset[str]] = field(default_factory=lambda: MappingProxyType({}))
    instruction_sets: tuple[tuple[str, str, tuple[str, ...]], ...] = ()

    def instructions_for(self, generation: str, categorization: str) -> tuple[str, ...] | None:
        """Return the blocks a compiled view renders exactly these two texts from; None when none does."""
        wanted = (_spaced(generation), _spaced(categorization))
        return next(
            (names for text, other, names in self.instruction_sets if (_spaced(text), _spaced(other)) == wanted),
            None,
        )

    def declares(self, kind: str, name: str) -> bool:
        """Report whether the project declares a semantic member of `kind` called `name`, casefolded."""
        return name.casefold() in self.members.get(kind, frozenset())

    def artifact(self, artifact_type: str, name: QualifiedName) -> Placed | None:
        """Return the compiled artifact of `artifact_type` that publishes `name`; None when none does."""
        return next(
            (item for item in self.placed if item.artifact_type == artifact_type and item.target.folded == name.folded),
            None,
        )

    def named(self, artifact_type: str, name: str) -> Placed | None:
        """Return the compiled artifact of `artifact_type` called `name`, casefolded; None when none is."""
        return next(
            (
                item
                for item in self.placed
                if item.artifact_type == artifact_type and item.name.casefold() == name.casefold()
            ),
            None,
        )

    def tool(self, name: QualifiedName) -> str | None:
        """Return the tool member that publishes or references `name`; None when none does."""
        defined = self.artifact("tool", name)
        if defined is not None:
            return defined.name
        referenced = self.tools.get(name.folded)
        return referenced.name if referenced is not None else None

    def model(self, name: QualifiedName) -> str | None:
        """Return the dbt model whose relation is `name`; None when no model's is."""
        return self.models.get(name.folded)

    def folders(self, artifact_type: str, database: str, schema: str) -> tuple[tuple[str, ...], ...]:
        """Return the folders that hold artifacts of the type publishing in `database.schema`, most used first."""
        counts: dict[tuple[str, ...], int] = {}
        for item in self.placed:
            if item.artifact_type == artifact_type and item.target.folded[:2] == (database, schema):
                counts[item.folder] = counts.get(item.folder, 0) + 1
        return tuple(sorted(counts, key=lambda folder: (-counts[folder], len(folder), folder)))


def _spaced(text: str) -> str:
    return " ".join(text.split())
