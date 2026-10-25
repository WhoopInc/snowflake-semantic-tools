"""`sst import`: read live objects and map each to the project files that would publish it.

`ImportObjects.run` lists the objects a request names (one, a schema's, or a database's),
reads each with the reads its type needs, maps it through `domain.importing`, and checks
that no file it would write exists. It writes nothing: the command writes what the report
holds, under `--write`. One object that cannot be read or mapped is reported and the others
still import. With `with_dependencies`, what an imported object names that the project does
not compile is imported in the same run, before the objects that name it.
"""

from __future__ import annotations

import json
import posixpath
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from types import MappingProxyType

from snowflake_semantic_tools.app.importing_index import (
    OWN_FOLDER_TYPES,
    artifact_directories,
    folder_below,
    project_index,
)
from snowflake_semantic_tools.app.importing_text import (
    argument_types,
    parse_manifest,
    parse_profile,
    parse_secure,
    parse_signature,
)
from snowflake_semantic_tools.app.plan_diff_composites import served_extension
from snowflake_semantic_tools.app.prior import served_version
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.importing.agent import (
    AgentPlacement,
    LiveAgent,
    import_agent,
    import_cowork_profile,
    is_cowork_profile,
)
from snowflake_semantic_tools.domain.importing.evals import import_eval
from snowflake_semantic_tools.domain.importing.model import (
    LISTED_AS,
    Dependency,
    ImportedFile,
    ImportedObject,
    ModelEdit,
    artifact_name,
)
from snowflake_semantic_tools.domain.importing.placement import Destination, destination, inherited, placement
from snowflake_semantic_tools.domain.importing.references import ProjectIndex
from snowflake_semantic_tools.domain.importing.semantic_view import ViewPlacement, import_semantic_view
from snowflake_semantic_tools.domain.importing.skill import BundlePlacement, LiveBundle, import_plugin, import_skill
from snowflake_semantic_tools.domain.importing.tool import TOOL_TYPES, LiveTool, ToolPlacement, import_tool
from snowflake_semantic_tools.domain.model.config_schema import configured_dir
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, TargetIdentity
from snowflake_semantic_tools.domain.model.lifecycle import extract_marker
from snowflake_semantic_tools.domain.model.lifecycle.marker import unmarked_comment
from snowflake_semantic_tools.domain.model.registry import SEMANTIC_REGISTRY
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.importing import ImportReadPort
from snowflake_semantic_tools.domain.render.skill_bundle import PLUGIN_MANIFEST

# The types `sst import` reads today, by the reader each is mapped with.
SUPPORTED_TYPES: tuple[str, ...] = ("semantic_view", "tool", "skill", "plugin", "agent", "cowork_profile", "eval")


@dataclass(frozen=True, slots=True)
class ImportScope:
    """What a request names: a database, a schema, or one object.

    Attributes:
        database: The database.
        schema: The schema; None for every schema of the database.
        name: The object; None for every object of the type in the scope.
    """

    database: Identifier
    schema: Identifier | None = None
    name: Identifier | None = None

    @classmethod
    def parse(cls, raw: str) -> ImportScope:
        """Parse `DB`, `DB.SCHEMA` or `DB.SCHEMA.NAME`, splitting only at dots outside double quotes.

        Raises:
            ValueError: the text is not one, two or three valid identifiers.
        """
        parts = _parts(raw)
        if not 1 <= len(parts) <= 3:
            raise ValueError(f"expected DB, DB.SCHEMA or DB.SCHEMA.NAME, found {raw!r}")
        identifiers = tuple(Identifier.parse(part) for part in parts)
        return cls(*identifiers)

    @property
    def qualified(self) -> QualifiedName | None:
        """The one object named; None for a schema or database."""
        if self.schema is None or self.name is None:
            return None
        return QualifiedName(self.database, self.schema, self.name)

    @property
    def sql(self) -> str:
        """The scope as SQL: one, two or three parts."""
        return ".".join(part.sql for part in (self.database, self.schema, self.name) if part is not None)


@dataclass(frozen=True, slots=True)
class ImportRequest:
    """One `sst import`: a type and a scope, narrowed by patterns.

    Attributes:
        like: Keep only objects whose name matches this LIKE pattern; None keeps every one.
        exclude: Leave out objects whose name matches this LIKE pattern.
        version: The agent version to read, such as `VERSION$3`; None for the one it serves.
        with_dependencies: Also import what imported objects name and the project does not compile.
        to_folder: The folder below the type's directory every object goes in, as `--to` names
            it; None for the folder each one's routes place it in.
        only: Keep only the objects of these names, as SHOW prints them; empty keeps every one.
        define: A tool member defines its object, which SST then publishes; else it references it.
        stage_path: A stage folder to read one skill from, `@DB.SCHEMA.STAGE/path/`, instead of the scope.
    """

    artifact_type: str
    scope: ImportScope
    like: str | None = None
    exclude: str | None = None
    version: str | None = None
    with_dependencies: bool = False
    to_folder: tuple[str, ...] | None = None
    only: tuple[str, ...] = ()
    define: bool = False
    stage_path: str | None = None


@dataclass(frozen=True, slots=True)
class ImportSetup:
    """What the run maps against: the project's names, its configuration, and the target.

    Attributes:
        index: The project's compiled names on the target.
        config: `sst_config.yml` as rendered for the target.
        target: The target imported from.
        role: The session's role, which every access failure names.
        exists: Whether a project-relative path is already in the project.
    """

    index: ProjectIndex
    config: Mapping[str, object]
    target: TargetIdentity
    role: str
    exists: Callable[[str], bool]


@dataclass(frozen=True, slots=True)
class ImportReport:
    """What one run read and would write.

    Attributes:
        objects: Each object read, dependencies first, in the order they were listed.
        diagnostics: What the run reported about its scope rather than one object.
        skipped: Objects the scope listed that are of the other agent kind, which a run over a
            scope passes over.
    """

    objects: tuple[ImportedObject, ...] = ()
    diagnostics: tuple[Diagnostic, ...] = ()
    skipped: tuple[QualifiedName, ...] = ()

    @property
    def imported(self) -> tuple[ImportedObject, ...]:
        """The objects whose files are written: none of their diagnostics is an error."""
        return tuple(item for item in self.objects if not item.refused)

    @property
    def refused(self) -> tuple[ImportedObject, ...]:
        """The objects an error stops from being imported."""
        return tuple(item for item in self.objects if item.refused)

    @property
    def files(self) -> tuple[ImportedFile, ...]:
        """Every file the imported objects write, in order."""
        return tuple(file for item in self.imported for file in item.files)

    @property
    def model_edits(self) -> tuple[ModelEdit, ...]:
        """Every dbt model edit the imported objects make, in order."""
        return tuple(edit for item in self.imported for edit in item.model_edits)

    @property
    def pending(self) -> bool:
        """Whether the run would write a file or edit a model."""
        return bool(self.files or self.model_edits)

    @property
    def has_errors(self) -> bool:
        """Whether an object was refused, or the run's scope could not be read."""
        return bool(self.refused) or any(item.blocks for item in self.diagnostics)


_Reader = Callable[["ImportObjects", QualifiedName, bool, str | None], ImportedObject | None]


class ImportObjects:
    """Read and map the objects one request names; write nothing."""

    def __init__(self, port: ImportReadPort, setup: ImportSetup) -> None:
        self._port = port
        self._setup = setup
        self._to_folder: tuple[str, ...] | None = None
        self._define = False
        # While a dependency is read: the folder of the object that names it.
        self._prefer: tuple[str, ...] | None = None

    def run(self, request: ImportRequest) -> ImportReport:
        """List the scope, read and map each object, then check the files each would write.

        Diagnostics:
            SST-IMP001: the object named, or the scope, is not found or not visible.
            SST-IMP006: the scope lists no object of the type, once the patterns apply.
            And each code the type's reader and mapper report, on its object.
        """
        self._to_folder = request.to_folder
        self._define = request.define
        if request.stage_path is not None:
            objects_read = [self._read_stage_skill(request.stage_path)]
            if request.with_dependencies:
                objects_read = self._with_dependencies(objects_read)
            return ImportReport(tuple(self._checked(objects_read)))
        listed, diagnostics = self._listed(request)
        single = request.scope.qualified is not None
        objects: list[ImportedObject] = []
        skipped: list[QualifiedName] = []
        for name in listed:
            imported = self._read(request.artifact_type, name, single=single, version=request.version)
            if imported is None:
                skipped.append(name)
            else:
                objects.append(imported)
        if request.with_dependencies:
            objects = self._with_dependencies(objects)
        return ImportReport(tuple(self._checked(objects)), tuple(diagnostics), tuple(skipped))

    def _listed(self, request: ImportRequest) -> tuple[tuple[QualifiedName, ...], list[Diagnostic]]:
        """Return the objects the request names, listing its scope, and what listing it reported."""
        scope = request.scope
        listed_as = LISTED_AS[request.artifact_type]
        named = scope.qualified
        if named is not None:
            return (named,), []
        try:
            kinds = tuple(TOOL_TYPES) if request.artifact_type == "tool" else (listed_as,)
            rows = tuple(
                row for kind in kinds for row in self._port.show_objects_in(kind, scope.database, scope.schema)
            )
        except SnowflakePortError:
            return (), [D("SST-IMP001", subject="import", type=_kind(scope), value=scope.sql, name=self._setup.role)]
        only = {name.casefold() for name in request.only}
        names = tuple(
            row.qualified_name
            for row in rows
            if _matches(request.like, row.name, default=True)
            and not _matches(request.exclude, row.name, default=False)
            and (not only or row.name.casefold() in only)
        )
        if not names:
            patterns = "".join(
                f" {flag} '{pattern}'"
                for flag, pattern in (("like", request.like), ("not like", request.exclude))
                if pattern
            )
            detail = f" whose name is{patterns}" if patterns else ""
            return (), [D("SST-IMP006", subject="import", type=request.artifact_type, value=scope.sql, detail=detail)]
        return names, []

    def _read(
        self, artifact_type: str, name: QualifiedName, *, single: bool, version: str | None
    ) -> ImportedObject | None:
        reader = _READERS[artifact_type]
        return reader(self, name, single, version)

    def _with_dependencies(self, objects: list[ImportedObject]) -> list[ImportedObject]:
        """Import what the objects name and the project does not compile, before the objects that name it.

        Each goes in the folder of the object that first names it, when that folder places it
        where its own routes would.
        """
        done = {name.sql for name in (item.live_name for item in objects)}
        added: list[ImportedObject] = []
        imported: set[str] = set()
        pending = [(dependency, item) for item in objects for dependency in item.dependencies]
        while pending:
            dependency, dependent = pending.pop(0)
            if dependency.name in done or dependency.artifact_type not in _READERS:
                continue
            done.add(dependency.name)
            self._prefer = self._folder_of(dependent)
            try:
                read = self._dependency(dependency)
            finally:
                self._prefer = None
            if read is None:
                continue
            added.append(read)
            if not read.refused:
                imported.add(dependency.name)
            pending.extend((item, read) for item in read.dependencies)
        return [*reversed(added), *(_resolved(item, frozenset(imported)) for item in objects)]

    def _dependency(self, dependency: Dependency) -> ImportedObject | None:
        if dependency.artifact_type == "skill" and dependency.name.startswith("@"):
            return self._read_stage_skill(dependency.name)
        name = _qualified(dependency.name)
        if name is None:
            return None
        return self._read(dependency.artifact_type, name, single=True, version=None)

    def _folder_of(self, item: ImportedObject) -> tuple[str, ...] | None:
        """Return the folder an imported object's first file is in, below its type's directory."""
        if not item.files:
            return None
        artifact = SEMANTIC_REGISTRY.artifacts[item.artifact_type]
        root = configured_dir(self._setup.config, artifact.dir_key, artifact.dir_key.removesuffix("_dir"))
        own = item.artifact_type in OWN_FOLDER_TYPES
        return folder_below(item.files[0].path, root, item.artifact_name, own=own)

    def _checked(self, objects: list[ImportedObject]) -> list[ImportedObject]:
        """Refuse each object a file of which exists; share a file two objects write alike.

        Diagnostics:
            SST-IMP004: a file is already in the project, or another object writes it otherwise.
        """
        written: dict[str, ImportedFile] = {}
        checked: list[ImportedObject] = []
        for item in objects:
            if item.refused:
                checked.append(item)
                continue
            files: list[ImportedFile] = []
            problems: list[Diagnostic] = []
            for file in item.files:
                earlier = written.get(file.path)
                if earlier == file:
                    continue
                if earlier is not None or self._setup.exists(file.path):
                    problems.append(
                        D("SST-IMP004", subject=item.artifact_key, path=file.path, value=item.live_name.sql)
                    )
                files.append(file)
            if not problems:
                written.update((file.path, file) for file in files)
            checked.append(replace(item, files=tuple(files), diagnostics=(*item.diagnostics, *problems)))
        return checked

    def _destination(self, artifact_type: str, name: QualifiedName) -> tuple[Destination, dict[str, object], str]:
        """Return the folder an object goes in, what it inherits there, and its type's directory."""
        artifact = SEMANTIC_REGISTRY.artifacts[artifact_type]
        config = self._setup.config
        block = config.get(artifact.routed_block)
        if self._to_folder is not None:
            located = placement(block, self._to_folder, self._setup.target) == name.folded[:2]
            place = Destination(self._to_folder, overridden=not located)
        else:
            used = self._setup.index.folders(artifact_type, *name.folded[:2])
            place = destination(block, self._setup.target, name, used)
            preferred = self._prefer
            if preferred is not None and placement(block, preferred, self._setup.target) == placement(
                block, place.folder, self._setup.target
            ):
                place = Destination(preferred, place.overridden)
        root = configured_dir(config, artifact.dir_key, artifact.dir_key.removesuffix("_dir"))
        return place, self._inherited(block, place.folder), root

    def _placement(self, artifact_type: str, name: QualifiedName) -> AgentPlacement:
        place, settings, root = self._destination(artifact_type, name)
        tools_dir = SEMANTIC_REGISTRY.artifacts["tool"].dir_key
        return AgentPlacement(
            root=root,
            tools_root=configured_dir(self._setup.config, tools_dir, tools_dir.removesuffix("_dir")),
            destination=place,
            inherited=MappingProxyType(settings),
        )

    def _inherited(self, block: object, folder: tuple[str, ...]) -> dict[str, object]:
        """Return what an artifact in `folder` inherits; tools run on the target's warehouse unless set."""
        settings = dict(inherited(block, folder, self._setup.target))
        if not settings.get("warehouse") and self._setup.target.warehouse:
            settings["warehouse"] = self._setup.target.warehouse
        return settings

    def _published(self, artifact_type: str, name: QualifiedName) -> Diagnostic | None:
        """Report an object the project already compiles an artifact to; `sst state adopt` takes it over.

        Diagnostics:
            SST-IMP007: the project compiles an artifact to the object.
        """
        placed = self._setup.index.artifact(artifact_type, name)
        if placed is None:
            return None
        key = f"{artifact_type}:{placed.name}"
        return D("SST-IMP007", subject=key, artifact=key, value=name.sql)

    def _read_view(self, name: QualifiedName) -> ImportedObject:
        """Read a semantic view's YAML, and map it.

        Diagnostics:
            SST-IMP001: the view is not found or not visible.
            SST-IMP003: it carries SST's ownership marker.
            SST-IMP007: the project already compiles an artifact to it.
            SST-IMP005: a read of it is refused, or Snowflake returns no definition.
            And each code `import_semantic_view` lists.
        """
        key = f"semantic_view:{artifact_name(name)}"
        step = "SHOW SEMANTIC VIEWS"
        try:
            row = self._port.show_row("SEMANTIC VIEW", name)
            if row is None:
                found = D("SST-IMP001", subject=key, type="semantic view", value=name.sql, name=self._setup.role)
                return self._failed("semantic_view", name, found)
            if extract_marker(row.get("comment")) is not None:
                return self._failed("semantic_view", name, D("SST-IMP003", subject=key, value=name.sql))
            published = self._published("semantic_view", name)
            if published is not None:
                return self._failed("semantic_view", name, published)
            step = "SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW"
            document = self._port.read_semantic_view_yaml(name)
        except SnowflakePortError as exc:
            return self._failed("semantic_view", name, self._refused(key, name, step, str(exc), "semantic view"))
        if document is None:
            detail = "Snowflake returned no definition: the function is unavailable, or the role cannot see the view"
            return self._failed("semantic_view", name, self._refused(key, name, step, detail, "semantic view"))
        place, settings, root = self._destination("semantic_view", name)
        imported = import_semantic_view(name, document, self._setup.index, ViewPlacement(root, place, settings))
        return self._located(imported, place, name)

    def _read_tool(self, name: QualifiedName) -> ImportedObject:
        """Read a search service, procedure or function, and map it to a tool member.

        Diagnostics:
            SST-IMP001: no search service, procedure or function has the name, or the role cannot see it.
            SST-IMP003: it carries SST's ownership marker.
            SST-IMP007: the project already compiles a tool to it, or references it.
            SST-IMP005: a read of it is refused.
            And each code `import_tool` lists.
        """
        key = f"tool:{artifact_name(name)}"
        step = "SHOW"
        try:
            found = next(
                ((kind, row) for kind in TOOL_TYPES if (row := self._port.show_row(kind, name)) is not None), None
            )
            if found is None:
                missing = D("SST-IMP001", subject=key, type="tool", value=name.sql, name=self._setup.role)
                return self._failed("tool", name, missing)
            kind, row = found
            comment = row.get("comment", row.get("description"))
            if extract_marker(comment) is not None:
                return self._failed("tool", name, D("SST-IMP003", subject=key, value=name.sql))
            published = self._published("tool", name)
            referenced = self._setup.index.tool(name)
            if published is None and referenced is not None:
                member = f"tool:{referenced.casefold()}"
                published = D("SST-IMP007", subject=member, artifact=member, value=name.sql)
            if published is not None:
                return self._failed("tool", name, published)
            step = f"the {kind.lower()}'s DESCRIBE"
            types = argument_types(row.get("arguments", ""))
            if kind == "CORTEX SEARCH SERVICE":
                described = self._port.describe_properties(kind, name) or {}
            else:
                described = self._port.describe_routine(kind, name, types)
        except SnowflakePortError as exc:
            return self._failed("tool", name, self._refused(key, name, step, str(exc), "tool"))
        live = LiveTool(
            name,
            TOOL_TYPES[kind],
            unmarked_comment(comment) or None,
            MappingProxyType(dict(described)),
            parse_signature(described.get("signature", ""), types),
        )
        place, settings, root = self._destination("tool", name)
        imported = import_tool(live, self._setup.index, ToolPlacement(root, place, settings), define=self._define)
        return self._located(imported, place, name) if self._define else imported

    def _read_extension(self, name: QualifiedName, single: bool, kind: str) -> ImportedObject | None:
        """Read an extension's served version, and map it; None for the other kind in a scope's run.

        Diagnostics:
            SST-IMP001: the extension is not found, not visible, or serves no version.
            SST-IMP002: one named alone is of the other kind.
            SST-IMP007: the project publishes it already.
            SST-IMP005: a read of it is refused.
            And each code `import_skill` or `import_plugin` lists.
        """
        key = f"{kind}:{artifact_name(name)}"
        step = "SHOW CORTEX EXTENSIONS"
        try:
            extension = self._port.observe_extension(name)
            if extension is None:
                missing = D("SST-IMP001", subject=key, type=kind, value=name.sql, name=self._setup.role)
                return self._failed(kind, name, missing)
            found = "plugin" if extension.extension_type == "PLUGIN" else "skill"
            if found != kind:
                if not single:
                    return None
                return self._failed(
                    kind, name, D("SST-IMP002", subject=key, value=name.sql, found=found, expected=kind)
                )
            published = self._published(kind, name)
            if published is not None:
                return self._failed(kind, name, published)
            step = "SHOW VERSIONS IN CORTEX EXTENSION"
            version = served_extension(extension, self._port.extension_versions(name))
            if version is None:
                missing = D("SST-IMP001", subject=key, type=f"{kind} version", value=name.sql, name=self._setup.role)
                return self._failed(kind, name, missing)
            step = "GET"
            location = version.location if version.location.endswith("/") else f"{version.location}/"
            files = self._files(location)
        except SnowflakePortError as exc:
            return self._failed(kind, name, self._refused(key, name, step, str(exc), kind))
        manifest = parse_manifest(files.get(PLUGIN_MANIFEST)) if kind == "plugin" else None
        bundle = LiveBundle(name, files, unmarked_comment(extension.comment) or None, manifest, name.sql)
        return self._bundle(kind, bundle)

    def _read_stage_skill(self, path: str) -> ImportedObject:
        """Read one skill folder from a stage path, as a STAGE skill source names it, and map it.

        Diagnostics:
            SST-IMP001: the path names no stage, or holds no file.
            SST-IMP005: a read of it is refused.
            And each code `import_skill` lists.
        """
        location = path if path.endswith("/") else f"{path}/"
        stage = _qualified(location.removeprefix("@").split("/", 1)[0]) or QualifiedName.parse(
            "UNKNOWN.UNKNOWN.UNKNOWN"
        )
        name = location.rstrip("/").rsplit("/", 1)[-1].casefold()
        try:
            files = self._files(location)
        except SnowflakePortError as exc:
            refused = self._failed("skill", stage, self._refused(f"skill:{name}", stage, "LIST", str(exc), "stage"))
            return replace(refused, artifact_name=name, read_from=location)
        if not files:
            missing = D("SST-IMP001", subject=f"skill:{name}", type="stage path", value=path, name=self._setup.role)
            return replace(self._failed("skill", stage, missing), artifact_name=name, read_from=location)
        return replace(self._bundle("skill", LiveBundle(stage, files, None, None, location)), read_from=location)

    def _files(self, location: str) -> dict[str, bytes]:
        """Read every file below a stage folder or extension version, by its path below it.

        Raises:
            SnowflakePortError: a listing or a read was refused, or a listed file could not be read.
        """
        files: dict[str, bytes] = {}
        for path in self._port.list_location(location):
            content = self._port.read_staged_file(f"{location}{path}")
            if content is None:
                raise SnowflakePortError(f"{location}{path} could not be read")
            files[path] = content
        return files

    def _bundle(self, kind: str, bundle: LiveBundle) -> ImportedObject:
        place, _, root = self._destination(kind, bundle.name)
        skills_dir = SEMANTIC_REGISTRY.artifacts["skill"].dir_key
        skills_root = configured_dir(self._setup.config, skills_dir, skills_dir.removesuffix("_dir"))
        placed = BundlePlacement(root, place, skills_root)
        mapper = import_plugin if kind == "plugin" else import_skill
        return mapper(bundle, self._setup.index, placed)

    def _read_eval(self, name: QualifiedName) -> ImportedObject:
        """Read an eval input table's rows, and map them to a dataset in the `--to` agent's folder.

        Diagnostics:
            SST-IMP001: no table has the name, or the role cannot see it.
            SST-IMP005: the read is refused, or the table lacks INPUT_QUERY and GROUND_TRUTH.
            And each code `import_eval` lists.
        """
        key = f"eval:{artifact_name(name)}"
        try:
            rows = self._port.eval_rows(name)
        except SnowflakePortError as exc:
            return self._failed("eval", name, self._refused(key, name, "SELECT", str(exc), "eval table"))
        if rows is None:
            return self._failed(
                "eval", name, D("SST-IMP001", subject=key, type="table", value=name.sql, name=self._setup.role)
            )
        artifact = SEMANTIC_REGISTRY.artifacts["eval"]
        root = configured_dir(self._setup.config, artifact.dir_key, artifact.dir_key.removesuffix("_dir"))
        folder = posixpath.join(root, *(self._to_folder or ()))
        return import_eval(name, rows, folder)

    def _located(self, imported: ImportedObject, place: Destination, name: QualifiedName) -> ImportedObject:
        """Report the location the file names itself, when no folder routes to the live one.

        Diagnostics:
            SST-IMP014: no folder of the type places the object where it is.
        """
        if not place.overridden or imported.refused:
            return imported
        artifact = SEMANTIC_REGISTRY.artifacts[imported.artifact_type]
        diagnostic = D(
            "SST-IMP014",
            subject=imported.artifact_key,
            artifact=imported.artifact_key,
            block=artifact.routed_block,
            value=f"{name.database.sql}.{name.schema.sql}",
            target=self._setup.target.name,
        )
        return replace(imported, diagnostics=(*imported.diagnostics, diagnostic))

    def _read_agent(
        self, name: QualifiedName, single: bool, version: str | None, *, cowork: bool
    ) -> ImportedObject | None:
        """Read an agent or CoWork profile, and map it; None for one of the other kind in a scope's run.

        Diagnostics:
            SST-IMP001: the agent, or the version asked for, is not found or not visible.
            SST-IMP002: one named alone is of the other kind.
            SST-IMP003: it carries SST's ownership marker.
            SST-IMP005: a read of it is refused, or its specification cannot be read.
        """
        artifact_type = "cowork_profile" if cowork else "agent"
        key = f"{artifact_type}:{artifact_name(name)}"
        step = "SHOW AGENTS"
        try:
            row = self._port.show_row("AGENT", name)
            if row is None:
                return self._failed(
                    artifact_type,
                    name,
                    D("SST-IMP001", subject=key, type="agent", value=name.sql, name=self._setup.role),
                )
            if extract_marker(row.get("comment")) is not None:
                return self._failed(artifact_type, name, D("SST-IMP003", subject=key, value=name.sql))
            published = self._published(artifact_type, name)
            if published is not None:
                return self._failed(artifact_type, name, published)
            step = "SHOW VERSIONS IN AGENT"
            versions = self._port.observe_agent_versions(name)
        except SnowflakePortError as exc:
            return self._failed(artifact_type, name, self._refused(key, name, step, str(exc)))
        served = versions.named(version) if version else served_version(versions.versions, None)
        if served is None:
            value = f"{name.sql} version {version}" if version else name.sql
            return self._failed(
                artifact_type,
                name,
                D("SST-IMP001", subject=key, type="agent version", value=value, name=self._setup.role),
            )
        if served.specification is None:
            detail = "the role may not read the specification of the version it serves"
            return self._failed(artifact_type, name, self._refused(key, name, "agent_spec", detail))
        try:
            spec = json.loads(served.specification)
        except ValueError as exc:
            return self._failed(artifact_type, name, self._refused(key, name, "agent_spec", f"not JSON: {exc}"))
        if not isinstance(spec, dict):
            return self._failed(artifact_type, name, self._refused(key, name, "agent_spec", "not a JSON object"))
        if is_cowork_profile(spec) != cowork:
            if not single:
                return None
            found, expected = ("cowork_profile", "agent") if not cowork else ("agent", "cowork_profile")
            return self._failed(
                artifact_type, name, D("SST-IMP002", subject=key, value=name.sql, found=found, expected=expected)
            )
        live = LiveAgent(
            name,
            unmarked_comment(row.get("comment")) or None,
            parse_profile(row.get("profile")),
            parse_secure(row.get("is_secure")),
            spec,
            served.alias,
        )
        placement = self._placement(artifact_type, name)
        mapper = import_cowork_profile if cowork else import_agent
        return self._located(mapper(live, self._setup.index, placement), placement.destination, name)

    def _refused(self, key: str, name: QualifiedName, step: str, detail: str, kind: str = "agent") -> Diagnostic:
        return D("SST-IMP005", subject=key, name=self._setup.role, type=kind, value=name.sql, step=step, detail=detail)

    @staticmethod
    def _failed(artifact_type: str, name: QualifiedName, diagnostic: Diagnostic) -> ImportedObject:
        return ImportedObject(artifact_type, name, artifact_name(name), diagnostics=(diagnostic,))


_READERS: Mapping[str, _Reader] = MappingProxyType(
    {
        "agent": lambda run, name, single, version: run._read_agent(name, single, version, cowork=False),
        "cowork_profile": lambda run, name, single, version: run._read_agent(name, single, version, cowork=True),
        "semantic_view": lambda run, name, _single, _version: run._read_view(name),
        "tool": lambda run, name, _single, _version: run._read_tool(name),
        "skill": lambda run, name, single, _version: run._read_extension(name, single, "skill"),
        "plugin": lambda run, name, single, _version: run._read_extension(name, single, "plugin"),
        "eval": lambda run, name, _single, _version: run._read_eval(name),
    }
)


def _resolved(item: ImportedObject, imported: frozenset[str]) -> ImportedObject:
    """Drop the dependencies the same run imported, and their SST-IMP012 reports."""
    remaining = tuple(dependency for dependency in item.dependencies if dependency.name not in imported)
    diagnostics = tuple(
        diagnostic
        for diagnostic in item.diagnostics
        if not (diagnostic.code == "SST-IMP012" and diagnostic.context.get("value") in imported)
    )
    return replace(item, dependencies=remaining, diagnostics=diagnostics)


def _matches(pattern: str | None, name: str, *, default: bool) -> bool:
    """Match a name as Snowflake's LIKE does, ignoring case: `%` any run, `_` one character."""
    if pattern is None:
        return default
    regex = "".join(".*" if char == "%" else "." if char == "_" else re.escape(char) for char in pattern)
    return re.fullmatch(regex, name, re.IGNORECASE | re.DOTALL) is not None


def _kind(scope: ImportScope) -> str:
    return "schema" if scope.schema is not None else "database"


def _qualified(value: str) -> QualifiedName | None:
    try:
        return QualifiedName.parse(value)
    except ValueError:
        return None


def _parts(raw: str) -> tuple[str, ...]:
    """Split a dotted name at dots outside double quotes."""
    parts: list[str] = []
    current: list[str] = []
    quoted = False
    for char in raw:
        if char == '"':
            quoted = not quoted
        if char == "." and not quoted:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)
    parts.append("".join(current))
    return tuple(part.strip() for part in parts)


__all__ = [
    "SUPPORTED_TYPES",
    "ImportObjects",
    "ImportReport",
    "ImportRequest",
    "ImportScope",
    "ImportSetup",
    "artifact_directories",
    "project_index",
]
