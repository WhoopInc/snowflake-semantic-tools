"""Orchestrate `sst enrich`: select dbt models, read what the warehouse holds, and edit their YAML.

Every edit is computed in memory before anything is written, so a run that stops early, or
fails reading a file, writes nothing; the caller decides whether to write at all. A model either
gains everything its components derive or, when one of its reads fails, nothing.
"""

from __future__ import annotations

import fnmatch
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

from snowflake_semantic_tools.domain.model.config_schema import EnrichmentConfig, enrichment_config
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog, DbtModel
from snowflake_semantic_tools.domain.model.diagnostic import D, Diagnostic, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.enrich import (
    COLUMN_SYNONYMS_SCHEMA,
    COLUMNS_PER_PROMPT,
    DEFAULT_COMPONENTS,
    TABLE_SYNONYMS_SCHEMA,
    ColumnUpdate,
    Component,
    EnrichOptions,
    ModelEnrichment,
    TableSynonymEdit,
    ViewTable,
    WarehouseColumn,
    avoided_names,
    collection_refusal,
    column_synonyms,
    column_synonyms_prompt,
    enrich_model,
    needs_table_synonyms,
    parse_column_synonyms,
    parse_table_synonyms,
    prompt_columns,
    sample_columns,
    synonym_columns,
    table_synonym_edits,
    table_synonyms_prompt,
    taken_names,
    yaml_column_name,
)
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName
from snowflake_semantic_tools.domain.model.semantic_view import SemanticView
from snowflake_semantic_tools.domain.ports.enrich import EnrichFilesPort, EnrichPort
from snowflake_semantic_tools.domain.ports.project import ProjectInputs
from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePortError

# A failure no other model would escape: the connection, a transient fault, or a privilege.
# It stops the run as a connection failure instead of failing one model.
_RUN_FAILURES = frozenset(("SST-PRT001", "SST-PRT003", "SST-PRT004"))


@dataclass(frozen=True, slots=True)
class EnrichRequest:
    """What one `sst enrich` run selects and fills.

    Attributes:
        paths: Project-relative paths; a model is selected when its SQL or YAML file is one of
            them or lies under one. Empty selects every model of the project.
        selected: Model names or globs; empty selects every model the paths do.
        excluded: Model names or globs taken out of the selection.
        options: The components the run fills and forces.
        database: Read every relation in this database instead of the manifest's.
        schema: Read every relation in this schema instead of the manifest's.
        fail_fast: Stop at the first model that fails, so nothing is written.
    """

    paths: tuple[str, ...] = ()
    selected: tuple[str, ...] = ()
    excluded: tuple[str, ...] = ()
    options: EnrichOptions = EnrichOptions(DEFAULT_COMPONENTS)
    database: Identifier | None = None
    schema: Identifier | None = None
    fail_fast: bool = False


@dataclass(frozen=True, slots=True)
class EditedFile:
    """One file the run edits: its text before and after.

    Attributes:
        before: The text it has; None for a file the run creates.
        reformatted: Writing it also changes lines the run did not edit.
    """

    path: str
    before: str | None
    after: str
    reformatted: bool = False

    @property
    def changed(self) -> bool:
        """Report whether writing the file would change it."""
        return self.before != self.after


@dataclass(frozen=True, slots=True)
class ModelReport:
    """What the run did for one selected model.

    Attributes:
        path: The YAML file its column updates go to.
        enrichment: What its columns gain; None when one of its reads failed.
        table_synonyms: The views whose `table_config` gains its synonyms.
    """

    model: str
    path: str
    enrichment: ModelEnrichment | None
    table_synonyms: tuple[TableSynonymEdit, ...] = ()

    @property
    def failed(self) -> bool:
        """Report whether the model was left unchanged because a read failed."""
        return self.enrichment is None


@dataclass(frozen=True, slots=True)
class EnrichReport:
    """Everything one run decided, before anything is written.

    Attributes:
        models: One report per model enriched, in name order; fewer than selected after a stop.
        files: Every file the run edits, in path order, changed or not.
        diagnostics: What the run found, in the order it found it.
        stopped: `--fail-fast` stopped the run at a failure, so nothing may be written.
        selected: How many models the selection matched.
    """

    models: tuple[ModelReport, ...] = ()
    files: tuple[EditedFile, ...] = ()
    diagnostics: DiagnosticBag = field(default_factory=DiagnosticBag)
    stopped: bool = False
    selected: int = 0

    @property
    def changed(self) -> tuple[EditedFile, ...]:
        """Return the files writing would change, in path order."""
        return tuple(item for item in self.files if item.changed)

    @property
    def writable(self) -> tuple[EditedFile, ...]:
        """Return the files to write: the changed ones, unless the run stopped."""
        return () if self.stopped else self.changed


def _matches(name: str, patterns: Sequence[str]) -> bool:
    folded = name.casefold()
    return any(fnmatch.fnmatchcase(folded, pattern.casefold()) for pattern in patterns)


def _under(file: str | None, paths: Sequence[str]) -> bool:
    if file is None:
        return False
    return any(file == path or file.startswith(path.rstrip("/") + "/") for path in paths)


def select_models(catalog: DbtCatalog, request: EnrichRequest) -> tuple[tuple[DbtModel, ...], list[Diagnostic]]:
    """Return the project's models the request selects, in name order, with what selecting found.

    Only the root project's models are selected; an installed package's are never edited.

    Diagnostics:
        SST-DBT031: a model selected by name has no relation.
    """
    models = [model for model in catalog.models if model.package_name in (None, catalog.project_name)]
    if request.paths:
        paths = [path.strip("/") for path in request.paths]
        models = [
            model for model in models if _under(model.original_file_path, paths) or _under(model.patch_file, paths)
        ]
    if request.selected:
        models = [model for model in models if _matches(model.name, request.selected)]
    if request.excluded:
        models = [model for model in models if not _matches(model.name, request.excluded)]
    diagnostics = [
        D("SST-DBT031", model=name, subject=f"dbt_model:{name}")
        for name in catalog.relationless_models
        if request.selected and _matches(name, request.selected) and not _matches(name, request.excluded)
    ]
    return tuple(sorted(models, key=lambda model: model.name.casefold())), diagnostics


def model_file(model: DbtModel) -> str:
    """Return the YAML file a model's updates go to: its patch file, else a new one beside its SQL."""
    if model.patch_file:
        return model.patch_file
    if model.original_file_path:
        stem, _, _ = model.original_file_path.rpartition(".")
        return f"{stem or model.original_file_path}.yml"
    return f"models/{model.name}.yml"


def relation(model: DbtModel, request: EnrichRequest) -> QualifiedName:
    """Return the relation a model's columns are read from, with the request's overrides applied.

    Raises:
        ValueError: dbt's relation name is not a three-part Snowflake name.
    """
    parsed = QualifiedName.parse(model.raw_relation_name or model.relation_name)
    return QualifiedName(request.database or parsed.database, request.schema or parsed.schema, parsed.name)


def view_tables(views: Sequence[SemanticView], model: str) -> tuple[ViewTable, ...]:
    """Describe each view that uses `model` as its table-synonym edit needs, in view order."""
    wanted = model.casefold()
    targets = []
    for view in views:
        if wanted not in view.referenced_models or view.source_path is None:
            continue
        mine = next((table for table in view.tables if table.logical_name.casefold() == wanted), None)
        others = [table for table in view.tables if table.logical_name.casefold() != wanted]
        taken = taken_names([table.logical_name for table in others], [s for table in others for s in table.synonyms])
        name = QualifiedName.parse(view.fqn).name.value
        targets.append(ViewTable(name, view.source_path, model, mine.synonyms if mine else (), taken))
    return tuple(targets)


class _ModelFailed(Exception):
    """One model's read failed; the diagnostic says which and why."""

    def __init__(self, diagnostic: Diagnostic) -> None:
        super().__init__(diagnostic.message)
        self.diagnostic = diagnostic


class EnrichProject:
    """Run `sst enrich` over one project: read, decide, and edit in memory; `write` writes."""

    def __init__(self, inputs: ProjectInputs, port: EnrichPort, files: EnrichFilesPort) -> None:
        self._inputs = inputs
        self._port = port
        self._files = files

    def run(self, request: EnrichRequest) -> EnrichReport:
        """Enrich every selected model in memory and report what each file would become.

        Raises:
            SnowflakePortError: The connection failed, a fault was transient, or a privilege was
                refused; no model would get further.
            ProjectError: The manifest, the semantic views, or a YAML file cannot be read.

        Diagnostics:
            SST-CFG038: the run reads row data and the project forbids collecting it.
            SST-DBT031: a model selected by name has no relation.
            SST-SNO030: a model's relation is missing or not visible.
            SST-SNO031: reading a model's values or asking Cortex failed.
            SST-PRS125: a file enrich writes is also reformatted.
        """
        settings = enrichment_config(self._inputs.config().tree)
        refusal = collection_refusal(request.options, allowed=settings.allow_sample_value_collection)
        if refusal is not None:
            return EnrichReport(diagnostics=DiagnosticBag((refusal,)), stopped=True)
        models, diagnostics = select_models(self._inputs.dbt_catalog(), request)
        views = self._inputs.load_project().views if request.options.includes(Component.TABLE_SYNONYMS) else ()
        reports: list[ModelReport] = []
        stopped = False
        for model in models:
            report, found = self._model(model, request, settings, views)
            reports.append(report)
            diagnostics.extend(found)
            if report.failed and request.fail_fast:
                stopped = True
                break
        files = self._edited_files(reports)
        diagnostics.extend(
            D("SST-PRS125", file=item.path, origin=Origin(item.path))
            for item in files
            if item.changed and item.reformatted
        )
        return EnrichReport(tuple(reports), files, DiagnosticBag(diagnostics), stopped, len(models))

    def write(self, report: EnrichReport) -> tuple[str, ...]:
        """Write the report's changed files, unless the run stopped; return the paths written.

        Raises:
            OSError: A file cannot be written; the files before it stay written.
        """
        for item in report.writable:
            self._files.write(item.path, item.after)
        return tuple(item.path for item in report.writable)

    def _model(
        self, model: DbtModel, request: EnrichRequest, settings: EnrichmentConfig, views: Sequence[SemanticView]
    ) -> tuple[ModelReport, list[Diagnostic]]:
        """Read one model's relation, values and synonyms, and decide what its YAML gains."""
        path = model_file(model)
        try:
            warehouse = self._columns(model, request)
            samples = self._samples(model, request, warehouse, settings)
            synonyms = self._column_synonyms(model, warehouse, samples, request.options, settings)
            enrichment = enrich_model(model, warehouse, request.options, settings, samples=samples, synonyms=synonyms)
            edits = self._table_synonyms(model, warehouse, view_tables(views, model.name), request.options, settings)
        except _ModelFailed as failure:
            return ModelReport(model.name, path, None), [failure.diagnostic]
        return ModelReport(model.name, path, enrichment, edits), list(enrichment.diagnostics)

    def _failure(self, model: DbtModel, step: str, error: SnowflakePortError) -> _ModelFailed:
        if error.diagnostic is not None and error.diagnostic.code in _RUN_FAILURES:
            raise error
        detail = str(error).strip().splitlines()[0] if str(error).strip() else type(error).__name__
        return _ModelFailed(
            D("SST-SNO031", model=model.name, step=step, detail=detail, subject=f"dbt_model:{model.name}")
        )

    def _columns(self, model: DbtModel, request: EnrichRequest) -> tuple[WarehouseColumn, ...]:
        target = relation(model, request)
        try:
            columns = self._port.relation_columns(target)
        except SnowflakePortError as error:
            raise self._failure(model, "reading columns", error) from error
        if columns is None:
            raise _ModelFailed(
                D("SST-SNO030", model=model.name, relation=target.sql, subject=f"dbt_model:{model.name}")
            )
        return columns

    def _samples(
        self, model: DbtModel, request: EnrichRequest, warehouse: Sequence[WarehouseColumn], settings: EnrichmentConfig
    ) -> dict[str, tuple[str, ...]]:
        columns = [column.name for column in sample_columns(model, warehouse, request.options)]
        if not columns:
            return {}
        try:
            found = self._port.distinct_values(relation(model, request), columns, settings.distinct_limit + 1)
        except SnowflakePortError as error:
            raise self._failure(model, "sampling values", error) from error
        return {name.casefold(): tuple(values) for name, values in found.items()}

    def _complete(
        self, model: DbtModel, step: str, settings: EnrichmentConfig, prompt: str, schema: Mapping[str, object]
    ) -> object:
        try:
            return self._port.complete_json(settings.synonym_model, prompt, schema)
        except SnowflakePortError as error:
            raise self._failure(model, step, error) from error

    def _column_synonyms(
        self,
        model: DbtModel,
        warehouse: Sequence[WarehouseColumn],
        samples: Mapping[str, Sequence[str]],
        options: EnrichOptions,
        settings: EnrichmentConfig,
    ) -> dict[str, tuple[str, ...]]:
        columns = synonym_columns(model, warehouse, options)
        proposals: dict[str, tuple[object, ...]] = {}
        for start in range(0, len(columns), COLUMNS_PER_PROMPT):
            batch = prompt_columns(model, columns[start : start + COLUMNS_PER_PROMPT], samples)
            prompt = column_synonyms_prompt(model.name, model.description, batch, max_count=settings.synonym_max_count)
            parsed = parse_column_synonyms(
                self._complete(model, "column synonyms", settings, prompt, COLUMN_SYNONYMS_SCHEMA)
            )
            if parsed is None:
                raise _ModelFailed(_shape_failure(model, "column synonyms"))
            proposals.update(parsed)
        return column_synonyms(model, warehouse, proposals, limit=settings.synonym_max_count)

    def _table_synonyms(
        self,
        model: DbtModel,
        warehouse: Sequence[WarehouseColumn],
        targets: Sequence[ViewTable],
        options: EnrichOptions,
        settings: EnrichmentConfig,
    ) -> tuple[TableSynonymEdit, ...]:
        if not needs_table_synonyms(targets, options):
            return ()
        names = [yaml_column_name(column.name) for column in warehouse]
        prompt = table_synonyms_prompt(
            model.name, model.description, names, avoided_names(targets), max_count=settings.synonym_max_count
        )
        proposals = parse_table_synonyms(
            self._complete(model, "table synonyms", settings, prompt, TABLE_SYNONYMS_SCHEMA)
        )
        if proposals is None:
            raise _ModelFailed(_shape_failure(model, "table synonyms"))
        return table_synonym_edits(targets, proposals, options, limit=settings.synonym_max_count)

    def _edited_files(self, reports: Sequence[ModelReport]) -> tuple[EditedFile, ...]:
        """Compute every file's text after the run, models' files first, then views', each in path order."""
        by_model_file: dict[str, dict[str, tuple[ColumnUpdate, ...]]] = {}
        by_view_file: dict[str, list[TableSynonymEdit]] = {}
        for report in reports:
            if report.enrichment is not None and report.enrichment.updates:
                by_model_file.setdefault(report.path, {})[report.model] = report.enrichment.updates
            for edit in report.table_synonyms:
                by_view_file.setdefault(edit.path, []).append(edit)
        edited = []
        for path, updates in sorted(by_model_file.items()):
            before = self._files.read(path)
            written = self._files.edit_models(before, path, updates)
            edited.append(EditedFile(path, before, written.text, written.reformatted))
        for path, edits in sorted(by_view_file.items()):
            before = self._files.read(path) or ""
            written = self._files.edit_views(before, path, edits)
            edited.append(EditedFile(path, before, written.text, written.reformatted))
        return tuple(edited)


def _shape_failure(model: DbtModel, step: str) -> Diagnostic:
    return D(
        "SST-SNO031",
        model=model.name,
        step=step,
        detail="Cortex answered in another shape than the one requested",
        subject=f"dbt_model:{model.name}",
    )
