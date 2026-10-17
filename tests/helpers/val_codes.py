"""What the per-code VAL tests share: the inputs they validate, read for one code."""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType
from typing import Any

from snowflake_semantic_tools.adapters.yaml.profiles import load_profile_catalog
from snowflake_semantic_tools.adapters.yaml.semantic.relationships import _Conditions, _parse_conditions
from snowflake_semantic_tools.app.compile import CompiledView, CompileResult
from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.gate import capture_baseline
from snowflake_semantic_tools.app.manifest import build_manifest
from snowflake_semantic_tools.app.validate import ValidateArtifacts
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Origin
from snowflake_semantic_tools.domain.enrich import Component, WarehouseColumn, enrich_model, resolve_options
from snowflake_semantic_tools.domain.model.config_schema import EnrichmentConfig
from snowflake_semantic_tools.domain.model.dbt import DbtColumn, DbtModel
from snowflake_semantic_tools.domain.model.eval import EvalBaselineRecord
from snowflake_semantic_tools.domain.model.lifecycle import QueryResult, RenderedArtifact
from snowflake_semantic_tools.domain.model.profile import ProfileCatalog
from snowflake_semantic_tools.domain.model.semantic_view import Column, ColumnKind, Relationship, SemanticView, Table
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolMember
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.render.semantic_view import render
from snowflake_semantic_tools.domain.sql import Sql
from snowflake_semantic_tools.domain.validate.semantic.dbt import dbt_column_diagnostics, dbt_model_diagnostics
from snowflake_semantic_tools.domain.validate.semantic_view import join_graph_diagnostics
from snowflake_semantic_tools.domain.validate.tool import validate_tool_catalog
from tests.helpers.agent_builders import dbt_catalog
from tests.helpers.compile_builders import compiled_as
from tests.helpers.eval_builders import attempt, compile_eval, gated_eval, run_result
from tests.helpers.file_trees import write_tree
from tests.helpers.semantic_members import ORDERS
from tests.helpers.snowflake_fake import FakeSnowflake
from tests.helpers.sql_values import authored


def checked_tools(*members: ToolMember) -> list[Diagnostic]:
    """What validating a tool catalog of `members` for targets `dev` and `prod` reports."""
    return list(validate_tool_catalog(ToolCatalog(members, "dev", frozenset(("dev", "prod"))), dbt_catalog()))


def dbt_column_findings(column: DbtColumn, code: str) -> list[Diagnostic]:
    """The diagnostics of `code` for a dbt model keyed on `id` that also has `column`."""
    model = DbtModel("model.t.m", "m", "DB.S.M", ("id",), (), (DbtColumn("id", "Key.", "NUMBER", "dimension"), column))
    return [item for item in dbt_column_diagnostics({"m": model}) if item.code == code]


def dbt_model_findings(code: str, **changes: Any) -> list[Diagnostic]:
    """The diagnostics of `code` for the `orders` model with `changes` applied."""
    model = dataclasses.replace(ORDERS, **changes)
    return [item for item in dbt_model_diagnostics({"orders": model}) if item.code == code]


SETTINGS = EnrichmentConfig(distinct_limit=3, display_limit=2)


def enrich_findings(
    column: DbtColumn,
    warehouse: tuple[WarehouseColumn, ...],
    samples: dict[str, list[str]],
    code: str,
    *components: Component,
) -> list[Diagnostic]:
    """The diagnostics of `code` from enriching `orders` with `column` against `warehouse`."""
    model = DbtModel("model.p.orders", "orders", "DB.S.ORDERS", (), (), (column,))
    options = resolve_options(frozenset(components), frozenset())
    result = enrich_model(model, warehouse, options, SETTINGS, samples=samples, synonyms={})
    return [item for item in result.diagnostics if item.code == code]


PASSING = (("q", "answer_correctness", True), ("q", "grounding", True))


def baseline() -> EvalBaselineRecord:
    """A baseline captured from two passing runs of the gated eval."""
    runs = run_result(attempt("run-1", *PASSING), attempt("run-2", *PASSING))
    return capture_baseline(gated_eval(), runs, reason="initial", captured_at="2026-09-01T00:00:00Z")


def load_profiles(root: Path, files: Mapping[str, str | bytes]) -> ProfileCatalog:
    """Write `files` below `root` and load the profile catalog there."""
    write_tree(root, files)
    return load_profile_catalog(
        root, profiles_dir="profiles", hooks_dir="hooks", mcp_servers_dir="mcp-servers", commands_dir="commands"
    )


ORIGIN = Origin("relationships.yml", 1, 1)


ENDPOINTS = ("orders", "customers")


def parsed_conditions(*conditions: str) -> _Conditions | Diagnostic:
    """Parse `conditions` for relationship `rel` between `orders` and `customers`."""
    return _parse_conditions(list(conditions), ENDPOINTS, "rel", ORIGIN, "relationship:rel")


class Scripted(FakeSnowflake):
    """A port that answers the spot-check reads with one fixed row."""

    def __init__(self, answers: dict[str, tuple[object, ...]]) -> None:
        super().__init__()
        self._answers = answers

    def query(self, sql: Sql, params: object = None) -> QueryResult:
        text = str(sql)
        for marker, row in self._answers.items():
            if marker in text:
                return QueryResult(("A",), (row,))
        return super().query(sql, params)


def validated_live(view: SemanticView, answers: dict[str, tuple[object, ...]]) -> list[Diagnostic]:
    """What a connected validate of `view` reports, Snowflake answering with `answers`."""
    compiled = CompileResult((CompiledView(view, render(view)),))
    return list(ValidateArtifacts(Scripted(answers)).run(compiled, strict=False, connected=True).diagnostics)


class TypedWarehouse(FakeSnowflake):
    """A port that describes tables as `tables` lists them, and refuses statements as `refusals` says.

    `DESCRIBE TABLE <fqn>` answers `tables[fqn]`'s `(name, type)` pairs under DESCRIBE's headings,
    and fails for a table not listed, as for one that does not exist. A query holding a key of
    `refusals` raises its value, with `{qid}` replaced by a query id no other refusal has.
    """

    def __init__(
        self, tables: Mapping[str, tuple[tuple[str, str], ...]], refusals: Mapping[str, str] | None = None
    ) -> None:
        super().__init__()
        self._tables = dict(tables)
        self._refusals = dict(refusals or {})
        self._refused = 0

    def query(self, sql: Sql, params: object = None) -> QueryResult:
        answer = super().query(sql, params)
        text = str(sql)
        for fragment, message in self._refusals.items():
            if fragment in text:
                self._refused += 1
                raise SnowflakePortError(message.replace("{qid}", f"01bc0000-0000-0000-0000-{self._refused:012d}"))
        if text.startswith("DESCRIBE TABLE "):
            fqn = text.removeprefix("DESCRIBE TABLE ")
            if fqn not in self._tables:
                raise SnowflakePortError(f"002003 (02000): Table '{fqn}' does not exist or not authorized.")
            return QueryResult(("name", "type", "kind"), tuple((*column, "COLUMN") for column in self._tables[fqn]))
        return answer


TABLES = tuple(Table(name, f"DB.S.{name}") for name in ("ORDERS", "CUSTOMERS", "LOCATIONS"))


def join_graph_findings(*relationships: Relationship, code: str) -> list[Diagnostic]:
    """The diagnostics of `code` for view `sales` joined by `relationships`."""
    view = SemanticView("DB.S.SALES", TABLES, relationships=relationships)
    return [item for item in join_graph_diagnostics(view, artifact="semantic_view:sales") if item.code == code]


def eval_artifact() -> RenderedArtifact:
    """The compiled eval, rendered for publish under its manifest."""
    result = compile_eval()
    return compiled_as(result, CompiledEval).rendered_for_publish(build_manifest(result).manifest_id)


# The headings TAG_REFERENCES_ALL_COLUMNS answers with, as measured on a live account.
TAG_HEADINGS = (
    "TAG_DATABASE",
    "TAG_SCHEMA",
    "TAG_NAME",
    "TAG_VALUE",
    "LEVEL",
    "OBJECT_DATABASE",
    "OBJECT_SCHEMA",
    "OBJECT_NAME",
    "DOMAIN",
    "COLUMN_NAME",
    "APPLY_METHOD",
)


class TaggedWarehouse(FakeSnowflake):
    """A port whose tables carry column tags: `tags[fqn]` holds `(column, tag, level, apply method)` rows.

    A tag read of a relation in `refused` raises its message, as for one the role cannot see.
    """

    def __init__(
        self,
        tags: Mapping[str, tuple[tuple[str, str, str, str], ...]],
        refused: Mapping[str, str] | None = None,
    ) -> None:
        super().__init__()
        self._tags = dict(tags)
        self._refused = dict(refused or {})
        self.tag_reads: list[str] = []

    def query(self, sql: Sql, params: object = None) -> QueryResult:
        text = str(sql)
        if "TAG_REFERENCES_ALL_COLUMNS" not in text:
            return super().query(sql, params)
        fqn = text.split("COLUMNS('", 1)[1].split("'", 1)[0]
        self.tag_reads.append(fqn)
        if fqn in self._refused:
            raise SnowflakePortError(self._refused[fqn], errno=2003)
        rows = tuple(
            (*tag.split("."), "x", level, *fqn.split("."), "COLUMN", column, method)
            for column, tag, level, method in self._tags.get(fqn, ())
        )
        return QueryResult(TAG_HEADINGS, rows)


def tagged_models(*columns: DbtColumn) -> SemanticView:
    """A view over `orders`, whose model has `columns`, reading relation `DB.S.ORDERS`."""
    model = DbtModel("model.p.orders", "orders", "DB.S.ORDERS", (), (), columns)
    return SemanticView(
        "DB.S.SALES",
        (Table("ORDERS", "DB.S.ORDERS"),),
        columns=(Column("ORDERS", "EMAIL", ColumnKind.DIMENSION, authored("ORDERS.EMAIL")),),
        table_models=MappingProxyType({"ORDERS": model}),
    )
