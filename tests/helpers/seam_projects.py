"""Small projects, manifests, and a fake dbt for the load, parse, and dbt seam code tests."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any

import pytest

from snowflake_semantic_tools.adapters.dbt.invoke import CompletedRun
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.project_source import YamlProjectSource
from snowflake_semantic_tools.adapters.yaml.documents import ParsedYaml
from snowflake_semantic_tools.adapters.yaml.parse import parse_yaml_bytes
from snowflake_semantic_tools.app.compile.agents import AgentCompileContext
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Origin
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.project import SemanticViewProject
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, ToolGroup, ToolMember, ToolOwnership
from tests.helpers.projects import project_paths

SCHEMA = "https://schemas.getdbt.com/dbt/manifest/v12.json"
VIEW = (
    "semantic_views:\n"
    "  - name: catalog\n"
    "    description: Use this view for questions about the product catalog.\n"
    "    tables:\n"
    "      - \"{{ ref('products') }}\"\n"
)


def parsed(text: str | bytes, path: str = "f.yml") -> ParsedYaml:
    """Parse one file's text as SST parses every YAML file."""
    return parse_yaml_bytes(text.encode("utf-8") if isinstance(text, str) else text, path)


def refused(text: str | bytes, path: str = "f.yml") -> tuple[Diagnostic, ...]:
    """The diagnostics a file that does not parse is refused with."""
    with pytest.raises(ProjectError) as raised:
        parsed(text, path)
    return raised.value.diagnostics


def model_node(
    name: str,
    relation: str | None = None,
    *,
    columns: Mapping[str, Mapping[str, Any]] | None = None,
    meta: Mapping[str, Any] | None = None,
    complete: bool = True,
    **fields: Any,
) -> dict[str, Any]:
    """One manifest model node; `complete` adds the checksum and contract a clean seam expects."""
    node: dict[str, Any] = {
        "resource_type": "model",
        "name": name,
        "description": f"The {name} model.",
        "relation_name": relation if relation is not None else f"db.sch.{name}",
        "config": {"meta": {"sst": dict(meta or {"primary_key": [f"{name}_id"]})}},
        "columns": dict(
            columns
            or {
                f"{name}_id": {
                    "name": f"{name}_id",
                    "description": "The key.",
                    "data_type": "VARCHAR",
                    "meta": {"sst": {"column_type": "dimension"}},
                }
            }
        ),
    }
    if complete:
        node["checksum"] = {"name": "sha256", "checksum": "0" * 64}
        node["config"]["contract"] = {"enforced": True}
    node.update(fields)
    return node


def attached_test(model_id: str) -> dict[str, Any]:
    """A `not_null` test attached to a model, which counts as the model having tests."""
    return {"resource_type": "test", "attached_node": model_id, "test_metadata": {"name": "not_null"}}


def manifest(
    nodes: Mapping[str, Mapping[str, Any]] | None = None, *, tested: bool = True, **root: Any
) -> dict[str, Any]:
    """A manifest document holding `nodes`, `products` by default, each model with a test unless not `tested`."""
    chosen = dict(nodes if nodes is not None else {"model.fixture.products": model_node("products")})
    if tested:
        for unique_id, node in list(chosen.items()):
            if node.get("resource_type") == "model":
                chosen[f"test.fixture.not_null_{node.get('name')}"] = attached_test(unique_id)
    return {
        "metadata": {"dbt_schema_version": SCHEMA, "dbt_version": "1.11.2", "project_name": "fixture"},
        "nodes": chosen,
        **root,
    }


@dataclass
class SmallProject:
    """A dbt + SST project on disk with one view over `products` unless told otherwise."""

    root: Path
    files: dict[str, str] = field(default_factory=dict)
    document: dict[str, Any] = field(default_factory=manifest)

    def write(self) -> Path:
        """Write the project and its manifest; return the manifest's path."""
        files = {
            "dbt_project.yml": "name: fixture\nprofile: fixture\nmodel-paths: [models]\ntarget-path: target\n",
            "profiles.yml": (
                "fixture:\n  target: dev\n  outputs:\n    dev:\n      type: snowflake\n"
                "      database: DB\n      schema: SCH\n"
            ),
            "sst_config.yml": "project:\n  semantic_models_dir: semantic_models\n",
            "semantic_models/semantic_views/views.yml": VIEW,
            **self.files,
        }
        for relative, text in files.items():
            path = self.root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text, encoding="utf-8")
        # dbt_project.yml names `models`; a model-paths entry must exist (SST-DIS009).
        (self.root / "models").mkdir(exist_ok=True)
        manifest_path = self.root / "manifest.json"
        manifest_path.write_text(json.dumps(self.document), encoding="utf-8")
        return manifest_path

    def load(self) -> SemanticViewProject:
        """Load the project the way `sst` loads it from a given manifest."""
        manifest_path = self.write()
        return YamlProjectSource(project_paths(self.root), manifest_path=manifest_path, invoke_dbt=False).load_project()


def found(project: SemanticViewProject, code: str) -> list[Diagnostic]:
    """The project's diagnostics of one code, in report order."""
    return [item for item in project.diagnostics if item.code == code]


@dataclass
class FakeDbt:
    """A dbt that answers each argument list from a script and records what it was asked to run.

    Attributes:
        version: What `dbt --version` prints, or the `OSError` starting dbt raises.
        parse: What `dbt parse` returns, or the `OSError` it raises.
        writes: A manifest `dbt parse` writes when it exits zero.
    """

    version: CompletedRun | OSError = field(
        default_factory=lambda: CompletedRun(0, "Core:\n  - installed: 1.11.2\n", "")
    )
    parse: CompletedRun | OSError = field(default_factory=lambda: CompletedRun(0, "", ""))
    writes: tuple[Path, Mapping[str, Any]] | None = None
    runs: list[tuple[str, ...]] = field(default_factory=list)

    def __call__(self, argv: Sequence[str], cwd: Path) -> CompletedRun:
        self.runs.append(tuple(argv))
        answer = self.version if argv[1] == "--version" else self.parse
        if isinstance(answer, OSError):
            raise answer
        if argv[1] == "parse" and answer.returncode == 0 and self.writes is not None:
            path, document = self.writes
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(document), encoding="utf-8")
        return answer


METRICS = "semantic_models/metrics/m.yml"
FILTERS = "semantic_models/filters/f.yml"
RELATIONSHIPS = "semantic_models/relationships/r.yml"
VIEWS = "semantic_models/semantic_views/views.yml"
# A relationship condition operand: the `products_id` column of `products`.
KEY = "{{ ref('products', 'products_id') }}"
ORDER_BY = "    window:\n      order_by: [\"{{ ref('products', 'products_id') }}\"]\n"


def metric_file(body: str, name: str = "total") -> dict[str, str]:
    """A metrics file with one metric over `products`, `body` its lines after `tables`."""
    head = f"snowflake_metrics:\n  - name: {name}\n    description: Total.\n"
    return {METRICS: head + "    tables: [\"{{ ref('products') }}\"]\n" + body}


def filter_entry(name: str) -> str:
    """A `snowflake_filters` file declaring one filter over `products`."""
    return (
        f"snowflake_filters:\n  - name: {name}\n    description: Cheap.\n"
        "    tables: [\"{{ ref('products') }}\"]\n"
        f"    expr: \"{KEY} = 'x'\"\n"
    )


def relationship_file(*conditions: str, name: str = "self_join") -> dict[str, str]:
    """A relationships file with one `products` self-join over `conditions`."""
    lines = "".join(f'      - "{condition}"\n' for condition in conditions)
    head = f"snowflake_relationships:\n  - name: {name}\n    description: Self.\n"
    return {
        RELATIONSHIPS: head
        + f"    left_table: products\n    right_table: products\n    relationship_conditions:\n{lines}"
    }


def view_file(body: str) -> dict[str, str]:
    """The views file with the `catalog` view over `products`, `body` its lines after `tables`."""
    return {VIEWS: VIEW + body}


def agent_context(tools: ToolCatalog | None = None) -> AgentCompileContext:
    """What an agent compiles against: one semantic view `sales`, and `tools` when given."""
    return AgentCompileContext(
        semantic_views={"sales": QualifiedName.parse("DB.S.SALES")},
        tools=tools or ToolCatalog((), "dev", frozenset(("dev",))),
        agents={},
        extensions={},
        variables={},
        database="DB",
        schema="S",
        warehouse="WH",
        query_timeout=60,
        orchestration_model="auto",
        budget_seconds=None,
        budget_tokens=None,
        tool_not_accessible=None,
        analytical_search=None,
        alias=None,
        allowed_models=frozenset(),
        skills={},
        plugins={},
    )


def procedure_catalog() -> ToolCatalog:
    """A tool catalog holding one referenced procedure, `platform.procedure`, for a generic tool."""
    procedure = ToolMember(
        "platform",
        "procedure",
        "procedure",
        ToolOwnership.REFERENCE,
        Origin("tools.yml"),
        "tools.yml",
        description="Procedure.",
        warehouse="WH",
        relations=MappingProxyType({"dev": "DB.S.PROCEDURE"}),
    )
    group = ToolGroup("platform", Origin("tools.yml"), "tools.yml", members=(procedure,))
    return ToolCatalog((group,), "dev", frozenset(("dev",)))
