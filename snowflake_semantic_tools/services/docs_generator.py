"""
Documentation Site Generator

Generates a self-contained interactive documentation site from an SST
manifest and lineage graph. Output is static HTML with embedded CSS and
JavaScript — no external dependencies required for viewing.

Supports two output formats:
  - html: Interactive site with catalog and lineage visualization
  - json: Raw catalog data for custom integrations
"""

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import jinja2

from snowflake_semantic_tools._version import __version__
from snowflake_semantic_tools.services.lineage_builder import LineageGraph
from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger("docs_generator")

TEMPLATES_DIR = Path(__file__).parent.parent / "templates" / "docs"


@dataclass
class DocsConfig:
    output_dir: Path = field(default_factory=lambda: Path("sst-docs"))
    format: str = "html"


@dataclass
class DocsResult:
    success: bool = True
    output_dir: Optional[Path] = None
    files_created: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    duration: float = 0.0


def _safe_get(data: Dict, key: str, default: Any = "") -> Any:
    val = data.get(key, default)
    return val if val is not None else default


class DocsGenerator:

    def __init__(self, manifest_data: Dict[str, Any], lineage_graph: LineageGraph):
        self.manifest = manifest_data
        self.graph = lineage_graph
        self.tables_data = manifest_data.get("tables", {})

    def generate(self, config: DocsConfig) -> DocsResult:
        result = DocsResult(output_dir=config.output_dir)

        try:
            config.output_dir.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            result.success = False
            result.errors.append(f"Could not create output directory {config.output_dir}: {e}")
            return result

        if config.format == "json":
            return self._generate_json(config, result)
        elif config.format == "html":
            return self._generate_html(config, result)
        else:
            result.success = False
            result.errors.append(f"Unsupported format: {config.format}. Use 'html' or 'json'.")
            return result

    def _generate_json(self, config: DocsConfig, result: DocsResult) -> DocsResult:
        try:
            export = {
                "metadata": self.manifest.get("metadata", {}),
                "catalog": self._build_catalog(),
                "lineage": self.graph.to_dict(),
                "summary": self._build_summary(),
            }
            output_path = config.output_dir / "data.json"
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(export, f, indent=2, default=str)
            result.files_created.append(str(output_path))
            logger.info(f"JSON export written: {output_path}")
        except Exception as e:
            result.success = False
            result.errors.append(f"Failed to write JSON export: {e}")

        return result

    def _generate_html(self, config: DocsConfig, result: DocsResult) -> DocsResult:
        try:
            env = jinja2.Environment(
                loader=jinja2.FileSystemLoader(str(TEMPLATES_DIR)),
                autoescape=jinja2.select_autoescape(["html"]),
                undefined=jinja2.StrictUndefined,
            )
            env.globals["render_card"] = self._render_card
        except Exception as e:
            result.success = False
            result.errors.append(f"Failed to initialize template engine: {e}")
            return result

        pico_css = self._read_asset("vendor/pico.min.css")
        app_css = self._read_asset("css/app.css")
        catalog_js = self._read_asset("js/catalog.js")
        lineage_js = self._read_asset("js/lineage.js")
        d3_js = self._read_asset("vendor/d3.min.js")

        catalog = self._build_catalog()
        summary = self._build_summary()
        search_data = self._build_search_data(catalog)
        generated_at = _safe_get(self.manifest.get("metadata", {}), "generated_at", "unknown")

        common_ctx = {
            "sst_version": __version__,
            "generated_at": generated_at,
            "pico_css": pico_css,
            "app_css": app_css,
        }

        try:
            index_tmpl = env.get_template("index.html.j2")
            index_html = index_tmpl.render(
                **common_ctx,
                catalog=catalog,
                summary=summary,
                search_data_json=json.dumps(search_data, default=str),
                catalog_js=catalog_js,
            )
            index_path = config.output_dir / "index.html"
            index_path.write_text(index_html, encoding="utf-8")
            result.files_created.append(str(index_path))
        except Exception as e:
            result.success = False
            result.errors.append(f"Failed to render index.html: {e}")
            return result

        try:
            lineage_tmpl = env.get_template("lineage.html.j2")
            lineage_html = lineage_tmpl.render(
                **common_ctx,
                graph_data_json=json.dumps(self.graph.to_d3_json(), default=str),
                lineage_js=lineage_js,
                d3_js=d3_js,
            )
            lineage_path = config.output_dir / "lineage.html"
            lineage_path.write_text(lineage_html, encoding="utf-8")
            result.files_created.append(str(lineage_path))
        except Exception as e:
            result.success = False
            result.errors.append(f"Failed to render lineage.html: {e}")
            return result

        try:
            export = {
                "metadata": self.manifest.get("metadata", {}),
                "catalog": catalog,
                "lineage": self.graph.to_dict(),
                "summary": summary,
            }
            data_path = config.output_dir / "data.json"
            with open(data_path, "w", encoding="utf-8") as f:
                json.dump(export, f, indent=2, default=str)
            result.files_created.append(str(data_path))
        except Exception as e:
            logger.warning(f"Could not write data.json: {e}")

        return result

    def _build_catalog(self) -> Dict[str, List[Dict]]:
        return {
            "tables": self.tables_data.get("tables", []),
            "metrics": self.tables_data.get("metrics", []),
            "relationships": self.tables_data.get("relationships", []),
            "filters": self.tables_data.get("filters", []),
            "custom_instructions": self.tables_data.get("custom_instructions", []),
            "verified_queries": self.tables_data.get("verified_queries", []),
            "semantic_views": self.tables_data.get("semantic_views", []),
        }

    def _build_summary(self) -> Dict[str, int]:
        catalog = self._build_catalog()
        counts = {k: len(v) for k, v in catalog.items()}
        counts["total"] = sum(counts.values())
        return counts

    def _build_search_data(self, catalog: Dict[str, List[Dict]]) -> List[Dict]:
        items = []
        idx = 0
        for t in catalog.get("tables", []):
            items.append(
                {
                    "id": idx,
                    "type": "table",
                    "name": _safe_get(t, "table_name"),
                    "description": _safe_get(t, "description"),
                    "expression": "",
                    "synonyms": [],
                }
            )
            idx += 1
        for m in catalog.get("metrics", []):
            items.append(
                {
                    "id": idx,
                    "type": "metric",
                    "name": _safe_get(m, "name"),
                    "description": _safe_get(m, "description"),
                    "expression": _safe_get(m, "expr"),
                    "synonyms": m.get("synonyms", []) or [],
                }
            )
            idx += 1
        for r in catalog.get("relationships", []):
            items.append(
                {
                    "id": idx,
                    "type": "relationship",
                    "name": _safe_get(r, "relationship_name"),
                    "description": f"{_safe_get(r, 'left_table_name')} \u2192 {_safe_get(r, 'right_table_name')}",
                    "expression": "",
                    "synonyms": [],
                }
            )
            idx += 1
        for sv in catalog.get("semantic_views", []):
            items.append(
                {
                    "id": idx,
                    "type": "semantic_view",
                    "name": _safe_get(sv, "name"),
                    "description": _safe_get(sv, "description"),
                    "expression": "",
                    "synonyms": [],
                }
            )
            idx += 1
        for f in catalog.get("filters", []):
            items.append(
                {
                    "id": idx,
                    "type": "filter",
                    "name": _safe_get(f, "name"),
                    "description": _safe_get(f, "description"),
                    "expression": _safe_get(f, "expr"),
                    "synonyms": [],
                }
            )
            idx += 1
        for ci in catalog.get("custom_instructions", []):
            items.append(
                {
                    "id": idx,
                    "type": "custom_instruction",
                    "name": _safe_get(ci, "name"),
                    "description": _safe_get(ci, "sql_generation") or _safe_get(ci, "question_categorization"),
                    "expression": "",
                    "synonyms": [],
                }
            )
            idx += 1
        for vq in catalog.get("verified_queries", []):
            items.append(
                {
                    "id": idx,
                    "type": "verified_query",
                    "name": _safe_get(vq, "name"),
                    "description": _safe_get(vq, "question"),
                    "expression": _safe_get(vq, "sql"),
                    "synonyms": [],
                }
            )
            idx += 1
        return items

    def _render_card(self, component_type: str, name: str, description: str, data: Dict) -> str:
        card_id = f"card-{component_type}-{name}".replace(" ", "-").lower()
        badge = f'<span class="sst-badge sst-badge-{component_type}">{component_type.replace("_", " ")}</span>'
        desc_html = f'<p class="sst-card-desc">{description}</p>' if description else ""

        meta_items = []
        if component_type == "table":
            db = _safe_get(data, "database")
            schema = _safe_get(data, "schema")
            pk = _safe_get(data, "primary_key")
            if db and schema:
                meta_items.append(f"{db}.{schema}")
            if pk:
                meta_items.append(f"PK: {pk}")
        elif component_type == "metric":
            tables = data.get("tables", [])
            if isinstance(tables, list) and tables:
                meta_items.append(", ".join(str(t) for t in tables[:3]))
        elif component_type == "relationship":
            left = _safe_get(data, "left_table_name")
            right = _safe_get(data, "right_table_name")
            if left and right:
                meta_items.append(f"{left} \u2192 {right}")
        elif component_type == "semantic_view":
            tables = data.get("tables", [])
            if isinstance(tables, list):
                meta_items.append(f"{len(tables)} tables")
        elif component_type == "filter":
            table = _safe_get(data, "table_name")
            if table:
                meta_items.append(table)

        meta_html = ""
        if meta_items:
            spans = "".join(f"<span>{m}</span>" for m in meta_items)
            meta_html = f'<div class="sst-card-meta">{spans}</div>'

        detail_parts = []
        if component_type in ("metric", "filter") and _safe_get(data, "expr"):
            detail_parts.append(f"<dt>Expression</dt><dd><pre>{_safe_get(data, 'expr')}</pre></dd>")
        if component_type == "verified_query":
            if _safe_get(data, "question"):
                detail_parts.append(f"<dt>Question</dt><dd>{_safe_get(data, 'question')}</dd>")
            if _safe_get(data, "sql"):
                detail_parts.append(f"<dt>SQL</dt><dd><pre>{_safe_get(data, 'sql')}</pre></dd>")
        if component_type == "custom_instruction":
            if _safe_get(data, "question_categorization"):
                detail_parts.append(
                    f"<dt>Question Categorization</dt><dd>{_safe_get(data, 'question_categorization')}</dd>"
                )
            if _safe_get(data, "sql_generation"):
                detail_parts.append(f"<dt>SQL Generation</dt><dd>{_safe_get(data, 'sql_generation')}</dd>")
        if data.get("synonyms"):
            detail_parts.append(f"<dt>Synonyms</dt><dd>{', '.join(str(s) for s in data['synonyms'])}</dd>")
        if _safe_get(data, "source_file"):
            detail_parts.append(
                f"<dt>Source</dt><dd style='font-size:0.8rem;color:var(--sst-text-muted)'>{_safe_get(data, 'source_file')}</dd>"
            )

        detail_html = ""
        if detail_parts:
            detail_html = f'<div class="sst-card-detail"><div class="sst-card-detail-inner"><dl>{"".join(detail_parts)}</dl></div></div>'

        lineage_link = (
            f'<a href="lineage.html" style="font-size:0.75rem;color:var(--sst-primary)">View in Lineage &rarr;</a>'
        )

        return (
            f'<div class="sst-card" id="{card_id}">'
            f'<div class="sst-card-header">'
            f"<div>{badge}<h3>{name}</h3>{desc_html}{meta_html}</div>"
            f'<span class="sst-card-expand">&#9660;</span>'
            f"</div>"
            f"{detail_html}"
            f"</div>"
        )

    @staticmethod
    def _read_asset(relative_path: str) -> str:
        asset_path = TEMPLATES_DIR / relative_path
        if not asset_path.exists():
            logger.warning(f"Asset not found: {asset_path}")
            return ""
        try:
            return asset_path.read_text(encoding="utf-8")
        except Exception as e:
            logger.warning(f"Could not read asset {asset_path}: {e}")
            return ""
