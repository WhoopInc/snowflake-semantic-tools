"""A sensitive column publishes without its sample values, and a view keeps each table's model."""

from __future__ import annotations

import json
from pathlib import Path

from tests.helpers.projects import load_project

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE = REPO_ROOT / "tests" / "fixtures" / "reference_project"
MANIFEST = REPO_ROOT / "tests" / "fixtures" / "reference_project_manifest.json"


def test_a_column_with_pii_tags_renders_no_sample_values_and_no_enum(tmp_path: Path) -> None:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    column = manifest["nodes"]["model.sst_reference_impl.products"]["columns"]["product_type"]
    column.setdefault("meta", {})["pii_tags"] = {"privacy_category": "indirect_identifier"}
    edited = tmp_path / "manifest.json"
    edited.write_text(json.dumps(manifest), encoding="utf-8")

    def product_type(path: Path) -> tuple[tuple[str, ...], bool, frozenset[str]]:
        views = [view for view in load_project(FIXTURE, manifest_path=path).views if "PRODUCTS" in view.table_models]
        member = next(item for item in views[0].columns if item.name.upper() == "PRODUCT_TYPE")
        return member.sample_values, member.is_enum, frozenset(views[0].table_models)

    published, _, tables = product_type(MANIFEST)
    assert published and "PRODUCTS" in tables
    assert product_type(edited)[:2] == ((), False)


def test_an_untagged_column_whose_values_look_like_personal_data_renders_none_of_them(tmp_path: Path) -> None:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    column = manifest["nodes"]["model.sst_reference_impl.products"]["columns"]["product_type"]
    meta = column.setdefault("config", {}).setdefault("meta", {}).setdefault("sst", {})
    column.setdefault("meta", {}).setdefault("sst", {})
    for holder in (meta, column["meta"]["sst"]):
        holder["sample_values"] = ["jaffle", "jane.doe@example.com"]
        holder["is_enum"] = True
    edited = tmp_path / "manifest.json"
    edited.write_text(json.dumps(manifest), encoding="utf-8")
    views = [view for view in load_project(FIXTURE, manifest_path=edited).views if "PRODUCTS" in view.table_models]
    member = next(item for item in views[0].columns if item.name.upper() == "PRODUCT_TYPE")
    assert (member.sample_values, member.is_enum) == ((), False)
