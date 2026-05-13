"""Tests for DiffService (semantic component diff)."""

import json
from unittest.mock import MagicMock, patch

import pytest

from snowflake_semantic_tools.services.diff_service import (
    ComponentChange,
    DiffConfig,
    DiffResult,
    DiffService,
    ViewDiff,
    _is_window_extension,
    _normalize_expr,
)

_MODULE = "snowflake_semantic_tools.services.diff_service"
_CLIENT_PATH = "snowflake_semantic_tools.infrastructure.snowflake.SnowflakeClient"


def _patch_client():
    return patch(_CLIENT_PATH, return_value=MagicMock())


@pytest.fixture(autouse=True)
def _mock_snowflake_client():
    with patch(_CLIENT_PATH, return_value=MagicMock()):
        yield


@pytest.fixture
def diff_config():
    return DiffConfig(database="DB", schema="SCHEMA")


@pytest.fixture
def mock_config():
    return MagicMock()


def _proposed(metrics=None, dimensions=None, facts=None, relationships=None, vqs=None):
    components = {}
    if metrics:
        components["METRIC"] = {m: {"EXPRESSION": f"SUM({m})", "TABLE": "T"} for m in metrics}
    if dimensions:
        components["DIMENSION"] = {f"T.{d}": {"EXPRESSION": d, "TABLE": "T"} for d in dimensions}
    if facts:
        components["FACT"] = {f"T.{f}": {"EXPRESSION": f, "TABLE": "T"} for f in facts}
    if relationships:
        components["RELATIONSHIP"] = {r: {"LEFT": "A", "RIGHT": "B"} for r in relationships}
    if vqs:
        components["AI_VERIFIED_QUERY"] = {v: {"QUESTION": f"What is {v}?"} for v in vqs}
    return components


def _deployed(metrics=None, dimensions=None, facts=None, relationships=None, vqs=None):
    components = {}
    if metrics:
        components["METRIC"] = {m: {"EXPRESSION": f"SUM({m})", "TABLE": "T", "DATA_TYPE": "NUMBER"} for m in metrics}
    if dimensions:
        components["DIMENSION"] = {
            f"T.{d}": {"EXPRESSION": d, "TABLE": "T", "DATA_TYPE": "VARCHAR"} for d in dimensions
        }
    if facts:
        components["FACT"] = {f"T.{f}": {"EXPRESSION": f, "TABLE": "T", "DATA_TYPE": "NUMBER"} for f in facts}
    if relationships:
        components["RELATIONSHIP"] = {r: {"TABLE": "A", "REF_TABLE": "B"} for r in relationships}
    if vqs:
        components["AI_VERIFIED_QUERY"] = {v: {"QUESTION": f"What is {v}?", "TABLE": ""} for v in vqs}
    return components


class TestNewViewDetection:
    def test_new_view_all_components_new(self, mock_config, diff_config):
        service = DiffService(mock_config)
        proposed_views = {"V1": _proposed(metrics=["M1", "M2"], dimensions=["D1"])}

        with patch.object(service, "_load_proposed", return_value=proposed_views), patch.object(
            service, "_get_deployed_view_names", return_value=set()
        ):
            result = service.diff(diff_config)

        assert result.success
        assert len(result.views) == 1
        assert result.views[0].status == "new"
        assert result.views[0].proposed_counts["METRIC"] == 2
        assert result.views[0].proposed_counts["DIMENSION"] == 1


class TestUnchangedDetection:
    def test_unchanged_view_no_changes(self, mock_config, diff_config):
        service = DiffService(mock_config)
        proposed_views = {"V1": _proposed(metrics=["M1"])}

        with patch.object(service, "_load_proposed", return_value=proposed_views), patch.object(
            service, "_get_deployed_view_names", return_value={"V1"}
        ), patch.object(service, "_describe_view", return_value=_deployed(metrics=["M1"])):
            result = service.diff(diff_config)

        assert result.success
        assert result.views[0].status == "unchanged"
        assert result.unchanged_count == 1


class TestNewComponents:
    def test_new_metric_detected(self, mock_config, diff_config):
        service = DiffService(mock_config)
        proposed_views = {"V1": _proposed(metrics=["M1", "M2"])}

        with patch.object(service, "_load_proposed", return_value=proposed_views), patch.object(
            service, "_get_deployed_view_names", return_value={"V1"}
        ), patch.object(service, "_describe_view", return_value=_deployed(metrics=["M1"])):
            result = service.diff(diff_config)

        assert result.views[0].status == "changed"
        new_metrics = [c for c in result.views[0].changes if c.kind == "METRIC" and c.status == "new"]
        assert len(new_metrics) == 1
        assert "M2" in new_metrics[0].name

    def test_new_verified_query_detected(self, mock_config, diff_config):
        service = DiffService(mock_config)
        proposed_views = {"V1": _proposed(metrics=["M1"], vqs=["VQ1"])}

        with patch.object(service, "_load_proposed", return_value=proposed_views), patch.object(
            service, "_get_deployed_view_names", return_value={"V1"}
        ), patch.object(service, "_describe_view", return_value=_deployed(metrics=["M1"])):
            result = service.diff(diff_config)

        new_vqs = [c for c in result.views[0].changes if c.kind == "AI_VERIFIED_QUERY" and c.status == "new"]
        assert len(new_vqs) == 1

    def test_new_relationship_detected(self, mock_config, diff_config):
        service = DiffService(mock_config)
        proposed_views = {"V1": _proposed(relationships=["R1", "R2"])}

        with patch.object(service, "_load_proposed", return_value=proposed_views), patch.object(
            service, "_get_deployed_view_names", return_value={"V1"}
        ), patch.object(service, "_describe_view", return_value=_deployed(relationships=["R1"])):
            result = service.diff(diff_config)

        new_rels = [c for c in result.views[0].changes if c.kind == "RELATIONSHIP" and c.status == "new"]
        assert len(new_rels) == 1


class TestRemovedComponents:
    def test_removed_dimension_detected(self, mock_config, diff_config):
        service = DiffService(mock_config)
        proposed_views = {"V1": _proposed(dimensions=["D1"])}

        with patch.object(service, "_load_proposed", return_value=proposed_views), patch.object(
            service, "_get_deployed_view_names", return_value={"V1"}
        ), patch.object(service, "_describe_view", return_value=_deployed(dimensions=["D1", "D2"])):
            result = service.diff(diff_config)

        removed = [c for c in result.views[0].changes if c.kind == "DIMENSION" and c.status == "removed"]
        assert len(removed) == 1
        assert "D2" in removed[0].name


class TestModifiedComponents:
    def test_modified_metric_expression(self, mock_config, diff_config):
        service = DiffService(mock_config)
        proposed = {"METRIC": {"M1": {"EXPRESSION": "COUNT(*)", "TABLE": "T"}}}
        deployed = {"METRIC": {"M1": {"EXPRESSION": "SUM(amount)", "TABLE": "T"}}}

        changes = DiffService._compare_components(proposed, deployed)

        assert len(changes) == 1
        assert changes[0].status == "modified"
        assert changes[0].detail == "expression changed"

    def test_case_insensitive_expression_match(self, mock_config, diff_config):
        proposed = {"METRIC": {"M1": {"EXPRESSION": "sum(orders.amount)", "TABLE": "T"}}}
        deployed = {"METRIC": {"M1": {"EXPRESSION": "SUM(ORDERS.AMOUNT)", "TABLE": "T"}}}

        changes = DiffService._compare_components(proposed, deployed)
        assert len(changes) == 0

    def test_window_metric_not_modified(self, mock_config, diff_config):
        proposed = {"METRIC": {"M1": {"EXPRESSION": "SUM(T.AMT)", "TABLE": "T"}}}
        deployed = {
            "METRIC": {
                "M1": {
                    "EXPRESSION": "SUM(T.AMT) OVER (\n        PARTITION BY T.ID ORDER BY T.DT ASC\n    )",
                    "TABLE": "T",
                }
            }
        }

        changes = DiffService._compare_components(proposed, deployed)
        assert len(changes) == 0


class TestMixedChanges:
    def test_mixed_across_components(self, mock_config, diff_config):
        service = DiffService(mock_config)
        proposed_views = {
            "V1": _proposed(metrics=["M1", "M_NEW"], dimensions=["D1"], vqs=["VQ1"]),
        }

        with patch.object(service, "_load_proposed", return_value=proposed_views), patch.object(
            service, "_get_deployed_view_names", return_value={"V1"}
        ), patch.object(service, "_describe_view", return_value=_deployed(metrics=["M1", "M_OLD"], dimensions=["D1"])):
            result = service.diff(diff_config)

        changes = result.views[0].changes
        new = [c for c in changes if c.status == "new"]
        removed = [c for c in changes if c.status == "removed"]
        assert len(new) >= 2
        assert len(removed) == 1

    def test_view_filter(self, mock_config):
        config = DiffConfig(database="DB", schema="S", views_filter=["V1"])
        service = DiffService(mock_config)
        proposed_views = {"V1": _proposed(metrics=["M1"]), "V2": _proposed(metrics=["M2"])}

        with patch.object(service, "_load_proposed", return_value=proposed_views), patch.object(
            service, "_get_deployed_view_names", return_value=set()
        ):
            result = service.diff(config)

        assert len(result.views) == 1
        assert result.views[0].name == "V1"

    def test_view_filter_case_insensitive(self, mock_config):
        config = DiffConfig(database="DB", schema="S", views_filter=["v1"])
        service = DiffService(mock_config)
        proposed_views = {"V1": _proposed(metrics=["M1"]), "V2": _proposed(metrics=["M2"])}

        with patch.object(service, "_load_proposed", return_value=proposed_views), patch.object(
            service, "_get_deployed_view_names", return_value=set()
        ):
            result = service.diff(config)

        assert len(result.views) == 1
        assert result.views[0].name == "V1"

    def test_removed_view_detected(self, mock_config, diff_config):
        service = DiffService(mock_config)
        proposed_views = {"V1": _proposed(metrics=["M1"])}

        with patch.object(service, "_load_proposed", return_value=proposed_views), patch.object(
            service, "_get_deployed_view_names", return_value={"V1", "V_OLD"}
        ), patch.object(service, "_describe_view", return_value=_deployed(metrics=["M1"])):
            result = service.diff(diff_config)

        removed = [v for v in result.views if v.status == "removed"]
        assert len(removed) == 1
        assert removed[0].name == "V_OLD"

    def test_removed_view_respects_filter(self, mock_config):
        config = DiffConfig(database="DB", schema="S", views_filter=["V1"])
        service = DiffService(mock_config)
        proposed_views = {"V1": _proposed(metrics=["M1"])}

        with patch.object(service, "_load_proposed", return_value=proposed_views), patch.object(
            service, "_get_deployed_view_names", return_value={"V1", "V_OLD"}
        ), patch.object(service, "_describe_view", return_value=_deployed(metrics=["M1"])):
            result = service.diff(config)

        removed = [v for v in result.views if v.status == "removed"]
        assert len(removed) == 0


class TestErrorPaths:
    def test_missing_manifest_d002(self, tmp_path, monkeypatch, mock_config, diff_config):
        monkeypatch.chdir(tmp_path)
        service = DiffService(mock_config)
        result = service.diff(diff_config)
        assert not result.success
        assert any("SST-D002" in e for e in result.errors)

    def test_corrupt_manifest_d002(self, tmp_path, monkeypatch, mock_config, diff_config):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "target"
        target.mkdir()
        (target / "sst_manifest.json").write_text("{bad")
        service = DiffService(mock_config)
        result = service.diff(diff_config)
        assert not result.success
        assert any("SST-D002" in e for e in result.errors)

    def test_connection_failure_d001(self, mock_config, diff_config):
        service = DiffService(mock_config)

        def fail_deployed(config, client, result):
            result.errors.append("SST-D001: connection refused")
            result.success = False
            return set()

        with patch.object(service, "_load_proposed", return_value={"V1": {}}):
            service._get_deployed_view_names = fail_deployed
            result = service.diff(diff_config)

        assert not result.success
        assert any("SST-D001" in e for e in result.errors)

    def test_no_views_d003(self, mock_config, diff_config):
        service = DiffService(mock_config)
        with patch.object(service, "_load_proposed", return_value={}), patch.object(
            service, "_get_deployed_view_names", return_value=set()
        ):
            result = service.diff(diff_config)
        assert not result.success
        assert any("SST-D003" in e for e in result.errors)

    def test_describe_failure_d004_continues(self, mock_config, diff_config):
        service = DiffService(mock_config)
        proposed_views = {"V1": _proposed(metrics=["M1"]), "V2": _proposed(metrics=["M2"])}

        def fail_describe(config, client, name, result):
            if name == "V1":
                result.warnings.append(f"SST-D004: permission denied for {name}")
                return None
            return _deployed(metrics=["M2"])

        with patch.object(service, "_load_proposed", return_value=proposed_views), patch.object(
            service, "_get_deployed_view_names", return_value={"V1", "V2"}
        ):
            service._describe_view = fail_describe
            result = service.diff(diff_config)

        assert result.success
        assert any("SST-D004" in w for w in result.warnings)
        assert len(result.views) == 1


class TestDiffResultProperties:
    def test_changed_count(self):
        result = DiffResult(
            views=[
                ViewDiff(name="V1", status="changed", changes=[ComponentChange("METRIC", "M1", "T", "new")]),
                ViewDiff(name="V2", status="unchanged"),
            ]
        )
        assert result.changed_count == 1
        assert result.unchanged_count == 1

    def test_view_diff_has_changes(self):
        v = ViewDiff(name="V1", status="changed", changes=[ComponentChange("METRIC", "M1", "T", "new")])
        assert v.has_changes

    def test_view_diff_no_changes(self):
        v = ViewDiff(name="V1", status="unchanged")
        assert not v.has_changes


class TestWindowExtension:
    def test_over_clause_detected(self):
        base = "SUM(T.AMT)"
        full = "SUM(T.AMT) OVER (PARTITION BY T.ID ORDER BY T.DT ASC)"
        assert _is_window_extension(base, full)

    def test_non_over_suffix_rejected(self):
        base = "SUM(T.AMT)"
        full = "SUM(T.AMT) + 1"
        assert not _is_window_extension(base, full)

    def test_empty_base_rejected(self):
        assert not _is_window_extension("", "SUM(X) OVER ()")

    def test_equal_strings_rejected(self):
        assert not _is_window_extension("SUM(X)", "SUM(X)")

    def test_case_insensitive_over(self):
        assert _is_window_extension("SUM(X)", "SUM(X) over (PARTITION BY Y)")


class TestNormalizeExpr:
    def test_collapses_whitespace(self):
        assert _normalize_expr("  SUM(  x  ) ") == "SUM( X )"

    def test_uppercases(self):
        assert _normalize_expr("sum(a)") == "SUM(A)"


class TestCustomInstructionDiff:
    def test_new_ci_detected(self):
        proposed = {"CUSTOM_INSTRUCTION": {"AI_SQL_GENERATION": {"VALUE": "Use foo"}}}
        deployed = {}
        changes = DiffService._compare_components(proposed, deployed)
        assert len(changes) == 1
        assert changes[0].kind == "CUSTOM_INSTRUCTION"
        assert changes[0].status == "new"

    def test_removed_ci_detected(self):
        proposed = {}
        deployed = {"CUSTOM_INSTRUCTION": {"AI_SQL_GENERATION": {"TABLE": "None", "AI_SQL_GENERATION": "Use foo"}}}
        changes = DiffService._compare_components(proposed, deployed)
        assert len(changes) == 1
        assert changes[0].status == "removed"

    def test_unchanged_ci(self):
        proposed = {"CUSTOM_INSTRUCTION": {"AI_SQL_GENERATION": {"VALUE": "Use foo"}}}
        deployed = {"CUSTOM_INSTRUCTION": {"AI_SQL_GENERATION": {"TABLE": "None", "AI_SQL_GENERATION": "Use foo"}}}
        changes = DiffService._compare_components(proposed, deployed)
        assert len(changes) == 0

    def test_modified_ci(self):
        proposed = {"CUSTOM_INSTRUCTION": {"AI_SQL_GENERATION": {"VALUE": "Use bar"}}}
        deployed = {"CUSTOM_INSTRUCTION": {"AI_SQL_GENERATION": {"TABLE": "None", "AI_SQL_GENERATION": "Use foo"}}}
        changes = DiffService._compare_components(proposed, deployed)
        assert len(changes) == 1
        assert changes[0].status == "modified"


class TestLoadProposed:
    def test_parses_manifest(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "target"
        target.mkdir()
        import json

        manifest = {
            "tables": {
                "semantic_views": [{"name": "V1", "tables": ["T1"]}],
                "tables": [{"table_name": "T1"}],
                "dimensions": [{"table_name": "T1", "name": "D1", "expr": "D1"}],
                "time_dimensions": [],
                "facts": [{"table_name": "T1", "name": "F1", "expr": "F1"}],
                "metrics": [{"name": "M1", "table_name": "T1", "tables": ["T1"], "expr": "SUM(X)"}],
                "relationships": [],
                "verified_queries": [],
                "custom_instructions": [],
            }
        }
        (target / "sst_manifest.json").write_text(json.dumps(manifest))

        from unittest.mock import MagicMock

        svc = DiffService(MagicMock())
        result = DiffResult()
        views = svc._load_proposed(result)

        assert "V1" in views
        assert "T1" in views["V1"]["TABLE"]
        assert "T1.D1" in views["V1"]["DIMENSION"]
        assert "T1.F1" in views["V1"]["FACT"]
        assert "M1" in views["V1"]["METRIC"]

    def test_multi_table_metric_included(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "target"
        target.mkdir()
        import json

        manifest = {
            "tables": {
                "semantic_views": [{"name": "V1", "tables": ["T1", "T2"]}],
                "tables": [{"table_name": "T1"}, {"table_name": "T2"}],
                "dimensions": [],
                "time_dimensions": [],
                "facts": [],
                "metrics": [{"name": "M1", "table_name": "T1", "tables": ["T1", "T2"], "expr": "SUM(X)"}],
                "relationships": [],
                "verified_queries": [],
                "custom_instructions": [],
            }
        }
        (target / "sst_manifest.json").write_text(json.dumps(manifest))

        from unittest.mock import MagicMock

        svc = DiffService(MagicMock())
        result = DiffResult()
        views = svc._load_proposed(result)

        assert "M1" in views["V1"]["METRIC"]

    def test_custom_instructions_concatenated(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "target"
        target.mkdir()
        import json

        manifest = {
            "tables": {
                "semantic_views": [{"name": "V1", "tables": ["T1"], "custom_instructions": ["CI1", "CI2"]}],
                "tables": [{"table_name": "T1"}],
                "dimensions": [],
                "time_dimensions": [],
                "facts": [],
                "metrics": [],
                "relationships": [],
                "verified_queries": [],
                "custom_instructions": [
                    {"name": "CI1", "sql_generation": "Rule A", "question_categorization": "Cat A"},
                    {"name": "CI2", "sql_generation": "Rule B"},
                ],
            }
        }
        (target / "sst_manifest.json").write_text(json.dumps(manifest))

        from unittest.mock import MagicMock

        svc = DiffService(MagicMock())
        result = DiffResult()
        views = svc._load_proposed(result)

        ci = views["V1"]["CUSTOM_INSTRUCTION"]
        assert ci["AI_SQL_GENERATION"]["VALUE"] == "Rule A\nRule B"
        assert ci["AI_QUESTION_CATEGORIZATION"]["VALUE"] == "Cat A"


class TestFullModeRendering:
    def test_modified_shows_old_new_values(self):
        changes = DiffService._compare_components(
            {"METRIC": {"M1": {"EXPRESSION": "COUNT(*)", "TABLE": "T"}}},
            {"METRIC": {"M1": {"EXPRESSION": "SUM(amount)", "TABLE": "T"}}},
        )
        assert len(changes) == 1
        assert changes[0].old_value is not None
        assert changes[0].new_value is not None
        assert "COUNT" in changes[0].new_value
        assert "SUM" in changes[0].old_value

    def test_new_component_has_no_old_value(self):
        changes = DiffService._compare_components(
            {"METRIC": {"M1": {"EXPRESSION": "SUM(X)", "TABLE": "T"}}},
            {},
        )
        assert changes[0].old_value is None

    def test_removed_component_has_no_new_value(self):
        changes = DiffService._compare_components(
            {},
            {"METRIC": {"M1": {"EXPRESSION": "SUM(X)", "TABLE": "T"}}},
        )
        assert changes[0].new_value is None
