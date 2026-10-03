"""SST-LOD016: a plain value YAML 1.1 reads as a boolean or null other than as written."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.locations import locate_project
from snowflake_semantic_tools.adapters.yaml.config import load_project_config
from snowflake_semantic_tools.adapters.yaml.parse import parse_yaml_bytes
from snowflake_semantic_tools.domain.diagnostics import Origin, Severity, render_diagnostic
from snowflake_semantic_tools.domain.validate.config import empty_is_block
from tests.helpers.seam_projects import parsed

_CONFIG = b"skills:\n  extensions:\n    default_prefix: DB.PARTNER\n    partner-glossary:\n    other: ~\n"


def test_sst_lod016_fires() -> None:
    [diagnostic] = [item for item in parsed(b"enabled: yes\n").diagnostics if item.code == "SST-LOD016"]
    assert diagnostic.severity is Severity.WARNING
    assert diagnostic.message == "f.yml:1: 'enabled' value 'yes' coerced to true"
    assert diagnostic.subject is None
    assert diagnostic.origin == Origin("f.yml", 1)
    # The headline locates the file once, though the message names it too.
    assert render_diagnostic(diagnostic).splitlines()[0] == (
        "f.yml:1: warning[SST-LOD016]: 'enabled' value 'yes' coerced to true"
    )


def test_sst_lod016_silent() -> None:
    assert "SST-LOD016" not in [
        item.code for item in parsed(b"enabled: true\nlabel: 'yes'\nnothing: null\n").diagnostics
    ]


def test_sst_lod016_silent_where_an_empty_config_entry_is_a_documented_empty_block(tmp_path: Path) -> None:
    # An extension entry with no `fqn:` takes `default_prefix`: its null is meant.
    findings = parse_yaml_bytes(_CONFIG, "sst_config.yml", null_means=empty_is_block).diagnostics
    assert [item.code for item in findings] == []
    # Without the schema, both empty values are reported, as in any other file.
    assert [item.context["key"] for item in parsed(_CONFIG).diagnostics] == ["partner-glossary", "other"]
    (tmp_path / "sst_config.yml").write_bytes(_CONFIG + b"project:\n  semantic_models_dir:\n  name: x\n")
    config = load_project_config(locate_project(tmp_path, None))
    assert [(item.code, item.context["key"]) for item in config.diagnostics if item.code == "SST-LOD016"] == [
        ("SST-LOD016", "semantic_models_dir")
    ]


def test_only_a_declared_block_or_map_reads_an_empty_value_as_meant() -> None:
    assert empty_is_block(("skills", "extensions", "partner-glossary"))
    assert empty_is_block(("semantic_views",))
    assert not empty_is_block(("skills", "extensions", "default_prefix"))
    assert not empty_is_block(("no_such_block",))
    assert not empty_is_block(("skills", 0))
