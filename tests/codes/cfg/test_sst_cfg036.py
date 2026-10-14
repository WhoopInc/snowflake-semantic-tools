"""SST-CFG036: a `skills.extensions` or `tags` entry cannot be qualified to a three-part name."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.app.compile.project import consumed_extensions
from snowflake_semantic_tools.domain.diagnostics import Origin, Severity
from snowflake_semantic_tools.domain.model.project import SemanticViewProject
from tests.helpers.artifact_builders import target
from tests.helpers.diagnostic_filters import coded
from tests.helpers.seam_projects import SmallProject, view_file


def test_sst_cfg036_fires() -> None:
    resolved, [diagnostic] = consumed_extensions(
        {"skills": {"extensions": {"glossary": None}}}, target(), file="sst_config.yaml"
    )
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG036", Severity.ERROR)
    assert diagnostic.message == (
        "skills.extensions: 'glossary' cannot be qualified -- no fqn: and the block sets no default_prefix"
    )
    assert diagnostic.suggestion == (
        "set fqn: on the entry under `skills.extensions` or `tags:`, or set default_prefix on that block"
    )
    assert (diagnostic.subject, diagnostic.origin) == ("config:skills.extensions.glossary", Origin("sst_config.yaml"))
    assert resolved == {}


def test_sst_cfg036_silent() -> None:
    resolved, diagnostics = consumed_extensions(
        {"skills": {"extensions": {"default_prefix": "DB.EXT", "glossary": None}}}, target()
    )
    assert (resolved["glossary"].sql, diagnostics) == ("DB.EXT.GLOSSARY", ())


def _tagged(root: Path, tags: str, ref: str = "tier") -> SemanticViewProject:
    return SmallProject(
        root,
        files={
            "sst_config.yml": f"project:\n  semantic_models_dir: semantic_models\ntags:\n{tags}",
            **view_file(
                f"    config:\n      tags:\n        - name: \"{{{{ tag('{ref}') }}}}\"\n          value: gold\n"
            ),
        },
    ).load()


def test_sst_cfg036_fires_for_a_tag_with_no_fqn_and_no_default_prefix(tmp_path: Path) -> None:
    for index, (tags, ref, reason) in enumerate(
        (
            ("  tier: {}\n", "tier", "the block sets no default_prefix"),
            ("  default_prefix: ''\n  tier: {}\n", "tier", "the block sets no default_prefix"),
            ("  default_prefix: DB.GOV\n  gov.tier: {}\n", "gov.tier", "the key is not a single identifier"),
        )
    ):
        project = _tagged(tmp_path / str(index), tags, ref)
        [diagnostic] = coded(project.diagnostics, "SST-CFG036")
        assert diagnostic.message == f"tags: '{ref}' cannot be qualified -- no fqn: and {reason}"
        assert (diagnostic.subject, diagnostic.origin) == (f"config:tags.{ref}", Origin("sst_config.yml"))
        # Never a name with a leading dot.
        assert project.views == ()


def test_sst_cfg036_silent_for_a_tag_its_fqn_names(tmp_path: Path) -> None:
    for index, tags in enumerate(
        ("  tier:\n    fqn: OTHER.GOV.TIER_LEVEL\n", "  gov.tier:\n    fqn: OTHER.GOV.TIER_LEVEL\n")
    ):
        ref = tags.split(":")[0].strip()
        project = _tagged(tmp_path / str(index), tags, ref)
        assert coded(project.diagnostics, "SST-CFG036") == []
        [view] = project.views
        assert [(tag.name, tag.value) for tag in view.tags] == [("OTHER.GOV.TIER_LEVEL", "gold")]
    [prefixed] = _tagged(tmp_path / "prefixed", "  default_prefix: '{{ target.database }}.GOV'\n  tier: {}\n").views
    assert [tag.name for tag in prefixed.tags] == ["DB.GOV.TIER"]
