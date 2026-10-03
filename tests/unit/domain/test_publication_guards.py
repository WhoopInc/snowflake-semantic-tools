"""The publication guards' remaining shapes: each sensitive pattern, malformed METADATA, and an
eval that mints no dataset version.
"""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.model.lifecycle import CompositeObservation
from snowflake_semantic_tools.domain.render.eval import dataset_version_comment
from snowflake_semantic_tools.domain.validate.publication import (
    dataset_metadata_diagnostics,
    eval_publication_diagnostics,
    run_config_diagnostics,
)
from tests.helpers.artifact_builders import rendered

CLEAN = '{"agent":"DB.S.AGENT","dataset_fingerprint":"' + "0" * 64 + '","source_table":"DB.S.SRC"}'


@pytest.mark.parametrize(
    ("metadata", "comment", "expected"),
    [
        (CLEAN, "SST eval questions 123-45-6789", [("COMMENT", "a US social security number")]),
        (CLEAN, "card 4111 1111 1111 1111", [("COMMENT", "a payment card number")]),
        ("not json", "SST", [("METADATA", "text that is not a JSON object")]),
        ('["DB.S.AGENT"]', "SST", [("METADATA", "text that is not a JSON object")]),
        (CLEAN, dataset_version_comment("SST_0123456789AB"), []),
    ],
)
def test_dataset_metadata_reports_each_field_by_what_it_matches(
    metadata: str, comment: str, expected: list[tuple[str, str]]
) -> None:
    found = dataset_metadata_diagnostics("eval:e", metadata=metadata, comment=comment)
    assert [(item.context["field"], item.context["detail"]) for item in found] == expected


def test_a_config_without_a_dataset_block_is_quiet_whether_or_not_the_dataset_exists() -> None:
    config = "evaluation:\n  source_metadata:\n    dataset_name: D\nmetrics: []\n"
    assert run_config_diagnostics("eval:e", config, dataset_exists=True) == ()


def test_an_artifact_that_mints_no_version_has_only_its_config_checked() -> None:
    artifact = rendered()
    assert eval_publication_diagnostics(artifact, CompositeObservation(artifact.key)) == ()
