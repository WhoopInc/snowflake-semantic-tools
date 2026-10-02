"""What a minted dataset carries besides its rows, and what SST never renders around it.

Two catalog conditions cannot arise from these renderers, and the tests pin why: the run
config never carries Snowflake's `dataset:` block, so no run re-creates a dataset that exists;
and a dataset version's METADATA and COMMENT are built from identifiers, hex digests and a
commit only, so no authored text -- personal or otherwise -- reaches them.
"""

from __future__ import annotations

import json
import re
from dataclasses import replace

import pytest

from snowflake_semantic_tools.domain.model.eval import EVAL_MINT_AUTO, EVAL_MINT_NEVER, EvalDatasetConfig
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.render.eval import (
    dataset_version_metadata,
    dataset_version_name,
    is_git_sha,
    render_add_version_statement,
    render_eval_config,
)
from tests.helpers.eval_builders import resolved_eval

AGENT = QualifiedName.parse("DB.S.SALES_AGENT")
SOURCE = QualifiedName.parse("DB.S.EVAL_SRC_SALES_AGENT_ABCDEF0")
DATASET = QualifiedName.parse("DB.S.EVAL_SALES_AGENT_ABCDEF0")
FINGERPRINT = "abcdef0123456789" * 4


@pytest.mark.parametrize("mint", [EVAL_MINT_AUTO, EVAL_MINT_NEVER, None])
def test_a_run_config_has_only_evaluation_and_metrics_so_it_never_re_creates_a_dataset(mint: str | None) -> None:
    value = resolved_eval()
    assert value.config.dataset is not None
    config = replace(value.config, dataset=replace(value.config.dataset, mint=mint))
    for dataset_config in (config, replace(config, dataset=None), replace(config, dataset=EvalDatasetConfig())):
        rendered = render_eval_config(dataset_config, value.custom_metrics, agent_target=AGENT, dataset_target=DATASET)
        top_level = [line.split(":", 1)[0] for line in rendered.splitlines() if line and not line.startswith(" ")]
        assert top_level == ["evaluation", "metrics"]


@pytest.mark.parametrize("git_sha", ["abc1234", "0123456789abcdef0123456789abcdef01234567", "WORKTREE", "", "a@b.co"])
def test_a_version_metadata_holds_identifiers_digests_and_only_a_commit_that_is_one(git_sha: str) -> None:
    metadata = json.loads(
        dataset_version_metadata(
            agent_target=AGENT, source_table=SOURCE, dataset_fingerprint=FINGERPRINT, git_sha=git_sha
        )
    )
    expected = {"agent": AGENT.sql, "dataset_fingerprint": FINGERPRINT, "source_table": SOURCE.sql}
    assert metadata == ({**expected, "git_sha": git_sha} if is_git_sha(git_sha) else expected)


def test_a_version_comment_is_its_digest_and_its_name_follows_the_questions() -> None:
    version = dataset_version_name(FINGERPRINT)
    assert version == "SST_ABCDEF012345" == dataset_version_name(FINGERPRINT)
    statement = str(render_add_version_statement(DATASET, SOURCE, version=version, metadata="{}"))
    [comment] = re.findall(r"COMMENT = '([^']*)'", statement)
    assert comment == "SST eval questions abcdef012345"
