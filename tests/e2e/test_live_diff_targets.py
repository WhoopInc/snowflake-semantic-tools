"""Two targets compared member by member against a real account: they agree, then differ in one member.

The live project is published twice, to targets `a` and `b`, each in its own scratch schema
with its own state table. Every fingerprint differs between them, the schemas being part of each
render, so `sst diff --from a --to b` agrees only because each target's physical names are
replaced by what they stand for before the members are compared. A metric's description then
changed and published to `b` alone is the one difference it reports.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
import yaml

from snowflake_semantic_tools.adapters.snowflake.connector import SnowflakeConnector
from snowflake_semantic_tools.domain.model.identifier import SchemaScope
from tests.helpers.e2e_cli import run_sst
from tests.helpers.live_project import PROFILE, orders_table_statements, project_args, published_project
from tests.helpers.live_snowflake import LiveAccount, profile_target

pytestmark = [pytest.mark.live, pytest.mark.e2e]


def test_two_targets_agree_member_by_member_then_differ_in_the_one_edited(
    live_account: LiveAccount,
    live_connector: SnowflakeConnector,
    scratch_schema: Callable[[str], SchemaScope],
    tmp_path_factory: pytest.TempPathFactory,
) -> None:
    first, second = scratch_schema("diffa"), scratch_schema("diffb")
    project, manifest = published_project(
        tmp_path_factory.mktemp("diff"), live_connector, first, key_pair=live_account.key_pair
    )
    made = live_connector.execute_script(orders_table_statements(second))
    assert made.ok, made.error
    # One profile, two targets: the same project, each in its own schema.
    outputs = {
        name: profile_target(scope.schema.value, key_pair=live_account.key_pair)
        for name, scope in (("a", first), ("b", second))
    }
    (project / "profiles.yml").write_text(
        yaml.safe_dump({PROFILE: {"target": "a", "outputs": outputs}}, sort_keys=False), encoding="utf-8"
    )
    args = project_args(project, manifest)
    for target in ("a", "b"):
        compiled = run_sst("--output", "json", "compile", *args, "--target", target)
        assert compiled.exit_code == 0, compiled.stdout
        applied = run_sst("--output", "json", "apply", *args, "--target", target, "--yes")
        assert applied.exit_code == 0, applied.stdout

    by_fingerprint = run_sst("--output", "json", "diff", *args, "--from", "a", "--to", "b", "--no-diff")
    assert by_fingerprint.exit_code == 2, by_fingerprint.stdout
    agreed = run_sst("--output", "json", "diff", *args, "--from", "a", "--to", "b")
    assert (agreed.exit_code, agreed.envelope["data"]["differences"]) == (0, []), agreed.stdout

    # A metric's description changed, then published to b alone, as a change is promoted.
    metrics = project / "semantic_models" / "metrics" / "live.yml"
    text = metrics.read_text(encoding="utf-8")
    metrics.write_text(text.replace("Number of distinct orders placed.", "Orders placed, each once."), encoding="utf-8")
    for step in ("compile", "apply"):
        ran = run_sst("--output", "json", step, *args, "--target", "b", *(("--yes",) if step == "apply" else ()))
        assert ran.exit_code == 0, ran.stdout
    differed = run_sst("--output", "json", "diff", *args, "--from", "a", "--to", "b")
    assert differed.exit_code == 2, differed.stdout
    [difference] = differed.envelope["data"]["differences"]
    assert difference["key"] == "semantic_view:live_orders"
    [member] = difference["diff"]
    assert (member["kind"], member["op"]) == ("metric", "changed")
    assert [(prop["property"], prop["before"], prop["after"]) for prop in member["properties"]] == [
        ("comment", "Number of distinct orders placed.", "Orders placed, each once.")
    ]
