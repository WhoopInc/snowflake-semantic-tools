"""What `sst plan` shows for a CoWork profile (a Snowflake private preview) it attaches to CoWork."""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.cli.plan_output import change_json
from snowflake_semantic_tools.cli.render.report import plan_lines
from snowflake_semantic_tools.cli.render.style import Style
from snowflake_semantic_tools.cli.render.terminal import Terminal
from snowflake_semantic_tools.domain.model.lifecycle import Action, ChangeReason
from snowflake_semantic_tools.domain.plan.attach import attach_changes
from tests.helpers.artifact_builders import changeset
from tests.helpers.cowork_attach import COWORK, MANIFEST, planned, profile, reading

_STYLE = Style(Terminal(width=140), stdout=True)


def test_a_detached_profile_plans_an_update_that_only_attaches_it_again() -> None:
    [change], _ = attach_changes((planned(profile(), Action.NOOP, ChangeReason.UNCHANGED),), reading())
    # Planned from the manifest that published it, so only the attachment differs.
    plan = replace(changeset(change), manifest_id=MANIFEST)
    lines = plan_lines(plan, _STYLE)
    row = next(line for line in lines if "cowork_profile:everyone" in line)
    assert row.split() == [
        "~",
        "update",
        "cowork_profile:everyone",
        "SNOWFLAKE_INTELLIGENCE.PROFILES.EVERYONE",
        "not",
        "attached",
        "to",
        "CoWork;",
        "CoWork",
        "attachment;",
        "no",
        "new",
        "version",
    ]
    assert f"      attached: no → {COWORK}" in plan_lines(plan, _STYLE, full=True)
    document = change_json(change, MANIFEST)
    assert (document["action"], document["reason"], document["attaches_to"]) == ("update", "detached", COWORK)
    assert document["properties"] == [{"property": "attached", "before": "no", "after": COWORK}]
    assert document["adds_version"] is False


def test_a_created_profile_names_what_it_attaches_to_in_json_and_no_property() -> None:
    [change], _ = attach_changes((planned(profile(), Action.CREATE, ChangeReason.NOT_PRESENT),), reading())
    document = change_json(change, MANIFEST)
    assert (document["attaches_to"], document["properties"]) == (COWORK, [])
    rehearsal = planned(profile(schema="PROFILES_DEV", wants=False), Action.CREATE, ChangeReason.NOT_PRESENT)
    assert change_json(rehearsal, MANIFEST)["attaches_to"] is None
