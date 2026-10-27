"""Plan's member diff for a CoWork profile, a Snowflake private preview: read as an agent is, keyed as its own kind."""

from __future__ import annotations

import json
from dataclasses import replace
from typing import Any

from snowflake_semantic_tools.app.compile.cowork_profiles import CompiledCoworkProfile
from snowflake_semantic_tools.app.fanout import Fanout
from snowflake_semantic_tools.app.plan_diff import compiled_fingerprints, diff_updates, from_state
from snowflake_semantic_tools.domain.model.agent import AgentProfile
from snowflake_semantic_tools.domain.model.lifecycle import Action, Change, ChangeReason
from snowflake_semantic_tools.domain.plan.member_diff import CHANGED, MemberDiff, PropertyDiff
from snowflake_semantic_tools.domain.ports.snowflake.catalog import AgentVersion
from tests.helpers.cowork_profiles import compile_profiles, compiled_profiles, cowork_profile, described_agent
from tests.helpers.plan_codes import entry
from tests.helpers.snowflake_fake import FakeSnowflake


def _profile(**fields: Any) -> CompiledCoworkProfile:
    [profile] = compiled_profiles(compile_profiles(cowork_profile(**fields), agents=(described_agent(),)))
    return profile


def _update(profile: CompiledCoworkProfile) -> Change:
    rendered = profile.rendered_artifact
    return Change(
        profile.artifact_key, "cowork_profile", Action.UPDATE, ChangeReason.FINGERPRINT_DIFFERS, rendered, None, (), 0
    )


def test_a_profile_is_compared_with_the_version_it_serves_and_its_properties_as_show_lists_them() -> None:
    profile = _profile(
        comment="Every question.", display=AgentProfile(display_name="Everyone"), response_instructions="Brief."
    )
    served = json.loads(profile.payload)
    served["instructions"] = {"response": "Long."}
    port = FakeSnowflake()
    target = profile.target.sql
    port.agent_version_rows[target] = (AgentVersion("VERSION$1", None, True, specification=json.dumps(served)),)
    port.show_rows[f"AGENT {target}"] = {
        "comment": f"Some questions. [sst:{'a' * 64}:{'b' * 64}]",
        "profile": '{"display_name":"Everyone"}',
        "is_secure": "true",
    }
    [compared], refused = diff_updates((_update(profile),), {profile.artifact_key: profile}, Fanout(port))
    assert refused == ()
    assert compared.member_diff == (
        MemberDiff("instruction", "response", CHANGED, (PropertyDiff("text", "Long.", "Brief."),)),
        MemberDiff(
            "cowork_profile",
            "properties",
            CHANGED,
            # A profile sets no SECURE of its own, so it publishes, and is compared as, false.
            (PropertyDiff("comment", "Some questions.", "Every question."), PropertyDiff("secure", "true", "false")),
        ),
    )


def test_a_profile_is_fingerprinted_member_by_member_with_its_own_properties_key() -> None:
    keys = [key for key, _ in compiled_fingerprints(_profile())]
    assert "cowork_profile:properties" in keys and "tool:router" in keys
    assert "agent:properties" not in keys


def test_a_profile_s_properties_are_named_from_state_only_when_its_entry_recorded_them() -> None:
    spec = ("tool:router", "1" * 64)
    profile = _profile()
    rendered = replace(profile.rendered_artifact, member_fingerprints=(("cowork_profile:properties", "2" * 64), spec))
    change = replace(_update(profile), rendered=rendered)
    older = {change.key: replace(entry(rendered, "m"), member_fingerprints=(spec,))}
    assert from_state((change,), older)[0].changed_members == ()
    recorded = {
        change.key: replace(older[change.key], member_fingerprints=(("cowork_profile:properties", "3" * 64), spec))
    }
    assert from_state((change,), recorded)[0].changed_members == ("cowork_profile:properties",)
