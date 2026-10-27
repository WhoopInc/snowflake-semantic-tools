"""An agent's or CoWork profile's orchestration model left on `auto`, and `allow_auto_model` keeping it so.

Each project compiles in memory, with each model's own `settings` standing for its `config:` and
the tree's `agents:` and `cowork_profiles:` blocks, root and folder routes, resolving the rest.
"""

from __future__ import annotations

from collections.abc import Mapping

import pytest

from snowflake_semantic_tools.app.compile.project import CompileProject
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.agent import AgentProfile
from tests.helpers.cowork_profiles import cowork_profile, described_agent
from tests.helpers.diagnostic_filters import coded
from tests.helpers.eval_inputs import settings_of
from tests.helpers.project_inputs import InMemoryProjectInputs

PINNED = "claude-sonnet-4-6"
ALLOWED = {"orchestration_models": ["auto", PINNED]}


def _warned(tree: Mapping[str, object], **config: Mapping[str, object]) -> list[str]:
    """Compile `sales` under agents/ops/, `billing`, and a profile routing to `sales`; return what SST-VAL560 names.

    Args:
        config: The `config:` settings of the model of each name: `sales`, `billing`, `everyone`.
    """
    agents = (
        described_agent("sales", folder=("ops",), settings=settings_of(config.get("sales", {}))),
        described_agent(
            "billing", profile=AgentProfile(display_name="Billing"), settings=settings_of(config.get("billing", {}))
        ),
    )
    profile = cowork_profile(settings=settings_of(config.get("everyone", {})))
    inputs = InMemoryProjectInputs(
        tree={"snowflake": ALLOWED, **tree}, agent_models=agents, cowork_profile_models=(profile,)
    )
    found = coded(CompileProject(inputs).run().diagnostics, "SST-VAL560")
    assert {item.severity for item in found} <= {Severity.WARNING}
    return [str(item.subject) for item in found]


EVERY = ["agent:sales", "agent:billing", "cowork_profile:everyone"]


def test_an_unset_and_an_explicit_auto_model_are_each_warned_once_per_agent_and_profile() -> None:
    assert _warned({}) == EVERY
    explicit = {"agents": {"+orchestration_model": "auto"}, "cowork_profiles": {"+orchestration_model": "auto"}}
    assert _warned(explicit) == EVERY
    assert _warned({}, sales={"orchestration_model": "auto"}) == EVERY


def test_a_pinned_model_is_silent_for_the_subject_that_pins_it() -> None:
    pinned = {"agents": {"+orchestration_model": PINNED}, "cowork_profiles": {"+orchestration_model": PINNED}}
    assert _warned(pinned) == []
    assert _warned({}, billing={"orchestration_model": PINNED}) == ["agent:sales", "cowork_profile:everyone"]
    # A route pins only the agents under its folder.
    assert _warned({"agents": {"ops": {"+orchestration_model": PINNED}}}) == [
        "agent:billing",
        "cowork_profile:everyone",
    ]


@pytest.mark.parametrize(
    ("tree", "config", "warned"),
    [
        ({"agents": {"+allow_auto_model": True}}, {}, ["cowork_profile:everyone"]),
        ({"agents": {"ops": {"+allow_auto_model": True}}}, {}, ["agent:billing", "cowork_profile:everyone"]),
        ({}, {"billing": {"allow_auto_model": True}}, ["agent:sales", "cowork_profile:everyone"]),
        ({"cowork_profiles": {"+allow_auto_model": True}}, {}, ["agent:sales", "agent:billing"]),
        ({}, {"everyone": {"allow_auto_model": True}}, ["agent:sales", "agent:billing"]),
        # An agent's own `config:` wins over the root, both ways.
        (
            {"agents": {"+allow_auto_model": True}},
            {"sales": {"allow_auto_model": False}},
            ["agent:sales", "cowork_profile:everyone"],
        ),
    ],
    ids=["agents-root", "agents-route", "agent-config", "profiles-root", "profile-config", "config-wins"],
)
def test_allow_auto_model_keeps_auto_for_the_subjects_it_resolves_for_only(
    tree: Mapping[str, object], config: Mapping[str, Mapping[str, object]], warned: list[str]
) -> None:
    assert _warned(tree, **config) == warned
