"""The specification SST writes for a CoWork profile: a router agent over subagents.

A profile renders as an agent whose first tool is the router, named `router`, whose
`tool_resources` entry lists the subagents; its other tools are built-ins with no resources.
It sets no `orchestration` section. A subagent's sample questions are plain text, not the
`question:` entries an agent's instructions take: the router refuses entries.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType

from snowflake_semantic_tools.domain.model.authored_schema import ValueKind
from snowflake_semantic_tools.domain.model.snowflake_spec import docs
from snowflake_semantic_tools.domain.model.snowflake_spec.agent import instructions_key, skills_key
from snowflake_semantic_tools.domain.model.snowflake_spec.model import (
    Authored,
    Derived,
    Documented,
    Fixed,
    Measured,
    Setting,
    SpecKey,
    SpecNode,
)

_KIND = "cowork_profile"
_STRING, _MAPPING, _LIST = ValueKind.STRING, ValueKind.MAPPING, ValueKind.LIST
# The router's tool type and name, and the source type each subagent's entry names.
ROUTER_TYPE = "agent_router"
ROUTER_NAME = "router"
SUBAGENT_SOURCE = "DATA_AGENT"


def _profile(path: str) -> Authored:
    return Authored(_KIND, path)


def _subagent(key: str) -> Authored:
    return _profile(f"spec.subagents.{key}")


ROUTER_RESOURCES = SpecNode(
    (
        SpecKey(
            "subagents",
            _LIST,
            "The agents the router chooses between, in order.",
            items=SpecNode(
                (
                    SpecKey("name", _STRING, "The subagent's three-part name.", (Derived("its agent's name"),)),
                    SpecKey("display_name", _STRING, "How the router names it.", (_subagent("display_name"),)),
                    SpecKey("description", _STRING, "What the router reads to choose it.", (_subagent("description"),)),
                    SpecKey(
                        "source",
                        _MAPPING,
                        "The agent the subagent runs.",
                        node=SpecNode(
                            (
                                SpecKey("type", _STRING, "The kind of source.", (Fixed(SUBAGENT_SOURCE),)),
                                SpecKey("fqn", _STRING, "The agent's three-part name.", (_subagent("agent"),)),
                                SpecKey(
                                    "sample_questions",
                                    _LIST,
                                    "Questions the subagent answers, as plain text.",
                                    (_subagent("sample_questions.question"),),
                                    items=ValueKind.STRING,
                                    evidence=Measured("An incomplete subagent entry still routes"),
                                ),
                            )
                        ),
                    ),
                )
            ),
        ),
    ),
    Documented(docs.COWORK_ROUTING),
)
COWORK_SPEC = SpecNode(
    (
        SpecKey(
            "models",
            _MAPPING,
            "The models the router runs.",
            node=SpecNode(
                (
                    SpecKey(
                        "orchestration",
                        _STRING,
                        "The orchestration model.",
                        (Setting("cowork_profiles", "orchestration_model"),),
                    ),
                ),
                Documented(docs.AGENTS_REST_API),
            ),
        ),
        instructions_key(_KIND),
        SpecKey(
            "tools",
            _LIST,
            "The router first, then the profile's built-in tools.",
            items=SpecNode(
                (
                    SpecKey(
                        "tool_spec",
                        _MAPPING,
                        "The tool.",
                        node=SpecNode(
                            (
                                SpecKey(
                                    "type",
                                    _STRING,
                                    "The tool's type.",
                                    (Fixed(ROUTER_TYPE), _profile("spec.tools.type")),
                                ),
                                SpecKey(
                                    "name",
                                    _STRING,
                                    "The tool's name.",
                                    (Fixed(ROUTER_NAME), _profile("spec.tools.name"), Derived("its type")),
                                ),
                                SpecKey(
                                    "description",
                                    _STRING,
                                    "Always empty for a built-in.",
                                    (Fixed(""),),
                                ),
                            )
                        ),
                    ),
                )
            ),
        ),
        SpecKey(
            "tool_resources",
            _MAPPING,
            "The router's resources, under its name; built-ins have none.",
            node=SpecNode((SpecKey(ROUTER_NAME, _MAPPING, "The router's subagents.", node=ROUTER_RESOURCES),)),
        ),
        skills_key(_KIND),
    ),
    Documented(docs.COWORK_PROFILES),
)
# Authored blocks whose keys SST copies into the specification unread, with where each lands.
COWORK_PASSTHROUGH: Mapping[str, tuple[str, ...]] = MappingProxyType({"spec.passthrough": ()})
# What a profile's files and settings hold that is not in its specification, with where it goes.
COWORK_NOT_IN_SPEC: Mapping[str, str] = MappingProxyType(
    {
        "cowork_profile:name": "the router agent object's name",
        "cowork_profile:comment": "the object's COMMENT",
        "cowork_profile:display.display_name": "the object's PROFILE, as an agent's profile",
        "cowork_profile:display.avatar": "the object's PROFILE, as an agent's profile",
        "cowork_profile:display.color": "the object's PROFILE, as an agent's profile",
        "cowork_profile:deprecated": "SST's own lifecycle marker",
        "cowork_profile:meta": "free-form metadata SST never sends",
        "cowork_profile:evals.dataset": "the profile's evals, which SST runs itself",
        "cowork_profile:evals.config": "the profile's evals, which SST runs itself",
        "cowork_profiles.database": "where the object is created",
        "cowork_profiles.schema": "where the object is created",
        "cowork_profiles.allow_auto_model": "a check on orchestration_model, not a value sent",
        "cowork_profiles.alias": "the version alias SST commits",
        "cowork_profiles.enabled": "whether SST publishes the profile at all",
        "cowork_profiles.attach": "whether SST attaches the profile to CoWork, a separate statement",
        "cowork_profiles.tags": "object tags, set by ALTER AGENT",
    }
)
