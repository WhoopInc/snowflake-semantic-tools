"""An agent's definition: the specification SST renders, and the one the version it serves holds.

Members are the parts of the specification an author edits apart: its models and orchestration
settings, each instruction, its sample questions, each tool with its resources, and each skill.
A value that is not text is compared as JSON with its keys sorted, so two specifications that differ
only in key order or spacing agree. Any other top-level key is one member of its own, so nothing the
specification holds goes uncompared.

The agent's own properties, which no version holds, are one more member, `agent properties`: its
comment without the ownership marker, its profile, its version alias, and whether it is SECURE.
"""

from __future__ import annotations

import json
from collections.abc import Sequence

from snowflake_semantic_tools.domain.model.lifecycle.marker import unmarked_comment
from snowflake_semantic_tools.domain.plan.member_diff import Definition, Member, Value, unique

# The member holding the agent's properties, keyed as `member_fingerprints` keys it.
PROPERTIES_KIND = "agent"
PROPERTIES_NAME = "properties"
PROPERTIES_KEY = f"{PROPERTIES_KIND}:{PROPERTIES_NAME}"


def agent_properties(
    *,
    comment: str | None = None,
    profile: str | None = None,
    aliases: Sequence[str] | None = None,
    secure: bool | None = None,
) -> Member:
    """Return the agent's properties as one member; each argument None was not read, and is left out.

    The comment is compared without any ownership marker; the profile as JSON with its keys
    sorted, text that is not JSON as it is; the aliases upper-cased, sorted and comma-joined, as
    an unquoted alias is stored; SECURE as `true` or `false`.
    """
    properties: list[tuple[str, Value]] = []
    if comment is not None:
        properties.append(("comment", unmarked_comment(comment)))
    if profile is not None:
        properties.append(("profile", _profile(profile)))
    if aliases is not None:
        properties.append(("alias", ", ".join(sorted(alias.upper() for alias in aliases))))
    if secure is not None:
        properties.append(("secure", "true" if secure else "false"))
    return Member(PROPERTIES_KIND, PROPERTIES_NAME, tuple(properties))


def with_properties(definition: Definition, properties: Member) -> Definition:
    """Return `definition` with the agent's properties member after its specification's members."""
    return Definition(unique((*definition.members, properties)), definition.kinds)


def _profile(profile: str) -> str:
    try:
        parsed = json.loads(profile) if profile.strip() else {}
    except ValueError:
        return profile
    return _text(parsed)


def spec_definition(spec: str) -> Definition:
    """Return the definition an agent specification holds, from its JSON text.

    Raises:
        ValueError: `spec` is not a JSON object.
    """
    parsed = json.loads(spec)
    if not isinstance(parsed, dict):
        raise ValueError("an agent specification is a JSON object")
    members: list[Member] = []
    resources = parsed.get("tool_resources")
    resources = resources if isinstance(resources, dict) else {}
    for key, value in parsed.items():
        if key == "instructions" and isinstance(value, dict):
            members.extend(_instructions(value))
        elif key == "tools" and isinstance(value, list):
            members.extend(_tools(value, resources))
        elif key == "skills" and isinstance(value, list):
            members.extend(_skills(value))
        elif key != "tool_resources":
            members.append(Member("setting", key, (("value", _text(value)),)))
    named = {member.name.casefold() for member in members if member.kind == "tool"}
    members.extend(
        Member("tool", name, tuple((f"resource.{field}", _text(item)) for field, item in _items(resource)))
        for name, resource in resources.items()
        if name.casefold() not in named
    )
    return Definition(unique(members))


def _instructions(instructions: dict[str, object]) -> list[Member]:
    members = []
    for key, value in instructions.items():
        if key == "sample_questions" and isinstance(value, list):
            questions = tuple(str(item.get("question", "")) if isinstance(item, dict) else str(item) for item in value)
            members.append(Member("instruction", key, (("questions", questions),)))
        else:
            members.append(Member("instruction", key, (("text", _text(value)),)))
    return members


def _tools(tools: list[object], resources: dict[str, object]) -> list[Member]:
    members = []
    for index, item in enumerate(tools):
        spec = item.get("tool_spec") if isinstance(item, dict) else None
        spec = spec if isinstance(spec, dict) else {"tool": item}
        name = str(spec.get("name") or f"#{index + 1}")
        properties: list[tuple[str, Value]] = [
            (field, _text(value)) for field, value in spec.items() if field != "name"
        ]
        properties += [(f"resource.{field}", _text(value)) for field, value in _items(resources.get(name))]
        members.append(Member("tool", name, tuple(properties)))
    return members


def _skills(skills: list[object]) -> list[Member]:
    members = []
    for index, item in enumerate(skills):
        fields = item if isinstance(item, dict) else {"value": item}
        properties = tuple((field, _text(value)) for field, value in fields.items() if field != "name")
        members.append(Member("skill", str(fields.get("name") or f"#{index + 1}"), properties))
    return members


def _items(value: object) -> list[tuple[str, object]]:
    return list(value.items()) if isinstance(value, dict) else []


def _text(value: object) -> str:
    """A value as it compares: text as it is, anything else as JSON with its keys sorted."""
    if isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True, ensure_ascii=False)
