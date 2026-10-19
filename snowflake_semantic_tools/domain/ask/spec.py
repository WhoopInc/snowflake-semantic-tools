"""Point an agent's compiled specification at scratch copies of what it calls, publishing nothing.

`sst ask` runs an agent as it compiles now. What the agent calls -- a semantic view, a search
service, a procedure or function, another agent, a skill -- is reached by name, so an edit to
one of them is tried only through a copy under another name. `rewrite_spec` renames each
reference the specification's `tool_resources` and `skills` hold, and nothing else: the tools,
their descriptions and the instructions are the compiled ones.

With copies in play, an inaccessible tool must fail the run rather than be skipped quietly, so
the rewritten specification sets `orchestration.tool_not_accessible: reject` unless the agent
already says what to do.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass

# The `tool_resources` fields that name an object, by what each names.
_RESOURCE_FIELDS = ("semantic_view", "search_service", "identifier", "agent_name")
TOOL_NOT_ACCESSIBLE = "tool_not_accessible"
REJECT = "reject"


@dataclass(frozen=True, slots=True)
class Rename:
    """Where one published object's scratch copy is.

    Attributes:
        name: The copy's qualified name, as SQL.
        version: For a skill, the copy's version alias; None for any other object.
    """

    name: str
    version: str | None = None


def rewrite_spec(spec_json: str, renames: Mapping[str, Rename]) -> tuple[str, tuple[str, ...]]:
    """Return the specification with each renamed reference pointed at its copy, and what was renamed.

    Args:
        renames: By the published object's qualified name, casefolded, where its copy is.

    Returns:
        The rewritten specification as JSON, and the casefolded names it renamed, in the order
        it met them. With no renames, the specification is returned as it was given.

    Raises:
        ValueError: the specification is not a JSON object.
    """
    if not renames:
        return spec_json, ()
    spec = json.loads(spec_json)
    if not isinstance(spec, dict):
        raise ValueError("an agent specification is a JSON object")
    renamed: list[str] = []
    resources = spec.get("tool_resources")
    for resource in resources.values() if isinstance(resources, dict) else ():
        if not isinstance(resource, dict):
            continue
        for field in _RESOURCE_FIELDS:
            value = resource.get(field)
            rename = renames.get(value.casefold()) if isinstance(value, str) else None
            if rename is not None and isinstance(value, str):
                resource[field] = rename.name
                renamed.append(value.casefold())
    skills = spec.get("skills")
    for skill in skills if isinstance(skills, list) else ():
        source = skill.get("source") if isinstance(skill, dict) else None
        path = source.get("path") if isinstance(source, dict) else None
        rename = renames.get(path.casefold()) if isinstance(path, str) else None
        if rename is not None and isinstance(source, dict) and isinstance(path, str):
            source["path"] = rename.name
            if rename.version is not None:
                source["version"] = rename.version
            renamed.append(path.casefold())
    if renamed:
        orchestration = spec.setdefault("orchestration", {})
        if isinstance(orchestration, dict):
            orchestration.setdefault(TOOL_NOT_ACCESSIBLE, REJECT)
    return json.dumps(spec, separators=(",", ":")), tuple(renamed)


def referenced_names(spec_json: str) -> tuple[str, ...]:
    """Return every object the specification's `tool_resources` and `skills` name, casefolded, in order.

    Raises:
        ValueError: the specification is not a JSON object.
    """
    spec = json.loads(spec_json)
    if not isinstance(spec, dict):
        raise ValueError("an agent specification is a JSON object")
    found: list[str] = []
    resources = spec.get("tool_resources")
    for resource in resources.values() if isinstance(resources, dict) else ():
        for field in _RESOURCE_FIELDS:
            value = resource.get(field) if isinstance(resource, dict) else None
            if isinstance(value, str):
                found.append(value.casefold())
    skills = spec.get("skills")
    for skill in skills if isinstance(skills, list) else ():
        source = skill.get("source") if isinstance(skill, dict) else None
        path = source.get("path") if isinstance(source, dict) else None
        if isinstance(path, str):
            found.append(path.casefold())
    return tuple(dict.fromkeys(found))
