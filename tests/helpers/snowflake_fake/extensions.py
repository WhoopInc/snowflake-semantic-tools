"""Cortex extensions as the fake keeps them, and the statements that change them.

An extension is a dict: `type`, `comment`, `versions` (each a dict of `name`, `alias`,
`location`, `files`, `sources`, `is_default`, `certification`), and `live`, the open live version
or None. A version's `sources` maps each file to the staged path holding its bytes.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError


def versions(extension: Mapping[str, object]) -> list[dict[str, object]]:
    """The extension's committed versions, oldest first."""
    listed = extension["versions"]
    assert isinstance(listed, list)
    return listed


def files(version: Mapping[str, object]) -> list[str]:
    """The paths in a version, relative to its location."""
    listed = version["files"]
    assert isinstance(listed, list)
    return listed


def quoted_arguments(statement: str) -> tuple[str, ...]:
    """The single-quoted literals in a statement, in order, each with `''` read as `'`."""
    values: list[str] = []
    current: list[str] = []
    quoted = False
    index = 0
    while index < len(statement):
        character = statement[index]
        if character == "'":
            if quoted and index + 1 < len(statement) and statement[index + 1] == "'":
                current.append("'")
                index += 1
            else:
                quoted = not quoted
                if not quoted:
                    values.append("".join(current))
                    current = []
        elif quoted:
            current.append(character)
        index += 1
    return tuple(values)


def apply_extension_statement(
    extensions: dict[str, dict[str, object]], stage_files: Sequence[str], normalized: str, original: str
) -> None:
    """Apply one CREATE or ALTER CORTEX EXTENSION statement that succeeded.

    Raises:
        SnowflakePortError: the statement alters an extension that does not exist, or commits
            a live version it has not opened.
    """
    tokens = normalized.split()
    upper = [token.upper() for token in tokens]
    quoted = tuple(value.replace("\\\\", "\\") for value in quoted_arguments(original))
    if upper[0] == "CREATE":
        name = tokens[6]
        if name not in extensions:
            extensions[name] = _created(name, quoted)
            if upper[7:8] == ["FROM"] and upper[9:10] == ["VERSION"]:
                # MEASURED: CREATE ... FROM <prefix> VERSION <alias> makes VERSION$1 that bundle.
                source = tokens[8]
                sources = _sources(stage_files, source)
                extensions[name]["versions"] = []
                _add_version(name, extensions[name], tokens[10], list(sources), sources)
        return
    name = tokens[3]
    if name not in extensions:
        raise SnowflakePortError(f"Cortex extension {name} does not exist")
    _alter(name, extensions[name], tokens, " ".join(upper[4:]), quoted, stage_files)


def _created(name: str, quoted: tuple[str, ...]) -> dict[str, object]:
    return {
        "type": quoted[0].upper(),
        "comment": quoted[1] if len(quoted) > 1 else None,
        "versions": [
            {
                "name": "VERSION$1",
                "alias": None,
                "location": f"snow://cortex_extension/{name}/versions/version$1/",
                "files": [],
                "is_default": False,
                "certification": None,
            }
        ],
        "live": None,
    }


def _alter(
    name: str,
    extension: dict[str, object],
    tokens: list[str],
    clause: str,
    quoted: tuple[str, ...],
    stage_files: Sequence[str],
) -> None:
    if clause.startswith("ADD VERSION ") and " FROM " in clause:
        sources = _sources(stage_files, tokens[8])
        _add_version(name, extension, tokens[6], list(sources), sources)
    elif clause.startswith("ADD LIVE VERSION "):
        extension["live"] = {
            "alias": tokens[7],
            "location": f"snow://cortex_extension/{name}/versions/live/",
            "files": [],
            "sources": {},
        }
    elif clause == "COMMIT":
        live = extension["live"]
        if not isinstance(live, dict):
            raise SnowflakePortError(f"Cortex extension {name} has no open live version")
        uploaded = live.get("sources")
        _add_version(name, extension, str(live["alias"]), files(live), uploaded if isinstance(uploaded, dict) else {})
        extension["live"] = None
    elif clause == "ABORT":
        extension["live"] = None
    elif clause.startswith("SET COMMENT"):
        extension["comment"] = quoted[0]
    elif clause.startswith("VERSION ") and "SET TAG" in clause:
        label = tokens[5].casefold()
        for version in versions(extension):
            if label in (str(version["alias"]).casefold(), str(version["name"]).casefold()):
                version["certification"] = quoted[0]


def _sources(stage_files: Sequence[str], source: str) -> dict[str, str]:
    """Each staged file under `source`, by its path below it, mapped to its staged path."""
    return {path[len(source) :]: path for path in sorted(stage_files) if path.startswith(source)}


def version_source(extensions: Mapping[str, Mapping[str, object]], path: str) -> str | None:
    """The staged path holding the bytes of a file of a committed or live version; None when none does."""
    for extension in extensions.values():
        live = extension.get("live")
        for version in (*versions(extension), *((live,) if isinstance(live, dict) else ())):
            location = str(version["location"])
            sources = version.get("sources")
            if path.casefold().startswith(location.casefold()) and isinstance(sources, dict):
                found = sources.get(path[len(location) :])
                return str(found) if found is not None else None
    return None


def _add_version(
    name: str, extension: dict[str, object], alias: str, contents: list[str], sources: dict[str, str]
) -> None:
    listed = versions(extension)
    if any(str(version["alias"]).casefold() == alias.casefold() for version in listed):
        raise SnowflakePortError(f"version alias {alias} already exists in {name}")
    system = f"VERSION${len(listed) + 1}"
    for version in listed:
        version["is_default"] = False
    listed.append(
        {
            "name": system,
            "alias": alias,
            "location": f"snow://cortex_extension/{name}/versions/{system.lower()}/",
            "files": list(contents),
            "sources": dict(sources),
            "is_default": True,
            "certification": None,
        }
    )
