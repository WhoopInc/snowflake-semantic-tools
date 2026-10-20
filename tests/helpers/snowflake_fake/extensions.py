"""Cortex extensions as the fake keeps them, and the statements that change them.

An extension is a dict: `type`, `comment`, `versions` (each a dict of `name`, `alias`,
`location`, `files`, `sources`, `is_default`, `certification`), `live`, the open live version
or None, and `default_pinned`, absent or False until SET DEFAULT_VERSION names a version. A
version's `sources` maps each file to the staged path holding its bytes.

MEASURED on a live account: SET DEFAULT_VERSION takes a version's system name, bare, quoted or
as a string literal, or LAST; an alias is refused (099103, `Invalid default`). A default set
this way stays where it is when a version is added; UNSET DEFAULT_VERSION makes the newest
version the default again, and later versions with it.
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
    """Apply one CREATE, ALTER or DROP CORTEX EXTENSION statement that succeeded.

    Raises:
        SnowflakePortError: the statement alters or drops an extension that does not exist,
            commits a live version it has not opened, or sets a default that is no version's
            system name.
    """
    tokens = normalized.split()
    upper = [token.upper() for token in tokens]
    quoted = tuple(value.replace("\\\\", "\\") for value in quoted_arguments(original))
    if upper[0] == "DROP":
        if_exists = upper[3:5] == ["IF", "EXISTS"]
        name = tokens[5] if if_exists else tokens[3]
        if extensions.pop(name, None) is None and not if_exists:
            raise SnowflakePortError(f"Cortex extension {name} does not exist")
        return
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
    elif clause.startswith("SET DEFAULT_VERSION"):
        _set_default(extension, " ".join(tokens[4:]).split("=", 1)[1].strip().strip("'\""))
    elif clause == "UNSET DEFAULT_VERSION":
        _follow_newest(extension)
    elif clause.startswith("VERSION ") and "SET TAG" in clause:
        label = tokens[5].casefold()
        for version in versions(extension):
            if label in (str(version["alias"]).casefold(), str(version["name"]).casefold()):
                version["certification"] = quoted[0]


def _set_default(extension: dict[str, object], name: str) -> None:
    """Pin the default on the version with this system name; LAST follows the newest instead."""
    if name.upper() == "LAST":
        _follow_newest(extension)
        return
    listed = versions(extension)
    if not any(str(version["name"]).casefold() == name.casefold() for version in listed):
        raise SnowflakePortError("099103 (22023): Invalid default")
    for version in listed:
        version["is_default"] = str(version["name"]).casefold() == name.casefold()
    extension["default_pinned"] = True


def _follow_newest(extension: dict[str, object]) -> None:
    listed = versions(extension)
    for version in listed:
        version["is_default"] = version is listed[-1]
    extension["default_pinned"] = False


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
    pinned = extension.get("default_pinned") is True
    if not pinned:
        for version in listed:
            version["is_default"] = False
    listed.append(
        {
            "name": system,
            "alias": alias,
            "location": f"snow://cortex_extension/{name}/versions/{system.lower()}/",
            "files": list(contents),
            "sources": dict(sources),
            "is_default": not pinned,
            "certification": None,
        }
    )
