"""File access and YAML location for `sst migrate refs` and `sst migrate split`."""

from __future__ import annotations

from pathlib import Path

import yaml

from snowflake_semantic_tools.adapters import bounded_yaml
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.paths import create_within, remove_within, write_within
from snowflake_semantic_tools.adapters.yaml.discover import YAML_SUFFIXES
from snowflake_semantic_tools.adapters.yaml.parse import _neutralize_templates, _restore_templates
from snowflake_semantic_tools.domain.migrate.refs import FilterSite

_HOOK_MANIFESTS = frozenset(("hook.yml", "hook.yaml"))
_AGENT_FILES = frozenset(("agent.yml", "agent.yaml"))


def semantic_files(project_dir: Path, semantic_models_dir: str) -> dict[str, str]:
    """Every YAML file under the semantic models directory, keyed by project-relative path.

    Raises:
        ProjectError: The directory does not exist.
    """
    root = project_dir / semantic_models_dir
    if not root.is_dir():
        raise ProjectError(f"no semantic model directory under {root}")
    return files_under(project_dir, semantic_models_dir, YAML_SUFFIXES)


def files_under(project_dir: Path, folder: str, suffixes: frozenset[str]) -> dict[str, str]:
    """Every file under a project folder with one of `suffixes`, keyed by project-relative path.

    Files in hidden folders, and hidden files, are left out; a missing folder holds none.
    """
    root = project_dir / folder
    if not root.is_dir():
        return {}
    return {
        path.relative_to(project_dir).as_posix(): path.read_text(encoding="utf-8")
        for path in sorted(root.rglob("*"))
        if path.is_file()
        and path.suffix.casefold() in suffixes
        and not any(part.startswith(".") for part in path.relative_to(root).parts)
    }


def agent_files(project_dir: Path, agents_dir: str) -> tuple[dict[str, str], dict[str, str]]:
    """Return the agent files under `agents_dir`, then the eval files in their `evals/` folders."""
    found = files_under(project_dir, agents_dir, YAML_SUFFIXES)
    agents = {path: text for path, text in found.items() if path.rsplit("/", 1)[-1] in _AGENT_FILES}
    evals = {path: text for path, text in found.items() if path.split("/")[-2:-1] == ["evals"]}
    return agents, evals


def hook_files(project_dir: Path, hooks_dir: str) -> dict[str, str]:
    """Return each hook manifest under `hooks_dir`."""
    found = files_under(project_dir, hooks_dir, YAML_SUFFIXES)
    return {path: text for path, text in found.items() if path.rsplit("/", 1)[-1] in _HOOK_MANIFESTS}


def create_file(project_dir: Path, path: str, text: str, *, crlf: bool) -> None:
    """Create a new file, failing when anything is already at `path`.

    Raises:
        FileExistsError: Something is already at `path`.
        UnsafeWrite: `path` lies outside the project, or a link is on the way to it.
        OSError: The file cannot be written.
    """
    data = text.replace("\n", "\r\n") if crlf else text
    create_within(project_dir, project_dir / path, data.encode("utf-8"), mode=0o644)


def remove_file(project_dir: Path, path: str) -> None:
    """Remove a file the split emptied.

    Raises:
        UnsafeWrite: `path` is a folder, or a link is on the way to it.
        OSError: The file cannot be removed.
    """
    remove_within(project_dir, project_dir / path)


def uses_crlf(project_dir: Path, path: str) -> bool:
    """Whether the file at `path` ends its lines with CRLF."""
    target = project_dir / path
    return target.is_file() and b"\r\n" in target.read_bytes()


def write_file(project_dir: Path, path: str, text: str) -> None:
    """Write rewritten text back with the line ending the file already used.

    Files are read with universal newlines, so the codemod only ever sees `\\n`;
    a CRLF file must not come back LF with every line changed in the diff.

    Raises:
        UnsafeWrite: The file is a symbolic link, or one is on the way to it.
        OSError: The file cannot be written.
    """
    target = project_dir / path
    write_within(project_dir, target, text.replace("\n", "\r\n") if uses_crlf(project_dir, path) else text)


def filter_sites(text: str, path: str) -> tuple[FilterSite, ...]:
    """Each `snowflake_filters` entry, or the one `filter:`, with its expression and where it ends."""
    if "snowflake_filters" not in text and "filter" not in text:
        return ()
    try:
        neutralized, templates = _neutralize_templates(text, path)
        root = bounded_yaml.compose(neutralized)
    except (ProjectError, yaml.YAMLError):
        return ()
    if not isinstance(root, yaml.MappingNode):
        return ()
    sites: list[FilterSite] = []
    for key, value in root.value:
        if key.value == "snowflake_filters" and isinstance(value, yaml.SequenceNode):
            entries = list(value.value)
        elif key.value == "filter" and isinstance(value, yaml.MappingNode):
            entries = [value]
        else:
            continue
        for entry in entries:
            if not isinstance(entry, yaml.MappingNode) or not entry.value:
                continue
            fields = {str(item_key.value): item_value for item_key, item_value in entry.value}
            expr = fields.get("expr")
            name = fields.get("name")
            if not isinstance(expr, yaml.ScalarNode):
                continue
            last = max(
                item_value.end_mark.line + 1 if item_value.end_mark.column > 0 else item_value.end_mark.line
                for _, item_value in entry.value
            )
            sites.append(
                FilterSite(
                    name=str(_restore_templates(name.value, templates)) if isinstance(name, yaml.ScalarNode) else "",
                    expr=str(_restore_templates(expr.value, templates)),
                    has_labels="labels" in fields,
                    last_line=last,
                    indent=entry.value[0][0].start_mark.column,
                )
            )
    return tuple(sites)
