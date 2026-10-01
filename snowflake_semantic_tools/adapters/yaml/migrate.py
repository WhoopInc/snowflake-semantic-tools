"""File access and YAML location for `sst migrate refs`."""

from __future__ import annotations

from pathlib import Path

import yaml

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.yaml.documents import YAML_SUFFIXES
from snowflake_semantic_tools.adapters.yaml.parse import _neutralize_templates, _restore_templates
from snowflake_semantic_tools.domain.model.migrate import FilterSite


def semantic_files(project_dir: Path, semantic_models_dir: str) -> dict[str, str]:
    """Every YAML file under the semantic models directory, keyed by project-relative path."""
    root = project_dir / semantic_models_dir
    if not root.is_dir():
        raise ProjectError(f"no semantic model directory under {root}")
    return {
        path.relative_to(project_dir).as_posix(): path.read_text(encoding="utf-8")
        for path in sorted(root.rglob("*"))
        if path.is_file()
        and path.suffix.casefold() in YAML_SUFFIXES
        and not any(part.startswith(".") for part in path.relative_to(root).parts)
    }


def write_file(project_dir: Path, path: str, text: str) -> None:
    """Write rewritten text back with the line ending the file already used.

    Files are read with universal newlines, so the codemod only ever sees `\\n`;
    a CRLF file must not come back LF with every line changed in the diff.
    """
    target = project_dir / path
    crlf = target.is_file() and b"\r\n" in target.read_bytes()
    target.write_text(text, encoding="utf-8", newline="\r\n" if crlf else "\n")


def filter_sites(text: str, path: str) -> tuple[FilterSite, ...]:
    """Each `snowflake_filters` entry with its expression and where its last line ends."""
    if "snowflake_filters" not in text:
        return ()
    try:
        neutralized, templates = _neutralize_templates(text, path)
        root = yaml.compose(neutralized, Loader=yaml.SafeLoader)
    except (ProjectError, yaml.YAMLError):
        return ()
    if not isinstance(root, yaml.MappingNode):
        return ()
    sites: list[FilterSite] = []
    for key, value in root.value:
        if key.value != "snowflake_filters" or not isinstance(value, yaml.SequenceNode):
            continue
        for entry in value.value:
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
