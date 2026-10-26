"""Wire `sst import` to the project: the names it maps against, and the files it writes.

The setup compiles the project for the target, in memory, as `sst diff` does, so a live name
resolves to whatever the project publishes there, and indexes the dbt manifest's relations
and the referencing tool members beside it. A compile that reports errors still indexes
every artifact that compiled. Writing creates each file and never replaces one: a file that
appeared since the run checked is refused by `create_within`, not overwritten. A semantic view's
dbt model metadata is edited in place, as `sst enrich` edits it.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from snowflake_semantic_tools.adapters.dbt.profiles import ProfileTarget
from snowflake_semantic_tools.adapters.dbt.yaml_writer import write_import_edits
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.adapters.paths import create_within, write_within
from snowflake_semantic_tools.adapters.resolved_config import resolved_config
from snowflake_semantic_tools.adapters.yaml.emit import emit_document
from snowflake_semantic_tools.adapters.yaml.format import keep_canonical
from snowflake_semantic_tools.adapters.yaml.styles import YamlStyle
from snowflake_semantic_tools.app.importing import ImportSetup, artifact_directories, project_index
from snowflake_semantic_tools.cli.runner import WriteFailure
from snowflake_semantic_tools.cli.wiring.compile import compile_result
from snowflake_semantic_tools.cli.wiring.project import project_inputs
from snowflake_semantic_tools.domain.importing.model import ImportedFile, ModelEdit
from snowflake_semantic_tools.domain.ports.snowflake.importing import ImportReadPort


def import_setup(
    paths: ProjectPaths, profile: ProfileTarget, port: ImportReadPort, manifest_path: Path | None
) -> ImportSetup:
    """Return what an import maps against on the profile's target.

    Raises:
        ProjectError: the configuration or the dbt manifest cannot be read.
    """
    target_name = profile.target_name
    result = compile_result(paths, target_name, manifest_path)
    inputs = project_inputs(paths, target_name, manifest_path)
    config = resolved_config(paths, target_name).tree
    index = project_index(
        result, inputs.dbt_catalog(), inputs.tool_catalog(), target_name, artifact_directories(config)
    )
    project_dir = paths.project_dir
    return ImportSetup(
        index,
        config,
        profile.identity,
        profile.identity.role or port.current_role(),
        lambda path: (project_dir / path).exists() or (project_dir / path).is_symlink(),
    )


def file_bytes(file: ImportedFile, *, style: YamlStyle | None = None) -> bytes:
    """Return the bytes a file holds: its document as canonical YAML, its text, or its data.

    `style` is the project's `format:` settings, which a document is written in.
    """
    if file.document is not None:
        return emit_document(file.document, file.path, style=style).encode("utf-8")
    if file.text is not None:
        return file.text.encode("utf-8")
    return file.data or b""


def write_imported(
    project_dir: Path, files: Sequence[ImportedFile], *, style: YamlStyle | None = None
) -> tuple[str, ...]:
    """Create each file in the project, never replacing one; return the paths written.

    `style` is the project's `format:` settings, as `file_bytes` takes them.

    Raises:
        WriteFailure: a file cannot be created, or something is already at its path.
    """
    written: list[str] = []
    for file in files:
        path = project_dir / file.path
        try:
            create_within(project_dir, path, file_bytes(file, style=style), mode=0o644)
        except OSError as exc:
            raise WriteFailure(path, exc) from exc
        written.append(file.path)
    return tuple(written)


def write_model_edits(
    project_dir: Path, edits: Sequence[ModelEdit], *, style: YamlStyle | None = None
) -> tuple[str, ...]:
    """Make each dbt model edit in its YAML file, in place; return the files edited.

    A file is edited as `sst enrich` edits one: comments, order and layout kept wherever nothing
    is set; a model's file that does not exist yet is created. A file already canonical under
    `style` stays so (`adapters.yaml.format.keep_canonical`).

    Raises:
        ProjectError: a file is not YAML, or a part an edit sets has another shape.
        WriteFailure: a file cannot be written.
    """
    by_file: dict[str, list[ModelEdit]] = {}
    for edit in edits:
        by_file.setdefault(edit.patch_file, []).append(edit)
    for patch_file, file_edits in by_file.items():
        path = project_dir / patch_file
        try:
            text = path.read_text(encoding="utf-8") if path.exists() else None
            edited = write_import_edits(text, patch_file, file_edits).text
            write_within(project_dir, path, keep_canonical(text, edited, patch_file, style or YamlStyle()))
        except OSError as exc:
            raise WriteFailure(path, exc) from exc
    return tuple(by_file)
