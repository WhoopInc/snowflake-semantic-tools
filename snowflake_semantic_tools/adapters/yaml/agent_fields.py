"""Read the fields an agent file and a CoWork profile file write alike.

An `agent.yml` and a `cowork_profile.yml` are found the same way, one per folder, and share
their instructions, sample questions, skills, reference fields, published text, and the check
for keys a block does not model; each loader keeps what only its own kind writes. CoWork
profiles are a Snowflake private preview (PrPr) feature, read to the preview's contract:
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/set-up-cowork-profile-routing
"""

from __future__ import annotations

import posixpath
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.yaml.documents import ParsedYaml
from snowflake_semantic_tools.adapters.yaml.fields import (
    optional_string,
    project_relative,
    read_sidecar,
    refused,
    unknown_keys,
)
from snowflake_semantic_tools.adapters.yaml.parse import parse_yaml_bytes
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.agent import AgentEvalFiles, AgentSkill
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.authored import NodePath, SourcePosition
from snowflake_semantic_tools.domain.parse.fields import unknown_field
from snowflake_semantic_tools.domain.parse.template import TemplateCall
from snowflake_semantic_tools.domain.resolve.calls import literal_problem, required_call
from snowflake_semantic_tools.domain.resolve.ref_fields import reference_functions


def folder_files(
    project_dir: Path, root: Path, spellings: tuple[str, ...], diagnostics: list[Diagnostic], *, detail: str
) -> list[Path]:
    """Return each artifact folder's one file, in path order, reporting a folder that holds two.

    A file is found in any folder below `root`, never in `root` itself.

    Args:
        spellings: The names an artifact's file may have; a folder holds one of them.
        detail: What the report of a folder that holds two says.

    Diagnostics:
        SST-VAL801: when a folder holds more than one of `spellings`; none is returned.
    """
    by_folder: dict[Path, list[Path]] = {}
    for spelling in spellings:
        for path in root.glob(f"*/**/{spelling}"):
            by_folder.setdefault(path.parent, []).append(path)
    for folder, paths in sorted(by_folder.items()):
        if len(paths) > 1:
            relative = project_relative(project_dir, folder)
            diagnostics.append(D("SST-VAL801", artifact=relative, detail=detail, origin=Origin(relative)))
    return sorted(paths[0] for paths in by_folder.values() if len(paths) == 1)


def read_document(project_dir: Path, path: Path, diagnostics: list[Diagnostic]) -> tuple[str, bytes, ParsedYaml] | None:
    """Read and parse one artifact file: its project-relative path, its bytes, and its tree.

    Returns:
        None when the file is refused or cannot be read or parsed, which is reported.

    Diagnostics:
        SST-PRT009: when the file, or a folder between the project and it, is a symbolic link,
            or it resolves outside the project.
        Every code `parse_yaml_bytes` raises, for a file that is not one YAML mapping.
        SST-LOD018: when the file cannot be read.
    """
    if refused(project_dir, path, diagnostics):
        return None
    relative = path.relative_to(project_dir).as_posix()
    try:
        raw = path.read_bytes()
        parsed = parse_yaml_bytes(raw, relative)
    except ProjectError as exc:
        diagnostics.extend(exc.diagnostics)
        return None
    except OSError:
        diagnostics.append(D("SST-LOD018", file=relative, path=relative, origin=Origin(relative)))
        return None
    return relative, raw, parsed


def _never_moved(path: NodePath, key: str) -> str | None:
    return None


def unknown_block_keys(
    relative: str,
    tree: Mapping[str, Any],
    lines: Mapping[NodePath, SourcePosition],
    subject: str,
    known: Mapping[NodePath, frozenset[str]],
    moved: Callable[[NodePath, str], str | None] = _never_moved,
) -> list[Diagnostic]:
    """Report each key a file writes in a block SST models and that the block does not model.

    A key is placed where it is written, else at the file's start.

    Args:
        known: The keys each block models, by the block's path; `*` stands for any list index.
        moved: Where a key a block no longer takes is written now, given the block's path and
            the key; None for a key that never moved.

    Diagnostics:
        SST-PRS126: the key moved, which the message says.
        SST-PRS022: the key is within edit distance of one the block models, which it names.
        SST-PRS004: otherwise.
    """
    found: list[Diagnostic] = []
    for pattern, allowed in known.items():
        for path, node in nodes(tree, pattern):
            for key in unknown_keys(node, allowed):
                position = lines.get((*path, key))
                origin = Origin(relative, position.line, position.col) if position else Origin(relative, 1, 1)
                field = label((*path, str(key)))
                reason = moved(path, str(key))
                found.append(
                    D("SST-PRS126", origin=origin, subject=subject, artifact=subject, field=field, reason=reason)
                    if reason is not None
                    else unknown_field(
                        str(key), allowed, label=field, artifact=relative, origin=origin, subject=subject
                    )
                )
    return found


def nodes(tree: Mapping[str, Any], pattern: NodePath) -> list[tuple[NodePath, Mapping[Any, Any]]]:
    """Return each mapping at `pattern` in `tree`, with its path; `*` matches every index of a list."""
    matched: list[tuple[NodePath, object]] = [((), tree)]
    for part in pattern:
        step: list[tuple[NodePath, object]] = []
        for path, node in matched:
            if part == "*" and isinstance(node, list):
                step.extend(((*path, index), item) for index, item in enumerate(node))
            elif part != "*" and isinstance(node, Mapping) and part in node:
                step.append(((*path, part), node[part]))
        matched = step
    return [(path, node) for path, node in matched if isinstance(node, Mapping)]


def label(path: NodePath) -> str:
    """Write a node path as a diagnostic names a field: `spec.tools[0].warehouse`."""
    text = ""
    for part in path:
        text += f"[{part}]" if isinstance(part, int) else (f".{part}" if text else part)
    return text


def config_origins(relative: str, lines: Mapping[NodePath, SourcePosition]) -> Callable[[str], Origin]:
    """Place a key of the file's `config:` block where it is written, else at the file's start."""

    def origin_of(key: str) -> Origin:
        position = lines.get(("config", key) if key else ("config",))
        return Origin(relative, position.line, position.col) if position is not None else Origin(relative, 1, 1)

    return origin_of


def published_text(
    value: object,
    path: NodePath,
    relative: str,
    lines: Mapping[NodePath, SourcePosition],
    diagnostics: list[Diagnostic],
    subject: str,
) -> str | None:
    """Read a text field published as written, reporting a template in it (SST-REF008)."""
    text = optional_string(value)
    position = lines.get(path)
    origin = Origin(relative, position.line, position.col) if position is not None else Origin(relative, 1, 1)
    problem = literal_problem(text or "", origin, field=label(path), artifact=subject, subject=subject)
    if problem is not None:
        diagnostics.append(problem)
    return text


def sample_questions(
    values: Any,
    kind: str,
    name: str,
    origin: Origin,
    diagnostics: list[Diagnostic],
    *,
    field: str = "spec.instructions.sample_questions",
) -> list[str]:
    """Each `question` of a `sample_questions` list, reporting SST-PRS118 for an entry without one.

    A question is published as written, so a template in one is SST-REF008.

    Args:
        kind: The artifact type the list belongs to, which keys the reports.
        field: Where the list is written; a subagent's own list is not under `spec.instructions`.
    """
    questions: list[str] = []
    for index, value in enumerate(values or []):
        if not isinstance(value, dict) or not isinstance(value.get("question"), str):
            diagnostics.append(
                D(
                    "SST-PRS118",
                    artifact=artifact_key(kind, name),
                    index=index,
                    origin=origin,
                    subject=artifact_key(kind, name.casefold()),
                )
            )
            continue
        question = str(value["question"])
        subject = artifact_key(kind, name.casefold())
        problem = literal_problem(
            question, origin, field=f"{field}[{index}].question", artifact=subject, subject=subject
        )
        if problem is not None:
            diagnostics.append(problem)
        questions.append(question)
    return questions


def instruction(
    project_dir: Path,
    source_file: str,
    value: object,
    source_files: list[str],
    diagnostics: list[Diagnostic],
    *,
    field: str,
    subject: str,
) -> str | None:
    """Read one instruction: its text as written, or the `{{ file('<path>') }}` sidecar it names.

    Text with no template is the instruction. A sidecar is read by `resolve_file`, relative to
    the declaring file's folder, without trailing whitespace, and is appended to `source_files`.

    Args:
        field: The instruction's key under `spec.instructions`, which a value of the wrong
            type is reported under.
        subject: The artifact's key, which every problem is reported against.

    Returns:
        The instruction; None when `value` is not a string or its sidecar was refused.

    Diagnostics:
        SST-PRS003: when the value is present and is not a string.
        Every code `read_sidecar` lists, for text that holds a template: it must be one
            `file()` call naming a sidecar `resolve_file` reads.
    """
    origin = Origin(source_file)
    if not isinstance(value, str):
        if value is not None:
            diagnostics.append(
                D(
                    "SST-PRS003",
                    artifact=source_file,
                    field=f"spec.instructions.{field}",
                    expected="string",
                    found=type(value).__name__,
                    origin=origin,
                    subject=subject,
                )
            )
        return None
    if "{{" not in value:
        return value
    sidecar = read_sidecar(
        project_dir,
        source_file,
        value,
        origin=origin,
        field=f"spec.instructions.{field}",
        artifact=subject,
        subject=subject,
    )
    if isinstance(sidecar, Diagnostic):
        diagnostics.append(sidecar)
        return None
    source_files.append(sidecar.path)
    return sidecar.text.rstrip()


def parse_skill(
    source_file: str,
    index: int,
    value: object,
    diagnostics: list[Diagnostic],
    subject: str,
    *,
    kind: str,
) -> AgentSkill | None:
    """Read one `spec.skills` entry; None when it cannot hold a skill.

    `source.path` is one call naming a project skill (`{{ skill() }}`), a project plugin
    (`{{ plugin() }}`) or a consumed extension (`{{ extension() }}`), and `ref` records which.
    A plugin entry's `name` is the `{{ skill('<member>') }}` call naming one of its members, and
    is kept as the member's name; any other entry's `name` is kept as written. A version holding
    a template is one `{{ var() }}` call, kept as the variable's name; any other is kept as
    written. An absent name, path or version, or one that is refused, reads as `""`.

    Args:
        kind: The artifact type whose file holds the entry, whose reference fields it reads.

    Diagnostics:
        SST-PRS018: when the entry is not a mapping with a `source:` mapping.
        SST-PRS002: when `source.type` is not a string, or `name` is present and not one.
        Every code `required_call` lists, for a `source.path` that is not one `skill()`,
            `plugin()` or `extension()` call, a plugin entry's `name` that is not one `skill()`
            call, or a templated `source.version` that is not one `var()` call.
        SST-REF008: when a consumed extension's `name` holds a template.
    """
    if not isinstance(value, dict) or not isinstance(value.get("source"), dict):
        diagnostics.append(
            D(
                "SST-PRS018",
                artifact=source_file,
                field="skills",
                index=index,
                expected="name/source mapping",
                found=type(value).__name__,
            )
        )
        return None
    source = value["source"]
    assert isinstance(source, dict)
    name = value.get("name")
    source_type = source.get("type")
    if not isinstance(source_type, str) or (name is not None and not isinstance(name, str)):
        diagnostics.append(D("SST-PRS002", artifact=source_file, field=f"skills[{index}].name/source.type"))
        return None
    origin = Origin(source_file)
    field = f"spec.skills[{index}]"
    located: dict[str, Any] = {"artifact": subject, "subject": subject}
    path_call = reference_call(
        source.get("path"),
        f"{kind}.spec.skills.source.path",
        diagnostics,
        origin,
        field=f"{field}.source.path",
        **located,
    )
    ref = path_call.function if path_call is not None else "extension"
    path = path_call.args[0] if path_call is not None else ""
    if name is not None and ref == "plugin":
        name_call = reference_call(
            name, f"{kind}.spec.skills.name", diagnostics, origin, field=f"{field}.name", **located
        )
        name = call_name(name_call) or ""
    elif (
        name is not None
        and ref == "extension"
        and (problem := literal_problem(name, origin, field=f"{field}.name", **located))
    ):
        diagnostics.append(problem)
        name = ""
    version = source.get("version")
    version_var = None
    if isinstance(version, str) and "{{" in version:
        reference = f"{kind}.spec.skills.source.version"
        version_var = call_name(
            reference_call(version, reference, diagnostics, origin, field=f"{field}.source.version", **located)
        )
        version = None
    literal = optional_string(version)
    return AgentSkill(name or "", source_type, path, literal or "", ref=ref, version_var=version_var)


def reference_call(
    value: object,
    reference: str,
    diagnostics: list[Diagnostic],
    origin: Origin,
    *,
    field: str,
    artifact: str,
    subject: str | None = None,
) -> TemplateCall | None:
    """Return the one call a reference field's `value` must be; None when it is absent or refused.

    Args:
        reference: The field's entry in `REFERENCE_FIELDS`, which names the functions it takes.

    A value that is not text is read as the text it prints as, so it is refused as a bare value.

    Diagnostics:
        Every code `required_call` lists.
    """
    if value is None:
        return None
    functions = reference_functions(reference)
    found = required_call(str(value), functions, origin, field=field, artifact=artifact, subject=subject)
    if isinstance(found, Diagnostic):
        diagnostics.append(found)
        return None
    return found


def call_name(call: TemplateCall | None) -> str | None:
    """Return the one name a reference call gives; None for no call."""
    return call.args[0] if call is not None else None


def parse_eval_files(
    source_file: str, value: object, diagnostics: list[Diagnostic], *, kind: str
) -> AgentEvalFiles | None:
    """Read an `evals:` block: the dataset and the config it publishes, each by name.

    An agent's and a CoWork profile's are read alike. `dataset:` is one
    `{{ eval_dataset('<name>') }}` call and `config:` one `{{ eval_config('<name>') }}` call;
    the names resolve among the files in the `evals/` folder beside the file, which is not
    read here.

    Args:
        kind: The file's kind, `agent` or `cowork_profile`, whose `REFERENCE_FIELDS` entries
            name the functions each field takes.

    Returns:
        None when the file declares no `evals:`; otherwise both names, each None when it is
        absent or not that one call, with the `evals/` folder.

    Diagnostics:
        SST-PRS003: when `evals:` is not a mapping.
        SST-PRS002: when `dataset` or `config` is absent.
        Every code `required_call` lists, for a value that is not one `eval_dataset()` or
            `eval_config()` call.
    """
    if value is None:
        return None
    origin = Origin(source_file)
    folder = posixpath.join(posixpath.dirname(source_file), "evals")
    if not isinstance(value, dict):
        diagnostics.append(
            D(
                "SST-PRS003",
                artifact=source_file,
                field="evals",
                expected="mapping",
                found=type(value).__name__,
                origin=origin,
            )
        )
        return AgentEvalFiles(origin, folder=folder)
    names: dict[str, str | None] = {}
    for field in ("dataset", "config"):
        raw = value.get(field)
        names[field] = None
        if raw is None:
            diagnostics.append(D("SST-PRS002", artifact=source_file, field=f"evals.{field}", origin=origin))
            continue
        call = reference_call(
            raw, f"{kind}.evals.{field}", diagnostics, origin, field=f"evals.{field}", artifact=source_file
        )
        names[field] = call_name(call)
    return AgentEvalFiles(origin, names["dataset"], names["config"], folder)
