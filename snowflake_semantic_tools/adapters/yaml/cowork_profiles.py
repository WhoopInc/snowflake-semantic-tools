"""Parse `cowork_profile.yml` files into CoWork profiles, before the agents they name resolve.

CoWork profiles are a Snowflake private preview (PrPr) feature; a profile file is read to the
preview's contract, as these pages describe it:
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/set-up-cowork-profile-routing

A profile file is found and read as an agent file is, and shares its instructions, skills,
passthrough, and `config:` with one (`agent_fields`); its `display:` block is read as an
agent's `profile:` block, and its `spec.subagents` and `spec.tools` are its own.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType
from typing import Any

from snowflake_semantic_tools.adapters.yaml.agent_fields import (
    config_origins,
    folder_files,
    instruction,
    label,
    parse_eval_files,
    parse_skill,
    published_text,
    read_document,
    reference_call,
    sample_questions,
    unknown_block_keys,
)
from snowflake_semantic_tools.adapters.yaml.fields import checked_list, checked_mapping, mapping, optional_string
from snowflake_semantic_tools.adapters.yaml.routes import routed_diagnostics
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, DiagnosticBag, Origin
from snowflake_semantic_tools.domain.model.agent import AgentProfile
from snowflake_semantic_tools.domain.model.artifact_key import artifact_key
from snowflake_semantic_tools.domain.model.authored import NodePath, SourcePosition
from snowflake_semantic_tools.domain.model.config_schema import resolve_settings, setting_value
from snowflake_semantic_tools.domain.model.cowork_profile import CoworkProfileModel, CoworkProfileTool, CoworkSubagent
from snowflake_semantic_tools.domain.parse.names import name_warnings
from snowflake_semantic_tools.domain.parse.passthrough import AGENT_SPEC_KEYS, passthrough_diagnostics
from snowflake_semantic_tools.domain.validate.cowork_profile import cowork_profile_diagnostics
from snowflake_semantic_tools.domain.validate.settings import item_config_diagnostics
from snowflake_semantic_tools.domain.validate.sql import name_problem

# The spellings of a CoWork profile file; a folder holds one of them.
COWORK_PROFILE_FILES = ("cowork_profile.yml", "cowork_profile.yaml")
_KIND = "cowork_profile"
# The keys each block of a profile file models, by the block's path; `*` stands for any list
# index. `meta` and `spec.passthrough` are free-form.
_PROFILE_KEYS: Mapping[NodePath, frozenset[str]] = MappingProxyType(
    {
        (): frozenset(("name", "comment", "display", "spec", "config", "deprecated", "meta", "evals")),
        ("display",): frozenset(("display_name", "avatar", "color")),
        ("spec",): frozenset(("instructions", "subagents", "tools", "skills", "passthrough")),
        ("spec", "instructions"): frozenset(("orchestration", "response", "sample_questions")),
        ("spec", "subagents", "*"): frozenset(("agent", "display_name", "description", "sample_questions")),
        ("spec", "tools", "*"): frozenset(("type", "name")),
        ("spec", "skills", "*"): frozenset(("name", "source")),
        ("spec", "skills", "*", "source"): frozenset(("type", "path", "version")),
        # `config:`'s own keys are the `cowork_profiles:` settings, which `item_config_diagnostics` checks.
        ("config", "tags", "*"): frozenset(("name", "value")),
        ("evals",): frozenset(("dataset", "config")),
    }
)


def load_cowork_profiles(
    project_dir: Path, *, cowork_profiles_dir: str = "cowork_profiles", config: Mapping[str, Any] | None = None
) -> tuple[tuple[CoworkProfileModel, ...], DiagnosticBag]:
    """Read each `cowork_profile.yml` or `.yaml` in a folder below the directory, in path order.

    A profile's folder may sit in grouping folders, which the `cowork_profiles:` folder routes
    name; the profile records them as its `folder`. A missing directory holds no profiles. A
    file that cannot be read or parsed, or names no profile, contributes its diagnostics and no
    profile; any other problem is reported and the profile kept without what could not be read.
    Each enabled profile is then checked on its own, as `cowork_profile_diagnostics` checks it.

    Args:
        config: The run's resolved configuration, whose `cowork_profiles:` folder routes are
            checked against the folders under the directory, and whose `cowork_profiles:` block
            each profile's settings resolve through, below its own `config:`; None checks no
            route and resolves each profile's settings from its `config:` alone. A profile whose
            `enabled` resolves to anything but false is enabled.

    Diagnostics:
        SST-CFG041: when a `cowork_profiles:` folder route names no folder; reported first.
        SST-VAL801: when a folder holds both spellings of the file; neither is read.
        Every code `read_document` lists, for a file that is refused, unreadable, or not one
            YAML mapping; the profile is left out.
        Every code `_parse_profile` lists.
        Every code `cowork_profile_diagnostics` lists, for an enabled profile.
    """
    root = project_dir / cowork_profiles_dir
    routes = routed_diagnostics(config, project_dir, _KIND) if config is not None else ()
    if not root.is_dir():
        return (), DiagnosticBag(routes)
    profiles: list[CoworkProfileModel] = []
    diagnostics: list[Diagnostic] = list(routes)
    detail = "CoWork profile folder has both cowork_profile.yml and cowork_profile.yaml"
    for path in folder_files(project_dir, root, COWORK_PROFILE_FILES, diagnostics, detail=detail):
        read = read_document(project_dir, path, diagnostics)
        if read is None:
            continue
        relative, _, parsed = read
        profile, problems = _parse_profile(project_dir, relative, dict(parsed.tree), parsed.line_index)
        diagnostics.extend((*parsed.diagnostics, *problems))
        if profile is None:
            continue
        folder = path.parent.relative_to(root).parts
        block = config.get("cowork_profiles") if config is not None else None
        settings = resolve_settings(block, folder, parsed.tree.get("config"))
        profile = replace(
            profile, folder=folder, enabled=setting_value(settings, "enabled") is not False, settings=settings
        )
        if profile.enabled:
            diagnostics.extend(cowork_profile_diagnostics(profile))
        profiles.append(profile)
    return tuple(profiles), DiagnosticBag(diagnostics)


def _parse_profile(
    project_dir: Path,
    relative: str,
    tree: Mapping[str, Any],
    lines: Mapping[NodePath, SourcePosition],
) -> tuple[CoworkProfileModel | None, tuple[Diagnostic, ...]]:
    """Build one profile, reporting each problem; None only when it has no name, or one SST cannot render.

    The instructions, sample questions, skills, and `config:` are read as an agent's are.

    Args:
        lines: Where each node of the profile file starts, which places each subagent and tool.

    Diagnostics:
        SST-PRS002: the profile has no `name`, it is not built; or a subagent has no `agent`,
            a tool no `type`, or a skill no `source.type`.
        SST-PRS005: the name is not a Snowflake identifier; it is not built.
        Every code `name_warnings` lists, at the name.
        SST-PRS003: `spec.subagents`, `spec.tools`, `spec.skills`, or a sample question list is
            not a list; `spec.instructions`, `meta`, or `spec.passthrough` is not a mapping; or
            an instruction is not a string. The field reads as empty.
        SST-PRS018: a `spec.subagents`, `spec.tools`, or `spec.skills` entry has the wrong shape.
        SST-PRS118: a sample question is not a mapping with a string `question`.
        SST-PRS023, SST-PRS024: as `passthrough_diagnostics` reports `spec.passthrough`.
        SST-PRS004, SST-PRS022: as `unknown_block_keys` reports a key a block does not model.
        SST-CFG004, SST-CFG053, SST-CFG054, SST-CFG008: as `item_config_diagnostics`
            reports the profile's `config:` block.
        SST-REF008: a field published as written holds a template: the `comment`,
            `display.display_name`, a subagent's `display_name` or `description`, or a sample
            question.
        Every code `required_call` lists, for a reference field, or an instruction holding a
            template, that is not one call of the function it takes.
        Every code `read_sidecar` lists, for an instruction's sidecar.
        Every code `parse_eval_files` lists, for the `evals:` block.
    """
    origin = Origin(relative, 1, 1)
    name = tree.get("name")
    if not isinstance(name, str) or not name:
        return None, (D("SST-PRS002", artifact=relative, field="name", origin=origin),)
    subject = artifact_key(_KIND, name.casefold())
    named = lines.get(("name",))
    at = Origin(relative, named.line, named.col) if named is not None else origin
    unusable = name_problem(name, artifact=subject, subject=subject, origin=at)
    if unusable is not None:
        return None, (unusable,)
    diagnostics: list[Diagnostic] = list(name_warnings(name, subject=subject, origin=at))
    read = _Reader(relative, lines, diagnostics, subject)
    display = mapping(tree.get("display"))
    spec = mapping(tree.get("spec"))
    instructions = read.mapped(spec.get("instructions"), "spec.instructions")
    source_files = [relative]
    orchestration_text, response_text = (
        instruction(project_dir, relative, instructions.get(key), source_files, diagnostics, field=key, subject=subject)
        for key in ("orchestration", "response")
    )
    questions = read.listed(instructions.get("sample_questions"), "spec.instructions.sample_questions")
    skills = tuple(
        skill
        for index, value in enumerate(read.listed(spec.get("skills"), "spec.skills"))
        if (skill := parse_skill(relative, index, value, diagnostics, subject, kind=_KIND)) is not None
    )
    passthrough = read.mapped(spec.get("passthrough"), "spec.passthrough")
    diagnostics.extend(
        passthrough_diagnostics(f"{name}.spec", passthrough, AGENT_SPEC_KEYS, subject=subject, origin=origin)
    )
    model = CoworkProfileModel(
        name=name,
        origin=origin,
        source_files=tuple(dict.fromkeys(source_files)),
        comment=read.published(tree.get("comment"), ("comment",)),
        display=AgentProfile(
            read.published(display.get("display_name"), ("display", "display_name")),
            optional_string(display.get("avatar")),
            optional_string(display.get("color")),
        ),
        orchestration_instructions=orchestration_text,
        response_instructions=response_text,
        sample_questions=tuple(sample_questions(questions, _KIND, name, origin, diagnostics)),
        subagents=read.subagents(read.listed(spec.get("subagents"), "spec.subagents"), name),
        tools=read.tools(read.listed(spec.get("tools"), "spec.tools")),
        skills=skills,
        meta=MappingProxyType(read.mapped(tree.get("meta"), "meta")),
        passthrough=MappingProxyType(passthrough),
        deprecated=bool(tree.get("deprecated", False)),
        evals=parse_eval_files(relative, tree.get("evals"), diagnostics, kind=_KIND),
    )
    diagnostics.extend(unknown_block_keys(relative, tree, lines, subject, _PROFILE_KEYS))
    diagnostics.extend(
        item_config_diagnostics(
            "cowork_profiles", tree.get("config"), subject=subject, origin_of=config_origins(relative, lines)
        )
    )
    return model, tuple(diagnostics)


class _Reader:
    """Read the fields of one profile file, reporting each problem into its diagnostics list."""

    def __init__(
        self, relative: str, lines: Mapping[NodePath, SourcePosition], diagnostics: list[Diagnostic], subject: str
    ) -> None:
        self._relative = relative
        self._lines = lines
        self._diagnostics = diagnostics
        self._subject = subject

    def listed(self, value: object, field: str) -> list[Any]:
        """Return a list field as written, as `checked_list` reads it."""
        return checked_list(
            value, self._diagnostics, field=field, artifact=self._relative, origin=self._at(()), subject=self._subject
        )

    def mapped(self, value: object, field: str) -> dict[Any, Any]:
        """Return a mapping field as written, as `checked_mapping` reads it."""
        return checked_mapping(
            value, self._diagnostics, field=field, artifact=self._relative, origin=self._at(()), subject=self._subject
        )

    def published(self, value: object, path: NodePath) -> str | None:
        """Read a text field the profile publishes as written, as `published_text` does."""
        return published_text(value, path, self._relative, self._lines, self._diagnostics, self._subject)

    def subagents(self, entries: list[Any], name: str) -> tuple[CoworkSubagent, ...]:
        """Read the `spec.subagents` entries in order; an entry that cannot name an agent is left out.

        Diagnostics:
            SST-PRS018: the entry is not a mapping.
            SST-PRS002: the entry writes no `agent`.
            Every code `required_call` lists, for an `agent` that is not one `agent()` or
                `tool()` call.
            SST-REF008: the `display_name` or `description` holds a template.
            SST-PRS003, SST-PRS118, SST-REF008: as `sample_questions` reads the entry's own.
        """
        found: list[CoworkSubagent] = []
        for index, value in enumerate(entries):
            path: NodePath = ("spec", "subagents", index)
            origin = self._at(path)
            if not isinstance(value, dict):
                self._diagnostics.append(self._wrong_shape("subagents", index, value, origin))
                continue
            field = label(path)
            if value.get("agent") is None:
                self._diagnostics.append(
                    D("SST-PRS002", artifact=self._relative, field=f"{field}.agent", origin=origin)
                )
                continue
            call = reference_call(
                value["agent"],
                f"{_KIND}.spec.subagents.agent",
                self._diagnostics,
                origin,
                field=f"{field}.agent",
                artifact=self._subject,
                subject=self._subject,
            )
            if call is None:
                continue
            questions = None
            if value.get("sample_questions") is not None:
                written = self.listed(value["sample_questions"], f"{field}.sample_questions")
                read = sample_questions(
                    written, _KIND, name, origin, self._diagnostics, field=f"{field}.sample_questions"
                )
                questions = tuple(read)
            found.append(
                CoworkSubagent(
                    reference=call.function,
                    name=call.args[0],
                    origin=origin,
                    display_name=self.published(value.get("display_name"), (*path, "display_name")),
                    description=self.published(value.get("description"), (*path, "description")),
                    sample_questions=questions,
                )
            )
        return tuple(found)

    def tools(self, entries: list[Any]) -> tuple[CoworkProfileTool, ...]:
        """Read the `spec.tools` entries in order; one that is not a mapping with a `type` is left out.

        Diagnostics:
            SST-PRS018: the entry is not a mapping.
            SST-PRS002: the entry declares no string `type`.
        """
        found: list[CoworkProfileTool] = []
        for index, value in enumerate(entries):
            origin = self._at(("spec", "tools", index))
            if not isinstance(value, dict):
                self._diagnostics.append(self._wrong_shape("tools", index, value, origin))
                continue
            type_name = value.get("type")
            if not isinstance(type_name, str):
                field = f"tools[{index}].type"
                self._diagnostics.append(D("SST-PRS002", artifact=self._relative, field=field, origin=origin))
                continue
            found.append(CoworkProfileTool(type=type_name, name=optional_string(value.get("name")), origin=origin))
        return tuple(found)

    def _at(self, path: NodePath) -> Origin:
        """Return where the node at `path` starts, else the file's start."""
        position = self._lines.get(path) if path else None
        if position is None:
            return Origin(self._relative, 1, 1)
        return Origin(self._relative, position.line, position.col)

    def _wrong_shape(self, field: str, index: int, value: object, origin: Origin) -> Diagnostic:
        return D(
            "SST-PRS018",
            origin=origin,
            artifact=self._relative,
            field=field,
            index=index,
            expected="mapping",
            found=type(value).__name__,
        )
