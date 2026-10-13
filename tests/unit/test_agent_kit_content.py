"""The agent kit's skills say only what the installed SST does.

The kit teaches a coding agent which commands to run, so a skill that names a command, a flag,
or a code SST no longer has would send the agent wrong. These checks read every kit file and
fail on any such mention, on a command the safety table in `sst-core` does not classify, on a
skill SST's own skill rules would refuse, and on a placeholder left unrendered.
"""

from __future__ import annotations

import re
import shutil
from collections import Counter
from collections.abc import Iterator
from pathlib import Path

import click
import pytest

from snowflake_semantic_tools._version import __version__
from snowflake_semantic_tools.adapters.agent_kit import KIT_ROOT, read_kit
from snowflake_semantic_tools.adapters.yaml.skills import load_skill_catalog
from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.domain.agent_kit import TOKENS
from snowflake_semantic_tools.domain.diagnostics import ERROR_REGISTRY
from snowflake_semantic_tools.domain.validate.skill import validate_skill_catalog

KIT = read_kit(__version__)
TEXTS = {
    f"{name}/{path}": data.decode("utf-8")
    for name, files in KIT.items()
    for path, data in files.items()
    if path.endswith(".md")
}
SPAN = re.compile(r"`([^`\n]+)`")
FENCE = re.compile(r"^```[^\n]*\n(.*?)^```", re.MULTILINE | re.DOTALL)
CODE = re.compile(r"\bSST-[A-Z]{3}\d{3}\b")
# 0.3 commands the migration skill names as gone; anywhere else they are a mistake.
RETIRED = {"deploy", "generate", "extract", "migrate-meta"}
MIGRATION = "sst-migrate-from-0-3"
# Where a command line ends, so a comment or the next command is not read as its flags.
_ENDS = {"#", "|", "&&", ";", "||"}


def _global_options() -> dict[str, bool]:
    """Every global flag, and whether it takes a value."""
    return {opt: not param.is_flag for param in cli.params if isinstance(param, click.Option) for opt in param.opts}


def _command_lines(text: str) -> Iterator[list[str]]:
    """Yield each `sst ...` command line in the text's code spans and fenced blocks, as tokens."""
    pieces = [
        *SPAN.findall(FENCE.sub("", text)),
        *(line for block in FENCE.findall(text) for line in block.splitlines()),
    ]
    for piece in pieces:
        tokens = piece.split()
        if tokens and tokens[0] == "sst":
            ended = next((index for index, token in enumerate(tokens) if token in _ENDS), len(tokens))
            yield tokens[:ended]


def _resolve(tokens: list[str]) -> tuple[click.Command | None, list[str], str]:
    """Walk `sst`'s global options and command path; return the command, its flags, and its path."""
    globals_ = _global_options()
    index = 1
    while index < len(tokens) and tokens[index].startswith("-"):
        index += 2 if globals_.get(tokens[index]) else 1
    command: click.Command = cli
    path = ["sst"]
    while isinstance(command, click.Group) and index < len(tokens) and not tokens[index].startswith("-"):
        if tokens[index].startswith("<"):
            path.append(tokens[index])
            return None, [], " ".join(path)
        child = command.commands.get(tokens[index])
        if child is None:
            path.append(tokens[index])
            return None, [], " ".join(path)
        command, index = child, index + 1
        path.append(child.name or "")
    flags = [token.split("=")[0] for token in tokens[index:] if token.startswith("-")]
    return command, flags, " ".join(path)


def _leaves(group: click.Group, prefix: str = "sst") -> Iterator[str]:
    for name, command in sorted(group.commands.items()):
        if command.hidden:
            continue
        if isinstance(command, click.Group):
            yield from _leaves(command, f"{prefix} {name}")
        else:
            yield f"{prefix} {name}"


@pytest.mark.parametrize("source", sorted(TEXTS))
def test_every_command_and_flag_a_skill_names_exists(source: str) -> None:
    known_globals = set(_global_options()) | {"--help"}
    problems: list[str] = []
    for tokens in _command_lines(TEXTS[source]):
        command, flags, path = _resolve(tokens)
        if command is None:
            if path.split()[-1] in RETIRED and source.startswith(MIGRATION):
                continue
            if not path.split()[-1].startswith("<"):
                problems.append(f"{' '.join(tokens)}: no command {path}")
            continue
        if isinstance(command, click.Group):
            continue
        own = {
            opt
            for param in command.params
            if isinstance(param, click.Option)
            for opt in (*param.opts, *param.secondary_opts)
        }
        problems.extend(
            f"{' '.join(tokens)}: {path} has no {flag}" for flag in flags if flag not in own | known_globals
        )
    assert not problems, "\n".join(problems)


@pytest.mark.parametrize("source", sorted(TEXTS))
def test_every_code_a_skill_names_is_registered(source: str) -> None:
    unknown = sorted(set(CODE.findall(TEXTS[source])) - set(ERROR_REGISTRY))
    assert not unknown, f"{source} names unregistered codes: {unknown}"


def test_the_safety_table_classifies_every_command_exactly_once() -> None:
    rows = re.findall(r"^\| `(sst [a-z0-9 -]+)` \|", TEXTS["sst-core/SKILL.md"], re.MULTILINE)
    counts = Counter(rows)
    assert sorted(counts) == sorted(_leaves(cli)), "sst-core's table must list each command, no more"
    assert all(count == 1 for count in counts.values()), [row for row, count in counts.items() if count > 1]


def test_the_router_names_every_other_skill() -> None:
    named = set(re.findall(r"^- `(sst-[a-z0-9-]+)`", TEXTS["sst-core/SKILL.md"], re.MULTILINE))
    assert named == set(KIT) - {"sst-core"}


def test_the_kit_passes_the_rules_sst_holds_published_skills_to(tmp_path: Path) -> None:
    shutil.copytree(KIT_ROOT, tmp_path / "skills")
    catalog = load_skill_catalog(tmp_path, skills_dir="skills", plugins_dir="plugins")
    assert sorted(skill.name for skill in catalog.skills) == sorted(KIT)
    found = [f"{item.code}: {item.message}" for item in (*catalog.diagnostics, *validate_skill_catalog(catalog))]
    assert not found, "\n".join(found)


def test_every_description_says_when_not_to_use_the_skill() -> None:
    def description(name: str) -> str:
        return " ".join(TEXTS[f"{name}/SKILL.md"].split("---")[1].split()).casefold()

    missing = [name for name in KIT if "do not use" not in description(name)]
    assert not missing, f"descriptions without a 'Do NOT use' clause: {missing}"


def test_rendering_leaves_no_placeholder_and_the_source_uses_only_known_ones() -> None:
    leftovers = {source: re.findall(r"@SST_[A-Z_]+@", text) for source, text in TEXTS.items()}
    assert not {source: found for source, found in leftovers.items() if found}
    used = {token for path in KIT_ROOT.rglob("*.md") for token in re.findall(r"@SST_[A-Z_]+@", path.read_text())}
    assert used <= set(TOKENS), sorted(used - set(TOKENS))


def test_the_checks_catch_a_stale_command_flag_or_code() -> None:
    lines = list(_command_lines("Run `sst publish` then `sst validate --no-such-flag`.\n"))
    resolved = [_resolve(tokens) for tokens in lines]
    assert resolved[0][0] is None and resolved[0][2] == "sst publish"
    assert resolved[1][1] == ["--no-such-flag"]
    assert "SST-ZZZ999" not in ERROR_REGISTRY
