"""Render the Snowflake specification reference, `docs/reference/snowflake-spec.md`.

One table per specification SST writes: an agent's, each tool type's resources, and a CoWork
profile's. A row per key, nested keys as dotted paths, `[]` for a list's entries and `<name>`
for a named mapping's: its kind, what fills it, what it does, and where Snowflake documents it
(or the measured behavior SST relies on). Then where each passthrough block lands, what an
authored file holds that is not in the specification, and every documentation page cited.
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping

from snowflake_semantic_tools.domain import snowflake_docs
from snowflake_semantic_tools.domain.model.snowflake_spec import (
    AGENT_NOT_IN_SPEC,
    AGENT_PASSTHROUGH,
    AGENT_SPEC,
    COWORK_NOT_IN_SPEC,
    COWORK_PASSTHROUGH,
    COWORK_SPEC,
    TOOL_RESOURCES,
    Authored,
    Derived,
    Documented,
    Evidence,
    Fixed,
    Setting,
    Source,
    SpecNode,
)

# Where a measured key's section is, relative to this page.
_BEHAVIOR = "../design/snowflake-behavior.md"
_HEADER = "| Key | Kind | Filled from | What it does | Evidence |"


def render_snowflake_spec(*, generated: str) -> str:
    """Render the page: an introduction, then each specification, passthrough, exclusions and pages."""
    lines = [
        "# Snowflake specification reference",
        "",
        generated,
        "",
        "Every key SST writes into the specifications it sends Snowflake, generated from the tables",
        "SST renders and imports them by (`domain/model/snowflake_spec/`). Each key says what fills it:",
        "an authored key, an `sst_config.yml` setting, a fixed value, or a value SST works out. Its",
        "evidence is the Snowflake page that documents it, or, for a key Snowflake does not document,",
        f"the measured behavior recorded in [`snowflake-behavior.md`]({_BEHAVIOR}).",
        "",
        "A passthrough block merges into what SST writes, mapping into mapping; a key it would",
        "overwrite is `SST-PRS023`. `sst import` keeps what a live specification holds beyond these",
        "keys in passthrough, at whatever depth it sits.",
        "",
        "## Agent specification",
        "",
        "The `CREATE AGENT ... FROM SPECIFICATION` body each `agent.yml` renders to.",
        "",
        *_table(AGENT_SPEC),
        "",
        "### Tool resources",
        "",
        "Each tool's `tool_resources` entry, by its type; a built-in tool's, and an empty one, are left out.",
    ]
    for tool_type, node in TOOL_RESOURCES.items():
        lines += ["", f"#### `{tool_type}`", "", *_table(node)]
    lines += [
        "",
        "## CoWork profile specification",
        "",
        "The specification each `cowork_profile.yml` renders to (Snowflake private preview).",
        "",
        *_table(COWORK_SPEC),
        "",
        "## Passthrough",
        "",
        "| Authored block | Merges into |",
        "| --- | --- |",
        *_landings("agent", AGENT_PASSTHROUGH),
        *_landings("cowork_profile", COWORK_PASSTHROUGH),
        "",
        "## Not in the specification",
        "",
        "What an authored file or setting holds that SST does not write into the specification.",
        "",
        "| Key | Where it goes |",
        "| --- | --- |",
        *_excluded(AGENT_NOT_IN_SPEC),
        *_excluded(COWORK_NOT_IN_SPEC),
        "",
        "## Pages cited",
        "",
        "| Page | Defines |",
        "| --- | --- |",
        *(
            f"| [{_cell(page.title)}]({page.url}){' (preview)' if page.preview else ''} | {_cell(page.defines)} |"
            for page in snowflake_docs.PAGES
        ),
    ]
    return "\n".join(lines) + "\n"


def _table(node: SpecNode) -> list[str]:
    return [_HEADER, "| --- | --- | --- | --- | --- |", *_rows(node, "", None)]


def _rows(node: SpecNode, prefix: str, covering: Evidence | None) -> Iterator[str]:
    """Yield a row per key below `node`, depth first, each with its own evidence or the nearest above."""
    inherited = node.evidence or covering
    for item in node.keys:
        path = f"{prefix}{item.name}"
        evidence = item.evidence or inherited
        sources = "; ".join(_source(source) for source in item.sources) or "its keys below"
        yield f"| `{path}` | {item.kind.value} | {sources} | {_cell(item.summary)} | {_evidence(evidence)} |"
        if item.node is not None:
            below = f"{path}.<name>." if item.named else f"{path}."
            yield from _rows(item.node, below, item.evidence or inherited)
        elif isinstance(item.items, SpecNode):
            yield from _rows(item.items, f"{path}[].", item.evidence or inherited)


def _source(source: Source) -> str:
    if isinstance(source, Authored):
        return f"`{source.path}` ({source.kind})"
    if isinstance(source, Setting):
        return f"`+{source.key}` ({source.block}:)"
    if isinstance(source, Fixed):
        return f"always `{source.value}`"
    if isinstance(source, Derived):
        return _cell(source.note)
    raise TypeError(source)


def _evidence(evidence: Evidence | None) -> str:
    if evidence is None:
        return ""
    if isinstance(evidence, Documented):
        return f"[{_cell(evidence.page.title)}]({evidence.page.url})"
    return f"measured: [{_cell(evidence.section)}]({_BEHAVIOR}#{_anchor(evidence.section)})"


def _landings(kind: str, blocks: Mapping[str, tuple[str, ...]]) -> Iterator[str]:
    for block, path in blocks.items():
        where = ".".join("[]" if part == "*" else part for part in path) or "the specification's top level"
        yield f"| `{block}` ({kind}) | `{where}` |" if path else f"| `{block}` ({kind}) | {where} |"


def _excluded(entries: Mapping[str, str]) -> Iterator[str]:
    yield from (f"| `{key}` | {_cell(reason)} |" for key, reason in entries.items())


def _cell(text: str) -> str:
    return " ".join(text.split()).replace("|", "\\|")


def _anchor(title: str) -> str:
    """GitHub's heading anchor, as the other reference pages link headings."""
    return re.sub(r"[^a-z0-9 _-]", "", title.casefold()).replace(" ", "-")
