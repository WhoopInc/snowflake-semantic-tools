"""`sst migrate split`: one file per member, and tool group members in their group's folder.

A semantic model file listing members under a list root (`snowflake_metrics:` and the rest)
becomes one file per member, `<same folder>/<name>.yml`, holding it under the singular root
(`metric:`). A tools file listing members does the same with `tool:`. A tools file listing tool
groups, as SST read them before groups went, becomes one `tool:` file per member under
`<file's folder>/<group>/`, so a `tools.<group>` route in `sst_config.yml` routes that folder:

- the group's `owner` becomes each member's `meta.owner`, unless the member names its own;
- its `description` becomes a comment heading each member's file;
- `immutable: true` becomes a folder route, or each member's `config.immutable` when the folder
  already holds other tool files;
- a `{{ file() }}` path a member writes is re-based on the new folder.

Each member keeps its comments: those above it, back to the member before, open its file, and the
file's own header opens the first. Indentation is re-based to two spaces under the root. Nothing
here reads or writes files: the plan says what to write, and the caller checks that no file it
names already exists.
"""

from __future__ import annotations

import posixpath
from collections.abc import Callable, Sequence
from dataclasses import dataclass

from snowflake_semantic_tools.domain.file_names import file_name
from snowflake_semantic_tools.domain.migrate.calls import relative_to
from snowflake_semantic_tools.domain.migrate.edits import dash_line, entry_span, is_comment, shift, value_end
from snowflake_semantic_tools.domain.migrate.outline import Entry, Node
from snowflake_semantic_tools.domain.migrate.semantic import LIST_ROOTS
from snowflake_semantic_tools.domain.parse.template import TemplateCall, TemplateSyntaxError, replace_template_calls

_ROOTS = {root: singular for singular, root in LIST_ROOTS.items()}


@dataclass(frozen=True, slots=True)
class NewFile:
    """One file the split writes: its project-relative path, its text, and the member it holds."""

    path: str
    text: str
    name: str


@dataclass(frozen=True, slots=True)
class SplitPlan:
    """How one file splits, or why it is left as it is.

    Attributes:
        files: The files to write, in member order; () when refused.
        refused: Why the file is left whole; None when it splits.
        routes: For a tools file, each folder under the tools directory that becomes an
            `+immutable` route, as its path parts.
    """

    path: str
    files: tuple[NewFile, ...] = ()
    refused: str | None = None
    routes: tuple[tuple[str, ...], ...] = ()


@dataclass(frozen=True, slots=True)
class _Group:
    """What a tool group passes on to each member it holds."""

    folder: str
    owner: str | None
    heading: tuple[str, ...]
    immutable_config: bool


def split_semantic(lines: Sequence[str], root: Node, path: str) -> SplitPlan | None:
    """Plan the split of one semantic model file; None when it lists no members.

    A file is refused when it holds any root key besides its member lists, or a member that is
    not a mapping with a `name:`, or two members of one name.
    """
    listed = [entry for entry in root.entries if entry.key in _ROOTS and entry.value.is_seq]
    if not listed:
        return None
    others = [entry.key for entry in root.entries if entry not in listed]
    if others:
        return SplitPlan(path, refused=f"it holds {', '.join(others)} beside its member lists")
    items = [(entry, item) for entry in listed for item in entry.value.items]
    return _plan(lines, root, path, items, lambda entry, _item: (_ROOTS[entry.key], posixpath.dirname(path), None))


def split_tools(
    lines: Sequence[str], root: Node, path: str, tools_dir: str, folder_taken: Callable[[str], bool]
) -> SplitPlan | None:
    """Plan the split of one tools file; None when it holds no `tools:` list.

    Args:
        tools_dir: The tools directory, project-relative, which routes are relative to.
        folder_taken: Whether a project-relative folder already holds tool files, so that a
            route on it would reach members beyond the group's.
    """
    listed = root.entry("tools")
    if listed is None or not listed.value.is_seq:
        return None
    others = [entry.key for entry in root.entries if entry is not listed]
    if others:
        return SplitPlan(path, refused=f"it holds {', '.join(others)} beside tools:")
    folder = posixpath.dirname(path)
    items: list[tuple[Entry, Node]] = []
    groups: dict[int, _Group] = {}
    routes: list[tuple[str, ...]] = []
    for item in listed.value.items:
        group = item.get("group") if item.is_map else None
        if group is None:
            items.append((listed, item))
            continue
        name = group.text or ""
        target = posixpath.join(folder, name)
        flag = item.get("immutable")
        immutable = flag is not None and (flag.text or "").casefold() == "true"
        taken = immutable and folder_taken(target)
        info = _Group(target, _owner(lines, item.get("owner")), _heading(name, item.get("description")), taken)
        if immutable and not taken:
            routes.append(tuple(relative_to(target, tools_dir).split("/")))
        for ownership in ("define", "reference"):
            members = item.get(ownership)
            for member in members.items if members is not None and members.is_seq else ():
                groups[id(member)] = info
                items.append((item.entry(ownership) or listed, member))
    plan = _plan(lines, root, path, items, lambda _entry, member: ("tool", *_group_place(groups, member, folder)))
    return SplitPlan(plan.path, plan.files, plan.refused, () if plan.refused else tuple(routes))


def _group_place(groups: dict[int, _Group], member: Node, folder: str) -> tuple[str, _Group | None]:
    group = groups.get(id(member))
    return (group.folder if group is not None else folder), group


def _plan(
    lines: Sequence[str],
    root: Node,
    path: str,
    items: list[tuple[Entry, Node]],
    place: Callable[[Entry, Node], tuple[str, str, _Group | None]],
) -> SplitPlan:
    """Write each listed item as a file of its own; refused when one has no usable name."""
    header = [line.lstrip() for line in lines[: root.entries[0].line] if is_comment(line) or line.strip() == "---"]
    explicit = any(line.strip() == "---" for line in lines[: root.entries[0].line])
    roots = {entry.line for entry in root.entries}
    files: list[NewFile] = []
    seen: set[str] = set()
    previous = root.entries[0].line
    for index, (holder, item) in enumerate(items):
        name = item.get("name") if item.is_map else None
        if name is None or not name.text:
            return SplitPlan(path, refused=f"an entry of {holder.key} is not a mapping with a name")
        singular, folder, group = place(holder, item)
        target = posixpath.join(folder, file_name(name.text) + ".yml")
        if target in seen:
            return SplitPlan(path, refused=f"two members would both be written to {target}")
        seen.add(target)
        dash_col = holder.value.col
        start = dash_line(lines, item, dash_col) if not holder.value.flow else item.line
        lead = [
            line.lstrip()
            for number, line in enumerate(lines[previous:start], previous)
            if is_comment(line) and number not in roots
        ]
        end = value_end(lines, item, dash_col)
        opening = (
            (header if index == 0 else ["---\n"] if explicit else []) + list(group.heading if group else ()) + lead
        )
        body = _member_body(lines, item, start, end, group, path, folder)
        files.append(NewFile(target, "".join(opening) + f"{singular}:{body}", name.text))
        previous = end
    trailing = [line.lstrip() for line in lines[previous:] if is_comment(line)]
    if trailing and files:
        last = files[-1]
        files[-1] = NewFile(last.path, last.text + "".join(trailing), last.name)
    return SplitPlan(path, tuple(files))


def _member_body(
    lines: Sequence[str], item: Node, start: int, end: int, group: _Group | None, source: str, folder: str
) -> str:
    """Return one member's text after its root key: ` {flow}` or a block indented by two."""
    if item.flow:
        return " " + lines[item.line][item.col : item.end_col] + "\n"
    extra: dict[int, str] = {}
    if group is not None:
        _group_lines(lines, item, group, extra)
    out: list[str] = []
    for number in range(start, end):
        line = lines[number]
        if number == start and number != item.line:
            # The dash stands alone on its line, above the member's first key.
            continue
        if number == start:
            dash = line.index("-")
            line = line[:dash] + " " + line[dash + 1 :]
        out.append(line if line.endswith("\n") else line + "\n")
        out.append(extra.get(number, ""))
    out.append(extra.get(-1, ""))
    body = shift("".join(out), 2 - item.col)
    if group is not None and folder != posixpath.dirname(source):
        body = _rebased(body, posixpath.dirname(source), folder)
    return "\n" + body


def _group_lines(lines: Sequence[str], item: Node, group: _Group, extra: dict[int, str]) -> None:
    """Add the group's `meta.owner` and `config.immutable` to a member, at the member's own indent.

    Each addition is keyed by the line it follows; -1 is the end of the member.
    """
    pad = " " * item.col
    meta = item.entry("meta")
    if group.owner is not None and item.get("owner") is None:
        if meta is None:
            extra[-1] = extra.get(-1, "") + f"{pad}meta:\n{pad}  owner: {group.owner}\n"
        elif meta.value.is_map and not meta.value.flow and meta.value.get("owner") is None and meta.value.entries:
            last = entry_span(lines, meta)[1] - 1
            extra[last] = extra.get(last, "") + " " * meta.value.entries[0].col + f"owner: {group.owner}\n"
    if group.immutable_config and item.get("config") is None:
        extra[-1] = extra.get(-1, "") + f"{pad}config:\n{pad}  immutable: true\n"


def _owner(lines: Sequence[str], owner: Node | None) -> str | None:
    """Return a group's `owner:` as it is written on its line; None when absent or not one line."""
    if owner is None or owner.text is None or owner.line != owner.end_line:
        return None
    return lines[owner.line][owner.col : owner.end_col]


def _heading(name: str, description: Node | None) -> tuple[str, ...]:
    """Return the comment lines carrying a group's description into each member's file."""
    text = description.text if description is not None else None
    if not text:
        return ()
    return (f"# From tool group {name}:\n", *(f"#   {line}\n".replace("   \n", "\n") for line in text.splitlines()))


def _rebased(body: str, old: str, new: str) -> str:
    """Re-base each `{{ file('<path>') }}` in a moved member from folder `old` to folder `new`."""

    def moved(call: TemplateCall) -> str:
        if len(call.args) != 1:
            return call.raw
        target = posixpath.normpath(posixpath.join(old, call.args[0]))
        return call.raw.replace(call.args[0], relative_to(target, new), 1)

    try:
        return replace_template_calls(body, "file", moved)
    except TemplateSyntaxError:
        return body
