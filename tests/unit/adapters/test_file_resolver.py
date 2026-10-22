"""One `file()` resolver for every sidecar: each call site follows the same rules.

A path resolves relative to the folder of the file that declares it. An absolute path, or one
that climbs out of the project, is SST-REF027; a symbolic link, even one pointing inside the
project, is SST-PRT009; a missing file SST-REF014; one holding only whitespace SST-LOD019.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.yaml.agents import load_agents
from snowflake_semantic_tools.adapters.yaml.coco_desktop_profiles import load_coco_desktop_profile_catalog
from snowflake_semantic_tools.adapters.yaml.discover import discover_yaml
from snowflake_semantic_tools.adapters.yaml.documents import load_documents
from snowflake_semantic_tools.adapters.yaml.fields import read_sidecar, resolve_file
from snowflake_semantic_tools.adapters.yaml.parse import parse_yaml_bytes
from snowflake_semantic_tools.adapters.yaml.semantic.readers import load_verified_queries
from snowflake_semantic_tools.adapters.yaml.tools import load_tool_catalog
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Origin
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from snowflake_semantic_tools.domain.parse.template import single_template_call

CONTENT = "SELECT 1"


@dataclass(frozen=True, slots=True)
class Read:
    """What one call site made of its sidecar: the codes it reported, the text, and its sources."""

    codes: tuple[str, ...]
    content: str | None
    sources: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Site:
    """One call site: the folder its declaring file sits in, and how to load it with a value."""

    folder: str
    load: Callable[[Path, str], Read]
    # Whether a value with no template is refused (SST-REF050) or read as inline text.
    bare_refused: bool


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _agent(project: Path, value: str) -> Read:
    _write(project / "agents/router/agent.yml", f"name: router\nspec:\n  instructions:\n    orchestration: {value}\n")
    [agent], diagnostics = load_agents(project)
    return Read(tuple(item.code for item in diagnostics), agent.orchestration_instructions, agent.source_files)


def _verified_query(project: Path, value: str) -> Read:
    _write(
        project / "semantic_models/queries/q.yml",
        f"verified_query:\n  name: q\n  question: How many?\n  sql: {value}\n",
    )
    documents = load_documents(discover_yaml(project, "semantic_models"), parse_yaml_bytes)
    queries, diagnostics = load_verified_queries(documents, project, "semantic_models")
    content = queries[0].sql if queries else None
    sources = (queries[0].sql_file,) if queries and queries[0].sql_file else ()
    return Read(tuple(item.code for item in diagnostics), content, sources)


def _tool(project: Path, value: str) -> Read:
    _write(
        project / "tools/ops/lookup.yml",
        f"tool:\n  name: lookup\n  type: procedure\n  language: sql\n  returns: NUMBER\n  body: {value}\n",
    )
    catalog = load_tool_catalog(
        project, DbtCatalog("v12", "1.11", "p", ()), target_name="dev", declared_targets=frozenset(("dev",))
    )
    [member] = catalog.members
    sources = (member.body_file,) if member.body is not None and member.body_file else ()
    return Read(tuple(item.code for item in catalog.diagnostics), member.body, sources)


def _hook(project: Path, value: str) -> Read:
    _write(project / "hooks/guard/hook.yml", f"event: PreToolUse\ncommand: bash\nscript: {value}\n")
    catalog = load_coco_desktop_profile_catalog(
        project, coco_desktop_profiles_dir="coco_desktop_profiles", hooks_dir="hooks", mcp_servers_dir="mcp-servers"
    )
    hook = catalog.hooks[0] if catalog.hooks else None
    content = hook.script.content.decode("utf-8") if hook is not None else None
    sources = (hook.script_file,) if hook is not None else ()
    return Read(tuple(item.code for item in catalog.diagnostics), content, sources)


SITES = {
    "agent instruction": Site("agents/router", _agent, bare_refused=False),
    "verified query sql": Site("semantic_models/queries", _verified_query, bare_refused=False),
    "tool body": Site("tools/ops", _tool, bare_refused=True),
    "hook script": Site("hooks/guard", _hook, bare_refused=True),
}


def _call(path: str) -> str:
    return f"\"{{{{ file('{path}') }}}}\""


@pytest.mark.parametrize("site", SITES.values(), ids=SITES.keys())
def test_a_path_resolves_relative_to_the_declaring_file_and_joins_its_sources(tmp_path: Path, site: Site) -> None:
    _write(tmp_path / site.folder / "side/body.txt", CONTENT + "\n")
    read = site.load(tmp_path, _call("side/body.txt"))
    assert read.codes == ()
    assert read.content is not None and read.content.strip() == CONTENT
    assert f"{site.folder}/side/body.txt" in read.sources


@pytest.mark.parametrize("site", SITES.values(), ids=SITES.keys())
def test_a_path_may_climb_to_another_folder_of_the_project(tmp_path: Path, site: Site) -> None:
    _write(tmp_path / "shared/body.txt", CONTENT)
    depth = "../" * len(Path(site.folder).parts)
    read = site.load(tmp_path, _call(f"{depth}shared/body.txt"))
    assert read.codes == ()
    assert "shared/body.txt" in read.sources


@pytest.mark.parametrize("site", SITES.values(), ids=SITES.keys())
@pytest.mark.parametrize(
    ("path", "code"),
    [
        ("../../../../../outside.txt", "SST-REF027"),
        ("/etc/hosts", "SST-REF027"),
        ("missing.txt", "SST-REF014"),
        ("empty.txt", "SST-LOD019"),
        ("linked.txt", "SST-PRT009"),
        ("linked_dir/body.txt", "SST-PRT009"),
    ],
)
def test_a_sidecar_that_cannot_be_used_is_one_code_at_every_site(
    tmp_path: Path, site: Site, path: str, code: str
) -> None:
    folder = tmp_path / site.folder
    _write(folder / "empty.txt", "  \n")
    _write(tmp_path / "real/body.txt", CONTENT)
    # Both links point inside the project, and are refused all the same.
    (folder / "linked.txt").symlink_to(tmp_path / "real/body.txt")
    (folder / "linked_dir").symlink_to(tmp_path / "real", target_is_directory=True)
    read = site.load(tmp_path, _call(path))
    assert read.codes == (code,)
    assert read.content is None or read.content.strip() != CONTENT


@pytest.mark.parametrize("site", SITES.values(), ids=SITES.keys())
def test_a_bare_value_is_refused_where_only_a_file_is_read(tmp_path: Path, site: Site) -> None:
    _write(tmp_path / site.folder / "body.txt", CONTENT)
    read = site.load(tmp_path, "body.txt")
    assert read.codes == (("SST-REF050",) if site.bare_refused else ())


def _origin() -> Origin:
    return Origin("agents/a/agent.yml", 3)


def test_the_resolver_reads_the_file_once_and_names_it_without_its_climb(tmp_path: Path) -> None:
    _write(tmp_path / "agents/shared/route.md", "Route.\n")
    call = single_template_call("{{ file('../shared/route.md') }}", "file")
    assert call is not None
    sidecar = resolve_file(tmp_path, "agents/a/agent.yml", call, origin=_origin())
    assert not isinstance(sidecar, Diagnostic)
    assert (sidecar.path, sidecar.text) == ("agents/shared/route.md", "Route.\n")


def test_a_text_sidecar_must_be_utf8_and_a_byte_sidecar_need_not_be(tmp_path: Path) -> None:
    (tmp_path / "agents/a").mkdir(parents=True)
    (tmp_path / "agents/a/blob.bin").write_bytes(b"\xff\xfe")
    value = "{{ file('blob.bin') }}"

    def read(*, text: bool) -> object:
        return read_sidecar(
            tmp_path, "agents/a/agent.yml", value, origin=_origin(), field="script", artifact="a", text=text
        )

    refused = read(text=True)
    assert isinstance(refused, Diagnostic) and refused.code == "SST-LOD006"
    accepted = read(text=False)
    assert not isinstance(accepted, Diagnostic) and getattr(accepted, "content", None) == b"\xff\xfe"


def test_a_sidecar_that_cannot_be_read_is_sst_lod018(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write(tmp_path / "agents/a/route.md", "Route.")

    def unreadable(self: Path) -> bytes:
        raise OSError("denied")

    monkeypatch.setattr(Path, "read_bytes", unreadable)
    found = read_sidecar(
        tmp_path, "agents/a/agent.yml", "{{ file('route.md') }}", origin=_origin(), field="x", artifact="a"
    )
    assert isinstance(found, Diagnostic) and found.code == "SST-LOD018"
