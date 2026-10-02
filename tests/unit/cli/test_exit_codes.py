"""The exit-code contract: every code each command documents, from a real invocation.

Exit 0 is OK, 1 an error, 2 a difference between declared and actual state, 3 a refused command
line, 4 a project that could not be resolved, 5 Snowflake unreachable, and 130 an interrupt.
Every case below is one real run of `sst`; the table at the bottom says which codes each command
may return, and every case's code must be one of its command's.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest
from click.testing import CliRunner, Result

from snowflake_semantic_tools.cli.main import cli
from snowflake_semantic_tools.domain.model.lifecycle import OwnershipMarker
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from tests.helpers.cli_projects import (
    REPO_ROOT,
    break_menu_view,
    common,
    compile_project,
    invoke_with_port,
    project_copy,
    skills_only_project,
)
from tests.helpers.recorded_snowflake import RecordedSnowflake

# Which exit codes each command may return.
EXIT_CODES = {
    "init": {0, 1, 3, 4},
    "debug": {0, 1, 3, 4, 5},
    "enrich": {0, 1, 2, 3, 4, 5},
    "compile": {0, 1, 3, 4},
    "validate": {0, 1, 3, 4, 5},
    "plan": {0, 1, 2, 3, 4, 5, 130},
    "apply": {0, 1, 3, 4, 5, 130},
    "list": {0, 1, 3, 4},
    "test": {0, 1, 3, 4, 5},
    "docs": {0, 1, 2, 3, 4},
    "clean": {0, 1, 3, 4},
    "migrate": {0, 1, 2, 3, 4},
    "explain": {0, 3},
    "drop": {0, 1, 3, 4, 5, 130},
    "diff": {0, 1, 2, 3, 4, 5},
    "baseline": {0, 1, 3, 4},
    "format": {0, 1, 2, 3, 4},
}
GOLDEN = REPO_ROOT / "tests" / "golden" / "expected" / "ddl"
DROP = ("DB.S.V", "--type", "semantic_view", "--target", "dev", "--yes")
PROFILES_FIXTURE = REPO_ROOT / "tests" / "fixtures" / "reference_project" / "profiles.yml"
Scenario = Callable[[Path, pytest.MonkeyPatch], Result]


def _run(*args: str) -> Result:
    return CliRunner().invoke(cli, list(args))


def _unreachable(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(params: object) -> RecordedSnowflake:
        raise SnowflakePortError("unreachable", diagnostic=None)

    monkeypatch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", refuse)


def _interrupt(monkeypatch: pytest.MonkeyPatch) -> None:
    def interrupt(params: object) -> RecordedSnowflake:
        raise KeyboardInterrupt

    monkeypatch.setattr("snowflake_semantic_tools.cli.main.SnowflakeConnector", interrupt)


def _dbt_project(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    (root / "dbt_project.yml").write_text("name: p\nprofile: p\n", encoding="utf-8")
    return root


def _yaml_project(root: Path, text: str) -> Path:
    (root / "sst_config.yml").write_text("project:\n  semantic_models_dir: semantic_models\n", encoding="utf-8")
    (root / "semantic_models").mkdir()
    (root / "semantic_models" / "views.yml").write_text(text, encoding="utf-8")
    return root


def _broken(tmp: Path) -> Path:
    project = project_copy(tmp)
    break_menu_view(project)
    return project


def _connected(tmp: Path, monkeypatch: pytest.MonkeyPatch, *args: str) -> Result:
    project = project_copy(tmp)
    compile_project(project)
    _unreachable(monkeypatch)
    return _run(args[0], *common(project), *args[1:])


SCENARIOS: dict[tuple[str, int], Scenario] = {
    ("init", 0): lambda tmp, _: _run("init", "--project-dir", str(_dbt_project(tmp / "p")), "--skip-prompts"),
    ("init", 1): lambda tmp, _: _run("init", "--project-dir", str(_dbt_project(tmp / "p")), "--check-only"),
    ("init", 3): lambda tmp, _: _run("init", "-s"),
    ("init", 4): lambda tmp, _: _run("init", "--project-dir", str(tmp / "not-dbt")),
    ("debug", 0): lambda tmp, _: _run("debug", "--project-dir", str(project_copy(tmp)), "--no-connect"),
    ("debug", 3): lambda tmp, _: _run("debug", "--select", "x"),
    ("debug", 4): lambda tmp, _: _run("debug", "--project-dir", str(tmp)),
    ("debug", 5): lambda tmp, mp: _debug_unreachable(tmp, mp),
    ("enrich", 3): lambda tmp, _: _run("enrich", "--select", "a,b"),
    ("enrich", 4): lambda tmp, _: _run("enrich", "--project-dir", str(tmp)),
    ("compile", 0): lambda tmp, _: _run("compile", *common(project_copy(tmp))),
    ("compile", 1): lambda tmp, _: _run("compile", *common(_broken(tmp))),
    ("compile", 3): lambda tmp, _: _run("compile", "--select", "a,b"),
    ("compile", 4): lambda tmp, _: _run("compile", "--project-dir", str(tmp)),
    ("validate", 0): lambda tmp, _: _run("validate", *common(project_copy(tmp))),
    ("validate", 1): lambda tmp, _: _run("validate", *common(project_copy(tmp)), "--strict"),
    ("validate", 3): lambda tmp, _: _run("validate", "--output", "csv"),
    ("validate", 4): lambda tmp, _: _run("validate", "--project-dir", str(tmp)),
    ("validate", 5): lambda tmp, mp: _connected(tmp, mp, "validate", "--snowflake-syntax-check"),
    ("plan", 0): lambda tmp, mp: _plan(tmp, mp, "--no-detailed-exitcode"),
    ("plan", 1): lambda tmp, mp: _plan_broken(tmp, mp),
    ("plan", 2): lambda tmp, mp: _plan(tmp, mp),
    ("plan", 3): lambda tmp, _: _run("plan", "--plan-out", "p.json", "--no-plan-out"),
    ("plan", 4): lambda tmp, _: _run("plan", *common(project_copy(tmp))),
    ("plan", 5): lambda tmp, mp: _connected(tmp, mp, "plan"),
    ("plan", 130): lambda tmp, mp: _interrupted(tmp, mp, "plan"),
    ("apply", 0): lambda tmp, mp: _apply_nothing(tmp, mp),
    ("apply", 1): lambda tmp, mp: _apply_broken(tmp, mp),
    ("apply", 3): lambda tmp, _: _run("apply", "--prune"),
    ("apply", 4): lambda tmp, _: _run("apply", *common(project_copy(tmp)), "--yes"),
    ("apply", 5): lambda tmp, mp: _connected(tmp, mp, "apply", "--yes"),
    ("apply", 130): lambda tmp, mp: _interrupted(tmp, mp, "apply", "--yes"),
    ("list", 0): lambda tmp, _: _listed(tmp),
    ("list", 3): lambda tmp, _: _run("list", "not-a-type"),
    ("list", 4): lambda tmp, _: _run("list", "--project-dir", str(project_copy(tmp))),
    ("test", 0): lambda tmp, _: _run(
        "test", *common(project_copy(tmp)), "--suite", "golden", "--golden-dir", str(GOLDEN)
    ),
    ("test", 1): lambda tmp, _: _run("test", *common(project_copy(tmp)), "--suite", "golden", "--golden-dir", str(tmp)),
    ("test", 3): lambda tmp, _: _run("test"),
    ("test", 4): lambda tmp, _: _run("test", "--project-dir", str(tmp), "--suite", "golden"),
    ("test", 5): lambda tmp, mp: _connected(tmp, mp, "test", "--suite", "smoke"),
    ("docs", 0): lambda tmp, _: _run("docs", "--project-dir", str(REPO_ROOT), "--check"),
    ("docs", 1): lambda tmp, _: _docs_unwritable(tmp),
    ("docs", 2): lambda tmp, _: _run("docs", "--project-dir", str(tmp), "--check"),
    ("docs", 3): lambda tmp, _: _run("docs", "--only", "errors"),
    ("clean", 0): lambda tmp, _: _run("clean", "--project-dir", str(project_copy(tmp))),
    ("clean", 1): lambda tmp, mp: _clean_refused(tmp, mp),
    ("clean", 3): lambda tmp, _: _run("clean", "--force"),
    ("clean", 4): lambda tmp, _: _run("clean", "--project-dir", str(tmp)),
    ("baseline", 0): lambda tmp, _: _run("baseline", "show", *common(project_copy(tmp))),
    ("baseline", 1): lambda tmp, _: _run("baseline", "add", "SST-VAL116", *common(project_copy(tmp))),
    ("baseline", 3): lambda tmp, _: _run("baseline", "renew"),
    ("baseline", 4): lambda tmp, _: _run("baseline", "add", "SST-CFG018", "--project-dir", str(tmp)),
    ("diff", 0): lambda tmp, mp: _diff(tmp, mp, "--from", "dev", "--to", "prod"),
    ("diff", 1): lambda tmp, _: _run("diff", *common(project_copy(tmp)), "--to", "saved.json"),
    ("diff", 2): lambda tmp, mp: _diff(tmp, mp),
    ("diff", 3): lambda tmp, _: _run("diff", "--select", "a,b"),
    ("diff", 4): lambda tmp, _: _run("diff", "--project-dir", str(tmp)),
    ("diff", 5): lambda tmp, mp: _connected(tmp, mp, "diff"),
    ("drop", 0): lambda tmp, mp: _drop(tmp, mp, exists=True),
    ("drop", 1): lambda tmp, mp: _drop(tmp, mp, exists=False),
    ("drop", 3): lambda tmp, _: _run("drop", "DB.S.V", "--type", "semantic_view", "--target", "dev"),
    ("drop", 4): lambda tmp, _: _run("drop", *DROP, "--project-dir", str(tmp)),
    ("drop", 5): lambda tmp, mp: _drop_without_snowflake(tmp, mp, _unreachable),
    ("drop", 130): lambda tmp, mp: _drop_without_snowflake(tmp, mp, _interrupt),
    ("explain", 0): lambda tmp, _: _run("explain", "SST-VAL009", "--project-dir", str(tmp)),
    ("explain", 3): lambda tmp, _: _run("explain", "SST-NOPE01"),
    ("format", 0): lambda tmp, _: _run("format", "--project-dir", str(_yaml_project(tmp, "a: 1\n"))),
    ("format", 1): lambda tmp, _: _run("format", "--project-dir", str(_yaml_project(tmp, "a: [1,\n"))),
    ("format", 2): lambda tmp, _: _run("format", "--project-dir", str(_yaml_project(tmp, "a:   1\n")), "--check"),
    ("format", 3): lambda tmp, _: _run("format", "--project-dir", str(_yaml_project(tmp, "a: 1\n")), "nothing.yml"),
    ("format", 4): lambda tmp, _: _run("format", "--project-dir", str(tmp)),
    ("migrate", 0): lambda tmp, _: _run("migrate", "refs", "--project-dir", str(project_copy(tmp))),
    ("migrate", 3): lambda tmp, _: _run("migrate", "refs", "--bogus"),
    ("migrate", 4): lambda tmp, _: _run("migrate", "refs", "--project-dir", str(tmp)),
}


def _debug_unreachable(tmp: Path, monkeypatch: pytest.MonkeyPatch) -> Result:
    project = project_copy(tmp)
    _unreachable(monkeypatch)
    return _run("debug", "--project-dir", str(project))


def _plan(tmp: Path, monkeypatch: pytest.MonkeyPatch, *flags: str) -> Result:
    project = project_copy(tmp)
    return invoke_with_port(monkeypatch, RecordedSnowflake(state={}), ["plan", *common(project), *flags])


def _diff(tmp: Path, monkeypatch: pytest.MonkeyPatch, *flags: str) -> Result:
    project = project_copy(tmp)
    compile_project(project)
    return invoke_with_port(monkeypatch, RecordedSnowflake(state={}), ["diff", *common(project), *flags])


def _drop_project(tmp: Path) -> Path:
    project = _dbt_project(tmp / "p")
    (project / "profiles.yml").write_text(PROFILES_FIXTURE.read_text(encoding="utf-8"), encoding="utf-8")
    (project / "dbt_project.yml").write_text("name: p\nprofile: sst_reference_impl\n", encoding="utf-8")
    return project


def _drop(tmp: Path, monkeypatch: pytest.MonkeyPatch, *, exists: bool) -> Result:
    marker = OwnershipMarker("a" * 64, "b" * 64)
    port = RecordedSnowflake(existing=("DB.S.V",) if exists else (), markers={"DB.S.V": marker})
    return invoke_with_port(monkeypatch, port, ["drop", *DROP, "--project-dir", str(_drop_project(tmp))])


def _drop_without_snowflake(
    tmp: Path, monkeypatch: pytest.MonkeyPatch, broken: Callable[[pytest.MonkeyPatch], None]
) -> Result:
    project = _drop_project(tmp)
    broken(monkeypatch)
    return _run("drop", *DROP, "--project-dir", str(project))


def _plan_broken(tmp: Path, monkeypatch: pytest.MonkeyPatch) -> Result:
    project = project_copy(tmp)
    compile_project(project)
    break_menu_view(project)
    return invoke_with_port(monkeypatch, RecordedSnowflake(state={}), ["plan", *common(project)])


def _apply_nothing(tmp: Path, monkeypatch: pytest.MonkeyPatch) -> Result:
    project = skills_only_project(tmp / "skills")
    assert _run("compile", "--project-dir", str(project)).exit_code == 0
    return invoke_with_port(monkeypatch, RecordedSnowflake(state={}), ["apply", "--project-dir", str(project), "--yes"])


def _apply_broken(tmp: Path, monkeypatch: pytest.MonkeyPatch) -> Result:
    project = project_copy(tmp)
    compile_project(project)
    break_menu_view(project)
    return invoke_with_port(monkeypatch, RecordedSnowflake(state={}), ["apply", *common(project), "--yes"])


def _interrupted(tmp: Path, monkeypatch: pytest.MonkeyPatch, *args: str) -> Result:
    project = project_copy(tmp)
    compile_project(project)
    _interrupt(monkeypatch)
    return _run(args[0], *common(project), *args[1:])


def _listed(tmp: Path) -> Result:
    project = project_copy(tmp)
    compile_project(project)
    return _run("list", "--project-dir", str(project), "semantic_view", "--long", "--exclude", "jaffle_menu")


def _docs_unwritable(tmp: Path) -> Result:
    (tmp / "docs").write_text("a file where the docs directory goes", encoding="utf-8")
    return _run("docs", "--project-dir", str(tmp))


def _clean_refused(tmp: Path, monkeypatch: pytest.MonkeyPatch) -> Result:
    project = project_copy(tmp)
    (project / "target" / "sst").mkdir(parents=True)

    def refuse(path: Path) -> None:
        raise PermissionError("read-only")

    monkeypatch.setattr("snowflake_semantic_tools.cli.commands.clean.shutil.rmtree", refuse)
    return _run("clean", "--project-dir", str(project))


@pytest.mark.parametrize(("command", "code"), sorted(SCENARIOS), ids=[f"{c}-{n}" for c, n in sorted(SCENARIOS)])
def test_each_documented_exit_code_is_returned_by_a_real_invocation(
    command: str, code: int, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    result = SCENARIOS[(command, code)](tmp_path, monkeypatch)
    assert result.exit_code == code, result.output
    assert code in EXIT_CODES[command]


def test_every_command_is_in_the_exit_code_table() -> None:
    assert set(EXIT_CODES) == set(cli.commands)
