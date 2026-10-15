"""The migrate use cases rewrite and split files in memory and report what they changed."""

from __future__ import annotations

from snowflake_semantic_tools.adapters.yaml.outline import outline
from snowflake_semantic_tools.app.migrate_refs import KINDS, MigrateRefs, RefsFiles
from snowflake_semantic_tools.app.migrate_split import MigrateSplit, SplitFiles
from snowflake_semantic_tools.domain.migrate.refs import FilterSite

GROUP = "tools:\n  - group: g\n    immutable: true\n    reference:\n      - name: r\n        type: stage\n"


def counts(**found: int) -> dict[str, int]:
    return {kind: found.get(kind, 0) for kind in KINDS}


def test_migrate_refs_rewrites_counts_and_reports_untouched_calls() -> None:
    files = {
        "b.yml": "tables:\n  - {{ table('orders') }}\nnote: \"{{ table('orders') }}\"\n",
        "a.yml": "snowflake_filters:\n  - name: done\n    expr: \"{{ column('orders', 'state') }} = 'x'\"\n",
        "c.yml": "clean: true\n",
    }
    seen: list[str] = []

    def locate(text: str, path: str) -> tuple[FilterSite, ...]:
        seen.append(path)
        if path != "a.yml":
            return ()
        return (FilterSite("done", "{{ ref('orders', 'state') }} = 'x'", False, 3, 4),)

    report = MigrateRefs(RefsFiles(files), locate, outline).run()
    assert seen == ["a.yml", "b.yml", "c.yml"]
    assert [item.path for item in report.changed] == ["a.yml", "b.yml"]
    assert report.changed[0].counts() == counts(column=1, labels=1)
    assert report.changed[1].counts() == counts(ref=1)
    assert report.untouched == 1
    assert "labels:\n      - filter\n" in report.changed[0].result.text


def test_migrate_refs_reads_every_kind_of_file() -> None:
    files = RefsFiles(
        semantic={"s/v.yml": "semantic_view:\n  name: v\n  enabled: true\n  metrics: [m]\n", "s/bad.yml": "a: [\n"},
        tools={
            "tools/t.yml": "tool:\n  name: t\n  type: cortex_search_service\n  on: \"{{ ref('m') }}\"\n  id_column: i\n"
        },
        agents={
            "agents/a/agent.yml": "name: a\nalias: x\nspec:\n  tools:\n    - name: t\n      type: cortex_search\n"
            "      search_service: \"{{ tool('g', 't') }}\"\n      id_column: i\n",
            "agents/b/agent.yml": "- a list\n",
        },
        evals={"agents/a/evals/config.yml": "agent: a\nname: c\n"},
        hooks={"hooks/h/hook.yml": "script: run.sh\n"},
        commands={"commands/c.md": "---\nskill: s\n---\n{{ tool('g', 't') }}\n"},
        config={"sst_config.yml": "x: \"{{ tool('g', 't') }}\"\n"},
    )
    report = MigrateRefs(files, lambda text, path: (), outline).run()
    by_path = {item.path: item for item in report.files}
    assert by_path["s/v.yml"].counts() == counts(reference=1, setting=1)
    assert by_path["tools/t.yml"].counts() == counts(tool_column=1)
    assert by_path["agents/a/agent.yml"].counts() == counts(tool=1, tool_column=1, setting=1)
    assert not by_path["agents/b/agent.yml"].result.changed and not by_path["s/bad.yml"].result.changed
    assert by_path["agents/a/evals/config.yml"].counts() == counts(removed=1)
    assert by_path["hooks/h/hook.yml"].counts() == counts(file=1)
    assert by_path["commands/c.md"].counts() == counts(reference=1, tool=1)
    assert by_path["sst_config.yml"].counts() == counts(tool=1)


def test_migrate_split_plans_each_file_refuses_overwrites_and_rewrites_the_configuration() -> None:
    files = SplitFiles(
        semantic={
            "s/metrics.yml": "snowflake_metrics:\n  - name: a\n  - name: b\n",
            "s/filters.yml": "snowflake_filters:\n  - name: a\n",
            "s/one.yml": "metric:\n  name: c\n",
            "s/views.yml": "semantic_views:\n  - name: taken\n",
            "s/bad.yml": "a: [\n",
            "s/list.yml": "- a\n",
        },
        tools={
            "tools/g.yml": GROUP,
            "tools/bad.yml": "x: [\n",
        },
        config=("sst_config.yml", "tools:\n  +schema: S\n"),
    )
    report = MigrateSplit(files, outline, {"s/taken.yml"}.__contains__).run()
    assert [(plan.path, plan.refused) for plan in report.plans] == [
        ("s/filters.yml", None),
        ("s/metrics.yml", "another list is split into s/a.yml too"),
        ("s/views.yml", "s/taken.yml already exists"),
        ("tools/g.yml", None),
    ]
    assert [plan.path for plan in report.splits] == ["s/filters.yml", "tools/g.yml"]
    assert [plan.path for plan in report.refused] == ["s/metrics.yml", "s/views.yml"]
    assert report.changed and report.config is not None
    assert report.config[1].text == "tools:\n  +schema: S\n  g:\n    +immutable: true\n"


def test_a_split_without_configuration_or_with_unreadable_configuration() -> None:
    empty = MigrateSplit(SplitFiles(semantic={}), outline, lambda path: False).run()
    assert empty.config is None and not empty.changed
    broken = MigrateSplit(SplitFiles(semantic={}, config=("c.yml", "a: [\n")), outline, lambda path: False).run()
    assert broken.config is not None and not broken.config[1].changed and not broken.changed
    claimed = MigrateSplit(
        SplitFiles(
            semantic={"a/x.yml": "snowflake_metrics:\n  - name: m\n", "a/y.yml": "snowflake_filters:\n  - name: m\n"}
        ),
        outline,
        lambda path: False,
    ).run()
    assert [plan.refused for plan in claimed.plans] == [None, "another list is split into a/m.yml too"]
    moved = MigrateSplit(
        SplitFiles(semantic={}, config=("c.yml", "skills:\n  stage:\n    +stage: S\n")), outline, lambda path: False
    )
    assert moved.run().changed
