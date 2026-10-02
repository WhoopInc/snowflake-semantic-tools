"""The migrate use case rewrites files in memory and reports what it changed."""

from __future__ import annotations

from snowflake_semantic_tools.app.migrate_refs import MigrateRefs
from snowflake_semantic_tools.domain.migrate.refs import FilterSite


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

    report = MigrateRefs(files, locate).run()
    assert seen == ["a.yml", "b.yml", "c.yml"]
    assert [item.path for item in report.changed] == ["a.yml", "b.yml"]
    assert report.changed[0].counts() == {"ref": 0, "bare": 0, "column": 1, "labels": 1}
    assert report.changed[1].counts() == {"ref": 1, "bare": 0, "column": 0, "labels": 0}
    assert report.untouched == 1
    assert "labels:\n      - filter\n" in report.changed[0].result.text
