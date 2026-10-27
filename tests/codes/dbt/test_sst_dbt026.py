"""SST-DBT026: `packages.yml` declares packages that are not installed, so dbt is not run."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.dbt.invoke import parse_project
from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.dbt_codes import refused, writing_dbt
from tests.helpers.seam_projects import FakeDbt


def test_sst_dbt026_fires(tmp_path: Path) -> None:
    (tmp_path / "packages.yml").write_text("packages:\n  - package: dbt-labs/dbt_utils\n    version: 1.3.0\n")
    dbt = FakeDbt()
    diagnostic = refused(tmp_path, dbt)
    assert (diagnostic.code, diagnostic.severity) == ("SST-DBT026", Severity.ERROR)
    assert diagnostic.message == "1 package(s) in packages.yml are not installed"
    assert dbt.runs == []


def test_sst_dbt026_silent(tmp_path: Path) -> None:
    (tmp_path / "packages.yml").write_text("packages:\n  - package: dbt-labs/dbt_utils\n    version: 1.3.0\n")
    (tmp_path / "dbt_packages" / "dbt_utils").mkdir(parents=True)
    assert parse_project(tmp_path, "dev", tmp_path / "target" / "manifest.json", runner=writing_dbt(tmp_path)) == ()


def test_sst_dbt026_silent_for_a_git_package_dbt_installed_under_its_own_name(tmp_path: Path) -> None:
    # dbt installs a package under the `name:` in its own dbt_project.yml, which package-lock.yml
    # records; the repository name is not the package name.
    git = "https://github.com/dbt-labs/dbt-audit-helper.git"
    (tmp_path / "packages.yml").write_text(f'packages:\n  - git: "{git}"\n    revision: 0.12.1\n')
    (tmp_path / "package-lock.yml").write_text(
        f'packages:\n  - name: audit_helper\n    git: "{git}"\n    revision: abc\n'
    )
    (tmp_path / "dbt_packages" / "audit_helper").mkdir(parents=True)
    assert parse_project(tmp_path, "dev", tmp_path / "target" / "manifest.json", runner=writing_dbt(tmp_path)) == ()


def test_sst_dbt026_fires_when_the_locked_name_is_not_installed(tmp_path: Path) -> None:
    git = "https://github.com/dbt-labs/dbt-audit-helper.git"
    (tmp_path / "packages.yml").write_text(f'packages:\n  - git: "{git}"\n')
    (tmp_path / "package-lock.yml").write_text(f'packages:\n  - name: audit_helper\n    git: "{git}"\n')
    (tmp_path / "dbt_packages" / "dbt-audit-helper").mkdir(parents=True)
    assert refused(tmp_path, FakeDbt()).code == "SST-DBT026"
