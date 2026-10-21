"""Rewrite the engine's goldens from the reference project: `python -m tests.helpers.update_goldens`.

Run from the repository root when a change to rendered output is intended, then review the
diff and commit it. The reference project compiles from the vendored dbt manifest, as
`tests/unit/test_goldens.py` compiles it, and each golden the output no longer equals is
rewritten; a DDL golden keeps its `--` provenance header. A second copy, with
`fixtures/scoped_views/semantic_views.yml` added at `semantic_models/semantic_views/scoped/`,
rewrites the DDL goldens of the views only it declares, which `tests/unit/test_golden_ddl.py`
compares. A third, whose CoWork profile has the eval in `fixtures/cowork_profile_eval/`
(`with_profile_eval`), rewrites the goldens of that eval, which `tests/unit/test_golden_evals.py`
compares. Refused when `$CI` is set to anything but empty, `0`, `false` or `no`: a pipeline that
rewrites its own expectations asserts nothing. The enrich goldens are not written here;
`tests/golden/README.md` says how to change them.
"""

from __future__ import annotations

import dataclasses
import os
import shutil
import sys
import tempfile
from pathlib import Path

from tests.helpers.goldens import DBT_MANIFEST, REPO_ROOT, GoldenFileStore, UpdateGoldens, compile_twice
from tests.helpers.reference_project import project_copy, with_profile_eval

_NOT_CI = frozenset({"", "0", "false", "no"})
SCOPED_VIEWS = REPO_ROOT / "tests" / "fixtures" / "scoped_views" / "semantic_views.yml"


def main() -> int:
    """Rewrite each golden that differs; print what was written. Exit 1 in CI or when a project fails to compile."""
    if os.environ.get("CI", "").strip().casefold() not in _NOT_CI:
        print("refusing to rewrite goldens in CI; run this locally and commit the result", file=sys.stderr)
        return 1
    with tempfile.TemporaryDirectory() as root:
        compiled = compile_twice(project_copy(Path(root) / "plain", offline=False), DBT_MANIFEST)
        scoped_project = project_copy(Path(root) / "scoped", offline=False)
        folder = scoped_project / "semantic_models" / "semantic_views" / "scoped"
        folder.mkdir(parents=True)
        shutil.copy(SCOPED_VIEWS, folder / "semantic_views.yml")
        scoped = compile_twice(scoped_project, DBT_MANIFEST)
        profile_eval = compile_twice(
            with_profile_eval(project_copy(Path(root) / "profile_eval", offline=False)), DBT_MANIFEST
        )
        for result in (compiled.result, scoped.result, profile_eval.result):
            if not result.success:
                for item in result.diagnostics:
                    print(f"{item.code}: {item.message}", file=sys.stderr)
                return 1
        plain = {(item.artifact_type, item.name) for item in compiled.result.compiled}
        added = tuple(item for item in scoped.result.compiled if (item.artifact_type, item.name) not in plain)
        evaluated = tuple(item for item in profile_eval.result.compiled if (item.artifact_type, item.name) not in plain)
        store = GoldenFileStore()
        written = UpdateGoldens(store, compiled.git_sha).run(compiled.result)
        written += UpdateGoldens(store, scoped.git_sha).run(dataclasses.replace(scoped.result, compiled=added))
        written += UpdateGoldens(store, profile_eval.git_sha).run(
            dataclasses.replace(profile_eval.result, compiled=evaluated)
        )
    for name in written:
        print(f"wrote {name}")
    count = len(compiled.result.compiled) + len(added) + len(evaluated)
    print(f"{len(written)} golden file(s) written for {count} artifact(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
