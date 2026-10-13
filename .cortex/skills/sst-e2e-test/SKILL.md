---
name: sst-e2e-test
description: "End-to-end QA for SST on the in-repo reference project: the test suite, offline validate, compile, golden suite, and the migrate refs codemod, plus plan/apply against a scratch schema only when the user asks. Use when: testing SST changes end to end, QA before a release or PR, verifying the pipeline. Triggers: e2e test, end-to-end, qa sst, full test, test pipeline, golden suite, reference project, integration test."
---

# SST End-to-End Testing

## Purpose

This skill checks that the `sst` CLI works end to end after a change. The offline phases use `tests/fixtures/reference_project`, a dbt + SST project with 14 artifacts of all seven types, compiled from its vendored dbt manifest, so they need no Snowflake account. They also exercise `sst migrate refs` on the 0.3 dialect corpus in `tests/fixtures/v1_dialect`.

The connected phase publishes to a scratch schema, and it runs only when the user asks for it.

Use this skill when:
- You've changed SST and want to verify nothing broke
- You're preparing a release or PR and need QA

The phases run in order, and each has pass criteria. If a phase fails, report the failure and do NOT continue to phases that depend on it.

Read `references/expected-outputs.md` for the baselines and `references/setup-guide.md` for the environment before starting.

## Conventions

- Run every command from the SST repo root (`git rev-parse --show-toplevel`).
- Commands use Poetry's environment. In another environment the user chose (see the `sst-test` skill), drop the `poetry run` prefix.
- Set the fixture paths once:
  ```bash
  P=tests/fixtures/reference_project
  M=tests/fixtures/reference_project_manifest.json
  ```
- Never write inside the repo except gitignored build output: `sst compile` writes `$P/target/sst/`, which git ignores, and `sst clean --project-dir $P` removes it. Never run `sst migrate refs --write` on `tests/fixtures/`.
- Never hardcode user-specific paths.

---

## Phase 1: Test Suite

Load the `sst-test` skill and follow its workflow: the suite, the coverage floors, and the static gates.

### Pass Criteria
- Every gate passes

Do NOT continue if a gate fails; report it (the `sst-test` skill's On Failure step).

---

## Phase 2: Validate (Offline)

```bash
poetry run sst validate --project-dir $P --manifest $M --no-strict --no-snowflake-syntax-check
```

### Pass Criteria
- Exit code 0
- stdout: `validated 14 artifact(s): 0 errors, 6 warnings`
- stderr ends with `0 errors; 6 warnings; 51 info; 51 info not shown (--show-info)`
- The warnings and the info codes match `expected-outputs.md`: the five expected artifact warnings, plus `SST-CFG034`, which `--no-strict` itself raises because it contradicts the fixture's `validation.strict: true`

Without `--no-strict`, exit 1 is expected: the fixture's `validation.strict: true` promotes the five warnings to errors (`5 errors; 0 warnings; 51 info`).

---

## Phase 3: Compile

```bash
OUT="$(mktemp -d)"
poetry run sst compile --project-dir $P --manifest $M --emit-ddl "$OUT"
ls "$OUT"
```

### Pass Criteria
- Exit code 0, with `compiled 14 artifact(s); manifest tests/fixtures/reference_project/target/sst/manifest.json` and `wrote 14 file(s) to <dir>`
- The file names match `expected-outputs.md`
- The rendered files reflect the change under test (for example, read `$OUT/jaffle_sales.sql` after a semantic view change)

---

## Phase 4: Golden Suite

```bash
poetry run sst test --suite golden --project-dir $P --manifest $M --golden-dir "$PWD/tests/golden/expected/ddl"
```

`--golden-dir` names the DDL goldens and must be absolute: a relative path is resolved against `--project-dir`. The other goldens are read from its sibling directories (`agent/`, `tool/`, `eval/`, `skill/`, `plugin/`, `profile/`).

### Pass Criteria
- `golden suite passed for 14 artifact(s)`, exit 0

### On Failure
The output is a unified diff per golden, and the suite exits 1. A diff is a regression unless the change was meant to alter rendered output.

**⚠️ MANDATORY STOPPING POINT**: Show the user each diff and ask whether it is intended. Do NOT change a golden without explicit approval. When approved, rewrite them with the same command plus `--update-golden` (refused when `$CI` is set; it keeps a DDL golden's `--` provenance header), never by hand, and review the resulting diff (`tests/golden/README.md`).

---

## Phase 5: The 0.3 Codemod

```bash
poetry run sst migrate refs --project-dir tests/fixtures/v1_dialect
```

This is a dry run and writes nothing. For the round trip, work on a copy:

```bash
V1="$(mktemp -d)/v1_dialect"
cp -R tests/fixtures/v1_dialect "$V1"
poetry run sst migrate refs --project-dir "$V1" --write
diff -r "$V1/semantic_models" tests/fixtures/v1_dialect/expected/converted/semantic_models
poetry run sst migrate refs --project-dir "$V1"
```

### Pass Criteria
- The dry run exits 2 and reports four files as `would rewrite`
- `--write` on the copy exits 0, and `diff -r` prints nothing
- The re-run prints `no legacy references found` and exits 0

---

## Phase 6: Connected (Only When the User Asks)

**⚠️ MANDATORY STOPPING POINT**: Skip this phase unless the user asked for a live run. Confirm the project, the target, and the scratch schema first. Never use a production target. This phase uses Snowflake compute and creates objects.

The reference project cannot be published as it is: its tool `relations:` maps cover only `dev` and `prod`, which have no credentials, and under its `verify` target compile stops with `SST-REF018` (see `references/setup-guide.md`). Use a project the user names, with a `profiles.yml` target that points at a scratch schema. Set `PROJ` and `T` to them.

To QA the engine itself against a real account instead, the repository's own live tests run the whole lifecycle in schemas they create and drop. They are configured as `docs/guides/testing-live.md` describes and run with `poetry run pytest -m live tests/contract_live tests/integration tests/e2e`; ask before running them too.

```bash
poetry run sst debug --project-dir "$PROJ" --target "$T" --no-connect  # offline: database and schema must be the scratch schema
poetry run sst debug --project-dir "$PROJ" --target "$T"               # connects: the role and account SST signs in as
poetry run sst validate --project-dir "$PROJ" --target "$T"            # syntax checks connect unless the project turns them off
poetry run sst compile --project-dir "$PROJ" --target "$T"             # plan and apply need a manifest for this target
poetry run sst plan --project-dir "$PROJ" --target "$T"                # never writes; saves $PROJ/target/sst/plan.json
```

`plan` exits 2 when there are changes. Add `--sql-out "$(mktemp -d)"` to see the statements.

**⚠️ MANDATORY STOPPING POINT**: Show the user the plan. Apply only after explicit approval.

```bash
poetry run sst apply --project-dir "$PROJ" --target "$T" --plan "$PROJ/target/sst/plan.json" --yes
poetry run sst test --suite smoke --project-dir "$PROJ" --target "$T"
poetry run sst plan --project-dir "$PROJ" --target "$T"                     # exit 0: nothing left to change
```

`--yes` skips the interactive confirmation, which the user already gave above. Without `--plan`, `apply` plans afresh; with it, it applies exactly the saved plan, which must still match what the project compiles and what the target holds.

### Pass Criteria
- `apply` exits 0
- The smoke suite passes
- The second `plan` exits 0

No SST command tears down a deployment. `sst drop <DB>.<SCHEMA>.<NAME> --type agent|semantic_view --target "$T" --yes` is break-glass: it drops one object SST owns and forgets it in state. `apply` also creates a state table (`SST_STATE` by default, from `state.+table`) and a run-lock table beside it (`SST_STATE_LOCK`). Leave cleanup to the user; never drop anything without explicit approval.

---

## Phase 7: Summary Report

Compile results from all phases into a summary:

```
Phase               | Status | Details
----------------------------------------------
1. Test suite       | ...    | all gates / failing gate
2. Validate         | ...    | 14 artifacts, 0 errors, 6 warnings (5 + SST-CFG034)
3. Compile          | ...    | 14 payload files
4. Golden suite     | ...    | 14 artifacts match
5. migrate refs     | ...    | 4 files pending, round trip matches
6. Connected        | ...    | SKIPPED unless requested
----------------------------------------------
Overall: PASS / FAIL
```

If any phase failed:
- List the specific failures
- Suggest next steps (which code to investigate, which tests to write)

If all phases passed:
- Confirm that SST works end to end
- Note any unexpected diagnostics or counts for follow-up

## Success Criteria

- All phases that ran report PASS
- No unexpected diagnostics on the reference project
- Goldens unchanged, or changed only with the user's approval
- No unhandled exceptions or tracebacks in any SST command

## Output

A phase-by-phase summary report (Phase 7) with PASS/FAIL/SKIPPED status for each phase, overall result, and actionable next steps if any phase failed.

## Stopping Points

- ✋ Phase 1: Before running tests (the `sst-test` skill asks which environment to use)
- ✋ Phase 4: Before accepting any golden change
- ✋ Phase 6: Before any connected command, and again before `apply`

## Troubleshooting

**`SST-MAN001`, `SST-MAN006`, or `compiled SST manifest is stale; run sst compile before ...`**
- `plan`, `apply`, and the smoke and eval suites need a current `sst compile` with the same `--target` first: `SST-MAN001` means there is no compiled manifest, `SST-MAN006` that it was compiled for another target, and the stale message that the project changed since.

**`missing golden .../tests/fixtures/reference_project/tests/golden/...`**
- `--golden-dir` was relative. Pass `"$PWD/tests/golden/expected/ddl"`.

**`validate` exits 1 with `error[SST-VAL528] (promoted from warning)`**
- `--no-strict` is missing; the fixture is strict by design.

**`error[SST-CFG013]: env_var('SST_TEST_SNOWFLAKE_ACCOUNT') is unset and has no default`**
- `--target verify` was given: that target reads `SST_TEST_SNOWFLAKE_*`. The offline phases use `dev`, the default.

**`error[SST-APL011]`: the run lock is held by a live run**
- Another `apply`, `drop`, or eval run holds the target's lock; wait for it. `--break-stale-lock` takes over only a lock whose run has expired (`SST-APL010`), never a live one.

**`SST-DBT026`, `SST-DBT027`, or `SST-DBT028` (SST could not run `dbt parse`)**
- Pass `--manifest`, or run `dbt deps`, put dbt on PATH, and keep `profiles.yml` where SST finds it (see `references/setup-guide.md`).

**`sst --version` shows the wrong version**
- The environment is not running this checkout. Run `poetry install` from the repo root, or reinstall the checkout in editable mode in the chosen environment.
