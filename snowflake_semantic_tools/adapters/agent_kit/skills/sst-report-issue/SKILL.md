---
name: sst-report-issue
description: |-
  Use when SST itself looks wrong and the user wants to report it: an internal error
  (SST-INT001), a Snowflake refusal SST does not recognize (SST-SNO001), a crash, output that
  contradicts SST's docs, or a missing feature. Confirms it is not a project problem, builds
  a minimal repro, removes anything private, drafts the issue, and files it only with the
  user's approval. Do NOT use for fixing the project's own YAML (use sst-fix-diagnostics),
  and never for a security vulnerability, which is reported privately.
---
# Reporting an SST bug or feature request

SST's issues are public at @SST_REPOSITORY@/issues. Treat everything that goes into an issue
as published: nothing private leaves the machine, and nothing is filed without the user
approving the exact text.

## 0. A security problem is never an issue

A way to read or write data SST should not, a leaked credential, or an injection is reported
privately, as @SST_REPOSITORY@/security/policy describes. Stop here, tell the user, and
draft nothing public.

## 1. Rule out the project

Most surprises are configuration. Before calling it a bug:

- `sst explain <CODE>` for every code involved, and follow its suggestion.
- `sst debug --no-connect` to confirm the configuration, profile, and target are the ones
  intended.
- Read the guide for the area at @SST_DOCS@ and the `sst-troubleshoot` skill.
- `sst --version`: the installed SST is @SST_VERSION@ unless it was upgraded since this kit
  was installed. Note whether a newer release exists.

It is likely SST's fault when there is a traceback or `SST-INT001`, an `SST-INT` code of
any kind, `SST-SNO001`, an exit code the CLI reference does not document for the command,
or behavior the docs contradict.

## 2. Look for an existing issue

```bash
gh issue list --repo <owner>/<name> --state all --search "SST-INT001 compile"
```

with `<owner>/<name>` taken from @SST_REPOSITORY@, and search for the code, the command, and
the key words of the message. If one matches, offer to add the new evidence there instead.

## 3. Reproduce it small

Build the smallest project that shows the problem, in a temporary folder outside the user's
repository: one or two models, one view, the one member involved, with neutral names such as
`orders` and `customers`. Prefer an offline reproduction (`--no-snowflake-syntax-check`,
`sst compile`); if it only happens connected, say so and keep the
connected command the user ran. Confirm it fails the same way before writing anything up.

## 4. Gather the evidence

- `sst --version`, `python --version`, `dbt --version`, the operating system, and the
  `authentication` line of `sst debug --no-connect`.
- The failing command, its exit code, and its `-o json` output. For an `SST-INT` or
  `SST-SNO` code, keep each diagnostic's `internal_detail`: it is what the maintainers need.
- For an `SST-SNO` code, the matching `sst_target/run_log.jsonl` lines (one per Snowflake
  refusal: the command and the code only).
- For a Snowflake refusal, `sst debug --snowflake-signatures`.
- Where in SST it happens: `sst explain <CODE>` lists, under `raised from:`, the package
  modules that raise a code. The installed source can be read, never edited, to cite the
  function involved:

  ```bash
  python -c "import snowflake_semantic_tools, pathlib; print(pathlib.Path(snowflake_semantic_tools.__file__).parent)"
  ```

  Use the Python that runs `sst`: the first line of the file `command -v sst` prints names it.

## 5. Remove everything private

Go through [the redaction checklist](reference/redaction.md) on every line of the draft,
the repro, and the logs. When in doubt, take it out.

## 6. Draft, show, and only then file

Write the draft from [the bug report template](reference/bug-report.md) to a file the
project does not commit, such as under `target/`, and show it to the user in full. Then,
only when the user says to file it:

```bash
gh issue create --repo <owner>/<name> --title "[Bug]: <one line>" --body-file <draft> --label bug
```

If `gh` is not signed in, or the user prefers to file it themselves, give them
@SST_REPOSITORY@/issues/new/choose and the draft to paste. A feature request follows the
same steps with the feature section of the template and the `enhancement` label.
