# Vendored semgrep rules

The `semgrep` job in `.github/workflows/test-and-lint.yml` scans the package with the rules in this
directory and nothing else (`semgrep scan --config .semgrep/ --metrics off`), so a scan's result
depends only on the commit and the pinned semgrep in `.github/requirements/semgrep.txt`, never on
what the Semgrep Registry serves that day.

| File | Registry ruleset | Rules |
|------|------------------|-------|
| `python.yaml` | `p/python` | 151 |
| `sql-injection.yaml` | `p/sql-injection` | 47 |

The two rulesets share 13 rules, so semgrep loads 185 distinct rules and runs the 151 that target
Python, the same counts a `--config p/python --config p/sql-injection` run reports. Both files were
fetched on 2026-10-04 for semgrep 1.179.0 and are committed unmodified.

## Refreshing

Fetch each ruleset over the registry's config endpoint, overwrite the file, and compare a run with
the vendored rules against a run straight from the registry; the rule counts and the findings must
match:

```bash
curl -sSfL -o .semgrep/python.yaml https://semgrep.dev/c/p/python
curl -sSfL -o .semgrep/sql-injection.yaml https://semgrep.dev/c/p/sql-injection
semgrep scan --config .semgrep/ --error --metrics off snowflake_semantic_tools
semgrep scan --config p/python --config p/sql-injection --error --metrics off snowflake_semantic_tools
```

Update the table and the date above, and review the diff of the rule files in the pull request
like any other change to a gate.

## Suppressing a finding

A rule loaded from this directory reports as `semgrep.<registry id>`. A `# nosemgrep: <registry
id>` comment, with a reason beside it, still silences it: semgrep matches the id on its suffix.
