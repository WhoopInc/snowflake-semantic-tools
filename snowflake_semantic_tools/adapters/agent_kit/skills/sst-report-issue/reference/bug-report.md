# Issue templates

These follow the repository's issue forms, so a draft pastes into them section by section.

## Bug report

Title: `[Bug]: <command> <what goes wrong, in one line>`

```markdown
### Problem Description
<One or two sentences: which command, what it did, why that is wrong.>

### Expected Behavior
<What should happen, citing the docs page or `sst explain` text that says so.>

### Actual Behavior
<The diagnostics verbatim, the exit code, and any traceback.>

### Steps to Reproduce
1. <Minimal files, inline or as a short list of YAML snippets with neutral names.>
2. <The exact command.>
3. <What it prints, and its exit code.>

### Environment
- SST version (`sst --version`):
- Python version:
- dbt version (`dbt --version`), if the project uses dbt:
- Authentication method (the `authentication` line of `sst debug`):
- Operating System:

### Relevant Logs
<The redacted `-o json` output, and for an SST-SNO code the matching `sst_target/run_log.jsonl` lines.>

### Additional Context
<Where in SST it happens (module and function), workarounds tried, related issues.>
```

## Feature request

Title: `[Feature]: <what you want, in one line>`

```markdown
### Problem Statement
<What the user is trying to do, and what stops them today.>

### Proposed Solution
<The command, flag, or YAML key that would solve it.>

### Alternatives Considered
<Workarounds tried, and why they fall short.>

### Example Usage
<A short example of the proposed command or YAML.>
```
