---
name: sst-create-issue
description: "Create a GitHub issue on the SST repo. Use when: filing bugs, requesting features, reporting doc issues. Triggers: create issue, file bug, feature request, report issue, open issue, new issue."
---

# Creating a GitHub Issue on SST

File a properly formatted GitHub issue on WhoopInc/snowflake-semantic-tools.

## Issue Templates

This repo has three issue templates in `.github/ISSUE_TEMPLATE/`. Always use one of them.

| Template | File | Title Prefix | Label |
|----------|------|-------------|-------|
| Bug Report | `bug_report.yml` | `[Bug]: ` | `bug` |
| Feature Request | `feature_request.yml` | `[Feature]: ` | `enhancement` |
| Documentation Issue | `documentation.yml` | `[Docs]: ` | `documentation` |

## Workflow

### Step 1: Search for duplicates

```bash
gh issue list --repo WhoopInc/snowflake-semantic-tools --search "<keywords>"
```

### Step 2: Determine issue type

Ask the user which type of issue they want to create (bug, feature, or docs).

### Step 3: Read the template

Read the corresponding template file from `.github/ISSUE_TEMPLATE/` to get the required fields and structure. Use the fields defined in the YAML template to build the issue body.

For bug reports, also gather environment info:
```bash
sst --version
python --version
```

### Step 4: Draft the issue

Populate the template fields based on the user's input and context. Use the correct title prefix and label from the table above.

**⚠️ MANDATORY STOPPING POINT**: Present the drafted issue title and body to the user for review. Do NOT create the issue until user approves the content.

### Step 5: Create the issue

After user approval:
```bash
gh issue create --repo WhoopInc/snowflake-semantic-tools \
  --title "<prefix> <concise description>" \
  --label "<label>" \
  --body "<filled-in template body>"
```

## Quality Checklist

A good issue has:
- A **specific title** (not "SST doesn't work")
- **Reproduction steps** that someone else can follow
- **Error messages** copied verbatim (not paraphrased)
- **Environment details** so the maintainer can replicate the setup
- **One issue per ticket** — don't combine multiple bugs or requests

## Stopping Points

- ✋ Step 4: Before creating the issue (present draft for user approval)

## Output

A GitHub issue URL with properly formatted content matching the repo's issue templates.
