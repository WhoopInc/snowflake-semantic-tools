# Redaction checklist

Every item below is removed or replaced before a draft is shown for filing. Replace a name
with a neutral one that keeps the shape (`ANALYTICS_DB` becomes `MY_DB`), so the repro still
reads.

## Always remove

- Credentials of any kind: passwords, tokens, private keys and their passphrases, and the
  values of environment variables that hold them.
- The Snowflake account: its identifier or locator, organization name, region, and any
  `*.snowflakecomputing.com` host.
- People: user names, email addresses, and names in comments, commit messages, or
  `verified_by` fields.
- Snowflake object names: roles, warehouses, databases, schemas, stages, tables, and views,
  unless they are already the neutral names of the repro.
- Data: sample values, query results, numbers from the warehouse, and the text of any
  question an agent was asked.
- Machine details: paths into a home folder (anonymise them as `/Users/.../` or
  `/home/.../`), host names, and IP addresses.
- Snowflake query IDs and session IDs.

## Check in the JSON output

`-o json` carries `invocation.argv`, `invocation.project_dir`, `invocation.config_file`, and
`invocation.target`, and each diagnostic's `message` and `location` can name objects and
paths. Redact those fields rather than dropping the whole envelope: the codes, severities,
`exit_code`, and `summary` are what a maintainer needs.

## Keep

- SST, Python, and dbt versions, the operating system, and the authentication method's name
  (such as `externalbrowser` or key pair), never its values.
- Diagnostic codes, exit codes, and tracebacks, with paths shortened to the package-relative
  part (`snowflake_semantic_tools/...`).
- The repro's YAML, with neutral names.
