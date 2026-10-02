# Security Policy

## Reporting Security Vulnerabilities

The security of Snowflake Semantic Tools is a top priority. If you discover a security vulnerability, please report it responsibly.

### Bug Bounty Program

WHOOP operates a bug bounty program to encourage responsible disclosure of security issues. Please report any security vulnerabilities through our HackerOne program:

**[WHOOP Bug Bounty Program on HackerOne](https://hackerone.com/whoop_bug_bounty)**

### What to Report

Please report any security concerns, including but not limited to:

- Authentication or authorization issues
- Code injection vulnerabilities
- Data exposure or privacy issues
- Dependency vulnerabilities
- Any other security-related bugs

### What to Include

When reporting a vulnerability, please include:

- A clear description of the vulnerability
- Steps to reproduce the issue
- Potential impact of the vulnerability
- Any suggested fixes (if available)

### Response Timeline

We take all security reports seriously and will:

- Acknowledge receipt of your report within 48 hours
- Provide an estimated timeline for a fix
- Keep you informed of our progress
- Credit you for the discovery (unless you prefer to remain anonymous)

## Trust model

SST publishes what a project declares; it does not sandbox it. The files in a project are
**trusted code**, run with the role of the `profiles.yml` target SST connects as:

- **Semantic YAML** and the **expressions** of metrics, dimensions, facts, and filters are
  rendered into DDL that runs with that role.
- **Verified queries** are SQL: connected validation runs `EXPLAIN` on each one, and on each
  expression, with that role, and publishing writes them into the semantic view.
- **Agent instructions**, tool definitions, and skills are published as written and decide what
  an agent does for whoever calls it; `sst test --suite evals` runs the agents.

Consequences:

- **Never run a credentialed `sst validate`, `sst plan`, or `sst apply` on an untrusted pull
  request**, such as one from a fork, and never give such a job Snowflake credentials. Every
  command that connects -- `plan`, `apply`, `enrich`, `test --suite smoke|evals`,
  `debug --test-connection`, and `validate` with Snowflake syntax checks -- runs the project's
  SQL. Check untrusted changes offline: `sst validate --no-snowflake-syntax-check`,
  `sst compile`, and `sst test --suite golden`. In GitHub Actions use the `pull_request`
  trigger, which withholds secrets from forks, never `pull_request_target`.
- **`sst enrich` sends sample values to Cortex.** Synonym generation calls `AI_COMPLETE` in the
  account with column names, types, descriptions, and up to five example values per column.
  Columns with `pii_tags` are never sampled and contribute no examples;
  `enrichment.allow_sample_value_collection: false` stops enrich reading row data.
- **Generated synonyms require human review.** SST refuses a synonym with quotes, control
  characters, template syntax, or more than 100 characters, and reports each refusal
  (`SST-PRS030`), but it cannot judge meaning: review the rest in the diff before merging.

[docs/guides/security.md](docs/guides/security.md) covers what SST checks and how to run it
with least privilege.

## Security Best Practices

When using Snowflake Semantic Tools:

- Keep your dependencies up to date
- Never commit credentials or sensitive data to your repository
- Use environment variables for authentication (see [Authentication](docs/guides/configuration.md#authentication))
- Connect with a dedicated, least-privileged role, and develop against a scratch schema
- Enable branch protection and require code reviews for production deployments

## Supported Versions

We provide security updates for the latest major version. Please ensure you're running the most recent version to receive security patches.

---

Thank you for helping keep Snowflake Semantic Tools and the WHOOP community secure!

