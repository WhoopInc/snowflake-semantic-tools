# CI/CD Guide

Integrating SST into CircleCI pipelines with dbt.

---

## Overview

SST integrates with dbt by reading your `profiles.yml` and using dbt targets for environment management.

**Recommended workflow:**

1. **PRs**: Run `sst validate` to catch errors early
2. **Main branch**: Run `sst deploy --target prod` to deploy semantic views to production

---

## CircleCI Configuration

### Complete Example

```yaml
version: 2.1

commands:
  setup-environment:
    steps:
      - checkout
      - run:
          name: Install dependencies
          command: |
            pip install --upgrade pip
            pip install dbt-snowflake snowflake-semantic-tools
            dbt deps

jobs:
  validate-semantic-models:
    docker:
      - image: cimg/python:3.11
    steps:
      - setup-environment
      - run:
          name: Setup Snowflake private key
          command: |
            echo "$SNOWFLAKE_PRIVATE_KEY" | sed 's/\\n/\n/g' > /tmp/snowflake_key.pem
            chmod 600 /tmp/snowflake_key.pem
      - run:
          name: Compile dbt manifest
          command: dbt compile --target prod
      - run:
          name: Validate semantic models
          command: sst validate --target prod

  deploy-semantic-models:
    docker:
      - image: cimg/python:3.11
    steps:
      - setup-environment
      - run:
          name: Setup Snowflake private key
          command: |
            echo "$SNOWFLAKE_PRIVATE_KEY" | sed 's/\\n/\n/g' > /tmp/snowflake_key.pem
            chmod 600 /tmp/snowflake_key.pem
      - run:
          name: Deploy semantic models
          command: sst deploy --target prod

workflows:
  version: 2
  semantic-models:
    jobs:
      # Validate on all branches
      - validate-semantic-models:
          context: snowflake-credentials
      
      # Deploy only on main
      - deploy-semantic-models:
          context: snowflake-credentials
          requires:
            - validate-semantic-models
          filters:
            branches:
              only: main
```

---

## Key Concepts

### 1. Use dbt Targets

Configure different environments in `~/.dbt/profiles.yml`:

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: abc12345
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      private_key_path: "{{ env_var('SNOWFLAKE_PRIVATE_KEY_PATH') }}"
      role: DEV_ROLE
      warehouse: DEV_WH
      database: ANALYTICS_DEV
      schema: SEMANTIC_VIEWS
    
    prod:
      type: snowflake
      account: abc12345
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      private_key_path: "{{ env_var('SNOWFLAKE_PRIVATE_KEY_PATH') }}"
      role: PROD_ROLE
      warehouse: PROD_WH
      database: ANALYTICS
      schema: SEMANTIC_VIEWS
```

Then in CI, use `--target prod` to deploy to production:

```bash
sst deploy --target prod
```

### 2. RSA Key Authentication

Store your private key as a CircleCI environment variable and write it to a file:

```yaml
- run:
    name: Setup Snowflake private key
    command: |
      echo "$SNOWFLAKE_PRIVATE_KEY" | sed 's/\\n/\n/g' > /tmp/snowflake_key.pem
      chmod 600 /tmp/snowflake_key.pem
```

Set the environment variable that your dbt profile references:

```yaml
- run:
    name: Deploy
    environment:
      SNOWFLAKE_PRIVATE_KEY_PATH: /tmp/snowflake_key.pem
      SNOWFLAKE_USER: CIRCLECI_SVC_USER
    command: sst deploy --target prod
```

### 3. Validate on PRs, Deploy on Main

Use CircleCI filters to control when jobs run:

```yaml
workflows:
  semantic-models:
    jobs:
      - validate:  # Runs on all branches
          context: snowflake-credentials
      
      - deploy:    # Only runs on main
          requires: [validate]
          filters:
            branches:
              only: main
```

---

## Environment Variables

Set these in CircleCI (Project Settings → Environment Variables):

| Variable | Description | Example |
|----------|-------------|---------|
| `SNOWFLAKE_USER` | Service account username | `CIRCLECI_SVC_USER` |
| `SNOWFLAKE_PRIVATE_KEY` | RSA private key | `-----BEGIN PRIVATE KEY-----\n...` |

**Optional:**
- `SNOWFLAKE_ACCOUNT` - If not in profiles.yml
- `SNOWFLAKE_ROLE` - If not in profiles.yml
- `SNOWFLAKE_WAREHOUSE` - If not in profiles.yml

---

## Integrating with dbt

If SST and dbt run in the same pipeline:

```yaml
workflows:
  build-and-deploy:
    jobs:
      # 1. Validate semantic models first (fast)
      - validate-semantic-models
      
      # 2. Build dbt models (creates tables)
      - dbt-build:
          requires: [validate-semantic-models]
      
      # 3. Deploy semantic views (after tables exist)
      - deploy-semantic-models:
          requires: [dbt-build]
          filters:
            branches:
              only: main
```

**Why this order:**
- Catch semantic model errors early (fast feedback)
- dbt creates/updates the actual tables
- SST creates semantic views referencing those tables

---

## Common Patterns

### Multiple Environments

Use different targets for different branches:

```yaml
- run:
    name: Deploy to appropriate environment
    command: |
      if [ "$CIRCLE_BRANCH" == "main" ]; then
        sst deploy --target prod
      elif [ "$CIRCLE_BRANCH" == "qa" ]; then
        sst deploy --target qa
      fi
```

### Defer to Production

For PR builds that reference production tables without deploying there:

```yaml
- run:
    name: Validate against prod tables
    command: sst validate --target prod --verify-schema
```

### Speed Up Deployment

Skip validation in deploy since it already ran:

```yaml
- run:
    name: Deploy (skip validation)
    command: sst deploy --target prod --skip-validation
```

---

## Troubleshooting

### "manifest.json not found"

**Fix:** Run `dbt compile` before SST commands:

```bash
dbt compile --target prod
sst validate --target prod
```

### "Table does not exist"

**Cause:** SST ran before dbt created the table.

**Fix:** Make sure dbt runs before SST:

```yaml
- deploy-semantic-models:
    requires:
      - dbt-build  # Wait for dbt
```

### "Permission denied" on private key

**Fix:** Set correct file permissions:

```bash
chmod 600 /tmp/snowflake_key.pem
```

### CircleCI IP ranges

If Snowflake requires network policies:

```yaml
jobs:
  validate:
    circleci_ip_ranges: true  # Use static IPs
```

Then add CircleCI's IP ranges to your Snowflake network policy.

---

## Best Practices

1. **Always validate before deploying**
   ```yaml
   - deploy:
       requires: [validate]
   ```

2. **Use service accounts with RSA keys**
   - Never use personal credentials
   - Use dedicated service account (e.g., `CIRCLECI_SVC_USER`)
   - Store private key securely in CircleCI secrets

3. **Run validation on every PR**
   - Catches errors before merge
   - No Snowflake writes needed for validation

4. **Deploy only from main branch**
   ```yaml
   filters:
     branches:
       only: main
   ```

5. **Use dbt targets for environment management**
   - Don't hardcode database/schema in CI config
   - Define in profiles.yml, reference with `--target`

---

## Next Steps

- [Authentication Guide](authentication.md) - RSA key pair setup
- [CLI Reference](../cli/index.md) - All SST commands
- [Validation Rules](../concepts/validation-rules.md) - What SST validates
