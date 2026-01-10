# Authentication Guide

Configure Snowflake authentication for Snowflake Semantic Tools (SST).

## Overview

**SST uses dbt's `~/.dbt/profiles.yml` for all Snowflake authentication.** This provides:

- **Single source of truth** - Same credentials used by dbt and SST
- **Multiple auth methods** - SSO, Personal Access Tokens (PAT), RSA keys, OAuth, Password (deprecated)
- **Environment variable support** - Secure credential management via `{{ env_var() }}`
- **Target management** - Easy switching between dev/prod environments

## Authentication Methods

| Method | Use Case | Security | Setup |
|--------|----------|----------|-------|
| **SSO/Browser** | Interactive development | High | Easy |
| **Personal Access Token (PAT)** | Personal automation/scripts | High | Easy |
| **RSA Key Pair** | CI/CD and production automation | Very High | Medium |
| **Password** | Legacy automation (deprecated) | Medium | Easy |
| **OAuth** | Enterprise integration | Very High | Complex |

## Quick Setup

### Step 1: Create profiles.yml

Create `~/.dbt/profiles.yml` if it doesn't exist:

```bash
mkdir -p ~/.dbt
touch ~/.dbt/profiles.yml
```

### Step 2: Configure your profile

Add your Snowflake connection details. The profile name must match the `profile:` field in your `dbt_project.yml`.

#### SSO/Browser Authentication (Recommended for Development)

```yaml
# ~/.dbt/profiles.yml
my_project:  # Must match 'profile:' in dbt_project.yml
  target: dev
  outputs:
    dev:
      type: snowflake
      account: abc12345.us-east-1  # Your Snowflake account
      user: your.email@company.com
      authenticator: externalbrowser
      role: DATA_ENGINEER
      warehouse: MY_WAREHOUSE
      database: ANALYTICS
      schema: DEV
```

When you run any SST command, your browser will open for authentication.

#### Password Authentication

> **Note on Password Authentication:** Username/password authentication is being phased out by Snowflake in favor of more secure methods like Personal Access Tokens (PATs), SSO, and RSA key pairs. Consider migrating to one of these alternatives.

```yaml
# ~/.dbt/profiles.yml  
my_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: abc12345.us-east-1
      user: service_account
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"  # Use env var for security
      role: SERVICE_ROLE
      warehouse: MY_WAREHOUSE
      database: ANALYTICS
      schema: DEV
```

#### Personal Access Token (PAT) Authentication

Snowflake Personal Access Tokens provide a more secure alternative to username/password authentication for **personal automation and scripts**. 

> **Not for CI/CD:** PATs are tied to individual user accounts and should not be used for shared CI/CD pipelines. Use RSA key pairs with service accounts for production automation instead.

**Step 1: Generate a PAT in Snowflake**

**Option A: Using Snowsight UI (Recommended)**

1. Log in to Snowsight
2. Click on your profile (bottom left corner)
3. Select **Settings**
4. Navigate to **Authentication**
5. Click **Generate new token**
6. Set a name and expiration date (default: 90 days)
7. Copy the token secret - **you can only view it once!**

**Option B: Using SQL**

```sql
ALTER USER my_username ADD PROGRAMMATIC ACCESS TOKEN my_token VALIDITY_DAYS 90;
```

This will return a secret token - save it securely as you can only view it once.

**Step 2: Configure dbt profile**

Use the PAT as a direct password replacement:

```yaml
# ~/.dbt/profiles.yml
my_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: abc12345.us-east-1
      user: my_username
      password: "{{ env_var('SNOWFLAKE_PAT') }}"  # Your PAT secret goes here
      role: MY_ROLE
      warehouse: MY_WAREHOUSE
      database: ANALYTICS
      schema: DEV
```

**Step 3: Set environment variable**
```bash
export SNOWFLAKE_PAT='your_personal_access_token_secret_here'
```

**Benefits:**
- More secure than username/password for personal scripts
- No password rotation needed
- Can be scoped to specific roles/warehouses
- Easier to revoke than passwords
- Better audit trail in Snowflake

**Use Cases:**
- Personal automation scripts
- Local development tools
- One-off data processing jobs

**Not suitable for:**
- Shared CI/CD pipelines (use RSA keys instead)
- Team/service accounts (use RSA keys instead)
- Production deployments (use RSA keys instead)

#### RSA Key Pair (Recommended for Production)

```yaml
# ~/.dbt/profiles.yml
my_project:
  target: prod
  outputs:
    prod:
      type: snowflake
      account: abc12345.us-east-1
      user: service_account
      private_key_path: ~/.ssh/snowflake_key.p8
      private_key_passphrase: "{{ env_var('SNOWFLAKE_KEY_PASSPHRASE') }}"  # Optional
      role: PROD_ROLE
      warehouse: PROD_WH
      database: ANALYTICS
      schema: PROD
```

#### OAuth

```yaml
# ~/.dbt/profiles.yml
my_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: abc12345.us-east-1
      user: oauth_user
      authenticator: oauth
      token: "{{ env_var('SNOWFLAKE_OAUTH_TOKEN') }}"
      role: OAUTH_ROLE
      warehouse: MY_WAREHOUSE
      database: ANALYTICS
      schema: DEV
```

### Step 3: Match profile name

Ensure `dbt_project.yml` references your profile:

```yaml
# dbt_project.yml
name: 'my_project'
profile: 'my_project'  # Must match profile name in profiles.yml
```

### Step 4: Test your setup

```bash
# Show configuration (no Snowflake connection needed)
sst debug

# Test Snowflake connection
sst debug --test-connection

# Test with specific target
sst debug --target prod --test-connection
```

**Example output:**
```
SST Debug (v0.2.2)

  ──────────────────────────────────────────────────
  Profile Configuration
  ──────────────────────────────────────────────────
  Profile:        my_project
  Target:         dev
  ──────────────────────────────────────────────────
  Account:        abc12345.us-east-1
  User:           your.email@company.com
  Role:           DATA_ENGINEER
  Warehouse:      MY_WAREHOUSE
  Database:       ANALYTICS
  Schema:         DEV
  Auth Method:    sso_browser
  ──────────────────────────────────────────────────
  profiles.yml:   ~/.dbt/profiles.yml
  dbt_project:    ./dbt_project.yml
  ──────────────────────────────────────────────────

  ✓ Configuration valid
```

## Using Targets

SST supports dbt's target system for managing multiple environments:

```yaml
# ~/.dbt/profiles.yml
my_project:
  target: dev  # Default target
  outputs:
    dev:
      type: snowflake
      account: abc12345.us-east-1
      user: dev_user
      authenticator: externalbrowser
      role: DEV_ROLE
      warehouse: DEV_WH
      database: DEV_DB
      schema: ANALYTICS
      
    prod:
      type: snowflake
      account: abc12345.us-east-1
      user: prod_user
      private_key_path: ~/.ssh/prod_key.p8
      role: PROD_ROLE
      warehouse: PROD_WH
      database: PROD_DB
      schema: ANALYTICS
```

Switch targets with the `--target` flag:

```bash
# Use default target (dev)
sst enrich --models customers,orders

# Use production target
sst enrich --models customers,orders --target prod

# Deploy to production
sst deploy --target prod
```

## Environment Variables with env_var()

Use dbt's `env_var()` function to keep sensitive values out of profiles.yml:

```yaml
# ~/.dbt/profiles.yml
my_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: "{{ env_var('SNOWFLAKE_ROLE', 'PUBLIC') }}"  # With default
      warehouse: MY_WAREHOUSE
      database: ANALYTICS
      schema: DEV
```

Set environment variables using one of these methods:

**Option 1: Shell profile (persists across sessions)**

Add exports to your shell configuration file:

```bash
# For macOS/Linux with zsh (default on modern macOS):
echo 'export SNOWFLAKE_PASSWORD="your_password"' >> ~/.zshrc
source ~/.zshrc

# For bash:
echo 'export SNOWFLAKE_PASSWORD="your_password"' >> ~/.bashrc
source ~/.bashrc
```

**Option 2: Export in current session (temporary)**

```bash
export SNOWFLAKE_ACCOUNT=abc12345.us-east-1
export SNOWFLAKE_USER=my_user
export SNOWFLAKE_PASSWORD=my_password
```

**Option 3: direnv with .envrc (recommended for projects)**

[direnv](https://direnv.net/) automatically loads environment variables when you enter a directory:

```bash
# Install direnv
brew install direnv  # macOS
# Add to ~/.zshrc: eval "$(direnv hook zsh)"

# Create .envrc in your project
echo 'export SNOWFLAKE_PASSWORD="your_password"' >> .envrc
direnv allow
```

**Security note:** Never commit files containing passwords to Git. Add `.envrc` to `.gitignore`.

## For dbt Cloud Users

If you use dbt Cloud exclusively, you'll need to create a local `profiles.yml` for SST. This is because:

- **SST connects directly to Snowflake** for operations like `enrich`, `extract`, and `generate`
- **dbt Cloud doesn't expose credentials locally** - connections are managed in the cloud
- **Many dbt Cloud users already have local profiles** for local development

### Setting Up profiles.yml for dbt Cloud

#### Step 1: Create the profiles directory

```bash
mkdir -p ~/.dbt
```

#### Step 2: Create profiles.yml

Create `~/.dbt/profiles.yml` with your Snowflake credentials:

```yaml
# ~/.dbt/profiles.yml
your_project_name:  # Must match 'profile:' in dbt_project.yml
  target: dev
  outputs:
    dev:
      type: snowflake
      account: your_account_identifier  # e.g., abc12345.us-east-1
      user: your_username
      
      # Choose ONE authentication method:
      
      # Option A: SSO/Browser (recommended for interactive use)
      authenticator: externalbrowser
      
      # Option B: Password
      # password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      
      # Option C: Key Pair (recommended for automation)
      # private_key_path: ~/.ssh/snowflake_key.p8
      
      role: YOUR_ROLE
      warehouse: YOUR_WAREHOUSE
      database: YOUR_DATABASE
      schema: YOUR_SCHEMA
```

#### Step 3: Find your profile name

Check your `dbt_project.yml` for the profile name:

```yaml
# dbt_project.yml
name: 'my_project'
profile: 'your_project_name'  # <-- Use this in profiles.yml
```

#### Step 4: Verify setup

```bash
# Show your configuration
sst debug

# Test the Snowflake connection
sst debug --test-connection
```

### Keeping credentials secure

- **Never commit profiles.yml to Git** - it's in dbt's default `.gitignore`
- **Use environment variables** for sensitive values:
  ```yaml
  password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
  ```
- **Consider key pair auth** for shared/CI environments

## RSA Key Pair Setup

For production deployments and CI/CD:

### Generate Keys

```bash
# Generate private key (unencrypted for automation)
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt

# Generate public key
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub

# For encrypted keys (more secure, requires passphrase)
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8
```

### Configure Snowflake

```sql
-- Copy content from rsa_key.pub (between BEGIN/END markers)
ALTER USER service_account SET RSA_PUBLIC_KEY='MIIBIjANBgkqh...';

-- Verify setup
DESC USER service_account;
```

### Configure profiles.yml

```yaml
my_project:
  target: prod
  outputs:
    prod:
      type: snowflake
      account: abc12345.us-east-1
      user: service_account
      private_key_path: /secure/path/rsa_key.p8
      private_key_passphrase: "{{ env_var('SNOWFLAKE_KEY_PASSPHRASE') }}"  # If encrypted
      role: PROD_ROLE
      warehouse: PROD_WH
      database: ANALYTICS
      schema: PROD
```

## Security Best Practices

### 1. Protect profiles.yml

```bash
# Set restrictive permissions
chmod 600 ~/.dbt/profiles.yml
```

### 2. Use .gitignore

Ensure sensitive files are never committed:

```gitignore
# dbt profile (contains credentials)
profiles.yml

# RSA keys
*.p8
*.key
*_rsa_key*
```

### 3. Rotate Credentials

- Rotate passwords quarterly
- Update RSA keys annually
- Audit service account usage

### 4. Principle of Least Privilege

Grant only necessary permissions:

```sql
-- Minimal permissions for SST
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE sst_role;
GRANT CREATE SCHEMA ON DATABASE metadata_db TO ROLE sst_role;
GRANT CREATE TABLE ON SCHEMA metadata_schema TO ROLE sst_role;
```

## Troubleshooting

### "No dbt profiles.yml found"

1. Run `sst debug` to see which paths are being searched
2. Create `~/.dbt/profiles.yml`
3. Verify file permissions: `chmod 600 ~/.dbt/profiles.yml`
4. Check profile name matches `dbt_project.yml`

### SSO Browser Doesn't Open

1. Check `authenticator: externalbrowser` in profile
2. Verify network allows browser redirects
3. Try password authentication as fallback

### "Profile not found"

```bash
# Use sst debug to see what SST is looking for
sst debug

# Or manually check your profile name
grep "profile:" dbt_project.yml

# Verify it exists in profiles.yml
cat ~/.dbt/profiles.yml
```

### Invalid RSA Key

1. Verify public key in Snowflake matches your key
2. Check private key file permissions: `chmod 600 rsa_key.p8`
3. Ensure key format is PKCS8

### Permission Denied

1. Verify role has necessary grants
2. Check warehouse is running
3. Confirm database/schema exist

## profiles.yml Reference

| Field | Required | Description |
|-------|----------|-------------|
| `type` | Yes | Must be `snowflake` |
| `account` | Yes | Snowflake account identifier |
| `user` | Yes | Snowflake username |
| `role` | No | Snowflake role |
| `warehouse` | No | Compute warehouse |
| `database` | No | Default database |
| `schema` | No | Default schema |
| `password` | Conditional | For password auth |
| `private_key_path` | Conditional | For key pair auth |
| `private_key_passphrase` | No | For encrypted keys |
| `authenticator` | Conditional | `externalbrowser` or `oauth` |
| `token` | Conditional | OAuth token |

## Next Steps

- [CLI Reference](../cli/index.md) - Command documentation
- [Getting Started](../getting-started.md) - Quick start guide
- [dbt Profile Setup](https://docs.getdbt.com/docs/core/connect-data-platform/snowflake-setup) - Official dbt docs
