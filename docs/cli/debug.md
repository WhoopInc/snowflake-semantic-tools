# sst debug

Show configuration and optionally test Snowflake connection.

---

## Overview

The `debug` command displays your current SST and dbt configuration, helping you verify that profiles are correctly set up before running other commands. Optionally test your Snowflake connection to ensure credentials are working.

**Snowflake Connection:** Optional (required for `--test-connection`)

---

## Quick Start

```bash
# Show current configuration
sst debug

# Test Snowflake connection
sst debug --test-connection

# Use specific target
sst debug --target prod --test-connection
```

---

## Syntax

```bash
sst debug [OPTIONS]
```

---

## Options

| Option | Short | Type | Description |
|--------|-------|------|-------------|
| `--target` | `-t` | TEXT | dbt target from profiles.yml (default: profile's default) |
| `--test-connection` | | FLAG | Test Snowflake connection |
| `--verbose` | `-v` | FLAG | Show additional details |

---

## What It Does

1. **Reads `dbt_project.yml`** - Finds the profile name
2. **Locates `profiles.yml`** - In project directory or `~/.dbt/`
3. **Parses the profile** - For the specified target
4. **Displays connection parameters** - Account, user, role, etc.
5. **Optionally tests connection** - Executes a simple query

---

## Examples

```bash
# Show configuration for default target
sst debug

# Show configuration for production target
sst debug --target prod

# Test connection to default target
sst debug --test-connection

# Test production connection
sst debug --target prod --test-connection

# Verbose output with additional details
sst debug --verbose
```

---

## Output

### Configuration Display

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

### With Connection Test

```
  Testing Snowflake connection...

  ✓ Connection successful!
    Connected as: YOUR_USER
    Current role: DATA_ENGINEER
    Warehouse: MY_WAREHOUSE
```

---

## Use Cases

### Verify Setup After Installation

```bash
sst debug
# Check all configuration values are correct
```

### Before Running Commands

```bash
# Verify target before deployment
sst debug --target prod

# Then deploy
sst deploy --target prod
```

### Troubleshoot Connection Issues

```bash
# Test if credentials work
sst debug --test-connection

# Test specific target
sst debug --target prod --test-connection
```

---

## Troubleshooting

### "No dbt_project.yml found"

Run from your dbt project root directory.

### "Profile not found"

```bash
# Check what profile SST is looking for
sst debug

# Verify it exists in profiles.yml
cat ~/.dbt/profiles.yml
```

The profile name in `profiles.yml` must match the `profile:` field in `dbt_project.yml`.

### "Connection failed"

1. Verify account identifier format: `account_identifier.region`
2. Check authentication method is correct
3. Verify network access (VPN, firewall)
4. Check Snowflake account status

### SSO Browser Doesn't Open

1. Verify `authenticator: externalbrowser` in profile
2. Check browser is accessible
3. Try password authentication as fallback

---

## Related

- [sst init](init.md) - Set up SST configuration
- [Authentication Guide](../guides/authentication.md) - Snowflake connection setup
- [Configuration Reference](../reference/config.md) - sst_config.yaml options
