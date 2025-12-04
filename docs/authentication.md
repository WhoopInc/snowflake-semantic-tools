# Authentication Guide

Configure secure authentication for Snowflake Semantic Tools.

## Authentication Methods

The tools support multiple authentication methods, automatically selected based on your configuration:

| Method | Use Case | Security | Setup Complexity |
|--------|----------|----------|------------------|
| **SSO/Browser** | Interactive development | High | Easy |
| **Password** | Simple automation | Medium | Easy |
| **RSA Key Pair** | Production | Very High | Medium |
| **OAuth** | Enterprise integration | Very High | Complex |

## Quick Setup

### Method 1: SSO/Browser Authentication (Recommended for Development)

Perfect for local development with corporate SSO:

```env
# .env file - No password needed!
SNOWFLAKE_ACCOUNT=abc12345
SNOWFLAKE_USER=your.email@company.com
SNOWFLAKE_WAREHOUSE=MY_WAREHOUSE
SNOWFLAKE_ROLE=DATA_ENGINEER
# Leave SNOWFLAKE_PASSWORD empty for SSO
```

When you run any command, your browser will open for authentication:
```bash
sst validate  # Browser opens automatically
```

### Method 2: Password Authentication

Simple setup for automation and scripts:

```env
# .env file
SNOWFLAKE_ACCOUNT=abc12345
SNOWFLAKE_USER=service_account
SNOWFLAKE_PASSWORD=your_secure_password
SNOWFLAKE_WAREHOUSE=MY_WAREHOUSE
SNOWFLAKE_ROLE=SERVICE_ROLE
```

**Security Tips:**
- Never commit passwords to Git
- Use environment variables in automation
- Rotate passwords regularly
- Consider RSA keys for production

### Method 3: RSA Key Pair (Recommended for Production)

Most secure method for service accounts and CI/CD:

#### Step 1: Generate Keys

```bash
# Generate private key (unencrypted for automation)
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt

# Generate public key
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub

# For encrypted keys (more secure, requires password)
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8
# Enter password when prompted
```

#### Step 2: Configure Snowflake

```sql
-- Get public key content (between BEGIN/END markers)
-- Copy the content from rsa_key.pub

-- Set public key for user
ALTER USER service_account SET RSA_PUBLIC_KEY='MIIBIjANBgkqh...';

-- Verify setup
DESC USER service_account;
```

#### Step 3: Configure Environment

```env
# .env file
SNOWFLAKE_ACCOUNT=abc12345
SNOWFLAKE_USER=service_account
SNOWFLAKE_WAREHOUSE=MY_WAREHOUSE
SNOWFLAKE_ROLE=SERVICE_ROLE

# RSA key authentication
SNOWFLAKE_PRIVATE_KEY_PATH=/secure/path/rsa_key.p8
# For encrypted keys only:
SNOWFLAKE_PRIVATE_KEY_PASSWORD=key_password
```

### Method 4: OAuth (Enterprise)

For enterprise SSO integration:

```env
# .env file
SNOWFLAKE_ACCOUNT=abc12345
SNOWFLAKE_USER=oauth_user
SNOWFLAKE_AUTHENTICATOR=oauth
SNOWFLAKE_TOKEN=your_oauth_token
SNOWFLAKE_WAREHOUSE=MY_WAREHOUSE
SNOWFLAKE_ROLE=OAUTH_ROLE
```

## Environment Variables Reference

| Variable | Required | Description | Example |
|----------|----------|-------------|---------|
| `SNOWFLAKE_ACCOUNT` | Yes | Snowflake account identifier | `abc12345` |
| `SNOWFLAKE_USER` | Yes* | Snowflake username | `service_account` |
| `SNOWFLAKE_USERNAME` | Yes* | Alternative to SNOWFLAKE_USER (dbt compatibility) | `service_account` |
| `SNOWFLAKE_PASSWORD` | Conditional | Password (if not using SSO/RSA) | `secure_password` |
| `SNOWFLAKE_WAREHOUSE` | No | Compute warehouse | `MY_WAREHOUSE` |
| `SNOWFLAKE_ROLE` | No | User role | `DATA_ENGINEER` |
| `SNOWFLAKE_DATABASE` | No | Default database | `PROD_DB` |
| `SNOWFLAKE_SCHEMA` | No | Default schema | `PUBLIC` |
| `SNOWFLAKE_PRIVATE_KEY_PATH` | Conditional | Path to RSA private key | `/path/to/rsa_key.p8` |
| `SNOWFLAKE_PRIVATE_KEY_PASSWORD` | Conditional | Password for encrypted RSA key | `key_password` |
| `SNOWFLAKE_AUTHENTICATOR` | No | Authentication method | `oauth` or `externalbrowser` |
| `SNOWFLAKE_TOKEN` | Conditional | OAuth token | `token_value` |

**Note:** Either `SNOWFLAKE_USER` or `SNOWFLAKE_USERNAME` is required. If both are set, `SNOWFLAKE_USER` takes precedence. `SNOWFLAKE_USERNAME` is supported for compatibility with dbt profiles.

## Testing Authentication

Verify your authentication setup:

```bash
# Test with validate (no Snowflake connection needed)
sst validate

# Test Snowflake connection with extract
sst extract \
  --db TEST_DB \
  --schema TEST_SCHEMA \
  --verbose
```

If authentication fails, you'll see:
```
ERROR: Failed to connect to Snowflake: 
  Incorrect username or password was specified
```

## Security Best Practices

### 1. Use .gitignore

Always exclude sensitive files:
```gitignore
# Add to .gitignore
.env
*.p8
*.key
*_rsa_key*
```

### 2. Rotate Credentials

- Rotate passwords quarterly
- Update RSA keys annually
- Audit service account usage

### 3. Principle of Least Privilege

Grant only necessary permissions:
```sql
-- Minimal permissions for extraction
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE service_role;
GRANT CREATE SCHEMA ON DATABASE metadata_db TO ROLE service_role;
GRANT CREATE TABLE ON SCHEMA metadata_schema TO ROLE service_role;
```

## CI/CD Configuration

For complete automation setup including authentication configuration, see the [Deployment Guide](cli-reference.md).

## Troubleshooting

### Connection Timeout

If SSO/browser authentication times out:
1. Check firewall allows browser redirect
2. Try password authentication instead
3. Verify account URL is correct

### Invalid RSA Key

If RSA authentication fails:
1. Verify public key in Snowflake matches your key
2. Check private key file permissions (should be 600)
3. Ensure key format is PKCS8

### Permission Denied

If you get permission errors:
1. Verify role has necessary grants
2. Check warehouse is running
3. Confirm database/schema exist

## Next Steps

- [CLI Reference](cli-reference.md) - Command documentation
- [Deployment Guide](cli-reference.md) - automation setup
- [Getting Started](getting-started.md) - Quick start guide