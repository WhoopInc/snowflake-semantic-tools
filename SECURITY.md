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

## Security Best Practices

When using Snowflake Semantic Tools:

- Keep your dependencies up to date
- Never commit credentials or sensitive data to your repository
- Use environment variables for authentication (see [Authentication Guide](docs/authentication.md))
- Review the `.env.example` template for proper credential management
- Enable branch protection and require code reviews for production deployments

## Supported Versions

We provide security updates for the latest major version. Please ensure you're running the most recent version to receive security patches.

---

Thank you for helping keep Snowflake Semantic Tools and the WHOOP community secure!

