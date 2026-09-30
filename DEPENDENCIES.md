# Dependency Licenses

This document verifies that every dependency is licensed under a permissive open
source license (MIT, Apache 2.0, BSD), as open source distribution requires.
Versions are the ranges `pyproject.toml` declares; `poetry.lock` pins the exact
releases.

## Runtime Dependencies

| Package | Version | License | Status |
|---------|---------|---------|--------|
| pyyaml | >=6.0.2,<7.0 | MIT | ✅ Permissive |
| snowflake-connector-python (with `secure-local-storage`) | >=3.12.3,<5.0 | Apache 2.0 | ✅ Permissive |
| click | >=8.1.7,<9.0 | BSD-3-Clause | ✅ Permissive |

The `secure-local-storage` extra adds keyring (MIT) to cache SSO tokens.

## Optional Dependencies

| Package | Version | Extra | License | Status |
|---------|---------|-------|---------|--------|
| dbt-snowflake | >=1.8,<2.0 | `dbt` | Apache 2.0 | ✅ Permissive |

## Development Dependencies

| Package | Version | License | Status |
|---------|---------|---------|--------|
| pytest | 8.3.4 | MIT | ✅ Permissive |
| pytest-cov | 6.0.0 | MIT | ✅ Permissive |
| mypy | 1.13.0 | MIT | ✅ Permissive |
| import-linter | ^2.1 | BSD-2-Clause | ✅ Permissive |
| types-PyYAML | ^6.0.12 | Apache 2.0 | ✅ Permissive |
| black | 24.10.0 | MIT | ✅ Permissive |
| isort | 5.13.2 | MIT | ✅ Permissive |
| pre-commit | 4.0.1 | MIT | ✅ Permissive |

## Summary

✅ **All dependencies are licensed under permissive open source licenses**

All dependencies meet the criteria for inclusion in Apache-2.0-licensed open source software:
- No GPL or copyleft licenses
- No proprietary dependencies
- All licenses allow commercial use and redistribution
