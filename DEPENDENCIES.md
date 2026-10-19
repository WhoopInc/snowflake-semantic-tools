# Dependency Licenses

This document records the license of every direct dependency, so that a release can be checked
against the requirements of Apache-2.0-licensed open source distribution. Versions are the ranges
`pyproject.toml` declares; `poetry.lock` pins the exact releases. A pull request that adds or
re-pins a dependency updates this file with `pyproject.toml` and `poetry.lock`;
`tests/unit/test_dependencies.py` fails when the tables below and `pyproject.toml` disagree.

## Runtime Dependencies

Installed with the package (`pip install snowflake-semantic-tools`).

| Package | Version | License | Status |
|---------|---------|---------|--------|
| pyyaml | >=6.0.2,<7.0 | MIT | ✅ Permissive |
| ruamel-yaml | >=0.18.6,<0.19 | MIT | ✅ Permissive |
| snowflake-connector-python (with `secure-local-storage`) | >=4.7.1,<5.0 | Apache 2.0 | ✅ Permissive |
| certifi | >=2024.7.4 | MPL-2.0 | ⚠️ Weak copyleft, used unmodified |
| cryptography | >=46.0.5 | Apache 2.0 or BSD-3-Clause | ✅ Permissive |
| click | >=8.3.3,<9.0 | BSD-3-Clause | ✅ Permissive |
| textual | >=8.2,<9.0 | MIT | ✅ Permissive |

The `secure-local-storage` extra adds keyring (MIT) to cache SSO tokens.

SST imports certifi and cryptography directly -- certifi's CA bundle verifies the agent stream, and
cryptography reads a key-pair profile's private key -- so both are declared, though the connector
requires them too. certifi is MPL-2.0: a file-level copyleft whose conditions attach to certifi's
own files, which SST neither modifies nor bundles.

Textual draws the `sst ask` conversation. It brings rich, pygments, markdown-it-py with
mdit-py-plugins, linkify-it-py and mdurl, platformdirs and typing-extensions: MIT, BSD-2-Clause and
PSF-2.0.

The connector's own transitive dependencies are all permissive: MIT or MIT-0, BSD, Apache 2.0 and
PSF-2.0.

## Optional Dependencies

| Package | Version | Extra | License | Status |
|---------|---------|-------|---------|--------|
| dbt-snowflake | >=1.10,<2.0 | `dbt` | Apache 2.0 | ✅ Permissive |

The extra installs dbt Core and its own dependency tree, which is dbt's, not SST's: SST never
imports dbt and only runs whichever `dbt` executable is on `PATH`. That tree is not all permissive.
It includes, for example, text-unidecode (Artistic License or GPL-2.0-or-later) and metricflow,
whose license depends on the release (`poetry.lock` pins it). Anyone redistributing an
environment with the extra installed should review dbt Core's licenses themselves.

## Development Dependencies

The `dev` group in `pyproject.toml`. These run the gates and are never installed with the package.

| Package | Version | License | Status |
|---------|---------|---------|--------|
| pre-commit | 4.0.1 | MIT | ✅ Permissive |
| pytest | 9.0.3 | MIT | ✅ Permissive |
| pytest-cov | 6.0.0 | MIT | ✅ Permissive |
| pytest-xdist | >=3.6 | MIT | ✅ Permissive |
| pytest-timeout | >=2.3 | MIT | ✅ Permissive |
| hypothesis | >=6.100 | MPL-2.0 | ⚠️ Weak copyleft, development only |
| mypy | 1.13.0 | MIT | ✅ Permissive |
| types-PyYAML | ^6.0.12 | Apache 2.0 | ✅ Permissive |
| ruff | >=0.16 | MIT | ✅ Permissive |
| import-linter | ^2.1 | BSD-2-Clause | ✅ Permissive |
| mutmut | >=3.2 | BSD-3-Clause | ✅ Permissive |
| bandit | >=1.8 | Apache 2.0 | ✅ Permissive |
| pip-audit | >=2.7 | Apache 2.0 | ✅ Permissive |

## CI-only Tools

Installed by CI outside the project's environment, so `poetry.lock` does not resolve them.

| Package | Version | Pinned in | License | Status |
|---------|---------|-----------|---------|--------|
| semgrep | 1.179.0 | `.github/requirements/semgrep.txt` | LGPL-2.1-or-later | ⚠️ Copyleft, CI only |

## Summary

✅ **Every direct runtime and optional dependency but certifi is under a permissive license** (MIT,
BSD, or Apache 2.0). certifi (MPL-2.0) is the one runtime dependency, direct or transitive, that is
not permissive, and it is used unmodified. The optional `dbt` extra's own tree is described above.

Two tools that never ship with the package carry copyleft licenses: hypothesis (MPL-2.0), which the
test suite imports, and semgrep (LGPL-2.1-or-later), which CI runs as a separate program. Neither is
distributed with SST, imported by the package, or modified.

For the package as `pip install snowflake-semantic-tools` installs it, without the `dbt` extra:

- No GPL or AGPL dependency
- No proprietary dependency
- Every license allows commercial use and redistribution
