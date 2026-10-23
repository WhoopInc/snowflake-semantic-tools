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
| certifi | >=2026.10.19 | MPL-2.0 | ⚠️ Weak copyleft, used unmodified |
| cryptography | >=46.0.5 | Apache 2.0 or BSD-3-Clause | ✅ Permissive |
| click | >=8.3.3,<9.0 | BSD-3-Clause | ✅ Permissive |
| textual | >=8.2,<9.0 | MIT | ✅ Permissive |
| msgspec | >=0.22,<0.23 | BSD-3-Clause | ✅ Permissive |

The `secure-local-storage` extra adds keyring (MIT) to cache SSO tokens.

SST imports certifi and cryptography directly -- certifi's CA bundle verifies the agent stream, and
cryptography reads a key-pair profile's private key -- so both are declared, though the connector
requires them too. certifi is MPL-2.0: a file-level copyleft whose conditions attach to certifi's
own files, which SST neither modifies nor bundles. Its floor is above the connector's on purpose:
every certifi before 2026.10.19 trusts the emSign root behind the compromised eMudhra
intermediates ([GHSA-6q48-6c7w-9gw2](https://github.com/certifi/python-certifi/security/advisories/GHSA-6q48-6c7w-9gw2)).

Textual draws the `sst ask` conversation; its tree is listed below with the connector's.

msgspec decodes dbt's `manifest.json` into typed structs that skip every field SST does not read,
which is most of a large manifest. It has no runtime dependencies of its own. It is 0.x, so the range
stops at the next minor release.

## Transitive Runtime Dependencies

Every other package `pip install snowflake-semantic-tools` can install, on any platform, as
`poetry.lock` resolves it. `tests/unit/test_dependencies.py` fails when a re-lock reaches a package
this table does not list, when a listed license is not permissive, and when an installed package's
own metadata names a copyleft or proprietary license.

| Package | Pulled in by | License |
|---------|--------------|---------|
| asn1crypto | snowflake-connector-python | MIT |
| backports-tarfile | jaraco-context (Python < 3.12) | MIT |
| boto3 | snowflake-connector-python | Apache 2.0 |
| botocore | snowflake-connector-python, boto3 | Apache 2.0 |
| cffi | cryptography (CPython) | MIT-0 |
| charset-normalizer | requests | MIT |
| filelock | snowflake-connector-python | MIT |
| idna | requests | BSD-3-Clause |
| importlib-metadata | keyring (Python < 3.12) | Apache 2.0 |
| jaraco-classes | keyring | MIT |
| jaraco-context | keyring | MIT |
| jaraco-functools | keyring | MIT |
| jeepney | keyring (Linux) | MIT |
| jmespath | boto3, botocore | MIT |
| keyring | snowflake-connector-python (`secure-local-storage`) | MIT |
| linkify-it-py | markdown-it-py (`linkify` extra, which textual asks for) | MIT |
| markdown-it-py | textual, rich, mdit-py-plugins | MIT |
| mdit-py-plugins | textual | MIT |
| mdurl | markdown-it-py | MIT |
| more-itertools | jaraco-classes, jaraco-functools | MIT |
| packaging | snowflake-connector-python | Apache 2.0 or BSD-2-Clause |
| platformdirs | textual, snowflake-connector-python | MIT |
| pycparser | cffi | BSD-3-Clause |
| pygments | textual, rich | BSD-2-Clause |
| pyjwt | snowflake-connector-python | MIT |
| pyopenssl | snowflake-connector-python | Apache 2.0 |
| python-dateutil | botocore | Apache 2.0 and BSD-3-Clause |
| pytz | snowflake-connector-python | MIT |
| pywin32-ctypes | keyring (Windows) | BSD-3-Clause |
| requests | snowflake-connector-python | Apache 2.0 |
| rich | textual | MIT |
| ruamel-yaml-clib | ruamel-yaml (CPython) | MIT |
| s3transfer | boto3 | Apache 2.0 |
| secretstorage | keyring (Linux) | BSD-3-Clause |
| six | python-dateutil | MIT |
| sortedcontainers | snowflake-connector-python | Apache 2.0 |
| tomlkit | snowflake-connector-python | MIT |
| typing-extensions | textual, snowflake-connector-python | PSF-2.0 |
| urllib3 | requests, botocore | MIT |
| zipp | importlib-metadata | MIT |

python-dateutil is dual-licensed by file (BSD-3-Clause for its older code, Apache 2.0 for newer
contributions); both are permissive.

## Optional Dependencies

| Package | Version | Extra | License | Status |
|---------|---------|-------|---------|--------|
| dbt-snowflake | >=1.10,<2.0 | `dbt` | Apache 2.0 | ✅ Permissive |

The extra installs dbt Core and its own dependency tree, which is dbt's, not SST's: SST never
imports dbt and only runs whichever `dbt` executable is on `PATH`. That tree is not all permissive.
It includes, for example, text-unidecode (Artistic License or GPL-2.0-or-later). metricflow, once
under the Business Source License, has been Apache 2.0 since dbt Labs relicensed it in October
2025; the release `poetry.lock` pins (0.213.0) is Apache-2.0. Anyone redistributing an
environment with the extra installed should review dbt Core's licenses themselves.

SST never depends on proprietary dbt. dbt Core 1.x and dbt OSS 2.x (`pip install dbt-oss`) are
Apache 2.0. The full dbt v2 distribution (`pip install dbt`) is under the dbt Product Licensing
Agreement; a user may install and run it beside SST, which is their choice under that agreement,
but SST neither requires nor installs it.

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
