"""Release hygiene: nothing specific to one engineer's environment reaches a commit.

WHY THIS FILE EXISTS. This repository is public, and its reference fixture is
vendored from a private project that carries scratch-schema names, production
measurements and pointers into design documents that do not ship. Scrubbing a tree
once does not survive the next re-vendor, so this test runs over every file git
would commit -- tracked files plus untracked files that are not ignored -- and a
regression fails in the ordinary unit gate instead of reaching review.

WHAT IT DELIBERATELY DOES NOT ENCODE. Real company, account, role and warehouse names are not
listed here in the clear: a denylist naming them would republish exactly what it forbids. They
are listed as SHA-256 digests of the case-folded word, and a committed line is checked word by
word against them; only the places the project genuinely lives -- its licence, security
policy, code owners and package metadata -- may name its owner. Every other pattern below is
safe to publish.
"""

from __future__ import annotations

import hashlib
import re
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
THIS_FILE = Path(__file__).resolve().relative_to(REPO_ROOT).as_posix()

# SHA-256 of each company-specific word, case-folded: the company itself, a maintainer's
# surname, an account locator, a role, a warehouse, and a scratch schema the fixture once used.
COMPANY_WORDS = frozenset(
    (
        "f4e12479bd37d7e5e5489b24e6769fa0779f3c314f80484d4f320148b2a0196c",
        "f68a241f6260c7607978dd8cb06c0f16364c93e938ed5ea1304bbb1a5aad4138",
        "8ff9d6046f1fc6455039d5072e8ff4e1cf5434664ac315229373fdf71119f133",
        "9f5ab2d04b54dd5195fbab7c81d85da666fbfaa09c872094abdfe99ee1cb16a4",
        "162f0a0866ef85b2e0fb85d974e0f0cc6d419bf2be45f82ee7b65b6a4a21d6cd",
        "b9cb8fd1dd174ac2748b8e3c8e3661901e6f5e35c80277da72d14032eae73b7d",
    )
)
# Where the project's owner is named on purpose.
COMPANY_HOMES = frozenset(("CODEOWNERS", "LICENSE", "SECURITY.md", "pyproject.toml"))
_WORD = re.compile(r"[a-z0-9_]+")


def company_words(line: str, digests: frozenset[str] = COMPANY_WORDS) -> list[str]:
    """The words of `line` whose case-folded digest is in `digests`."""
    return [word for word in _WORD.findall(line.casefold()) if hashlib.sha256(word.encode()).hexdigest() in digests]


# (rule id, pattern, why a match must not be committed)
RULES = (
    (
        "personal-namespace",
        re.compile(r"(?i)\b(?:scratch|dbt)[._][a-z0-9_]*luizzi|\bdbt_matthew\b"),
        "names a single engineer's scratch or dbt development schema",
    ),
    (
        "home-directory-path",
        re.compile(r"/(?:Users|home)/(?!\.\.\./)[^/\s'\"`]+/"),
        "is an absolute path into a home directory; anonymise it as /Users/.../",
    ),
    (
        "private-package-index",
        re.compile(r"(?i)codeartifact"),
        "names a private package index",
    ),
    (
        "internal-design-doc",
        re.compile(
            r"harness\.md|audit-0\.3|snowflake-drift\.md|reference-surface\.md|errors/catalog\.md"
            r"|(?<![\w/-])(?:specs|gaps|migration|grants)/[\w./-]*\.(?:md|ya?ml)"
            r"|(?<![\w/-])reference-impl/"
            r"|(?<![\w/.-])engine/(?:README|pseudocode)"
            r"|(?<![\w-])\d{2}-[a-z-]+\.md"
        ),
        "points into design documents that do not ship with this repository",
    ),
)


@pytest.fixture(scope="module")
def committable() -> tuple[tuple[str, str | None], ...]:
    """Every path git would commit, with its text, or None for a binary file."""
    try:
        listed = subprocess.run(
            ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
            cwd=REPO_ROOT,
            capture_output=True,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        pytest.skip(f"release hygiene needs a git checkout: {error}")
    files = []
    for relative in sorted(path for path in listed.stdout.decode("utf-8").split("\0") if path):
        path = REPO_ROOT / relative
        if not path.is_file():
            continue
        data = path.read_bytes()
        files.append((relative, None if b"\0" in data[:8192] else data.decode("utf-8", errors="replace")))
    return tuple(files)


def test_no_dbt_user_id_is_committed(committable: tuple[tuple[str, str | None], ...]) -> None:
    offenders = [relative for relative, _ in committable if Path(relative).name == ".user.yml"]
    assert not offenders, f"dbt writes an anonymous user id to .user.yml; it must stay untracked: {offenders}"


@pytest.mark.parametrize(("rule", "pattern", "why"), RULES, ids=[rule for rule, _, _ in RULES])
def test_committable_files_carry_no_environment_specific_content(
    committable: tuple[tuple[str, str | None], ...], rule: str, pattern: re.Pattern[str], why: str
) -> None:
    hits = [
        f"{relative}:{number}: {line.strip()[:160]}"
        for relative, text in committable
        if text is not None and relative != THIS_FILE
        for number, line in enumerate(text.splitlines(), start=1)
        if pattern.search(line)
    ]
    assert not hits, f"{rule}: a committed line {why}\n" + "\n".join(hits)


def test_committable_files_name_no_company_identifier_outside_where_the_project_lives(
    committable: tuple[tuple[str, str | None], ...],
) -> None:
    hits = [
        f"{relative}:{number}: {line.strip()[:160]}"
        for relative, text in committable
        if text is not None and relative != THIS_FILE and relative not in COMPANY_HOMES
        for number, line in enumerate(text.splitlines(), start=1)
        if company_words(line)
    ]
    assert not hits, (
        "a committed line names a company-specific identifier; use a neutral placeholder, or read it "
        "from configuration (docs/guides/testing-live.md)\n" + "\n".join(hits)
    )


def test_the_company_identifier_check_matches_whole_words_case_folded() -> None:
    # A synthetic word stands in for the real ones, which this file never spells out.
    digests = frozenset((hashlib.sha256(b"acme_wh").hexdigest(),))
    assert company_words("USE WAREHOUSE ACME_WH;", digests) == ["acme_wh"]
    assert company_words("database: Acme_Wh.SCRATCH", digests) == ["acme_wh"]
    assert company_words("acme_wh2 and xacme_wh and acme-wh", digests) == []
    assert len(COMPANY_WORDS) == 6 and all(re.fullmatch(r"[0-9a-f]{64}", digest) for digest in COMPANY_WORDS)


# A rule that has never matched anything is indistinguishable from one that cannot.
# Synthetic lines only: each rule must fire on the shape it exists for and stay quiet
# on the nearest legitimate line.
BITES = {
    "personal-namespace": (["-- TESTED in `SCRATCH.XX_LUIZZI`", "sst extract --schema dbt_matthew"], ["dbt_project"]),
    "home-directory-path": (["cd /Users/jane.doe/project/"], ["PosixPath('/Users/.../models')"]),
    "private-package-index": (["resolved against an internal CodeArtifact index"], ["a private package mirror"]),
    "internal-design-doc": (
        [
            "(specs/tools/tools.md X007)",
            "see harness.md section 2",
            "`gaps/README.md`",
            "engine/pseudocode/01-registry.md",
            "`reference-impl/expected/ddl/`",
            "errors/catalog.md",
        ],
        ["load_fixture('errors/metrics/circular_dependency.yml')", "negative/18-forbidden-meta.yml"],
    ),
}


@pytest.mark.parametrize(("rule", "pattern", "why"), RULES, ids=[rule for rule, _, _ in RULES])
def test_each_rule_bites(rule: str, pattern: re.Pattern[str], why: str) -> None:
    must_match, must_not_match = BITES[rule]
    assert [line for line in must_match if not pattern.search(line)] == [], f"{rule} misses a line it {why}"
    assert [line for line in must_not_match if pattern.search(line)] == [], f"{rule} fires on a legitimate line"
