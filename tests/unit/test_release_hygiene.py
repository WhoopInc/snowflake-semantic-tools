"""Release hygiene: nothing specific to one engineer's environment reaches a commit.

WHY THIS FILE EXISTS. This repository is public, and a comment written while
debugging against one environment easily carries that environment with it:
scratch-schema names, dated measurements, package-mirror details and pointers into
documents that do not ship. Scrubbing a tree once does not survive the next edit,
so this test runs over every file git would commit -- tracked files plus untracked
files that are not ignored -- and a regression fails in the ordinary unit gate
instead of reaching review.

WHAT IT DELIBERATELY DOES NOT ENCODE. Real company, person, account, role and
warehouse names, and the names of documents that do not ship, are not listed here
in the clear: a denylist naming them would republish exactly what it forbids. They
are listed as SHA-256 digests of the case-folded word or path, and a committed line
is checked against them: company words token by token, personal names only where
they name a schema or package someone owns (`personal_namespace`), and document
paths by their root, name and trailing segments (`design_doc_path`). Only the places
the project genuinely lives -- its licence, security policy, code owners and package
metadata -- may name its owner. Every other pattern below is safe to publish.
"""

from __future__ import annotations

import hashlib
import re
import subprocess
from collections.abc import Callable
from functools import partial
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


def _digest(word: str) -> str:
    return hashlib.sha256(word.encode()).hexdigest()


# SHA-256 of a maintainer's first name and surname, case-folded.
PERSONAL_NAMES = frozenset(
    (
        "68be7550846ecd878947b4eb0ac13d3cca3cf6c4940c94d90163e0a15e947203",
        "f68a241f6260c7607978dd8cb06c0f16364c93e938ed5ea1304bbb1a5aad4138",
    )
)
# A schema named for one engineer: `scratch.`, `dbt_` or `dbt-` followed by their name, quoted or
# not, or a bare token that puts their name before `_scratch`, as `JDOE_SCRATCH_SST` does.
_NAMESPACE = re.compile(r"(?i)\b(?:scratch|dbt)\"?[._-]\"?([a-z0-9_]+)")
_OWNED_SCRATCH = re.compile(r"(?i)\b([a-z0-9]+(?:_[a-z0-9]+)*?)_scratch(?![a-z0-9])")


def personal_namespace(line: str, names: frozenset[str] = PERSONAL_NAMES) -> bool:
    """Whether `line` names a schema or package one engineer owns, by a name in `names`.

    The candidates are what follows `scratch.`, `dbt_` or `dbt-` (a quoted identifier included,
    as in `SCRATCH."JDOE_SST"`) and what precedes `_scratch` in one token. Each `_`-separated
    part of a candidate matches when any suffix of it digests into `names`, so an initial
    before the surname (`jdoe`) still matches.
    """
    candidates = [match.group(1) for pattern in (_NAMESPACE, _OWNED_SCRATCH) for match in pattern.finditer(line)]
    for candidate in candidates:
        for part in candidate.casefold().split("_"):
            if any(_digest(part[start:]) in names for start in range(len(part))):
                return True
    return False


# SHA-256 of each path into the document tree that does not ship, case-folded: the
# files most often cited by name, its top-level directories, and the root of the private
# planning tree `.gitignore` keeps out. A directory digested
# as `dir/` is cited by any path under it; one digested as `dir/*doc` only by a
# document (.md, .yml, .yaml) under it, because this repository has its own `dir/`.
DESIGN_DOC_PATHS = frozenset(
    (
        "0516a774ca80a202dba0830e2c820ba4bc9470954b1a6147cd6470ee78870be9",
        "08ce22a052f102c5231df9e8981f3e6cd1a499fdece32b1d30ca59c160c49503",
        "11ea156b50240915fb91f83dcdb8af658bd43e4d17349760b06b0394f6f18ba4",
        "4c98e78e44f7858f64fb67895a3df0854311ffa899f9c2a5127161824ee0fec4",
        "721d067c99856a6035fa33468bdd94e7595d01d44ce10879f921495cd90fc852",
        "981e10c60cf6eece90b01a37d4419eddffdd9d63ec80bce28975dde194608f66",
        "a760d2d0ff9477f8db823921d4410e5366c8a385b1e3e4f2cef4a7f843a6ffd1",
        "c39b5ce7c9b738dccb89180ecbdd2e408d1c7369e9c475ff9efaa57368a65f1e",
        "dab3991a2dc8dbaf34ce99ad0ed36731f5e827f75ad1247113892c94602ceba3",
        "dfcee006de17459ec1dbb5515e0c7a3386240771cbdfbee5591c668e236fd830",
        "f17dd853e33da0c5b9ff53c3b6b4f9c9a543112c99fd91f67f7f69f4b56f39be",
        "1f3216ef8377362cd914299691eeb992fd98f005d624a20a7b47a0b43af6e90c",
    )
)
# Committed lines a rule would flag that must stay, by file and the digest of the stripped line:
# the `.gitignore` entry that keeps the private planning tree out has to name its root.
EXEMPT_LINES = frozenset(((".gitignore", "a480edcc3f25e54ee96a97524bd8a29a686ce2f17b5555494e9a9ef4e15d85e0"),))
_PATH = re.compile(r"(?<![\w/.-])[\w.-]+(?:/[\w.-]+)*/?")
_DOCUMENT = re.compile(r"\.(?:md|ya?ml)$")
# A numbered chapter, `01-registry.md`: the shape of that tree, not a file in this one.
_NUMBERED_DOC = re.compile(r"(?<![\w-])\d{2}-[a-z-]+\.md")


def design_doc_path(line: str, paths: frozenset[str] = DESIGN_DOC_PATHS) -> bool:
    """Whether `line` cites a path whose root, name or trailing segments digest into `paths`."""
    if _NUMBERED_DOC.search(line):
        return True
    for match in _PATH.finditer(line.casefold()):
        token = match.group().rstrip(".")
        segments = token.rstrip("/").split("/")
        candidates = {token, segments[0]} | {"/".join(segments[index:]) for index in range(len(segments))}
        if len(segments) > 1 or token.endswith("/"):
            candidates.add(segments[0] + "/")
        if len(segments) > 1 and _DOCUMENT.search(token):
            candidates.add(segments[0] + "/*doc")
        if any(_digest(candidate) in paths for candidate in candidates):
            return True
    return False


_ISO_DATE = r"\b\d{4}-\d{2}-\d{2}\b"
_MEASURED = r"\bmeasured\b"

# (rule id, matcher, why a match must not be committed)
RULES: tuple[tuple[str, Callable[[str], object], str], ...] = (
    (
        "personal-namespace",
        personal_namespace,
        "names a schema or package a single engineer owns",
    ),
    (
        "home-directory-path",
        re.compile(r"/(?:Users|home)/(?!\.\.\./)[^/\s'\"`]+/").search,
        "is an absolute path into a home directory; anonymise it as /Users/.../",
    ),
    (
        "private-package-index",
        re.compile(r"(?i)codeartifact").search,
        "names a private package index",
    ),
    (
        "private-package-mirror",
        re.compile(r"(?i)\bprivate\s+(?:package\s+)?(?:mirror|index)|\binternal\s+package\s+(?:mirror|index)").search,
        "describes resolving packages through a private mirror or index",
    ),
    (
        "dated-measurement",
        re.compile(f"(?i){_MEASURED}.{{0,40}}?{_ISO_DATE}|{_ISO_DATE}.{{0,40}}?{_MEASURED}").search,
        "dates a measurement; keep the behaviour it records and drop when it was observed",
    ),
    (
        "internal-design-doc",
        design_doc_path,
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


@pytest.mark.parametrize(("rule", "matcher", "why"), RULES, ids=[rule for rule, _, _ in RULES])
def test_committable_files_carry_no_environment_specific_content(
    committable: tuple[tuple[str, str | None], ...], rule: str, matcher: Callable[[str], object], why: str
) -> None:
    hits = [
        f"{relative}:{number}: {line.strip()[:160]}"
        for relative, text in committable
        if text is not None and relative != THIS_FILE
        for number, line in enumerate(text.splitlines(), start=1)
        if matcher(line) and (relative, _digest(line.strip())) not in EXEMPT_LINES
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


def test_every_digest_set_is_well_formed() -> None:
    for digests, size in ((PERSONAL_NAMES, 2), (DESIGN_DOC_PATHS, 12)):
        assert len(digests) == size and all(re.fullmatch(r"[0-9a-f]{64}", digest) for digest in digests)


def test_each_exempt_line_is_still_committed(committable: tuple[tuple[str, str | None], ...]) -> None:
    # An exemption that matches no committed line would let the same text back in anywhere
    # else in its file unnoticed once it moved, so a stale one fails.
    present = {
        (relative, _digest(line.strip()))
        for relative, text in committable
        if text is not None
        for line in text.splitlines()
    }
    assert present >= EXEMPT_LINES


def _synthetic(*words: str) -> frozenset[str]:
    return frozenset(_digest(word) for word in words)


# The digest rules bite against synthetic words, so this file never spells out the real ones.
SYNTHETIC_MATCHERS: dict[str, Callable[[str], object]] = {
    "personal-namespace": partial(personal_namespace, names=_synthetic("jane", "doe")),
    "internal-design-doc": partial(
        design_doc_path, paths=_synthetic("notes/", "drafts/*doc", "runbook.md", "review-1.2", "errors/index.md")
    ),
}

# A rule that has never matched anything is indistinguishable from one that cannot.
# Synthetic lines only: each rule must fire on the shape it exists for and stay quiet
# on the nearest legitimate line.
BITES = {
    "personal-namespace": (
        [
            "-- TESTED in `SCRATCH.XX_DOE`",
            "sst extract --schema dbt_jane",
            "database: scratch.jdoe_sst",
            'USE SCHEMA SCRATCH."JDOE_SST"',
            '"SCRATCH"."DOE_X".ORDERS',
            "schema: JANE_SCRATCH_SST",
            "jdoe_scratch",
            "pip install dbt-jane",
        ],
        [
            "dbt_project",
            "jane and doe",
            "dbt_utils.unique_combination_of_columns",
            "pip install dbt-core dbt-snowflake",
            'SCRATCH."ANALYTICS"',
            "<USERNAME>_SCRATCH_SST",
            "doe_scratchpad",
        ],
    ),
    "home-directory-path": (["cd /Users/jane.doe/project/"], ["PosixPath('/Users/.../models')"]),
    "private-package-index": (["resolved against an internal CodeArtifact index"], ["pip resolves from PyPI"]),
    "private-package-mirror": (
        ["resolved against a private package mirror", "pinned on the Private Index", "the internal package mirror"],
        ["This mirrors the clause order in the DDL", "an internal index into the rule list", "the package index"],
    ),
    "dated-measurement": (
        [
            "# MEASURED, as of 2026-09-24: the view serves the default",
            "the build was measured on 2026-09-24",
            "2026-09-24, measured",
        ],
        [
            "# MEASURED: DESCRIBE AGENT answers one row",
            'captured_at="2026-09-01T00:00:00Z"',
            "measured on its own paths",
        ],
    ),
    "internal-design-doc": (
        [
            "(notes/tools/tools.md, section 7)",
            "see runbook.md section 2",
            "`drafts/README.md`",
            "chapters/01-registry.md",
            "`review-1.2/expected/ddl/`",
            "errors/index.md",
        ],
        [
            "load_fixture('errors/metrics/circular_dependency.yml')",
            "negative/18-forbidden-meta.yml",
            "take notes and drafts",
            "src/notes/x.py",
            "each module under `drafts/`, such as drafts/cfg.py",
        ],
    ),
}


@pytest.mark.parametrize(("rule", "matcher", "why"), RULES, ids=[rule for rule, _, _ in RULES])
def test_each_rule_bites(rule: str, matcher: Callable[[str], object], why: str) -> None:
    matcher = SYNTHETIC_MATCHERS.get(rule, matcher)
    must_match, must_not_match = BITES[rule]
    assert [line for line in must_match if not matcher(line)] == [], f"{rule} misses a line it {why}"
    assert [line for line in must_not_match if matcher(line)] == [], f"{rule} fires on a legitimate line"
