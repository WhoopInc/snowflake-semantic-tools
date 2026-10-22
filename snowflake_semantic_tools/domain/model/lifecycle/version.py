"""The comment SST gives each agent version it commits, and the specification fingerprint in it.

Every version SST commits carries `git:<sha> sst-spec:<digest>`: the commit it was published
from, `<sha>-dirty` when the project had uncommitted changes, and the fingerprint of its
specification alone. Plan reads the fingerprint back from
SHOW VERSIONS IN AGENT to tell whether the newest committed version already holds the
specification being planned, so a change to only the agent's comment, profile, tags, alias or
`secure` moves no version. A version committed before SST wrote the fingerprint is recognised
by its specification instead, when the role may read it.

An eval-model comparison commits versions of a published agent too, to evaluate an
orchestration model on: a hold copy of the released specification, one version per candidate
model, and a restore copy. Each carries `sst-eval-model:<label>` and `run:<run id>` after the
usual tokens, and each candidate an alias starting `EVAL_`. None of them is a release: plan
never takes one for the newest version SST published, never unsets an `EVAL_` alias, and a
project may not declare an alias with that prefix.
"""

from __future__ import annotations

import json
import re
from hashlib import sha256

# What precedes the specification fingerprint in a version's comment.
SPEC_TOKEN = "sst-spec:"
# What follows the sha of a version published while the project had uncommitted changes.
DIRTY_SUFFIX = "-dirty"
_SPEC = re.compile(r"(?:^|\s)sst-spec:([0-9a-f]{64})(?=\s|$)", re.IGNORECASE)
# What precedes the label of a version an eval-model comparison commits: a model, `hold` or `restore`.
EVAL_MODEL_TOKEN = "sst-eval-model:"
# What every alias an eval-model comparison sets starts with, compared casefolded; no project
# alias may start with it.
EVAL_ALIAS_PREFIX = "EVAL_"
_EVAL_MODEL = re.compile(r"(?:^|\s)sst-eval-model:\S+(?=\s|$)", re.IGNORECASE)
_EVAL_MODEL_RUN = re.compile(r"(?:^|\s)sst-eval-model:(\S+)\s+run:(\S+)(?=\s|$)", re.IGNORECASE)
_NOT_ALIAS_CHARACTER = re.compile(r"[^A-Z0-9]")


def spec_fingerprint(specification: str) -> str:
    """Fingerprint an agent specification: the SHA-256 of its canonical JSON, keys sorted.

    Text that is not JSON is fingerprinted as written, its lines right-stripped, so it matches
    no rendered specification unless it is the same text.
    """
    try:
        document = json.loads(specification)
        text = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    except ValueError:
        text = "\n".join(line.rstrip() for line in specification.splitlines()).strip()
    return sha256(text.encode("utf-8")).hexdigest()


def version_comment(git_sha: str, specification: str, *, dirty: bool = False) -> str:
    """Return the comment a committed version carries: `git:<sha> sst-spec:<fingerprint>`.

    A `dirty` version, published while the project had uncommitted changes, reads
    `git:<sha>-dirty`, since the commit does not hold its specification.
    """
    label = f"{git_sha}{DIRTY_SUFFIX}" if dirty else git_sha
    return f"git:{label} {SPEC_TOKEN}{spec_fingerprint(specification)}"


def commented_spec_fingerprint(comment: str | None) -> str | None:
    """Read the specification fingerprint from a version's comment; None when it carries none."""
    match = _SPEC.search(comment or "")
    return match.group(1).lower() if match else None


def held_spec_fingerprint(comment: str | None, specification: str | None) -> str | None:
    """Return the fingerprint of the specification a committed version holds.

    The comment's fingerprint wins, since SST wrote it with the version; a version without one
    is fingerprinted from its specification. None when neither is there, as for a secure
    agent's version the role may not read.
    """
    commented = commented_spec_fingerprint(comment)
    if commented is not None:
        return commented
    return spec_fingerprint(specification) if specification is not None else None


def eval_model_comment(git_sha: str, specification: str, label: str, run_id: str, *, dirty: bool = False) -> str:
    """Return the comment an eval-model version carries: `version_comment`'s, then its label and run.

    Reads `git:<sha> sst-spec:<fingerprint> sst-eval-model:<label> run:<run id>`.

    Args:
        label: The orchestration model the version evaluates, or `hold` or `restore` for the
            copies of the released specification a comparison commits around the candidates.
        run_id: The comparison that committed the version.

    Raises:
        ValueError: `label` or `run_id` is empty or holds whitespace, so the comment would not
            read back as one token each.
    """
    for value in (label, run_id):
        if not value or any(character.isspace() for character in value):
            raise ValueError(f"an eval-model version's label and run id are single tokens, not {value!r}")
    return f"{version_comment(git_sha, specification, dirty=dirty)} {EVAL_MODEL_TOKEN}{label} run:{run_id}"


def is_eval_model_comment(comment: str | None) -> bool:
    """Report whether a version's comment marks it an eval-model version, its token compared casefolded."""
    return _EVAL_MODEL.search(comment or "") is not None


def eval_model_run(comment: str | None) -> tuple[str, str] | None:
    """Read the label and the run id an eval-model version's comment carries, as written.

    Returns:
        `(label, run id)`, such as `("claude-opus-4-8", "c7f2a19e")`; None for a comment
        `eval_model_comment` did not write.
    """
    match = _EVAL_MODEL_RUN.search(comment or "")
    return (match.group(1), match.group(2)) if match else None


def eval_model_alias(model: str) -> str:
    """Return the alias an eval-model comparison sets on a model's version, such as `EVAL_CLAUDE_OPUS_4_8`.

    The model is upper-cased and every character but an ASCII letter or digit becomes `_`, so
    the alias is a plain identifier whatever the model is called.
    """
    return EVAL_ALIAS_PREFIX + _NOT_ALIAS_CHARACTER.sub("_", model.upper())


def is_eval_alias(alias: str) -> bool:
    """Report whether an alias starts with the prefix eval-model versions reserve, compared casefolded."""
    return alias.strip().casefold().startswith(EVAL_ALIAS_PREFIX.casefold())
