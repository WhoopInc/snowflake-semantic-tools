"""Artifact and member keys: the `<kind>:<name>` identity SST persists and matches on.

A key is a kind, a colon, and a name: `semantic_view:jaffle_menu`, `metric:order_count`,
`tool:docs_search`, `eval:sales`. The kind is the name of an artifact or member type in the
registry (`registry.py`), or of an input only a profile carries, such as `command`. A name
may itself contain a colon, so a key splits at its first colon only.

Keys are persisted in state, manifests, and saved plans, and the same form is a
diagnostic's `subject` and what `--select` matches. So the caller normalizes the name,
casefolding it or not exactly as that kind always has, and nothing here normalizes
either part: a key spelled differently no longer matches the state and plans written
with it.
"""

from __future__ import annotations


def artifact_key(kind: str, name: str) -> str:
    """Build the key naming one artifact or member.

    Both parts are used as given; the caller applies its kind's normalization first.
    """
    return f"{kind}:{name}"


def split_artifact_key(key: str) -> tuple[str, str]:
    """Split a key into its kind and name at the first colon.

    The name keeps any later colon, and neither part is normalized.

    Raises:
        ValueError: the key has no colon, so it names no kind.
    """
    kind, separator, name = key.partition(":")
    if not separator:
        raise ValueError(f"artifact key {key!r} has no ':' between a kind and a name")
    return kind, name
