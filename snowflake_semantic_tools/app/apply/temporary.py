"""What `apply --temporary` may do: which targets refuse it, and what it tells the user it ignored.

A temporary agent lives only as long as the session that created it, so it can never be the
deployed artifact: a target whose name reads as production refuses it. Its alias is ignored,
and the run records nothing about it in state, so the next permanent apply is planned as if
it never ran.
"""

from __future__ import annotations

import re

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.lifecycle import ApplyOptions, ChangeSet

# The target-name words that mark a production-like environment, matched as whole parts of
# the name split on `_`, `-` and `.`: `prod`, `prod_us` and `eu-production` match, `product` does not.
PRODUCTION_WORDS = frozenset({"prod", "production", "prd"})


def production_like(target_name: str) -> bool:
    """Report whether a target's name marks it as a production-like environment."""
    return any(part in PRODUCTION_WORDS for part in re.split(r"[_\-.]", target_name.casefold()))


def temporary_refusal(changeset: ChangeSet, options: ApplyOptions) -> tuple[Diagnostic, ...]:
    """Refuse each temporary artifact a `--temporary` run would publish to a production-like target.

    Diagnostics:
        SST-APL013: one per temporary artifact, when the target is production-like.
    """
    if not options.temporary or not production_like(changeset.target.name):
        return ()
    return tuple(
        D("SST-APL013", artifact=change.key, target=changeset.target.name)
        for change in changeset.changes
        if change.rendered is not None and change.rendered.temporary
    )


def temporary_notes(changeset: ChangeSet) -> tuple[Diagnostic, ...]:
    """Report each temporary artifact whose declared alias the temporary publish ignores.

    Diagnostics:
        SST-APL015: one per temporary artifact that declares an alias.
    """
    return tuple(
        D("SST-APL015", artifact=change.key)
        for change in changeset.changes
        if change.rendered is not None and change.rendered.temporary and change.rendered.desired_alias is not None
    )
