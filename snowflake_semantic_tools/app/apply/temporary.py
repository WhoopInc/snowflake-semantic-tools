"""What `apply --temporary` may do: which targets refuse it, and what it tells the user it ignored.

A temporary agent lives only as long as the session that created it, so it can never be the
deployed artifact: a target whose name reads as production refuses it. Its alias is ignored,
and the run records nothing about it in state, so the next permanent apply is planned as if
it never ran. Only agents are temporary: the views, tools and skills the run publishes are
permanent. An eval of an agent the run publishes as temporary is skipped, since the agent is
gone once the run's session ends and the eval could never run against it.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic
from snowflake_semantic_tools.domain.model.identifier import production_like
from snowflake_semantic_tools.domain.model.lifecycle import Action, ApplyOptions, ChangeSet


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
    """Report each temporary artifact whose alias the publish ignores, then each eval it skips.

    Diagnostics:
        SST-APL015: one per temporary artifact that declares an alias.
        SST-APL105: one per eval `temporary_evals` skips, naming its agent.
    """
    ignored = tuple(
        D("SST-APL015", artifact=change.key)
        for change in changeset.changes
        if change.rendered is not None and change.rendered.temporary and change.rendered.desired_alias is not None
    )
    skipped = temporary_evals(changeset)
    return (
        *ignored,
        *(
            D("SST-APL105", artifact=change.key, value=", ".join(change.depends_on))
            for change in changeset.changes
            if change.key in skipped
        ),
    )


def temporary_evals(changeset: ChangeSet) -> frozenset[str]:
    """Return the key of each eval the run would publish whose agent it publishes as a temporary agent.

    A temporary agent ends with the run's session, so its eval could never run; publishing the
    eval would leave a permanent eval of an agent that does not exist.
    """
    temporary_agents = {
        change.key
        for change in changeset.changes
        if change.action in _WRITES and change.rendered is not None and change.rendered.temporary
    }
    return frozenset(
        change.key
        for change in changeset.changes
        if change.artifact_type == "eval"
        and change.action in _WRITES
        and any(key in temporary_agents for key in change.depends_on)
    )


_WRITES = frozenset((Action.CREATE, Action.UPDATE))
