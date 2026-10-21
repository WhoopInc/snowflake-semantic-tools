"""What `apply --temporary` may do: which targets refuse it, and what it tells the user it ignored.

A temporary agent lives only as long as the session that created it, so it can never be the
deployed artifact: a target whose name reads as production refuses it. Its alias is ignored,
and the run records nothing about it in state, so the next permanent apply is planned as if
it never ran. Only agents are temporary: the views, tools and skills the run publishes are
permanent. An eval of an agent the run publishes as temporary is skipped, since the agent is
gone once the run's session ends and the eval could never run against it.

A CoWork profile is skipped too. CoWork profiles are a Snowflake private preview (PrPr), as
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/about-cowork-profiles and
https://docs.snowflake.com/en/LIMITEDACCESS/snowflake-cortex/set-up-cowork-profile-routing
describe them. A temporary profile, a TEMPORARY agent with an agent_router tool, has not been
measured on Snowflake, and a permanent profile would route to agents that end with the run's
session, so a temporary run publishes none.
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


def temporary_notes(changeset: ChangeSet, *, temporary: bool = False) -> tuple[Diagnostic, ...]:
    """Report each temporary artifact whose alias the publish ignores, then each eval and profile it skips.

    Args:
        temporary: The run is `apply --temporary`, which leaves every CoWork profile out.

    Diagnostics:
        SST-APL015: one per temporary artifact that declares an alias.
        SST-APL105: one per eval `temporary_evals` skips, naming its agent.
        SST-APL106: one per CoWork profile `temporary_profiles` skips.
    """
    ignored = tuple(
        D("SST-APL015", artifact=change.key)
        for change in changeset.changes
        if change.rendered is not None and change.rendered.temporary and change.rendered.desired_alias is not None
    )
    skipped = temporary_evals(changeset)
    profiles = temporary_profiles(changeset, temporary=temporary)
    return (
        *ignored,
        *(
            D("SST-APL105", artifact=change.key, value=", ".join(change.depends_on))
            for change in changeset.changes
            if change.key in skipped
        ),
        *(
            D("SST-APL106", subject=change.key, artifact=change.key)
            for change in changeset.changes
            if change.key in profiles
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


def temporary_profiles(changeset: ChangeSet, *, temporary: bool) -> frozenset[str]:
    """Return the key of each CoWork profile a `--temporary` run would publish; none for a permanent run.

    A temporary run publishes no profile, as the module says, whatever agents it routes to.
    """
    if not temporary:
        return frozenset()
    return frozenset(
        change.key
        for change in changeset.changes
        if change.artifact_type == _COWORK_PROFILE and change.action in _WRITES
    )


def temporary_skips(changeset: ChangeSet, *, temporary: bool) -> frozenset[str]:
    """Return the key of each change the run skips because it is temporary: an eval, or a profile."""
    return temporary_evals(changeset) | temporary_profiles(changeset, temporary=temporary)


_WRITES = frozenset((Action.CREATE, Action.UPDATE))
# Snowflake private preview (PrPr): CoWork profiles
_COWORK_PROFILE = "cowork_profile"
