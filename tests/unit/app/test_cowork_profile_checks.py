"""A CoWork profile's rendered spec is checked as an agent's is: its size, and what publication will not survive.

CoWork profiles are a Snowflake private preview; a profile publishes an AGENT, so its spec meets
the same limits an agent's does.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from tests.helpers.cowork_profiles import compile_profiles, compiled_profiles, cowork_profile, described_agent
from tests.helpers.diagnostic_filters import coded


def test_a_profile_spec_over_the_size_limit_is_an_error_and_does_not_compile() -> None:
    result = compile_profiles(cowork_profile(orchestration_instructions="x" * 100_000), agents=(described_agent(),))
    [diagnostic] = coded(result.diagnostics, "SST-VAL511")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.message.startswith("agent 'everyone': rendered spec is 100")
    assert diagnostic.subject == "cowork_profile:everyone"
    assert compiled_profiles(result) == ()


def test_a_profile_spec_near_the_size_limit_warns_and_still_compiles() -> None:
    result = compile_profiles(cowork_profile(orchestration_instructions="x" * 90_000), agents=(described_agent(),))
    [diagnostic] = coded(result.diagnostics, "SST-VAL512")
    assert (diagnostic.severity, diagnostic.subject) == (Severity.WARNING, "cowork_profile:everyone")
    assert coded(result.diagnostics, "SST-VAL511") == []
    assert len(compiled_profiles(result)) == 1


def test_a_dollar_quote_in_a_profile_spec_is_an_error_at_its_offset() -> None:
    result = compile_profiles(cowork_profile(response_instructions="Costs are $$ in USD."), agents=(described_agent(),))
    [diagnostic] = coded(result.diagnostics, "SST-RND011")
    assert diagnostic.severity is Severity.ERROR
    assert diagnostic.subject == "cowork_profile:everyone"
    assert diagnostic.message.startswith("agent 'everyone': spec contains '$$' at ")
    assert compiled_profiles(result) == ()


def test_a_profile_always_renders_its_router_so_it_never_has_no_tools() -> None:
    result = compile_profiles(cowork_profile(), agents=(described_agent(),))
    assert [coded(result.diagnostics, code) for code in ("SST-RND010", "SST-RND011", "SST-RND013")] == [[], [], []]
    assert len(compiled_profiles(result)) == 1
