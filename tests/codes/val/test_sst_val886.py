"""SST-VAL886: CoWork profile eval left out where CoWork lists the profile.

A CoWork profile's eval (a Snowflake private preview), on a target that publishes the profile to
SNOWFLAKE_INTELLIGENCE.PROFILES, does not compile; on a rehearsal target it compiles there.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.diagnostic_filters import codes, only
from tests.helpers.eval_builders import REHEARSAL, compile_profile_eval
from tests.helpers.eval_inputs import concierge_eval


def test_sst_val886_fires() -> None:
    listed = QualifiedName.parse("snowflake_intelligence.profiles.concierge")
    result = compile_profile_eval(concierge_eval(), listed)
    diagnostic = only(result.diagnostics, "SST-VAL886")
    # An info, so strict mode, which a plan of the CoWork target runs in, never promotes it.
    assert diagnostic.severity is Severity.INFO
    assert diagnostic.message == (
        "eval 'concierge' is not compiled: its cowork profile publishes to SNOWFLAKE_INTELLIGENCE.PROFILES, "
        "where CoWork lists it"
    )
    assert diagnostic.subject == "eval:cowork_profile:concierge"
    assert result.compiled == ()


def test_sst_val886_silent() -> None:
    result = compile_profile_eval(concierge_eval(), REHEARSAL)
    assert "SST-VAL886" not in codes(result.diagnostics)
    [compiled] = result.compiled
    assert isinstance(compiled, CompiledEval)
    assert compiled.dataset_target.sql.startswith("DB.PROFILES.EVAL_CONCIERGE_")
