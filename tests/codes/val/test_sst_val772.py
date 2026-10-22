"""SST-VAL772: a comparison has no candidate model to compare.

Fires when neither the command line nor the eval's settings name a candidate; a named one stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.app.compile.evals import CompiledEval
from snowflake_semantic_tools.app.evals.compare import requested_models
from snowflake_semantic_tools.domain.diagnostics import Diagnostic, Severity
from tests.helpers.eval_builders import compiled_eval_of


def evaluated() -> CompiledEval:
    return compiled_eval_of()


def test_sst_val772_fires() -> None:
    found = requested_models((evaluated(),), given=None, runs=None, allowed=frozenset())
    assert isinstance(found, Diagnostic)
    assert (found.code, found.severity) == ("SST-VAL772", Severity.ERROR)
    assert found.message == "eval 'sales_agent' names no candidate model to compare with its pinned one"
    assert found.subject == "eval:sales_agent"


def test_sst_val772_silent() -> None:
    found = requested_models((evaluated(),), given=("claude-opus-4-8",), runs=None, allowed=frozenset())
    assert not isinstance(found, Diagnostic)
