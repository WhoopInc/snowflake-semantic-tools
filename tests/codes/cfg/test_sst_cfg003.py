"""SST-CFG003: a key `sst_config.yml` does not declare where it is written.

An exemplar of the per-code pair `tests/codes/test_codes_tested.py` requires: `_fires` builds the
smallest input that reports the code and pins its severity, message, and subject; `_silent` builds
the nearest legitimate input and shows the code stays quiet.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.config_schema import validate_config
from snowflake_semantic_tools.domain.model.diagnostic import Severity


def test_sst_cfg003_fires() -> None:
    [diagnostic] = validate_config({"validation": {"strictness": True}})
    assert (diagnostic.code, diagnostic.severity) == ("SST-CFG003", Severity.WARNING)
    assert diagnostic.message == "unknown config key 'validation.strictness'"
    assert diagnostic.subject == "config:validation.strictness"


def test_sst_cfg003_silent() -> None:
    assert [item.code for item in validate_config({"validation": {"strict": True}})] == []
