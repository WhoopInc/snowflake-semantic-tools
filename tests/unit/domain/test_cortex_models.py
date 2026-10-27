"""`lifecycle_concern`: what about a model's `SHOW CORTEX BASE MODELS` row makes it a risk to depend on."""

from __future__ import annotations

import pytest

from snowflake_semantic_tools.domain.ports.snowflake.cortex_models import CortexBaseModel, lifecycle_concern


@pytest.mark.parametrize(
    ("row", "concern"),
    [
        (None, "not in SHOW CORTEX BASE MODELS"),
        (CortexBaseModel("m", "internal"), "marked INTERNAL by SHOW CORTEX BASE MODELS"),
        (CortexBaseModel("m", "EOL", eol_date="2026-01-31"), "past end-of-life (2026-01-31)"),
        (CortexBaseModel("m", "EOL"), "past end-of-life (no date listed)"),
        (CortexBaseModel("m", "LEGACY", eol_date="2027-06-30"), "legacy; end-of-life: 2027-06-30"),
        (CortexBaseModel("m", "LEGACY"), "legacy; end-of-life: no date announced"),
        (CortexBaseModel("m", "PUPR"), "in public preview"),
        (CortexBaseModel("m", "prpr"), "in private preview"),
    ],
)
def test_a_risky_lifecycle_is_named(row: CortexBaseModel | None, concern: str) -> None:
    assert lifecycle_concern(row) == concern


@pytest.mark.parametrize("status", ["GA", "SOMETHING_NEW", None])
def test_a_ga_model_or_an_unknown_status_is_no_concern(status: str | None) -> None:
    assert lifecycle_concern(CortexBaseModel("m", status)) is None
