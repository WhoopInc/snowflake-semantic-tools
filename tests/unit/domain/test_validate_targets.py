"""Two artifacts of different types publishing to one Snowflake name are reported once per name."""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.validate.targets import shared_targets


def test_a_name_shared_across_types_is_reported_and_one_type_sharing_is_not() -> None:
    shared = QualifiedName.parse("DB.SCH.SALES")
    registry = QualifiedName.parse("DB.SCH.REGISTRY")
    published = (
        ("semantic_view", "semantic_view:sales", shared),
        ("agent", "agent:sales", QualifiedName.parse("db.sch.sales")),
        ("profile", "profile:a", registry),
        ("profile", "profile:b", registry),
    )
    found = shared_targets(published)
    assert [(item.code, item.subject, item.context["a"]) for item in found] == [
        ("SST-VAL843", "agent:sales", "semantic_view:sales")
    ]
