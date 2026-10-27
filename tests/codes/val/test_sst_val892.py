"""SST-VAL892: a Cortex Code profile publishes to a schema an agent or CoWork profile publishes to."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.validate.coco_profile import schema_problems
from tests.helpers.skill_inputs import profile


def test_sst_val892_fires() -> None:
    others = [("sales", QualifiedName.parse("DB.SCH.SALES")), ("everyone", QualifiedName.parse("DB.SCH.EVERYONE"))]
    [diagnostic] = schema_problems(others, [(profile(), QualifiedName.parse("db.sch.ANALYST"))])
    assert (diagnostic.code, diagnostic.severity) == ("SST-VAL892", Severity.ERROR)
    assert diagnostic.message == "coco profile 'analyst' publishes to DB.SCH, where 'sales' publishes too"
    assert diagnostic.subject == "coco_profile:analyst"


def test_sst_val892_silent() -> None:
    others = [("sales", QualifiedName.parse("DB.SCH.SALES"))]
    assert schema_problems(others, [(profile(), QualifiedName.parse("CORTEX_CODE.CONFIG.ANALYST"))]) == ()
