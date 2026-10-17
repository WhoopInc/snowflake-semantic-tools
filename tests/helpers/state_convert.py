"""A scripted catalog for `convert_state_tables`: tables, their kind and rows, and live locks."""

from __future__ import annotations

from snowflake_semantic_tools.adapters.snowflake.connector.convert import StateConvertMethods
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from tests.helpers.snowflake_fake.driver import FakeDriverConnector, FakeDriverSession, Rows

STATE_TABLE = QualifiedName.parse("DB.S.SST_STATE")


class Catalog:
    """The tables of schema DB.S, their kind, their rows, and the live locks; renames move them."""

    def __init__(self, *, kind: str = "NO", rows: int = 3, locks: tuple[tuple[str, str], ...] = ()) -> None:
        self.tables: dict[str, tuple[str, int]] = {"SST_STATE": (kind, rows), "SST_STATE_LOCK": (kind, 1)}
        self.locks = list(locks)
        self.statements: list[str] = []
        # Rows a rival apply writes to the state table during the copy, as a race would.
        self.write_during_copy = 0

    def respond(self, statement: str, binds: tuple[object, ...]) -> tuple[Rows | None, int]:
        self.statements.append(statement)
        if statement.startswith("SELECT IS_HYBRID"):
            found = self.tables.get(str(binds[1]))
            return ([(found[0],)] if found else []), 0
        if statement.startswith("SELECT COUNT(*) FROM"):
            name = statement.split()[-1].rsplit(".", 1)[1]
            return [(self.tables[name][1],)], 0
        if statement.startswith("SELECT TARGET_NAME, RUN_ID FROM"):
            return list(self.locks), 0
        if statement.startswith(("CREATE HYBRID TABLE IF NOT EXISTS", "CREATE TABLE IF NOT EXISTS")):
            name = statement.split()[6 if "HYBRID" in statement else 5].rsplit(".", 1)[1]
            self.tables[name] = ("YES" if "HYBRID" in statement else "NO", 0)
            return None, 0
        if statement.startswith("INSERT INTO"):
            copy = statement.split()[2].rsplit(".", 1)[1]
            self.tables[copy] = (self.tables[copy][0], self.tables["SST_STATE"][1])
            kind, rows = self.tables["SST_STATE"]
            self.tables["SST_STATE"] = (kind, rows + self.write_during_copy)
            return None, rows
        if statement.startswith("ALTER TABLE") and " RENAME TO " in statement:
            old, new = statement.split()[2].rsplit(".", 1)[1], statement.split()[-1].rsplit(".", 1)[1]
            self.tables[new] = self.tables.pop(old)
            return None, 0
        if statement.startswith("DROP TABLE IF EXISTS"):
            self.tables.pop(statement.split()[-1].rsplit(".", 1)[1], None)
            return None, 0
        raise AssertionError(f"unexpected statement: {statement}")


class Connector(FakeDriverConnector, StateConvertMethods):
    def __init__(self, catalog: Catalog) -> None:
        super().__init__(FakeDriverSession(respond=catalog.respond))

    def _dict_rows(self, statement: object) -> tuple[dict[str, object], ...]:
        names = ("TARGET_NAME", "ARTIFACT_KEY", "FINGERPRINT", "QUALIFIED_NAME", "MANIFEST_ID", "STATE_MANIFEST_ID")
        return tuple({"name": name} for name in names)
