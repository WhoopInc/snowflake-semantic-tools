"""The fake's `ExecutionPort`: queries answer and scripts change the account.

A query is logged, then answers with the next of `query_results` (raising it when it is an
exception), else from the account: `SELECT COUNT(*) AS ROW_COUNT FROM <table>` counts
`table_row_counts`, and every other query returns no rows. A script is logged, then fails
with the next of `execute_results` when that is a failure, or when one of its statements
holds a `refused` fragment; otherwise each statement takes effect in order:

- CREATE STAGE IF NOT EXISTS records the stage and its file format (and SNOWFLAKE_SSE as its
  type); CREATE TABLE makes the table exist; INSERT INTO counts its rows.
- ALTER DATASET ... ADD VERSION appends the version; SYSTEM$CREATE_EVALUATION_DATASET makes
  the dataset exist, owned by the session's role.
- CREATE and ALTER CORTEX EXTENSION change the extension (`extensions.py`).

A statement that cannot take effect fails the script there, as Snowflake reports it.
"""

from __future__ import annotations

from collections.abc import Sequence

from snowflake_semantic_tools.domain.model.identifier import SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import ExecResult, ExecutionError, QueryResult
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.sql import Sql
from tests.helpers.snowflake_fake.extensions import apply_extension_statement, quoted_arguments
from tests.helpers.snowflake_fake.world import Sent, SnowflakeWorld

_ROW_COUNT = "SELECT COUNT(*) AS ROW_COUNT FROM "


class FakeExecution(SnowflakeWorld):
    """`ExecutionPort` over the shared account and script."""

    def query(self, sql: Sql, params: object = None) -> QueryResult:
        return self._answer(Sent("query", (str(sql),), params, getattr(self._in_scope, "scope", None)))

    def query_in_context(self, scope: SchemaScope, sql: Sql, params: object = None) -> QueryResult:
        # Runs through `query`, so a test that answers `query` answers both; the log keeps the scope.
        self._in_scope.scope = scope.sql
        try:
            return self.query(sql, params)
        finally:
            self._in_scope.scope = None

    def execute_script(self, statements: Sequence[Sql]) -> ExecResult:
        texts = tuple(str(statement) for statement in statements)
        self.log.append(Sent("script", texts))
        self._check("execute_script")
        if self.execute_results:
            scripted = self.execute_results.pop(0)
            if not scripted.ok:
                return scripted
        else:
            scripted = ExecResult(True)
        refused = next((text for text in texts if self._refusal(text) is not None), None)
        if refused is not None:
            return ExecResult(False, error=ExecutionError(f"recorded refusal: {refused[:60]}"))
        for text in texts:
            try:
                self._take_effect(text)
            except SnowflakePortError as exc:
                return ExecResult(False, error=ExecutionError(str(exc)))
        return scripted

    def try_execute(self, sql: Sql) -> ExecResult:
        return self.execute_script((sql,))

    def _answer(self, sent: Sent) -> QueryResult:
        self.log.append(sent)
        self._check("query")
        if self.query_results:
            answer = self.query_results.pop(0)
            if isinstance(answer, BaseException):
                raise answer
            return answer
        text = sent.statements[0]
        if not self.answer_unscripted:
            raise AssertionError(f"unexpected query: {text}")
        if text.upper().startswith(_ROW_COUNT):
            return QueryResult(("ROW_COUNT",), ((self.table_row_counts.get(text[len(_ROW_COUNT) :], 0),),))
        return QueryResult()

    def _take_effect(self, statement: str) -> None:
        normalized = " ".join(statement.split())
        upper = normalized.upper()
        if upper.startswith("CREATE STAGE IF NOT EXISTS "):
            name = normalized.split()[5]
            marker = "FILE_FORMAT = ("
            index = upper.find(marker)
            self.stage_formats[name] = normalized[index + len(marker) :].rsplit(")", 1)[0] if index >= 0 else ""
            if "SNOWFLAKE_SSE" in upper:
                self.stage_types[name] = "INTERNAL NO CSE"
            self._exists(name)
        elif upper.startswith(("CREATE CORTEX EXTENSION", "ALTER CORTEX EXTENSION")):
            apply_extension_statement(self.extensions, tuple(self.stage_files), normalized, statement)
        elif upper.startswith("CREATE TABLE "):
            self._exists(normalized.split()[2])
        elif upper.startswith("INSERT INTO "):
            self.table_row_counts[normalized.split()[2]] = upper.count("SELECT")
        elif upper.startswith("ALTER DATASET ") and " ADD VERSION " in upper:
            tokens = normalized.split()
            self.dataset_version_names.setdefault(tokens[2], []).append(tokens[5].strip("'"))
        elif "SYSTEM$CREATE_EVALUATION_DATASET" in upper:
            quoted = quoted_arguments(normalized)
            if len(quoted) >= 3 and self.existing is not None:
                self.existing.add(quoted[2])
                # The role that creates a dataset owns it.
                self.dataset_owners.setdefault(quoted[2], self.role)

    def _exists(self, name: str) -> None:
        if self.existing is not None:
            self.existing.add(name)
