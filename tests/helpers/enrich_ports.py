"""Offline enrich ports: relations, values and Cortex answers given up front, and in-memory files."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence

from snowflake_semantic_tools.adapters.dbt.yaml_writer import write_model_updates
from snowflake_semantic_tools.adapters.yaml.view_writer import write_table_synonyms
from snowflake_semantic_tools.domain.enrich import ColumnUpdate, TableSynonymEdit, WarehouseColumn
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.ports.enrich import WrittenFile
from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePortError

Answer = Callable[[str, Mapping[str, object]], object]


class ScriptedEnrich:
    """Answer enrich's reads from dictionaries, recording every call in order.

    Relations are keyed by their SQL name, such as `DB.SCH.ORDERS`. A relation in `failures`
    raises its error from whichever read reaches it first; Cortex raises `cortex_failure`
    when one is set, and otherwise returns what `answer` makes of the prompt and schema.
    """

    def __init__(
        self,
        *,
        columns: Mapping[str, Sequence[tuple[str, str]]] | None = None,
        values: Mapping[str, Mapping[str, Sequence[str]]] | None = None,
        answer: Answer | None = None,
        failures: Mapping[str, SnowflakePortError] | None = None,
        cortex_failure: SnowflakePortError | None = None,
    ) -> None:
        self._columns = {
            relation: tuple(WarehouseColumn(name, data_type) for name, data_type in described)
            for relation, described in (columns or {}).items()
        }
        self._values = {relation: dict(found) for relation, found in (values or {}).items()}
        self._answer = answer or (lambda _prompt, _schema: {})
        self._failures = dict(failures or {})
        self._cortex_failure = cortex_failure
        self.calls: list[tuple[object, ...]] = []
        self.prompts: list[str] = []

    def _fail(self, relation: QualifiedName) -> None:
        if relation.sql in self._failures:
            raise self._failures[relation.sql]

    def relation_columns(self, relation: QualifiedName) -> tuple[WarehouseColumn, ...] | None:
        self.calls.append(("columns", relation.sql))
        self._fail(relation)
        return self._columns.get(relation.sql)

    def distinct_values(
        self, relation: QualifiedName, columns: Sequence[str], limit: int
    ) -> Mapping[str, tuple[str, ...]]:
        self.calls.append(("values", relation.sql, tuple(columns), limit))
        self._fail(relation)
        found = self._values.get(relation.sql, {})
        return {column: tuple(found.get(column, ()))[:limit] for column in columns}

    def complete_json(self, model: str, prompt: str, schema: Mapping[str, object]) -> object:
        self.calls.append(("cortex", model))
        self.prompts.append(prompt)
        if self._cortex_failure is not None:
            raise self._cortex_failure
        return self._answer(prompt, schema)


class InMemoryFiles:
    """Project files held as text by path, edited by the production writers, written in memory."""

    def __init__(self, texts: Mapping[str, str] | None = None) -> None:
        self.texts = dict(texts or {})
        self.written: list[str] = []

    def read(self, path: str) -> str | None:
        return self.texts.get(path)

    def edit_models(self, text: str | None, path: str, updates: Mapping[str, Sequence[ColumnUpdate]]) -> WrittenFile:
        return write_model_updates(text, path, updates)

    def edit_views(self, text: str, path: str, edits: Sequence[TableSynonymEdit]) -> WrittenFile:
        return write_table_synonyms(text, path, edits)

    def write(self, path: str, text: str) -> None:
        self.texts[path] = text
        self.written.append(path)
