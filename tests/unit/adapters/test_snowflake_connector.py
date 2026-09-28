from __future__ import annotations

from typing import Sequence

import pytest

from snowflake_semantic_tools.adapters.snowflake.connector import SnowflakeConnector
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import ExecResult
from snowflake_semantic_tools.domain.ports.snowflake import SnowflakePortError, StagedFileMetadata, StageObservation


class StubSnowflakeConnector(SnowflakeConnector):
    def __init__(self, rows_by_prefix: dict[str, tuple[dict[str, object], ...]]) -> None:
        self._rows_by_prefix = rows_by_prefix

    def _dict_rows(self, sql: str) -> tuple[dict[str, object], ...]:
        for prefix, rows in self._rows_by_prefix.items():
            if sql.startswith(prefix):
                return rows
        return ()


class RecordingUploadConnector(SnowflakeConnector):
    def __init__(self) -> None:
        self.statements: tuple[str, ...] = ()

    def execute_script(self, statements: Sequence[str]) -> ExecResult:
        self.statements = tuple(statements)
        return ExecResult(True)


def test_dataset_exists_requires_an_exact_show_result() -> None:
    connector = StubSnowflakeConnector(
        {
            "SHOW DATASETS": (
                {"name": "EVAL_DATASET_OLD", "database_name": "DB", "schema_name": "SCH"},
                {"name": "EVAL_DATASET", "database_name": "DB", "schema_name": "OTHER"},
            )
        }
    )
    name = QualifiedName.parse("DB.SCH.EVAL_DATASET")

    assert not connector.dataset_exists(name)
    assert not connector.object_exists("DATASET", name)


def test_dataset_exists_accepts_only_the_exact_database_schema_and_name() -> None:
    connector = StubSnowflakeConnector(
        {"SHOW DATASETS": ({"name": "eval_dataset", "database_name": "db", "schema_name": "sch"},)}
    )

    assert connector.dataset_exists(QualifiedName.parse("DB.SCH.EVAL_DATASET"))


def test_stage_observation_preserves_the_exact_file_format() -> None:
    connector = StubSnowflakeConnector(
        {
            "SHOW STAGES": ({"name": "EVAL_CONFIGS", "database_name": "DB", "schema_name": "SCH"},),
            "DESCRIBE STAGE": (
                {"property": "TYPE", "property_value": "CSV"},
                {"property": "FIELD_DELIMITER", "property_value": "NONE"},
                {"property": "RECORD_DELIMITER", "property_value": "\\n"},
                {"property": "SKIP_HEADER", "property_value": 0},
                {"property": "FIELD_OPTIONALLY_ENCLOSED_BY", "property_value": "NONE"},
                {"property": "ESCAPE_UNENCLOSED_FIELD", "property_value": "NONE"},
            ),
        }
    )

    assert connector.observe_stage(QualifiedName.parse("DB.SCH.EVAL_CONFIGS")) == StageObservation(
        True,
        "TYPE=CSV FIELD_DELIMITER=NONE RECORD_DELIMITER=\\n SKIP_HEADER=0 "
        "FIELD_OPTIONALLY_ENCLOSED_BY=NONE ESCAPE_UNENCLOSED_FIELD=NONE",
    )


def test_staged_file_observation_is_exact_and_returns_metadata() -> None:
    exact_path = "@DB.SCH.EVAL_CONFIGS/agent/config.yaml"
    connector = StubSnowflakeConnector(
        {
            "LIST": (
                {
                    "name": "DB.SCH.EVAL_CONFIGS/agent/config.yaml.bak",
                    "size": 9,
                    "md5": "wrong",
                    "last_modified": "later",
                },
                {
                    "name": "DB.SCH.EVAL_CONFIGS/agent/config.yaml",
                    "size": "42",
                    "md5": "abc123",
                    "last_modified": "Mon, 28 Sep 2026 00:00:00 GMT",
                },
            )
        }
    )

    assert connector.observe_staged_file(exact_path) == StagedFileMetadata(
        exact_path,
        "DB.SCH.EVAL_CONFIGS/agent/config.yaml",
        42,
        "abc123",
        "Mon, 28 Sep 2026 00:00:00 GMT",
    )
    assert connector.stage_file_exists(exact_path)


def test_staged_file_observation_accepts_internal_stage_name_shape_only_for_exact_path() -> None:
    exact_path = "@DB.SCH.EVAL_CONFIGS/agent/config.yaml"
    connector = StubSnowflakeConnector(
        {
            "LIST": (
                {"name": "eval_configs/agent/config.yaml.bak", "size": 9, "md5": "wrong"},
                {"name": "other_stage/agent/config.yaml", "size": 9, "md5": "wrong"},
                {"name": "eval_configs/agent/config.yaml", "size": 42, "md5": "abc123"},
            )
        }
    )

    assert connector.observe_staged_file(exact_path) == StagedFileMetadata(
        exact_path,
        "DB.SCH.EVAL_CONFIGS/agent/config.yaml",
        42,
        "abc123",
    )


@pytest.mark.parametrize(
    "stage_path",
    (
        "DB.SCH.STAGE/path/file.yaml",
        "@DB.SCH.STAGE",
        "@DB.SCH.STAGE/../file.yaml",
        "@DB.SCH.STAGE/path/./file.yaml",
        "@DB.SCH.STAGE/path//file.yaml",
        "@DB.SCH.STAGE/path\\file.yaml",
        "@DB.SCH.STAGE/path/'file.yaml",
        '@DB.SCH.STAGE/path/"file.yaml',
        "@DB.SCH.STAGE/path/*.yaml",
        "@DB.SCH.STAGE/path/file?.yaml",
        "@DB.SCH.STAGE/path/file[0].yaml",
        "@DB.SCH.STAGE/path/file name.yaml",
        "@DB.SCH.STAGE/path/file\n.yaml",
    ),
)
def test_stage_file_operations_reject_unsafe_paths(stage_path: str) -> None:
    connector = StubSnowflakeConnector({})

    with pytest.raises(SnowflakePortError, match="safe segments"):
        connector.observe_staged_file(stage_path)


def test_upload_rejects_unsafe_paths_before_touching_the_file_system() -> None:
    connector = RecordingUploadConnector()

    with pytest.raises(SnowflakePortError, match="safe segments"):
        connector.upload("@DB.SCH.STAGE/../config.yaml", b"content")
    assert connector.statements == ()


def test_upload_accepts_safe_arbitrary_basename_and_disables_compression() -> None:
    connector = RecordingUploadConnector()

    result = connector.upload("@DB.SCH.STAGE/evals/agent/config-v2.yml", b"content")
    assert result is None
    statement = connector.statements[0]
    assert "config-v2.yml' @DB.SCH.STAGE/evals/agent/" in statement
    assert statement.endswith("OVERWRITE=TRUE AUTO_COMPRESS=FALSE")
