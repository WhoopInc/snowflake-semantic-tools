from __future__ import annotations

from datetime import datetime, timedelta, timezone
from threading import RLock
from typing import Sequence

import pytest

from snowflake_semantic_tools.adapters.clock import SystemClock
from snowflake_semantic_tools.adapters.snowflake.connector import SnowflakeConnector
from snowflake_semantic_tools.domain.model.identifier import QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import ExecResult, QueryResult
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


def test_extension_observation_parses_show_rows_by_name() -> None:
    connector = StubSnowflakeConnector(
        {
            "SHOW CORTEX EXTENSIONS": (
                {"name": "OTHER", "type": "skill", "comment": "x", "owner": "R"},
                {
                    "name": "GROWTH_ANALYTICS",
                    "type": "plugin",
                    "comment": "Growth",
                    "owner": "R",
                    "effective_version": "VERSION$12",
                    "latest_certified_version": "",
                },
            ),
            "SHOW VERSIONS IN CORTEX EXTENSION": (
                {
                    "name": "version$12",
                    "alias": "SST_ABC",
                    "location_uri": "snow://cortex_extension/DB.S.GROWTH_ANALYTICS/versions/version$12/",
                    "is_default": "true",
                    "certification_status": "CERTIFIED",
                },
                {"name": "VERSION$1", "alias": "", "location_uri": "", "is_default": "false"},
                {"name": None},
            ),
            "SHOW STAGES": ({"name": "SKILL_BUNDLES", "type": "INTERNAL NO CSE"},),
        }
    )
    name = QualifiedName.parse("DB.S.GROWTH_ANALYTICS")
    observed = connector.observe_extension(name)
    assert observed is not None
    assert (observed.extension_type, observed.comment, observed.effective_version) == ("PLUGIN", "Growth", "VERSION$12")
    assert observed.latest_certified_version is None
    assert connector.observe_extension(QualifiedName.parse("DB.S.ABSENT")) is None
    versions = connector.extension_versions(name)
    assert [(item.name, item.alias, item.is_default, item.certification_status) for item in versions] == [
        ("VERSION$12", "SST_ABC", True, "CERTIFIED"),
        ("VERSION$1", None, False, None),
    ]
    assert connector.stage_type(QualifiedName.parse("DB.S.SKILL_BUNDLES")) == "INTERNAL NO CSE"
    assert connector.stage_type(QualifiedName.parse("DB.S.ABSENT")) is None


def test_list_location_returns_paths_relative_to_a_prefix_or_version() -> None:
    connector = StubSnowflakeConnector(
        {
            "LIST '@DB.S.SKILL_BUNDLES/month-close/SST_ABC/'": (
                {"name": "skill_bundles/month-close/SST_ABC/skills/month-close/SKILL.md"},
                {"name": "skill_bundles/other/SST_ABC/stray.md"},
                {"name": "no-separator"},
            ),
            "LIST 'snow://": (
                {"name": "/versions/version$12/skills/a/SKILL.md"},
                {"name": "/versions/version$1/skills/a/SKILL.md"},
            ),
        }
    )
    assert connector.list_location("@DB.S.SKILL_BUNDLES/month-close/SST_ABC/") == ("skills/month-close/SKILL.md",)
    assert connector.list_location("snow://cortex_extension/DB.S.GROWTH/versions/version$12/") == ("skills/a/SKILL.md",)
    for invalid in (
        "@DB.S.SKILL_BUNDLES/month-close",
        "snow://cortex_extension/DB.S.GROWTH/versions/version$12/extra/",
        "snow://cortex_extension/not a name/versions/live/",
        "snow://elsewhere/x/",
    ):
        with pytest.raises(SnowflakePortError):
            connector.list_location(invalid)


def test_upload_accepts_a_live_extension_version_path() -> None:
    connector = RecordingUploadConnector()
    connector.upload("snow://cortex_extension/DB.S.KIT/versions/live/skills/a/SKILL.md", b"x")
    assert connector.statements[0].endswith(
        " 'snow://cortex_extension/DB.S.KIT/versions/live/skills/a/' OVERWRITE=TRUE AUTO_COMPRESS=FALSE"
    )
    with pytest.raises(SnowflakePortError):
        connector.upload("snow://cortex_extension/DB.S.KIT/versions/live/skills/../SKILL.md", b"x")
    with pytest.raises(SnowflakePortError):
        connector.upload("snow://cortex_extension/DB.S.KIT/versions/version$1/", b"x")


class StateRowsConnector(SnowflakeConnector):
    """Returns APPLIED_AT the way the driver does: a datetime from TIMESTAMP_TZ."""

    def __init__(self, applied_at: tuple[object, ...]) -> None:
        self._applied_at = applied_at

    def object_exists(self, object_type: str, qualified_name: QualifiedName) -> bool:
        return True

    def _dict_rows(self, sql: str) -> tuple[dict[str, object], ...]:
        return ({"name": "COMPONENT_FINGERPRINTS"}, {"name": "PHYSICAL_RESOURCES"})

    def query(self, sql: str, params: object = None) -> QueryResult:  # type: ignore[override]
        return QueryResult(
            (),
            tuple(
                (
                    f"skill:s{index}",
                    "f" * 64,
                    "DB.S.X",
                    "m" * 64,
                    "abc1234",
                    value,
                    "run",
                    "applied",
                    "f" * 64,
                    "{}",
                    "[]",
                )
                for index, value in enumerate(self._applied_at)
            ),
        )


def test_state_applied_at_reads_back_in_the_clock_form_it_was_written_in() -> None:
    written = SystemClock().now_iso()
    connector = StateRowsConnector(
        (
            datetime.fromisoformat(written.replace("Z", "+00:00")),
            datetime(2026, 9, 29, 4, 56, 32, 77209, tzinfo=timezone(timedelta(hours=-7))),
            datetime(2026, 9, 29, 11, 56, 32),
            "2026-09-29T11:56:32Z",
        )
    )
    state = connector.read_state(QualifiedName.parse("DB.S.SST_STATE"), "verify")
    assert state is not None
    assert [state[f"skill:s{index}"].applied_at for index in range(4)] == [
        written,
        "2026-09-29T11:56:32.077209Z",
        "2026-09-29T11:56:32Z",
        "2026-09-29T11:56:32Z",
    ]


def test_routine_observation_skips_builtins_and_reads_catalog_and_description() -> None:
    marker = "[sst:" + "a" * 64 + ":" + "b" * 64 + "]"
    connector = StubSnowflakeConnector(
        {
            "SHOW FUNCTIONS": (
                {"name": "!=", "schema_name": "", "catalog_name": "", "is_builtin": "Y", "description": "x"},
                {"name": "SYSTEM$CLASSIFY", "schema_name": None, "catalog_name": None, "is_builtin": "Y"},
                {
                    "name": "SUPPLY_COST",
                    "schema_name": "S",
                    "catalog_name": "DB",
                    "is_builtin": "N",
                    "description": f"{marker} Monthly supply cost",
                    "created_on": "then",
                },
            ),
        }
    )
    rows = connector.show_objects("FUNCTION", SchemaScope.from_qualified_name(QualifiedName.parse("DB.S.X")))
    assert [(row.qualified_name.sql, row.comment, row.object_type) for row in rows] == [
        ("DB.S.SUPPLY_COST", f"{marker} Monthly supply cost", "FUNCTION")
    ]
    observed = connector.describe_marker(QualifiedName.parse("DB.S.SUPPLY_COST"), "FUNCTION")
    assert observed is not None and observed.manifest_id == "a" * 64


def test_connection_prompts_go_to_stderr_so_json_stdout_stays_one_envelope(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def connect(**params: object) -> object:
        print("Initiating login request with your identity provider...")
        return object()

    monkeypatch.setattr("snowflake_semantic_tools.adapters.snowflake.connector.snowflake.connector.connect", connect)
    SnowflakeConnector({"account": "a"})
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "Initiating login request" in captured.err


class _ConnectorFailure(Exception):
    def __init__(self, message: str, sqlstate: str | None = None) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate
        self.errno = 250001


def test_a_failed_connection_names_the_account(monkeypatch: pytest.MonkeyPatch) -> None:
    def connect(**params: object) -> object:
        raise _ConnectorFailure("Could not connect to Snowflake backend", "08001")

    monkeypatch.setattr("snowflake_semantic_tools.adapters.snowflake.connector.snowflake.connector.connect", connect)
    with pytest.raises(SnowflakePortError) as raised:
        SnowflakeConnector({"account": "acme-prod"})
    diagnostic = raised.value.diagnostic
    assert diagnostic is not None and diagnostic.code == "SST-PRT001"
    assert diagnostic.message == "connection to acme-prod failed: Could not connect to Snowflake backend"
    assert raised.value.errno == 250001


@pytest.mark.parametrize(
    ("message", "sqlstate", "code"),
    [
        ("Statement reached its statement or warehouse timeout", None, "SST-PRT003"),
        ("network drop", "08006", "SST-PRT003"),
        ("SQL access control error: Insufficient privileges to operate on schema 'S'", "42501", "SST-PRT004"),
        ("Invalid credentials", "28000", "SST-PRT004"),
        ("SQL compilation error", "42000", None),
    ],
)
def test_query_failures_carry_the_diagnostic_a_command_reports(
    message: str, sqlstate: str | None, code: str | None
) -> None:
    class FailingConnector(SnowflakeConnector):
        def __init__(self) -> None:
            self._lock = RLock()

            class Connection:
                def cursor(self, *args: object) -> object:
                    raise _ConnectorFailure(message, sqlstate)

            self._connection = Connection()

    with pytest.raises(SnowflakePortError) as raised:
        FailingConnector().query("SELECT 1")
    diagnostic = raised.value.diagnostic
    assert (diagnostic.code if diagnostic is not None else None) == code
