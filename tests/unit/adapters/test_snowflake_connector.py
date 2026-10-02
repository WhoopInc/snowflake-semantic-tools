from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timedelta, timezone
from threading import RLock

import pytest
from snowflake.connector.errors import Error as DriverError
from snowflake.connector.errors import OperationalError, ProgrammingError

from snowflake_semantic_tools.adapters.clock import SystemClock
from snowflake_semantic_tools.adapters.snowflake.connector import SnowflakeConnector
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, SchemaScope
from snowflake_semantic_tools.domain.model.lifecycle import ExecResult, QueryResult
from snowflake_semantic_tools.domain.ports.snowflake.catalog import StageObservation
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.stage import StagedFileMetadata
from snowflake_semantic_tools.domain.sql import Sql, sql
from snowflake_semantic_tools.domain.state import AppliedEntry
from snowflake_semantic_tools.domain.state.lock import LockClaim, StateWrite
from tests.helpers.sql_values import texts


class StubSnowflakeConnector(SnowflakeConnector):
    def __init__(self, rows_by_prefix: dict[str, tuple[dict[str, object], ...]]) -> None:
        self._rows_by_prefix = rows_by_prefix

    def _dict_rows(self, sql: Sql) -> tuple[dict[str, object], ...]:
        for prefix, rows in self._rows_by_prefix.items():
            if str(sql).startswith(prefix):
                return rows
        return ()


class RecordingUploadConnector(SnowflakeConnector):
    def __init__(self) -> None:
        self.statements: tuple[str, ...] = ()

    def execute_script(self, statements: Sequence[Sql]) -> ExecResult:
        self.statements = texts(statements)
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

    connector.upload("@DB.SCH.STAGE/evals/agent/config-v2.yml", b"content")
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

    def _dict_rows(self, sql: Sql) -> tuple[dict[str, object], ...]:
        return ({"name": "COMPONENT_FINGERPRINTS"}, {"name": "PHYSICAL_RESOURCES"})

    def query(self, sql: Sql, params: object = None) -> QueryResult:
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

    monkeypatch.setattr(
        "snowflake_semantic_tools.adapters.snowflake.connector.session.snowflake.connector.connect", connect
    )
    SnowflakeConnector({"account": "a"})
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "Initiating login request" in captured.err


class _ConnectorFailure(DriverError):
    def __init__(self, message: str, sqlstate: str | None = None) -> None:
        super().__init__(message)
        # A driver error may carry no SQLSTATE at all, which the stub's `str` cannot express.
        self.sqlstate = sqlstate  # type: ignore[assignment]
        self.errno = 250001


def test_a_failed_connection_names_the_account(monkeypatch: pytest.MonkeyPatch) -> None:
    def connect(**params: object) -> object:
        raise _ConnectorFailure("Could not connect to Snowflake backend", "08001")

    monkeypatch.setattr(
        "snowflake_semantic_tools.adapters.snowflake.connector.session.snowflake.connector.connect", connect
    )
    with pytest.raises(SnowflakePortError) as raised:
        SnowflakeConnector({"account": "acme-prod"})
    diagnostic = raised.value.diagnostic
    assert diagnostic is not None and diagnostic.code == "SST-PRT001"
    assert diagnostic.message == "could not connect to acme-prod: Could not connect to Snowflake backend"
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

            self._connection = Connection()  # type: ignore[assignment]  # a double, not a driver connection

    with pytest.raises(SnowflakePortError) as raised:
        FailingConnector().query(sql("SELECT 1"))
    diagnostic = raised.value.diagnostic
    assert (diagnostic.code if diagnostic is not None else None) == code


class _Session:
    """A driver session double: its own single cursor, recording statements, failing where scripted."""

    def __init__(self, failures: Mapping[str, BaseException] | None = None) -> None:
        self.failures = dict(failures or {})
        self.executed: list[str] = []
        self.description: tuple[tuple[str], ...] | None = None
        self.sfqid = "query-id"
        self.rowcount = 1

    def cursor(self, *args: object) -> _Session:
        return self

    def execute(self, sql: str, params: object = None) -> None:
        self.executed.append(sql)
        failure = next((error for prefix, error in self.failures.items() if sql.startswith(prefix)), None)
        if failure is not None:
            raise failure

    def fetchall(self) -> list[object]:
        return []

    def close(self) -> None:
        pass


class SessionConnector(SnowflakeConnector):
    def __init__(self, session: _Session, scoped: _Session | None = None) -> None:
        self._lock = RLock()
        self._connection = session  # type: ignore[assignment]  # a double, not a driver connection
        self._scoped_double = scoped or session
        self.siblings: list[SessionConnector] = []

    def sibling(self) -> SessionConnector:
        opened = SessionConnector(self._scoped_double)
        self.siblings.append(opened)
        return opened


STATE_TABLE = QualifiedName.parse("DB.S.SST_STATE")
SCOPE = SchemaScope.from_qualified_name(STATE_TABLE)
ENTRY = AppliedEntry("f" * 64, "DB.S.V", "2026-09-29T00:00:00Z", "run", "applied", "d" * 64, "m" * 64)
WRITE = StateWrite({"k": ENTRY})
CLAIM = LockClaim("run", "ROLE", "host")
DRIVER_CALLS: tuple[tuple[str, str, Callable[[SnowflakeConnector], object]], ...] = (
    ("query", "SELECT 1", lambda port: port.query(sql("SELECT 1"))),
    ("query_in_context", "SELECT 1", lambda port: port.query_in_context(SCOPE, sql("SELECT 1"))),
    ("show_objects", "SHOW TABLES", lambda port: port.show_objects("TABLE", SCOPE)),
    ("write_state", "MERGE INTO", lambda port: port.write_state(STATE_TABLE, "dev", "m" * 64, WRITE)),
    ("retire_state", "DELETE FROM", lambda port: port.write_state(STATE_TABLE, "dev", "m", StateWrite({}, ("k",)))),
    (
        "acquire_run_lock",
        "MERGE INTO",
        lambda port: port.acquire_run_lock(STATE_TABLE, "dev", CLAIM, break_stale=False),
    ),
    ("extend_run_lock", "UPDATE", lambda port: port.extend_run_lock(STATE_TABLE, "dev", CLAIM)),
    ("release_run_lock", "DELETE FROM", lambda port: port.release_run_lock(STATE_TABLE, "dev", "run")),
)


@pytest.mark.parametrize(
    ("prefix", "call"), [item[1:] for item in DRIVER_CALLS], ids=[item[0] for item in DRIVER_CALLS]
)
def test_a_programming_error_in_a_driver_call_propagates_rather_than_passing_as_a_snowflake_failure(
    prefix: str, call: Callable[[SnowflakeConnector], object]
) -> None:
    session = _Session({prefix: TypeError("unsupported parameter type: Decimal")})
    with pytest.raises(TypeError, match="unsupported parameter type"):
        call(SessionConnector(session))


@pytest.mark.parametrize(
    "error",
    [
        ProgrammingError(msg="SQL compilation error", errno=1003, sqlstate="42000"),
        # The driver lets transport failures (its vendored `requests` errors, socket timeouts:
        # all OSError) escape from a large result's chunk download.
        TimeoutError("The read operation timed out"),
    ],
    ids=["driver-error", "transport-error"],
)
@pytest.mark.parametrize(
    ("prefix", "call"), [item[1:] for item in DRIVER_CALLS], ids=[item[0] for item in DRIVER_CALLS]
)
def test_a_driver_or_transport_failure_is_still_reported_as_a_port_error(
    prefix: str, call: Callable[[SnowflakeConnector], object], error: Exception
) -> None:
    with pytest.raises(SnowflakePortError) as raised:
        call(SessionConnector(_Session({prefix: error})))
    assert str(raised.value) == str(error) and raised.value.__cause__ is error


def test_a_programming_error_mid_state_write_still_rolls_the_transaction_back() -> None:
    session = _Session({"MERGE INTO": TypeError("unsupported parameter type: Decimal")})
    with pytest.raises(TypeError):
        SessionConnector(session).write_state(STATE_TABLE, "dev", "m" * 64, WRITE)
    assert session.executed[-3:] == ["BEGIN", session.executed[-2], "ROLLBACK"]
    assert session.executed[-2].startswith("MERGE INTO DB.S.SST_STATE AS target")


def test_a_failed_rollback_keeps_the_error_that_aborted_the_state_write() -> None:
    aborted = ProgrammingError(msg="Numeric value 'x' is not recognized", errno=100038, sqlstate="22018")
    session = _Session({"MERGE INTO": aborted, "ROLLBACK": OperationalError(msg="Connection is closed")})
    with pytest.raises(SnowflakePortError) as raised:
        SessionConnector(session).write_state(STATE_TABLE, "dev", "m" * 64, WRITE)
    assert str(raised.value) == str(aborted) == "100038 (22018): Numeric value 'x' is not recognized"
    assert (raised.value.sqlstate, raised.value.errno) == ("22018", 100038)
    assert raised.value.__cause__ is aborted
    assert session.executed[-1] == "ROLLBACK"
    assert getattr(aborted, "__notes__", []) == ["ROLLBACK also failed: Connection is closed"]


def test_query_in_context_runs_on_a_scoped_session_and_never_changes_the_main_one() -> None:
    main, scoped = _Session(), _Session()
    connector = SessionConnector(main, scoped)
    agents = SchemaScope(Identifier.parse("AGENTS"), Identifier.parse("EVALS"))
    connector.query_in_context(agents, sql("SELECT 1"))
    connector.query(sql("SELECT 2"))
    connector.query_in_context(agents, sql("SELECT 3"))
    assert main.executed == ["SELECT 2"]
    assert scoped.executed == [
        "USE DATABASE AGENTS",
        "USE SCHEMA AGENTS.EVALS",
        "SELECT 1",
        "USE DATABASE AGENTS",
        "USE SCHEMA AGENTS.EVALS",
        "SELECT 3",
    ]
    assert len(connector.siblings) == 1


# What a driver or key library might echo on a failure: none of it may reach a diagnostic.
_LEAKY = (
    "250001 (08001): Failed to connect: password=hunter2 token: ey.J9 passphrase='open sesame' "
    'private_key_file_pwd="pw with spaces" while reading /var/lib/sst-keys/rsa_key.p8 '
    "and C:\\keys\\snowflake.pem"
)
_SECRETS = ("hunter2", "ey.J9", "open sesame", "pw with spaces", "rsa_key.p8", "snowflake.pem", "sst-keys")


def test_a_failed_connection_reports_no_credential_and_chains_no_driver_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def connect(**params: object) -> object:
        raise _ConnectorFailure(_LEAKY, "08001")

    monkeypatch.setattr(
        "snowflake_semantic_tools.adapters.snowflake.connector.session.snowflake.connector.connect", connect
    )
    with pytest.raises(SnowflakePortError) as raised:
        SnowflakeConnector({"account": "acme", "password": "hunter2", "private_key_file": "/tmp/rsa_key.p8"})
    error = raised.value
    diagnostic = error.diagnostic
    assert diagnostic is not None and diagnostic.code == "SST-PRT001"
    shown = (str(error), diagnostic.message, repr(error.args))
    assert not [secret for secret in _SECRETS for text in shown if secret in text]
    assert str(error).startswith("250001 (08001): Failed to connect: password=<redacted> token: <redacted>")
    assert (error.errno, error.sqlstate) == (250001, "08001")
    assert error.__cause__ is None and error.__context__ is None


def test_a_failed_statement_reports_no_credential() -> None:
    leaky = ProgrammingError(msg=_LEAKY, errno=1003, sqlstate="42000")
    with pytest.raises(SnowflakePortError) as raised:
        SessionConnector(_Session({"SELECT": leaky})).query(sql("SELECT 1"))
    assert not [secret for secret in _SECRETS if secret in str(raised.value)]
    assert (raised.value.errno, raised.value.sqlstate) == (1003, "42000")
    result = SessionConnector(_Session({"CREATE": leaky})).execute_script((sql("CREATE VIEW V AS SELECT 1"),))
    assert result.error is not None and not [secret for secret in _SECRETS if secret in result.error.message]
    assert (result.error.errno, result.error.sqlstate) == (1003, "42000")


def test_a_message_without_credentials_is_kept_as_written() -> None:
    plain = ProgrammingError(msg="Object 'DB.S.V' does not exist or not authorized.", errno=2003, sqlstate="02000")
    with pytest.raises(SnowflakePortError) as raised:
        SessionConnector(_Session({"SELECT": plain})).query(sql("SELECT 1"))
    assert str(raised.value) == str(plain)


class _GetSession(_Session):
    """The driver's GET, recorded: a prefix match that downloads every staged file it matches.

    Each match is written into the `file://` target and reported as one row of the shape the
    connector's dictionary cursor returns for GET: `file`, `size`, `status`, `message`.
    """

    def __init__(self, staged: Mapping[str, bytes], status: str = "DOWNLOADED") -> None:
        super().__init__()
        self.staged = dict(staged)
        self.status = status
        self.rows: list[dict[str, object]] = []

    def execute(self, sql: str, params: object = None) -> None:
        super().execute(sql, params)
        source, target = (part.strip("'") for part in sql.removeprefix("GET ").split(" ", 1))
        directory = target.removeprefix("file://")
        self.rows = []
        for path, content in sorted(self.staged.items()):
            if path.startswith(source):
                name = path.rsplit("/", 1)[-1]
                with open(f"{directory}/{name}", "wb") as handle:
                    handle.write(content)
                self.rows.append({"file": name, "size": len(content), "status": self.status, "message": ""})

    def fetchall(self) -> list[object]:
        return list(self.rows)


def test_a_staged_eval_config_is_read_from_the_file_get_reports_not_a_prefix_sibling() -> None:
    config = "@DB.S.EVAL_CONFIGS/sales/abcdef0.yaml"
    session = _GetSession({config: b"evaluation: {}\n", f"{config}.bak": b"stale\n"})
    assert SessionConnector(session).read_staged_file(config) == b"evaluation: {}\n"
    assert session.executed[0].startswith(f"GET '{config}' 'file://")

    only_sibling = _GetSession({f"{config}.bak": b"stale\n"})
    assert SessionConnector(only_sibling).read_staged_file(config) is None


def test_a_staged_file_get_does_not_report_downloaded_fails_closed() -> None:
    config = "@DB.S.EVAL_CONFIGS/sales/abcdef0.yaml"
    session = _GetSession({config: b"evaluation: {}\n"}, status="FAILED")
    with pytest.raises(SnowflakePortError, match="reported FAILED"):
        SessionConnector(session).read_staged_file(config)


def test_show_row_reads_the_named_object_as_text() -> None:
    connector = StubSnowflakeConnector(
        {"SHOW AGENTS LIKE": ({"name": "SALES_AGENT_OLD"}, {"name": "SALES_AGENT", "OWNER": "ADMIN", "comment": None})}
    )
    row = connector.show_row("AGENT", QualifiedName.parse("DB.S.SALES_AGENT"))
    assert row == {"name": "SALES_AGENT", "owner": "ADMIN", "comment": ""}
    assert connector.show_row("AGENT", QualifiedName.parse("DB.S.OTHER")) is None


def test_describe_properties_reads_property_rows_or_a_single_row() -> None:
    present: dict[str, tuple[dict[str, object], ...]] = {
        "SHOW AGENTS LIKE": ({"name": "SALES_AGENT", "database_name": "DB", "schema_name": "S"},)
    }
    properties = StubSnowflakeConnector(
        {
            **present,
            "DESCRIBE AGENT": ({"name": "agent_spec", "value": "{}"}, {"property": "ALIASES", "property_value": None}),
        }
    )
    name = QualifiedName.parse("DB.S.SALES_AGENT")
    assert properties.describe_properties("AGENT", name) == {"agent_spec": "{}", "aliases": ""}
    single = StubSnowflakeConnector({**present, "DESCRIBE AGENT": ({"name": "SALES_AGENT", "agent_spec": "{}"},)})
    assert single.describe_properties("AGENT", name) == {"name": "SALES_AGENT", "agent_spec": "{}"}
    assert StubSnowflakeConnector({}).describe_properties("AGENT", name) is None


def test_object_parameter_reads_one_warehouse_parameter() -> None:
    connector = StubSnowflakeConnector(
        {
            "SHOW PARAMETERS LIKE": (
                {"key": "OTHER", "value": "1"},
                {"key": "STATEMENT_TIMEOUT_IN_SECONDS", "value": 300},
            )
        }
    )
    assert connector.object_parameter("warehouse", "WH", "STATEMENT_TIMEOUT_IN_SECONDS") == "300"
    assert StubSnowflakeConnector({}).object_parameter("WAREHOUSE", "WH", "STATEMENT_TIMEOUT_IN_SECONDS") is None
    with pytest.raises(SnowflakePortError, match="unsupported parameter object type"):
        connector.object_parameter("DATABASE", "DB", "DATA_RETENTION_TIME_IN_DAYS")
