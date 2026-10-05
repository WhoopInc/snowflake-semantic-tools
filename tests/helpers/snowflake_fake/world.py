"""What the Snowflake fake holds: the account it models, the calls it was sent, what is scripted.

Every role of the fake (`catalog`, `execution`, `stage`, `profile_registry`, `state`,
`preflight`) reads and writes this one world, so a statement one role runs is what another
role then observes, as on one Snowflake session.

- The account: objects SHOW lists by (type, schema), grants, ownership markers, GET_DDL
  definitions, what exists, stages and their files, extensions, the profile registry, the
  state table and its run locks. A test sets these to stage what Snowflake already holds.
- The log: `log`, every call that reached the session, in order. `queries`, `scripts`,
  and `uploads` are read-only views of it by kind.
- The script: `execute_results` and `query_results` answer the next script or query, in
  order; `refused` statement fragments fail any script that sends one; `fail` arms a
  method to raise. When nothing is scripted, a call answers from the account.
"""

from __future__ import annotations

import hashlib
import threading
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import ExecResult, GrantRow, OwnershipMarker, QueryResult, ShowRow
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.stage import StagedFileMetadata
from snowflake_semantic_tools.domain.state import AppliedEntry
from snowflake_semantic_tools.domain.state.lock import StateWrite
from tests.helpers.snowflake_fake.run_locks import InMemoryRunLocks

# MEASURED: Cortex agent evaluation adds this version to a dataset it runs against. It is
# not SST's, and SST must never drop it.
SYSTEM_DATASET_VERSION = "SYSTEM_AI_OBS_CORTEX_AGENT_DATASET_VERSION_DO_NOT_DELETE"


@dataclass
class PreflightAnswers:
    """What the preflight reads answer; by default everything exists and nothing is locked.

    A test fills the sets it rehearses. Names are compared as `QualifiedName.sql` and
    `SchemaScope.sql` spell them; databases and warehouses folded.
    """

    missing_databases: set[str] = field(default_factory=set)
    missing_schemas: set[str] = field(default_factory=set)
    missing_relations: set[str] = field(default_factory=set)
    missing_warehouses: set[str] = field(default_factory=set)
    lacking: dict[str, tuple[str, ...]] = field(default_factory=dict)
    # What the session's primary role lacks by itself and through its hierarchy, by schema.
    role_lacking: dict[str, tuple[str, ...]] = field(default_factory=dict)
    locked: dict[str, tuple[QualifiedName, ...]] = field(default_factory=dict)
    references: dict[str, tuple[QualifiedName, ...]] = field(default_factory=dict)


@dataclass(frozen=True)
class Sent:
    """One call that reached the session: a query, a script, or an upload.

    `statements` holds a query's one statement, a script's statements, or an upload's stage
    path; `scope` is the schema a `query_in_context` ran in.
    """

    kind: str
    statements: tuple[str, ...]
    params: object = None
    scope: str | None = None
    content: bytes = b""


@dataclass
class _Armed:
    error: BaseException
    remaining: int | None


class SnowflakeWorld:
    """The account, the log, and the script the fake's roles share; see the module docstring."""

    def __init__(
        self,
        *,
        objects: Mapping[tuple[str, str], tuple[ShowRow, ...]] | None = None,
        grants: Mapping[str, tuple[GrantRow, ...]] | None = None,
        markers: Mapping[str, OwnershipMarker | None] | None = None,
        definitions: Mapping[str, str] | None = None,
        existing: tuple[str, ...] | None = None,
        state: Mapping[str, AppliedEntry] | None = None,
        role: str = "TEST_ROLE",
        account_locator: str = "TEST_ACCOUNT",
        stage_formats: Mapping[str, str] | None = None,
        stage_files: tuple[str, ...] = (),
        staged_file_metadata: Mapping[str, StagedFileMetadata] | None = None,
        staged_file_contents: Mapping[str, bytes] | None = None,
        agent_versions: Mapping[tuple[str, str], str] | None = None,
        execute_results: tuple[ExecResult, ...] = (),
        query_results: tuple[QueryResult | BaseException, ...] = (),
    ) -> None:
        # SHOW's rows by (object type upper-cased, `SchemaScope.sql`); `show` files rows here.
        self.objects: dict[tuple[str, str], tuple[ShowRow, ...]] = dict(objects or {})
        self.grants: dict[str, tuple[GrantRow, ...]] = dict(grants or {})
        self.markers: dict[str, OwnershipMarker | None] = dict(markers or {})
        # The type of each object named here, by qualified name: a marker read under another
        # type finds nothing, as SHOW <type> LIKE lists no object of a different type.
        self.object_types: dict[str, str] = {}
        # GET_DDL's answer by qualified name; an object missing here has no readable definition.
        self.definitions: dict[str, str] = dict(definitions or {})
        # The qualified names that exist; None models an account where every TABLE OR VIEW does.
        self.existing: set[str] | None = set(existing) if existing is not None else None
        self.remote_state: Mapping[str, AppliedEntry] | None = MappingProxyType(dict(state or {}))
        self.state_manifest: str | None = None
        self.state_writes: list[StateWrite] = []
        self.run_locks = InMemoryRunLocks()
        self.role = role
        self.account_locator = account_locator
        self.stage_formats: dict[str, str] = dict(stage_formats or {})
        self.stage_types: dict[str, str] = {}
        self.stage_files: set[str] = set(stage_files)
        self.staged_file_metadata: dict[str, StagedFileMetadata] = dict(staged_file_metadata or {})
        self.staged_file_contents: dict[str, bytes] = dict(staged_file_contents or {})
        self.stage_files.update(self.staged_file_metadata)
        self.stage_files.update(self.staged_file_contents)
        self.agent_versions: dict[tuple[str, str], str] = {
            (name, selector.casefold()): version for (name, selector), version in (agent_versions or {}).items()
        }
        self.live_agents: set[str] = set()
        # The versions of each dataset, by qualified name; ADD VERSION appends to them.
        self.dataset_version_names: dict[str, list[str]] = {}
        # Each dataset's owner, by qualified name, as SHOW DATASETS lists it; a dataset not
        # named here is owned by the session's role. MEASURED: SHOW GRANTS ON DATASET is a
        # syntax error, so SHOW DATASETS' `owner` is the only way to read who owns one.
        self.dataset_owners: dict[str, str] = {}
        self.table_row_counts: dict[str, int] = {}
        # Column (name, type) pairs by qualified name; the profile registry's shape lands here.
        self.tables: dict[str, tuple[tuple[str, str], ...]] = {}
        self.extensions: dict[str, dict[str, object]] = {}
        self.profile_rows: dict[str, dict[str, dict[str, object]]] = {}
        # SHOW rows and DESCRIBE properties by "<TYPE> <qualified name>"; parameters by
        # (type, object name, parameter), each upper-cased.
        self.show_rows: dict[str, Mapping[str, str]] = {}
        self.descriptions: dict[str, Mapping[str, str]] = {}
        self.object_parameters: dict[tuple[str, str, str], str] = {}
        self.preflight = PreflightAnswers()
        self.halted: str | None = None
        self.log: list[Sent] = []
        self.execute_results: list[ExecResult] = list(execute_results)
        self.query_results: list[QueryResult | BaseException] = list(query_results)
        # False: a query `query_results` does not answer is a test error, not an empty answer.
        self.answer_unscripted = True
        # Statements containing any of these fragments fail, to rehearse refusals.
        self.refused: tuple[str, ...] = ()
        self._armed: dict[str, _Armed] = {}
        # The schema of the `query_in_context` call running on each thread, for the log.
        self._in_scope = threading.local()

    # -- the log ------------------------------------------------------------------------------

    @property
    def queries(self) -> list[tuple[str, object]]:
        """Every query sent, as (statement, params), in order."""
        return [(sent.statements[0], sent.params) for sent in self.log if sent.kind == "query"]

    @property
    def scripts(self) -> list[tuple[str, ...]]:
        """Every script sent, as its statements, in order."""
        return [sent.statements for sent in self.log if sent.kind == "script"]

    @property
    def uploads(self) -> list[tuple[str, bytes]]:
        """Every upload sent, as (stage path, content), in order."""
        return [(sent.statements[0], sent.content) for sent in self.log if sent.kind == "upload"]

    # -- failure injection --------------------------------------------------------------------

    def fail(self, method: str, error: BaseException, *, times: int | None = None) -> None:
        """Make the port method named `method` raise `error`: `times` calls, or every call."""
        self._armed[method] = _Armed(error, times)

    def refuse(self, *methods: str) -> None:
        """Make each named method raise the `SnowflakePortError` "<method> refused"."""
        for method in methods:
            self.fail(method, SnowflakePortError(f"{method} refused"))

    def heal(self, method: str) -> None:
        """Disarm a failure `fail` armed."""
        self._armed.pop(method, None)

    def _check(self, method: str) -> None:
        armed = self._armed.get(method)
        if armed is None:
            return
        if armed.remaining is not None:
            armed.remaining -= 1
            if armed.remaining <= 0:
                del self._armed[method]
        raise armed.error

    def _refusal(self, statement: str) -> str | None:
        return statement if any(fragment in statement for fragment in self.refused) else None

    # -- staging the account ------------------------------------------------------------------

    def show(self, *rows: ShowRow) -> None:
        """File each row where SHOW lists it: under its object type, in its own schema."""
        for row in rows:
            key = (row.object_type.upper(), f"{row.database_name}.{row.schema_name}")
            self.objects[key] = (*self.objects.get(key, ()), row)

    def stage_file(
        self, stage_path: str, content: bytes | None = None, *, size: int | None = None, md5: str | None = None
    ) -> None:
        """Put a file on a stage before the test runs, as LIST then reports it; nothing is logged."""
        self.stage_files.add(stage_path)
        if content is not None:
            self.staged_file_contents[stage_path] = content
        if md5 is None and content is not None:
            md5 = hashlib.md5(content, usedforsecurity=False).hexdigest()
        self.staged_file_metadata[stage_path] = StagedFileMetadata(
            stage_path,
            stage_path[1:] if stage_path.startswith("@") else stage_path,
            size if size is not None else len(content or b""),
            md5,
        )
