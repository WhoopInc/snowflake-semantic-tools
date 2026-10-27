"""The stages apply stages files on: created when absent, once per stage per run, never altered.

An agent's specification is uploaded to its `apply.agent_spec_stage` stage before the agent
is created from it. `UploadStages` makes sure that stage exists first, as the skill and
profile handlers do for theirs: `CREATE STAGE IF NOT EXISTS` with server-side encryption
when it is absent, nothing when it already is an internal SSE stage. A stage of any other
kind fails the change, and SST never replaces, alters, or drops a stage.
"""

from __future__ import annotations

from dataclasses import replace
from threading import Lock

from snowflake_semantic_tools.app.apply.errors import _script_error
from snowflake_semantic_tools.app.lifecycle.composite import create_sse_stage_sql
from snowflake_semantic_tools.app.lifecycle.ports import CatalogPublicationPort
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.model.lifecycle import SSE_STAGE_TYPE, ClassifiedError, ErrorKind


class UploadStages:
    """Ensure each stage a run uploads to is an internal SSE stage, creating it when absent.

    One instance serves one apply run and is shared by its workers: a stage found or created
    usable is remembered, so it is read and created at most once per run, and a lock keeps
    two workers from creating it at once. A failure is not remembered, so the next change
    that stages there tries again.
    """

    def __init__(self) -> None:
        self._lock = Lock()
        self._ready: set[tuple[str, str, str]] = set()

    def ensure(self, port: CatalogPublicationPort, stage: QualifiedName) -> ClassifiedError | None:
        """Make `stage` exist as an internal SSE stage, on `port`; None once it does.

        Returns:
            The failure: SST-APL001 for a stage of another kind, which is left as it is, or
            one that is absent after it was created; the classified Snowflake error when
            CREATE STAGE failed.

        Raises:
            SnowflakePortError: the stage could not be read.
        """
        with self._lock:
            if stage.folded in self._ready:
                return None
            found = port.stage_type(stage)
            if found is None:
                result = port.execute_script((create_sse_stage_sql(stage),))
                if not result.ok:
                    error = _script_error(result, "no error reported")
                    return replace(error, message=f"stage {stage.sql} could not be created: {error.message}")
                found = port.stage_type(stage)
            if found is None:
                return ClassifiedError("SST-APL001", f"stage {stage.sql} is absent after creation", ErrorKind.UNKNOWN)
            if found.upper() != SSE_STAGE_TYPE:
                detail = (
                    f"stage {stage.sql} is {found or 'untyped'}, expected {SSE_STAGE_TYPE}; SST never alters a stage"
                )
                return ClassifiedError("SST-APL001", detail, ErrorKind.UNKNOWN)
            self._ready.add(stage.folded)
            return None
