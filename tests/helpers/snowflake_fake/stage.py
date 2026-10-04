"""The fake's `StagePort`: files on stages and in Cortex extension versions.

An upload is logged, then lands: on a stage it replaces the file and its LIST metadata
(size and MD5 of the content); under `snow://` it joins the extension's open live version, and
fails when none is open there, as PUT does.
"""

from __future__ import annotations

from hashlib import md5

from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.stage import StagedFileMetadata
from tests.helpers.snowflake_fake.extensions import files, versions
from tests.helpers.snowflake_fake.world import Sent, SnowflakeWorld


class FakeStage(SnowflakeWorld):
    """`StagePort` over the shared account."""

    def observe_staged_file(self, stage_path: str) -> StagedFileMetadata | None:
        self._check("observe_staged_file")
        if stage_path not in self.stage_files:
            return None
        return self.staged_file_metadata.get(stage_path, _unmeasured(stage_path))

    def read_staged_file(self, stage_path: str) -> bytes | None:
        self._check("read_staged_file")
        return self.staged_file_contents.get(stage_path)

    def upload(self, stage_path: str, content: bytes) -> None:
        self.log.append(Sent("upload", (stage_path,), content=content))
        self._check("upload")
        if stage_path.startswith("snow://"):
            for extension in self.extensions.values():
                live = extension.get("live")
                if isinstance(live, dict) and stage_path.startswith(str(live["location"])):
                    files(live).append(stage_path[len(str(live["location"])) :])
                    return
            raise SnowflakePortError(f"no open live version accepts {stage_path}")
        self.stage_files.add(stage_path)
        self.staged_file_contents[stage_path] = content
        self.staged_file_metadata[stage_path] = StagedFileMetadata(
            stage_path=stage_path,
            name=stage_path[1:] if stage_path.startswith("@") else stage_path,
            size=len(content),
            md5=md5(content, usedforsecurity=False).hexdigest(),
        )

    def list_location(self, location: str) -> tuple[str, ...]:
        self._check("list_location")
        if location.startswith("snow://"):
            for extension in self.extensions.values():
                for version in versions(extension):
                    if str(version["location"]).casefold() == location.casefold():
                        return tuple(sorted(files(version)))
            return ()
        return tuple(sorted(path[len(location) :] for path in self.stage_files if path.startswith(location)))


def _unmeasured(stage_path: str) -> StagedFileMetadata:
    return StagedFileMetadata(
        stage_path=stage_path,
        name=stage_path[1:] if stage_path.startswith("@") else stage_path,
        size=0,
    )
