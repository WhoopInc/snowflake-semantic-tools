"""Files on stages and in Cortex extension versions, listed, observed, read, and written by path.

Every path is validated before anything runs. A stage path is `@<db>.<schema>.<stage>/<path>`
and an extension version path `snow://cortex_extension/<db>.<schema>.<name>/versions/<version>/<path>`;
each segment of `<path>` uses only the characters a staged file name may, so a path never
needs quoting inside the LIST, GET, or PUT that carries it.
"""

from __future__ import annotations

import re
from pathlib import PurePosixPath

from snowflake_semantic_tools.adapters.paths import scratch_folder, write_within
from snowflake_semantic_tools.adapters.snowflake.connector.session import Session, _require_ok
from snowflake_semantic_tools.domain.model.identifier import QualifiedName
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError
from snowflake_semantic_tools.domain.ports.snowflake.stage import StagedFileMetadata, StagePort
from snowflake_semantic_tools.domain.sql import Sql, literal, local_file, sql, stage_path
from snowflake_semantic_tools.domain.validate.stage_path import SAFE_SEGMENT_CHARACTERS as _SAFE_SEGMENT

_EXTENSION_URI = re.compile(
    r"snow://cortex_extension/(?P<name>[A-Za-z0-9_$.]+)/versions/(?P<version>version\$[0-9]+|live)/(?P<path>.*)",
    re.IGNORECASE,
)


class StageMethods(Session, StagePort):
    """Move files on stages and in extension versions, through a local temporary directory."""

    def observe_staged_file(self, stage_path: str) -> StagedFileMetadata | None:
        stage_path = _validated_stage_path(stage_path)
        rows = self._dict_rows(sql("LIST {location}", location=literal(stage_path)))
        expected_name = stage_path[1:]
        matches = tuple(row for row in rows if _staged_file_name_matches(row.get("name"), stage_path))
        if len(matches) > 1:
            raise SnowflakePortError(f"stage path returned duplicate files: {stage_path}")
        if not matches:
            return None
        row = matches[0]
        try:
            size = int(row.get("size") or 0)
        except (TypeError, ValueError) as exc:
            raise SnowflakePortError(f"stage file {stage_path} returned invalid size metadata") from exc
        return StagedFileMetadata(
            stage_path=stage_path,
            name=expected_name,
            size=size,
            md5=str(row["md5"]) if row.get("md5") is not None else None,
            last_modified=str(row["last_modified"]) if row.get("last_modified") is not None else None,
        )

    def stage_file_exists(self, stage_path: str) -> bool:
        return self.observe_staged_file(stage_path) is not None

    def read_staged_file(self, stage_path: str) -> bytes | None:
        """Download one staged file and return its bytes; None when the stage holds no such file.

        GET matches a prefix, so it can download siblings (`a.yaml` brings `a.yaml.bak`); the
        file is found by name among the rows the driver reports, never assumed to be there.

        Raises:
            SnowflakePortError: GET failed, or reported the file as anything but downloaded.
        """
        stage_path = _validated_stage_path(stage_path)
        with scratch_folder("sst-stage-read-") as temp_dir:
            statement = sql("GET {source} {target}", source=literal(stage_path), target=local_file(str(temp_dir)))
            downloaded = _downloaded_name(self._dict_rows(statement), stage_path)
            if downloaded is None:
                return None
            return (temp_dir / downloaded).read_bytes()

    def upload(self, stage_path: str, content: bytes) -> None:
        stage_path = _validated_upload_target(stage_path)
        basename = PurePosixPath(stage_path).name
        with scratch_folder("sst-upload-") as temp_dir:
            local_path = temp_dir / basename
            write_within(temp_dir, local_path, content)
            # The PUT target is always a directory, so it ends in a separator.
            destination = _put_destination(stage_path.rsplit("/", 1)[0] + "/")
            result = self.execute_script(
                (
                    sql(
                        "PUT {source} {destination} OVERWRITE=TRUE AUTO_COMPRESS=FALSE",
                        source=local_file(str(local_path)),
                        destination=destination,
                    ),
                )
            )
            _require_ok(result, "stage upload failed")

    def list_location(self, location: str) -> tuple[str, ...]:
        """List the location and keep the names below it, extension paths compared casefolded.

        LIST prefixes a stage file's path with the stage's own name, which need not match the
        name the location gave, so a stage prefix is compared, exactly, after the first separator.
        """
        location = _validated_location(location)
        rows = self._dict_rows(sql("LIST {location}", location=literal(location)))
        if location.startswith("snow://"):
            marker = location[location.index("/versions/") :]
            names = (str(row.get("name") or "") for row in rows)
            return tuple(sorted(name[len(marker) :] for name in names if name.casefold().startswith(marker.casefold())))
        prefix = location[1:].split("/", 1)[1]
        relative: list[str] = []
        for row in rows:
            returned = str(row.get("name") or "")
            if "/" not in returned:
                continue
            path = returned.split("/", 1)[1]
            if path.startswith(prefix):
                relative.append(path[len(prefix) :])
        return tuple(sorted(relative))


def _put_destination(directory: str) -> Sql:
    """Name a PUT target directory: an extension path quoted, a stage path as the stage location.

    Either ends in `/`, the stage's root included. Both are validated already, so each
    segment is safe; the stage path is rebuilt from its parts so it reaches the statement
    through `stage_path`.
    """
    if directory.startswith("snow://"):
        return literal(directory)
    stage, path = directory[1:].split("/", 1)
    if not path:
        # `stage_path` names the root without a separator; a PUT target always has one.
        return sql("{stage}/", stage=stage_path(QualifiedName.parse(stage)))
    return stage_path(QualifiedName.parse(stage), path)


def _staged_file_name_matches(value: object, stage_path: str) -> bool:
    expected_stage, expected_relative = stage_path[1:].split("/", 1)
    returned = str(value or "")
    if "/" not in returned:
        return False
    returned_stage, returned_relative = returned.split("/", 1)
    if returned_relative != expected_relative:
        return False
    stage_name = QualifiedName.parse(expected_stage).name.folded
    return returned_stage.casefold() in {expected_stage.casefold(), stage_name.casefold()}


def _validated_stage_path(value: str, *, directory: bool = False) -> str:
    """Return a stage path unchanged once it is known safe to embed in a quoted LIST, GET, or PUT.

    The path is `@<db>.<schema>.<stage>/` and then safe segments: no quote, backslash, glob,
    whitespace, or control character anywhere, no empty, `.`, or `..` segment, and nothing
    absolute. A `directory` path must end in a separator.

    Raises:
        SnowflakePortError: the path breaks any of these rules.
    """
    message = "stage path must use safe segments and a safe basename under @<db>.<schema>.<stage>"
    if not value.startswith("@") or any(character.isspace() or ord(character) < 32 for character in value):
        raise SnowflakePortError(message)
    if any(character in value for character in ("\\", "'", '"', "*", "?", "[", "]", "{", "}")):
        raise SnowflakePortError(message)
    try:
        stage, path = value[1:].split("/", 1)
        QualifiedName.parse(stage)
    except ValueError:
        raise SnowflakePortError(message) from None
    if directory:
        if not path.endswith("/"):
            raise SnowflakePortError(message)
        path = path[:-1]
    parts = path.split("/")
    if not parts or any(not part or part in (".", "..") for part in parts):
        raise SnowflakePortError(message)
    if any(any(character not in _SAFE_SEGMENT for character in part) for part in parts):
        raise SnowflakePortError(message)
    if PurePosixPath(path).is_absolute():
        raise SnowflakePortError(message)
    return value


def _extension_uri(value: str, *, directory: bool) -> str:
    """Return an extension version path unchanged once it is known safe to embed in a LIST or PUT.

    The version is `version$<n>` or `live`, matched case-insensitively. A `directory` path is
    the version's root, with nothing after it; any other path continues with safe segments.

    Raises:
        SnowflakePortError: the path breaks any of these rules.
    """
    message = "extension path must be snow://cortex_extension/<db>.<schema>.<name>/versions/<version>/<safe path>"
    match = _EXTENSION_URI.fullmatch(value)
    if match is None:
        raise SnowflakePortError(message)
    try:
        QualifiedName.parse(match.group("name"))
    except ValueError:
        raise SnowflakePortError(message) from None
    path = match.group("path")
    if directory:
        if path:
            raise SnowflakePortError(message)
        return value
    parts = path.split("/")
    if any(
        not part or part in (".", "..") or any(character not in _SAFE_SEGMENT for character in part) for part in parts
    ):
        raise SnowflakePortError(message)
    return value


def _validated_upload_target(value: str) -> str:
    if value.startswith("snow://"):
        return _extension_uri(value, directory=False)
    return _validated_stage_path(value)


def _validated_location(value: str) -> str:
    if value.startswith("snow://"):
        return _extension_uri(value, directory=True)
    return _validated_stage_path(value, directory=True)


def _downloaded_name(rows: tuple[dict[str, object], ...], stage_path: str) -> str | None:
    """Return the local name GET wrote the staged file to, from its `file`/`status` result rows.

    Raises:
        SnowflakePortError: the file's row reports a status other than DOWNLOADED.
    """
    name = PurePosixPath(stage_path).name
    row = next((item for item in rows if PurePosixPath(str(item.get("file") or "")).name == name), None)
    if row is None:
        return None
    status = str(row.get("status") or "")
    if status.upper() != "DOWNLOADED":
        raise SnowflakePortError(
            f"stage download of {stage_path} reported {status or 'no status'}: {row.get('message')}"
        )
    return name
