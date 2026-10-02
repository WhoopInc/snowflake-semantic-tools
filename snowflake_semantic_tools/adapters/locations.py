"""Where one run finds its configuration file and its dbt `profiles.yml`.

`locate_project` is the only place that decides which `sst_config.yml` a run reads, and
`ProjectPaths.profiles_file` the only place that decides which `profiles.yml` a target resolves
against. Every reader takes the `ProjectPaths` value, so `--config` and discovery can never
disagree about the file. Discovery looks in exactly one directory, the project root: there is no
parent-directory walk and no home-directory fallback for the configuration.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import NoReturn

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.config_schema import CONFIG_FILE, CONFIG_NAMES

PROFILES_FILE = "profiles.yml"


@dataclass(frozen=True, slots=True)
class ProjectPaths:
    """The project directory, the configuration file a run reads, and where profiles are found.

    Attributes:
        config_file: The configuration file in use; None only for a run that needs none, such
            as `sst init` in a project that has no configuration yet.
        candidates: Every configuration path checked, in the order they were checked.
        shadowed: Discovered configuration files `--config` or `$SST_CONFIG` took precedence
            over; reported as SST-CFG032.
        profiles_dir: The directory `--profiles-dir` or `$SST_PROFILES_DIR` named; None searches.
        allow_unsupported_manifest_schema: Read a dbt manifest of an unsupported schema version
            for this run instead of refusing it, as `--allow-unsupported-manifest-schema` asks.
    """

    project_dir: Path
    config_file: Path | None
    candidates: tuple[Path, ...] = ()
    shadowed: tuple[Path, ...] = ()
    profiles_dir: Path | None = None
    allow_unsupported_manifest_schema: bool = False

    @property
    def config_name(self) -> str:
        """Name the configuration file as diagnostics do: project-relative, else its full path."""
        if self.config_file is None:
            return CONFIG_FILE
        return _relative(self.project_dir, self.config_file)

    @property
    def diagnostics(self) -> tuple[Diagnostic, ...]:
        """Report what discovery found without refusing it.

        Diagnostics:
            SST-CFG032: an explicit configuration file shadows one discovered in the project.
        """
        if self.config_file is None or not self.shadowed:
            return ()
        return (
            D(
                "SST-CFG032",
                origin=Origin(self.config_name),
                subject=f"config:{self.config_name}",
                used=str(self.config_file),
                shadowed=", ".join(str(path) for path in self.shadowed),
            ),
        )

    def profiles_candidates(self, environ: Mapping[str, str] | None = None) -> tuple[Path, ...]:
        """Return every `profiles.yml` path searched, in order, the first existing one winning.

        `--profiles-dir` (or `$SST_PROFILES_DIR`), then `$DBT_PROFILES_DIR`, then the project
        root, then `~/.dbt`.
        """
        values = os.environ if environ is None else environ
        directories = [self.profiles_dir] if self.profiles_dir is not None else []
        if values.get("DBT_PROFILES_DIR"):
            directories.append(Path(values["DBT_PROFILES_DIR"]))
        directories.extend((self.project_dir, Path.home() / ".dbt"))
        return tuple(directory / PROFILES_FILE for directory in directories)

    def profiles_directory(self) -> Path:
        """Return the directory of the `profiles.yml` targets resolve against; the project's when none exists.

        This is the directory `dbt parse` is given, so dbt reads the profiles SST does, and reports
        their absence itself.
        """
        found = next((path for path in self.profiles_candidates() if path.is_file()), None)
        return found.parent if found is not None else self.project_dir

    def profiles_file(self) -> Path:
        """Return the `profiles.yml` a target resolves against.

        Raises:
            ProjectError: no candidate exists (SST-CFG009).

        Diagnostics:
            SST-CFG009: no `profiles.yml` exists at any searched location; raised.
        """
        candidates = self.profiles_candidates()
        for path in candidates:
            if path.is_file():
                return path
        diagnostic = D("SST-CFG009", subject="config:profiles.yml")
        searched = ", ".join(str(path) for path in candidates)
        raise ProjectError(f"{diagnostic.message} (searched {searched})", diagnostics=(diagnostic,))


def locate_project(
    project_dir: Path,
    config: Path | None = None,
    *,
    profiles_dir: Path | None = None,
    required: bool = True,
) -> ProjectPaths:
    """Resolve which configuration file a run reads: `config` when given, else the one in `project_dir`.

    Args:
        config: The file `--config` or `$SST_CONFIG` named; it bypasses discovery entirely.
        required: False lets a run with no configuration file go on, with `config_file` None.

    Raises:
        ProjectError: `config` names no file, the project root holds no configuration file and
            one is required (SST-CFG001), or it holds both spellings (SST-CFG005).

    Diagnostics:
        SST-CFG001: there is no configuration file at the path given or discovered; raised.
        SST-CFG005: both `sst_config.yml` and `sst_config.yaml` exist; raised.
    """
    discovered = tuple(project_dir / name for name in CONFIG_NAMES)
    present = tuple(path for path in discovered if path.is_file())
    if config is not None:
        if not config.is_file():
            _refuse(D("SST-CFG001", subject="config:--config", path=str(config)))
        return ProjectPaths(project_dir, config, (config, *discovered), present, profiles_dir)
    if len(present) > 1:
        _refuse(
            D(
                "SST-CFG005",
                subject="config:discovery",
                count=len(present),
                used=str(present[0]),
                shadowed=", ".join(str(path) for path in present[1:]),
            )
        )
    if not present and required:
        _refuse(D("SST-CFG001", subject="config:discovery", path=str(project_dir)))
    return ProjectPaths(project_dir, present[0] if present else None, discovered, (), profiles_dir)


def _refuse(diagnostic: Diagnostic) -> NoReturn:
    raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))


def _relative(root: Path, path: Path) -> str:
    """Return `path` relative to `root` with POSIX separators; its full path when outside it."""
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return str(path)
