"""Resolve one dbt profile target into connection and lifecycle values."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml

from ..domain.model.identifier import Identifier, QualifiedName, TargetIdentity

_ENV_VAR = re.compile(r"^\{\{\s*env_var\(\s*['\"]([^'\"]+)['\"](?:\s*,\s*['\"]([^'\"]*)['\"])?\s*\)\s*\}\}$")


def _resolve_env(value: object) -> object:
    if not isinstance(value, str):
        return value
    match = _ENV_VAR.fullmatch(value.strip())
    if match is None:
        return value
    name, default = match.groups()
    if name in os.environ:
        return os.environ[name]
    if default is not None:
        return default
    raise ValueError(f"environment variable {name} is required")


def _read_yaml(path: Path) -> dict[str, Any]:
    value = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a mapping")
    return {str(key): item for key, item in value.items()}


class ProfileTarget:
    def __init__(
        self,
        *,
        profile_name: str,
        target_name: str,
        connection_params: dict[str, object],
        identity: TargetIdentity,
        state_table: QualifiedName,
    ) -> None:
        self.profile_name = profile_name
        self.target_name = target_name
        self.connection_params = connection_params
        self.identity = identity
        self.state_table = state_table


def load_profile_target(project_dir: Path, target_name: str | None = None) -> ProfileTarget:
    project = _read_yaml(project_dir / "dbt_project.yml")
    profile_name = project.get("profile")
    if not isinstance(profile_name, str) or not profile_name:
        raise ValueError("dbt_project.yml declares no profile")
    profiles = _read_yaml(project_dir / "profiles.yml")
    profile = profiles.get(profile_name)
    if not isinstance(profile, dict):
        raise ValueError(f"profiles.yml has no profile {profile_name!r}")
    selected = target_name or profile.get("target")
    outputs = profile.get("outputs")
    output = outputs.get(selected) if isinstance(outputs, dict) else None
    if not isinstance(selected, str) or not isinstance(output, dict):
        raise ValueError(f"profiles.yml has no target {selected!r}")
    resolved = {str(key): _resolve_env(value) for key, value in output.items()}
    database = resolved.get("database")
    schema = resolved.get("schema")
    if not isinstance(database, str) or not isinstance(schema, str):
        raise ValueError(f"target {selected!r} must set database and schema")
    params = {
        key: value
        for key, value in resolved.items()
        if key
        in {
            "account",
            "user",
            "password",
            "authenticator",
            "role",
            "warehouse",
            "database",
            "schema",
            "private_key_file",
            "private_key_file_pwd",
        }
        and value not in (None, "")
    }
    query_tag = resolved.get("query_tag")
    if isinstance(query_tag, str) and query_tag:
        params["session_parameters"] = {"QUERY_TAG": query_tag}
    account = resolved.get("account")
    identity = TargetIdentity(
        selected,
        str(account or ""),
        Identifier.parse(database),
        Identifier.parse(schema),
        str(resolved["role"]) if resolved.get("role") else None,
        str(resolved["warehouse"]) if resolved.get("warehouse") else None,
    )
    config_path = project_dir / "sst_config.yml"
    config = _read_yaml(config_path) if config_path.is_file() else {}
    state_config = config.get("state")
    state_name = "SST_STATE"
    state_database = database
    state_schema = schema
    if isinstance(state_config, dict):
        raw_table = state_config.get("+table")
        raw_database = state_config.get("+database")
        raw_schema = state_config.get("+schema")
        if raw_table:
            state_name = str(_resolve_env(raw_table))
        if raw_database:
            state_database = str(raw_database).replace("{{ target.database }}", database)
        if raw_schema:
            state_schema = str(raw_schema).replace("{{ target.schema }}", schema)
    state_table = QualifiedName.from_parts(state_database, state_schema, state_name)
    return ProfileTarget(
        profile_name=profile_name,
        target_name=selected,
        connection_params=params,
        identity=identity,
        state_table=state_table,
    )
