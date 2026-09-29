"""Resolve one dbt profile target into connection and lifecycle values."""

from __future__ import annotations

import base64
import binascii
import os
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any, NoReturn

import yaml

from ..domain.model.diagnostic import D, Diagnostic, Origin
from ..domain.model.identifier import Identifier, QualifiedName, TargetIdentity
from .project import ProjectError

_ENV_VAR = re.compile(
    r"\{\{\s*env_var\(\s*['\"]([^'\"]+)['\"](?:\s*,\s*['\"]([^'\"]*)['\"])?\s*\)"
    r"(?:\s*\|\s*(as_number|as_bool|as_native|as_text))?\s*\}\}"
)
_TEMPLATE = re.compile(r"\{\{|\{%")
# dbt-snowflake fields SST passes to the connector, by the connector's name for each.
_CONNECTION_KEYS: Mapping[str, str] = {
    "account": "account",
    "user": "user",
    "password": "password",
    "authenticator": "authenticator",
    "role": "role",
    "warehouse": "warehouse",
    "database": "database",
    "schema": "schema",
    "host": "host",
    "port": "port",
    "protocol": "protocol",
    "insecure_mode": "insecure_mode",
    "client_session_keep_alive": "client_session_keep_alive",
    "connect_timeout": "login_timeout",
    "token": "token",
    "private_key_path": "private_key_file",
    "private_key_file": "private_key_file",
    "private_key_file_pwd": "private_key_file_pwd",
}
# Read too, but not passed through as written.
_SPECIAL_KEYS = frozenset(("private_key", "private_key_passphrase", "query_tag"))
# dbt settings with no meaning for SST's single connection.
_IGNORED_KEYS = frozenset(
    (
        "type",
        "threads",
        "connect_retries",
        "retry_on_database_errors",
        "retry_all",
        "reuse_connections",
        "s3_stage_vpce_dns_name",
        "platform_detection_timeout_seconds",
    )
)
_READ_KEYS = frozenset((*_CONNECTION_KEYS, *_SPECIAL_KEYS))
_INTEGER_KEYS = frozenset(("port", "connect_timeout"))
_BOOLEAN_KEYS = frozenset(("insecure_mode", "client_session_keep_alive"))


def _resolve_env(value: object) -> object:
    """Render every `{{ env_var('NAME') }}` in a string; other templates are left in place.

    dbt's `as_number`, `as_bool`, and `as_native` filters type a value that is one
    template on its own; inside a longer string the rendered text is used.
    """
    if not isinstance(value, str):
        return value

    def substitute(match: re.Match[str]) -> str:
        name, default, _ = match.groups()
        if name in os.environ:
            return os.environ[name]
        if default is not None:
            return default
        raise ValueError(f"environment variable {name} is required")

    whole = _ENV_VAR.fullmatch(value.strip())
    if whole is None:
        return _ENV_VAR.sub(substitute, value)
    text = substitute(whole)
    if whole.group(3) in ("as_number", "as_bool", "as_native"):
        typed = yaml.safe_load(text) if text.strip() else text
        return typed if isinstance(typed, (bool, int, float)) else text
    return text


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
        diagnostics: tuple[Diagnostic, ...] = (),
        inline_key: tuple[str, object] | None = None,
    ) -> None:
        self.profile_name = profile_name
        self.target_name = target_name
        self._params = connection_params
        self.identity = identity
        self.state_table = state_table
        self.diagnostics = diagnostics
        # Decoded only when a connection is opened, so an offline compile needs no key.
        self._inline_key = inline_key

    @property
    def connection_params(self) -> dict[str, object]:
        params = dict(self._params)
        if self._inline_key is not None:
            value, passphrase = self._inline_key
            params["private_key"] = _inline_private_key(self.target_name, value, passphrase)
        return params

    @property
    def authentication(self) -> str:
        """How the connection authenticates, for display; never the credential itself."""
        params = self._params
        if self._inline_key is not None:
            return "key pair (private_key)"
        if "private_key_file" in params:
            return "key pair (private key file)"
        if "token" in params and str(params.get("authenticator", "")).casefold() == "oauth":
            return "OAuth access token"
        if "authenticator" in params:
            return str(params["authenticator"])
        return "password" if "password" in params else "connector default"


def resolve_profile_name(project_dir: Path) -> str:
    """The `profiles.yml` profile: dbt's `profile:`, or `project.target_profile` without dbt."""
    config_path = project_dir / "sst_config.yml"
    config = _read_yaml(config_path) if config_path.is_file() else {}
    project_block = config.get("project")
    configured = project_block.get("target_profile") if isinstance(project_block, dict) else None
    dbt_project_path = project_dir / "dbt_project.yml"
    if not dbt_project_path.is_file():
        if not isinstance(configured, str) or not configured:
            raise ValueError(
                "the project has no dbt_project.yml; set project.target_profile in sst_config.yml "
                "to name its profiles.yml profile"
            )
        return configured
    profile_name = _read_yaml(dbt_project_path).get("profile")
    if not isinstance(profile_name, str) or not profile_name:
        raise ValueError("dbt_project.yml declares no profile")
    if configured is not None and configured != profile_name:
        raise ValueError(
            f"project.target_profile {configured!r} disagrees with dbt_project.yml profile {profile_name!r}"
        )
    return profile_name


def profile_output(project_dir: Path, target_name: str | None = None) -> tuple[str, str, dict[str, object]]:
    """The profile name, target name, and fields of one output, `env_var()` rendered in those SST reads.

    A field SST ignores is left as written, so an unset variable or a dbt-only
    filter there cannot stop a run.
    """
    profile_name = resolve_profile_name(project_dir)
    profiles = _read_yaml(project_dir / "profiles.yml")
    profile = profiles.get(profile_name)
    if not isinstance(profile, dict):
        raise ValueError(f"profiles.yml has no profile {profile_name!r}")
    selected = target_name or profile.get("target")
    outputs = profile.get("outputs")
    output = outputs.get(selected) if isinstance(outputs, dict) else None
    if not isinstance(selected, str) or not isinstance(output, dict):
        diagnostic = D("SST-CFG010", target=selected, profile=profile_name)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    for key, value in output.items():
        # Checked as written, so a rendered secret that happens to contain `{{` is fine.
        if str(key) in _READ_KEYS and isinstance(value, str) and _TEMPLATE.search(_ENV_VAR.sub("", value)):
            _refuse("SST-CFG049", selected, key=str(key), problem="holds a template other than env_var()")
    return (
        profile_name,
        selected,
        {str(key): (_resolve_env(value) if str(key) in _READ_KEYS else value) for key, value in output.items()},
    )


def _refuse(code: str, target: str, **context: Any) -> NoReturn:
    diagnostic = D(code, origin=Origin("profiles.yml"), subject="config:profiles.yml", target=target, **context)
    raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))


def _checked(target: str, key: str, value: object) -> object:
    if key in _INTEGER_KEYS:
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        if isinstance(value, str) and value.strip().isdigit():
            return int(value)
        _refuse("SST-CFG049", target, key=key, problem="must be a whole number")
    if key in _BOOLEAN_KEYS:
        if isinstance(value, bool):
            return value
        if isinstance(value, str) and value.strip().casefold() in ("true", "false"):
            return value.strip().casefold() == "true"
        _refuse("SST-CFG049", target, key=key, problem="must be true or false")
    return value


def _inline_private_key(target: str, value: object, passphrase: object) -> bytes:
    """dbt's `private_key`, PEM or base64 DER, as the DER bytes the connector takes."""
    from cryptography.exceptions import UnsupportedAlgorithm
    from cryptography.hazmat.primitives import serialization

    if not isinstance(value, str) or not value.strip():
        _refuse("SST-CFG050", target, detail="private_key must be a PEM or base64-encoded DER key")
    text = value.strip()
    password = str(passphrase).encode("utf-8") if passphrase not in (None, "") else None
    try:
        if text.startswith("-----BEGIN"):
            key = serialization.load_pem_private_key(text.encode("utf-8"), password=password)
        else:
            # dbt accepts base64 wrapped across lines, so whitespace is not part of it.
            key = serialization.load_der_private_key(
                base64.b64decode("".join(text.split()), validate=True), password=password
            )
    except (ValueError, TypeError, binascii.Error, UnsupportedAlgorithm) as exc:
        # The exception type only: a key's parse error must never echo key material.
        _refuse("SST-CFG050", target, detail=f"private_key cannot be read ({type(exc).__name__})")
    return key.private_bytes(
        serialization.Encoding.DER, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )


def _connection_params(
    target: str, output: Mapping[str, object]
) -> tuple[dict[str, object], tuple[str, object] | None, tuple[Diagnostic, ...]]:
    """dbt-snowflake profile fields, translated to the Snowflake connector's arguments.

    An inline `private_key` is returned apart, with its passphrase, to be decoded
    when a connection is opened.
    """
    present = {key: value for key, value in output.items() if value not in (None, "")}
    refresh = [key for key in ("oauth_client_id", "oauth_client_secret") if key in present]
    if refresh:
        _refuse(
            "SST-CFG050",
            target,
            detail=f"{' and '.join(refresh)} ask dbt to exchange a refresh token, which SST does not do",
        )
    if "private_key" in present and any(key in present for key in ("private_key_path", "private_key_file")):
        _refuse("SST-CFG050", target, detail="private_key and a private key file are both set")
    files = {str(present[key]) for key in ("private_key_path", "private_key_file") if key in present}
    if len(files) > 1:
        _refuse("SST-CFG050", target, detail="private_key_path and private_key_file name different files")
    params: dict[str, object] = {}
    diagnostics: list[Diagnostic] = []
    for key, value in present.items():
        if key in _CONNECTION_KEYS:
            params[_CONNECTION_KEYS[key]] = _checked(target, key, value)
        elif (
            key not in _SPECIAL_KEYS
            and key not in _IGNORED_KEYS
            and key not in ("oauth_client_id", "oauth_client_secret")
        ):
            diagnostics.append(
                D("SST-CFG048", origin=Origin("profiles.yml"), subject="config:profiles.yml", target=target, key=key)
            )
    passphrase = present.get("private_key_passphrase")
    inline_key: tuple[str, object] | None = None
    if "private_key" in present:
        key_value = present["private_key"]
        if not isinstance(key_value, str):
            _refuse("SST-CFG050", target, detail="private_key must be a PEM or base64-encoded DER key")
        inline_key = (key_value, passphrase)
    elif passphrase is not None and "private_key_file" in params:
        params.setdefault("private_key_file_pwd", _checked(target, "private_key_passphrase", passphrase))
    # dbt reads `token` only for OAuth; imply it only when nothing else authenticates.
    other = inline_key is not None or any(key in params for key in ("password", "private_key_file"))
    if "token" in params and "authenticator" not in params and not other:
        params["authenticator"] = "oauth"
    return params, inline_key, tuple(diagnostics)


def load_profile_target(project_dir: Path, target_name: str | None = None) -> ProfileTarget:
    profile_name, selected, resolved = profile_output(project_dir, target_name)
    database = resolved.get("database")
    schema = resolved.get("schema")
    if not isinstance(database, str) or not isinstance(schema, str):
        raise ValueError(f"target {selected!r} must set database and schema")
    params, inline_key, diagnostics = _connection_params(selected, resolved)
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
        diagnostics=diagnostics,
        inline_key=inline_key,
    )
