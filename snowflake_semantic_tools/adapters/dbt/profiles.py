"""Resolve one dbt profile target into connection and lifecycle values.

Which `profiles.yml` and which configuration file are read is decided by `ProjectPaths`; this
module never looks for either itself.
"""

from __future__ import annotations

import base64
import binascii
import os
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any, NoReturn

import yaml

from snowflake_semantic_tools.adapters import bounded_yaml
from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.adapters.locations import ProjectPaths
from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.config_schema import target_text
from snowflake_semantic_tools.domain.model.dbt import DbtTarget
from snowflake_semantic_tools.domain.model.identifier import Identifier, QualifiedName, TargetIdentity
from snowflake_semantic_tools.domain.state.lock import HYBRID_TABLES, STANDARD_TABLES, TABLE_KIND_SETTING

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
# Credentials: a value written for one of these that renders empty is refused, never dropped.
_SECRET_KEYS = frozenset(("password", "token", "private_key", "private_key_passphrase", "private_key_file_pwd"))
# Read by SST, and silently left to account defaults when absent.
_DEFAULTED_KEYS = ("role", "warehouse")
_INTEGER_KEYS = frozenset(("port", "connect_timeout"))
_BOOLEAN_KEYS = frozenset(("insecure_mode", "client_session_keep_alive"))


def _resolve_env(value: object) -> object:
    """Render every `{{ env_var('NAME') }}` in a string; other templates are left in place.

    dbt's `as_number`, `as_bool`, and `as_native` filters type a value that is one
    template on its own; inside a longer string the rendered text is used.

    Raises:
        ProjectError: a variable is unset and the call gives no default (SST-CFG013).

    Diagnostics:
        SST-CFG013: `env_var()` names an unset variable with no default; raised.
    """
    if not isinstance(value, str):
        return value

    def substitute(match: re.Match[str]) -> str:
        name, default, _ = match.groups()
        if name in os.environ:
            return os.environ[name]
        if default is not None:
            return default
        diagnostic = D("SST-CFG013", origin=Origin("profiles.yml"), subject="config:profiles.yml", var=name)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))

    whole = _ENV_VAR.fullmatch(value.strip())
    if whole is None:
        return _ENV_VAR.sub(substitute, value)
    text = substitute(whole)
    if whole.group(3) in ("as_number", "as_bool", "as_native"):
        try:
            typed = bounded_yaml.safe_load(text) if text.strip() else text
        except yaml.YAMLError:
            # Not a YAML scalar at all, so it stays the text, as any value that is not one does.
            return text
        return typed if isinstance(typed, (bool, int, float)) else text
    return text


def read_plain_yaml(path: Path) -> dict[str, Any]:
    """Read one YAML mapping as plain YAML: `dbt_project.yml`, or the configuration file for its profile.

    dbt's files are dbt's YAML, so anchors, aliases and merge keys are allowed here; SST's strict
    parser, which refuses them (SST-LOD013, SST-LOD014), is for SST's own files.

    Raises:
        ProjectError: the file cannot be read (SST-PRT009), is not valid YAML (SST-CFG002), or does
            not hold a mapping (SST-LOD002).

    Diagnostics:
        SST-PRT009: the file cannot be read; raised.
        SST-CFG002: the file is not valid YAML; raised.
        SST-LOD002: the file's root is not a mapping; raised.
    """
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        unreadable = D("SST-PRT009", path=str(path), detail=str(exc))
        raise ProjectError(unreadable.message, diagnostics=(unreadable,)) from exc
    try:
        value = bounded_yaml.safe_load(text)
    except yaml.YAMLError as exc:
        diagnostic = D(
            "SST-CFG002", origin=Origin(path.name), subject=f"config:{path.name}", path=str(path), detail=str(exc)
        )
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,)) from exc
    if not isinstance(value, dict):
        diagnostic = D("SST-LOD002", subject=f"config:{path.name}", file=path.name, found=type(value).__name__)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    return {str(key): item for key, item in value.items()}


def _read_profiles(path: Path) -> dict[str, Any]:
    """Read the `profiles.yml` at `path` as dbt does, as plain YAML.

    Raises:
        ProjectError: The file is not YAML, or not a mapping of profiles (SST-DBT019).
        OSError: The file cannot be read.

    Diagnostics:
        SST-DBT019: the file does not parse, or its root is not a mapping; raised.
    """
    text = path.read_text(encoding="utf-8")
    try:
        value = bounded_yaml.safe_load(text)
    except yaml.YAMLError as exc:
        mark = getattr(exc, "problem_mark", None)
        where = f"line {mark.line + 1}: " if mark is not None else ""
        _refuse_profiles(f"{where}{getattr(exc, 'problem', None) or exc}", exc)
    if not isinstance(value, dict):
        _refuse_profiles(f"the root is {type(value).__name__}, not a mapping of profiles")
    return {str(key): item for key, item in value.items()}


def _refuse_profiles(detail: str, cause: Exception | None = None) -> NoReturn:
    diagnostic = D(
        "SST-DBT019", origin=Origin("profiles.yml"), subject="config:profiles.yml", path="profiles.yml", detail=detail
    )
    raise ProjectError(diagnostic.message, diagnostics=(diagnostic,)) from cause


class ProfileTarget:
    """One resolved dbt target: its connection arguments, identity, and state table.

    An inline `private_key` is held undecoded and decoded only when `connection_params` is
    read, so resolving a target for an offline compile needs no usable key.
    """

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
        state_table_kind: str = STANDARD_TABLES,
    ) -> None:
        self.profile_name = profile_name
        self.target_name = target_name
        self._params = connection_params
        self.identity = identity
        self.state_table = state_table
        # `standard` or `hybrid`, as `state.+table_kind` sets; `connection_params` carries it.
        self.state_table_kind = state_table_kind
        self.diagnostics = diagnostics
        # Decoded only when a connection is opened, so an offline compile needs no key.
        self._inline_key = inline_key

    @property
    def connection_params(self) -> dict[str, object]:
        """Return a fresh copy of the connector arguments, with any inline key decoded to DER.

        Read only to open a connection, so an offline run never needs a credential.

        Raises:
            ProjectError: `account` or `user` is absent (SST-CFG012), or the inline
                `private_key` cannot be read (SST-CFG050).

        Diagnostics:
            SST-CFG012: the target sets no `account` or no `user`; raised.
            SST-CFG050: the inline `private_key` cannot be read; raised.
        """
        for field in ("account", "user"):
            if not self._params.get(field):
                _refuse_profile("SST-CFG012", self.profile_name, field=field)
        params = dict(self._params)
        if self._inline_key is not None:
            value, passphrase = self._inline_key
            params["private_key"] = _inline_private_key(self.target_name, value, passphrase)
        return params

    def with_session_parameters(self, parameters: Mapping[str, object]) -> ProfileTarget:
        """Return this target with `parameters` added to the session parameters every connection sets."""
        params = dict(self._params)
        session = params.get("session_parameters")
        params["session_parameters"] = {**(session if isinstance(session, dict) else {}), **parameters}
        return ProfileTarget(
            profile_name=self.profile_name,
            target_name=self.target_name,
            connection_params=params,
            identity=self.identity,
            state_table=self.state_table,
            diagnostics=self.diagnostics,
            inline_key=self._inline_key,
            state_table_kind=self.state_table_kind,
        )

    @property
    def connection_warnings(self) -> tuple[Diagnostic, ...]:
        """Report each setting a connection would leave to the account's defaults.

        Diagnostics:
            SST-CFG014: the target sets no `role` or no `warehouse`.
        """
        return tuple(
            D(
                "SST-CFG014",
                origin=Origin("profiles.yml"),
                subject="config:profiles.yml",
                profile=self.profile_name,
                field=key,
            )
            for key in _DEFAULTED_KEYS
            if not self._params.get(key)
        )

    @property
    def secrets(self) -> tuple[str, ...]:
        """Return every credential value this target holds, so output can refuse to print one."""
        values = [str(self._params[key]) for key in sorted(_SECRET_KEYS) if self._params.get(key)]
        if self._inline_key is not None:
            values.extend(str(part) for part in self._inline_key if part not in (None, ""))
        return tuple(values)

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


def resolve_profile_name(files: ProjectPaths) -> str:
    """Return the `profiles.yml` profile: dbt's `profile:`, or `project.target_profile` without dbt.

    Raises:
        ProjectError: neither names a profile, or the two disagree (SST-CFG056); a file cannot be
            read as YAML, as `read_plain_yaml` says.

    Diagnostics:
        SST-CFG056: neither names a profile, or the two disagree; raised.
    """
    config = read_plain_yaml(files.config_file) if files.config_file is not None else {}
    project_block = config.get("project")
    configured = project_block.get("target_profile") if isinstance(project_block, dict) else None
    dbt_project_path = files.project_dir / "dbt_project.yml"
    if not dbt_project_path.is_file():
        if not isinstance(configured, str) or not configured:
            _unnamed("the project has no dbt_project.yml, and project.target_profile is not set", files.config_name)
        return configured
    profile_name = read_plain_yaml(dbt_project_path).get("profile")
    if not isinstance(profile_name, str) or not profile_name:
        _unnamed("dbt_project.yml declares no profile", "dbt_project.yml")
    if configured is not None and configured != profile_name:
        _unnamed(
            f"project.target_profile {configured!r} disagrees with dbt_project.yml profile {profile_name!r}",
            files.config_name,
        )
    return profile_name


def _unnamed(reason: str, file: str) -> NoReturn:
    diagnostic = D("SST-CFG056", origin=Origin(file), subject=f"config:{file}", reason=reason)
    raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))


def declared_targets(files: ProjectPaths) -> tuple[str, frozenset[str], str | None]:
    """Return the profile's name, every target it declares, and its default `target:`.

    The default target is rendered as `_default_target` renders it.

    Raises:
        ProjectError: no `profiles.yml` exists (SST-CFG009), it does not declare the profile
            (SST-CFG010), or its `target:` cannot be rendered (SST-CFG013, SST-CFG049).

    Diagnostics:
        SST-CFG009: no `profiles.yml` exists at any searched location; raised.
        SST-CFG010: `profiles.yml` does not declare the profile; raised.
        SST-CFG013: `target:` calls `env_var()` on an unset variable with no default; raised.
        SST-CFG049: `target:` holds a template other than `env_var()`; raised.
    """
    profile_name = resolve_profile_name(files)
    profile = _read_profiles(files.profiles_file()).get(profile_name)
    if not isinstance(profile, dict):
        diagnostic = D("SST-CFG010", subject="config:profiles.yml", target="(any)", profile=profile_name)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    outputs = profile.get("outputs")
    targets = frozenset(str(key) for key in outputs) if isinstance(outputs, dict) else frozenset()
    return profile_name, targets, _default_target(profile)


def _default_target(profile: Mapping[str, object]) -> str | None:
    """Return a profile's `target:` with each `env_var()` rendered, as dbt renders it; None when unset.

    Raises:
        ProjectError: the value holds another template (SST-CFG049), or names an unset variable
            with no default (SST-CFG013).

    Diagnostics:
        SST-CFG049: `target:` holds a template other than `env_var()`; raised.
        SST-CFG013: `target:` calls `env_var()` on an unset variable with no default; raised.
    """
    value = profile.get("target")
    if not isinstance(value, str):
        return None
    if _TEMPLATE.search(_ENV_VAR.sub("", value)):
        _refuse("SST-CFG049", value, key="target", problem="holds a template other than env_var()")
    return str(_resolve_env(value))


def profile_output(
    files: ProjectPaths, target_name: str | None = None, *, profile: str | None = None
) -> tuple[str, str, dict[str, object]]:
    """Return the profile name, target name, and fields of one output, with `env_var()` rendered.

    A field SST ignores is left as written, so an unset variable or a dbt-only
    filter there cannot stop a run. `profile` names the profile instead of the project.

    Raises:
        ProjectError: the target is not declared, is not a Snowflake target, or holds a value
            SST cannot use.

    Diagnostics:
        SST-CFG009: no `profiles.yml` exists at any searched location; raised.
        SST-CFG010: the profile has no such target; raised.
        SST-CFG011: the target's `type` is not `snowflake`; raised.
        SST-CFG013: an `env_var()` SST reads, the default `target:` among them, is unset and has
            no default; raised.
        SST-CFG049: a field SST reads, the default `target:` among them, holds a template other
            than `env_var()`; raised.
        SST-PRT011: a credential written for the target renders empty; raised.
    """
    profile_name = profile or resolve_profile_name(files)
    profiles = _read_profiles(files.profiles_file())
    declared = profiles.get(profile_name)
    if not isinstance(declared, dict):
        diagnostic = D(
            "SST-CFG010", subject="config:profiles.yml", target=target_name or "(default)", profile=profile_name
        )
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    selected = target_name or _default_target(declared)
    outputs = declared.get("outputs")
    output = outputs.get(selected) if isinstance(outputs, dict) else None
    if not isinstance(selected, str) or not isinstance(output, dict):
        diagnostic = D("SST-CFG010", target=selected, profile=profile_name)
        raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    adapter = output.get("type")
    if adapter not in (None, "snowflake"):
        _refuse_profile("SST-CFG011", profile_name, found=str(adapter))
    for key, value in output.items():
        # Checked as written, so a rendered secret that happens to contain `{{` is fine.
        if str(key) in _READ_KEYS and isinstance(value, str) and _TEMPLATE.search(_ENV_VAR.sub("", value)):
            _refuse("SST-CFG049", selected, key=str(key), problem="holds a template other than env_var()")
    resolved = {str(key): (_resolve_env(value) if str(key) in _READ_KEYS else value) for key, value in output.items()}
    for key in sorted(_SECRET_KEYS & set(resolved)):
        if output[key] not in (None, "") and resolved[key] in (None, ""):
            diagnostic = D(
                "SST-PRT011", origin=Origin("profiles.yml"), subject="config:profiles.yml", value=f"{selected}.{key}"
            )
            raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))
    return profile_name, selected, resolved


def _refuse_profile(code: str, profile: str, **context: Any) -> NoReturn:
    diagnostic = D(code, origin=Origin("profiles.yml"), subject="config:profiles.yml", profile=profile, **context)
    raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))


def _refuse(code: str, target: str, **context: Any) -> NoReturn:
    diagnostic = D(code, origin=Origin("profiles.yml"), subject="config:profiles.yml", target=target, **context)
    raise ProjectError(diagnostic.message, diagnostics=(diagnostic,))


def _checked(target: str, key: str, value: object) -> object:
    """Return a whole-number or boolean field as its type, also read from text; others as given.

    A YAML `true` is not a whole number.

    Diagnostics:
        SST-CFG049: a whole-number field is not one, or a boolean field is not true or false; raised.
    """
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
    """Translate a dbt-snowflake output's fields into the Snowflake connector's arguments.

    A field that is absent, None or empty is not read. The steps run in a fixed order, so the
    first problem found is the one raised: refused authentication, then each field in the
    output's order, then the inline key.

    Returns:
        The connector arguments; an inline `private_key` with its passphrase, returned apart
        to be decoded when a connection is opened, or None; a report per field SST ignores.

    Diagnostics:
        SST-CFG050: the output asks for a refresh-token exchange, sets two private keys, or sets
            an inline key that is not a string; raised.
        SST-CFG049: a whole-number or boolean field has another value; raised.
        SST-CFG048: a field SST does not read; returned.
    """
    present = {key: value for key, value in output.items() if value not in (None, "")}
    _refuse_unusable_auth(target, present)
    params, diagnostics = _connector_arguments(target, present)
    inline_key = _inline_key(target, present)
    passphrase = present.get("private_key_passphrase")
    if inline_key is None and passphrase is not None and "private_key_file" in params:
        params.setdefault("private_key_file_pwd", _checked(target, "private_key_passphrase", passphrase))
    # dbt reads `token` only for OAuth; imply it only when nothing else authenticates.
    other = inline_key is not None or any(key in params for key in ("password", "private_key_file"))
    if "token" in params and "authenticator" not in params and not other:
        params["authenticator"] = "oauth"
    return params, inline_key, diagnostics


def _refuse_unusable_auth(target: str, present: Mapping[str, object]) -> None:
    """Refuse authentication SST cannot use: a refresh-token exchange, or two private keys.

    Diagnostics:
        SST-CFG050: `oauth_client_id` or `oauth_client_secret` is set, an inline key and a key
            file are both set, or `private_key_path` and `private_key_file` differ; raised.
    """
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


def _connector_arguments(
    target: str, present: Mapping[str, object]
) -> tuple[dict[str, object], tuple[Diagnostic, ...]]:
    """Pass each connection field through under the connector's name, checking its type.

    A field SST neither passes through, reads apart, nor ignores as dbt-only is reported and
    dropped.

    Diagnostics:
        SST-CFG049: a whole-number or boolean field has another value; raised.
        SST-CFG048: a field SST does not read; returned.
        SST-CFG051: `insecure_mode` is true, so the connection skips OCSP revocation checks;
            returned, and the setting is still honored.
    """
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
    if params.get("insecure_mode") is True:
        diagnostics.append(D("SST-CFG051", origin=Origin("profiles.yml"), subject="config:profiles.yml", target=target))
    return params, tuple(diagnostics)


def _inline_key(target: str, present: Mapping[str, object]) -> tuple[str, object] | None:
    """Return an inline `private_key`, undecoded, with its passphrase; None when there is none.

    Diagnostics:
        SST-CFG050: `private_key` is not a string; raised.
    """
    if "private_key" not in present:
        return None
    key_value = present["private_key"]
    if not isinstance(key_value, str):
        _refuse("SST-CFG050", target, detail="private_key must be a PEM or base64-encoded DER key")
    return (key_value, present.get("private_key_passphrase"))


def load_profile_target(
    files: ProjectPaths,
    target_name: str | None = None,
    *,
    profile: str | None = None,
    state: Mapping[str, object] | None = None,
) -> ProfileTarget:
    """Resolve one profiles.yml target into its connection arguments, identity, and state table.

    `target_name` None selects the profile's default target, and `profile` None the profile the
    project names. The state table is `SST_STATE` in the target's database and schema unless
    `state`, the resolved configuration's `state:` block, overrides a part.

    Raises:
        OSError: profiles.yml cannot be read.
        ValueError: the state table the `state:` block names is not a valid name.
        ProjectError: profiles.yml cannot be found or parsed, the profile cannot be named, or the
            target is absent, sets no database or schema, or holds a value SST cannot use.

    Diagnostics:
        SST-CFG010: the profile has no such target; raised.
        SST-CFG012: the target sets no database or no schema; raised.
        SST-CFG049: a field holds a template other than env_var(), a value of the wrong type, or a
            database or schema that is not a valid identifier; raised.
        SST-CFG050: the target's authentication cannot be used; raised.
        SST-CFG048: a field SST does not read; carried on the target.
    """
    profile_name, selected, resolved = profile_output(files, target_name, profile=profile)
    database, schema = target_location(profile_name, selected, resolved)
    for key, value in (("database", database), ("schema", schema)):
        try:
            Identifier.parse(value)
        except ValueError:
            _refuse("SST-CFG049", selected, key=key, problem=f"{value!r} is not a valid identifier")
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
    state_table = _state_table(state, database, schema)
    kind = _state_table_kind(state)
    if kind == HYBRID_TABLES:
        # Every session SST opens on the target learns the kind from its settings, and reads a
        # hybrid table's latest writes rather than one up to 100 ms stale.
        params[TABLE_KIND_SETTING] = kind
        session = params.get("session_parameters")
        params["session_parameters"] = {**(session if isinstance(session, dict) else {}), "READ_LATEST_WRITES": True}
    return ProfileTarget(
        profile_name=profile_name,
        target_name=selected,
        connection_params=params,
        identity=identity,
        state_table=state_table,
        diagnostics=diagnostics,
        inline_key=inline_key,
        state_table_kind=kind,
    )


def target_location(profile: str, target: str, fields: Mapping[str, object]) -> tuple[str, str]:
    """Return the database and schema a resolved profile target sets, refusing it when either is unset.

    Raises:
        ProjectError: the target sets no database or no schema (SST-CFG012).

    Diagnostics:
        SST-CFG012: the target sets no database, no schema, or neither; raised.
    """
    database, schema = fields.get("database"), fields.get("schema")
    missing = [key for key, value in (("database", database), ("schema", schema)) if value is None or str(value) == ""]
    if missing:
        _refuse_profile("SST-CFG012", profile, field=f"{' and '.join(missing)} on target '{target}'")
    return str(database), str(schema)


def _state_table_kind(state_config: Mapping[str, object] | None) -> str:
    """Return the kind `state.+table_kind` names, `hybrid` or `standard`; `standard` when unset.

    Any other value is refused where the configuration is validated (SST-CFG008).
    """
    block = state_config if isinstance(state_config, Mapping) else {}
    return HYBRID_TABLES if str(block.get("+table_kind") or "").strip().lower() == HYBRID_TABLES else STANDARD_TABLES


def _state_table(state_config: Mapping[str, object] | None, database: str, schema: str) -> QualifiedName:
    """Locate the state table: `SST_STATE` in the target's database and schema by default.

    The `state:` block overrides each part: `+table` with `env_var()` rendered, and
    `+database` and `+schema` with their `{{ target.* }}` templates rendered as written in the
    profile; a part that is unset or renders empty keeps its default.
    """
    block = state_config if isinstance(state_config, Mapping) else {}
    raw_table = block.get("+table")
    state_name = str(_resolve_env(raw_table)) if raw_table else "SST_STATE"
    target = DbtTarget(database, schema)
    return QualifiedName.from_parts(
        target_text(block.get("+database"), target, database) or database,
        target_text(block.get("+schema"), target, schema) or schema,
        state_name,
    )
