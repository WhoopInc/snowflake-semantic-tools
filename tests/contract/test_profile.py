from __future__ import annotations

import base64
from pathlib import Path

import pytest

from snowflake_semantic_tools.adapters.profile import load_profile_target
from snowflake_semantic_tools.adapters.project import ProjectError


def test_profile_target_resolves_env_and_fixed_state_location(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "dbt_project.yml").write_text("profile: test\n", encoding="utf-8")
    (tmp_path / "profiles.yml").write_text(
        "test:\n  target: verify\n  outputs:\n    verify:\n      type: snowflake\n"
        "      account: \"{{ env_var('ACCOUNT') }}\"\n      user: user\n      database: SCRATCH\n"
        "      schema: SST_1_REFERENCE_IMPL\n      query_tag: SST_1_REFERENCE_IMPL\n",
        encoding="utf-8",
    )
    (tmp_path / "sst_config.yml").write_text(
        'state:\n  +table: SST_STATE\n  +database: "{{ target.database }}"\n  +schema: "{{ target.schema }}"\n',
        encoding="utf-8",
    )
    monkeypatch.setenv("ACCOUNT", "acct")
    value = load_profile_target(tmp_path)
    assert value.identity.scope.sql == "SCRATCH.SST_1_REFERENCE_IMPL"
    assert value.state_table.sql == "SCRATCH.SST_1_REFERENCE_IMPL.SST_STATE"
    assert value.connection_params["session_parameters"] == {"QUERY_TAG": "SST_1_REFERENCE_IMPL"}


def test_profile_target_fails_closed_on_missing_env(tmp_path: Path) -> None:
    (tmp_path / "dbt_project.yml").write_text("profile: test\n", encoding="utf-8")
    (tmp_path / "profiles.yml").write_text(
        "test:\n  target: x\n  outputs:\n    x:\n      type: snowflake\n"
        "      account: \"{{ env_var('MISSING') }}\"\n      database: DB\n      schema: S\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="MISSING"):
        load_profile_target(tmp_path)


def target_with(tmp_path: Path, fields: str) -> Path:
    (tmp_path / "dbt_project.yml").write_text("profile: test\n", encoding="utf-8")
    (tmp_path / "profiles.yml").write_text(
        "test:\n  target: x\n  outputs:\n    x:\n      type: snowflake\n      account: acct\n"
        "      database: DB\n      schema: S\n" + "".join(f"      {line}\n" for line in fields.splitlines()),
        encoding="utf-8",
    )
    return tmp_path


def refused(tmp_path: Path, fields: str) -> tuple[str, str]:
    with pytest.raises(ProjectError) as caught:
        load_profile_target(target_with(tmp_path, fields))
    diagnostic = caught.value.diagnostics[0]
    return diagnostic.code, diagnostic.message


def test_dbt_snowflake_fields_map_to_connector_arguments(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KEY_DIR", "/keys")
    monkeypatch.setenv("SUFFIX", "dev")
    target = load_profile_target(
        target_with(
            tmp_path,
            "user: \"svc_{{ env_var('SUFFIX') }}\"\n"
            "private_key_path: \"{{ env_var('KEY_DIR') }}/rsa.p8\"\n"
            "private_key_passphrase: secret\n"
            'connect_timeout: "30"\n'
            'client_session_keep_alive: "true"\n'
            "port: 443\nhost: acct.snowflakecomputing.com\n"
            "threads: \"{{ env_var('UNSET_THREADS') | as_number }}\"\n"
            "retry_all: true\ncolour: blue\n",
        )
    )
    params = target.connection_params
    assert params["user"] == "svc_dev"
    assert params["private_key_file"] == "/keys/rsa.p8" and params["private_key_file_pwd"] == "secret"
    assert (params["login_timeout"], params["client_session_keep_alive"], params["port"]) == (30, True, 443)
    assert "threads" not in params and "retry_all" not in params and "private_key_path" not in params
    assert [(item.code, item.message) for item in target.diagnostics] == [
        ("SST-CFG048", "target 'x': 'colour' is not a setting SST reads, so it is ignored")
    ]
    assert target.authentication == "key pair (private key file)"

    token = load_profile_target(target_with(tmp_path, "token: abc"))
    assert token.connection_params["authenticator"] == "oauth" and token.authentication == "OAuth access token"
    sso = load_profile_target(target_with(tmp_path, "authenticator: externalbrowser"))
    assert sso.authentication == "externalbrowser"
    assert load_profile_target(target_with(tmp_path, "password: p")).authentication == "password"
    assert load_profile_target(target_with(tmp_path, "")).authentication == "connector default"


def test_profile_values_sst_cannot_use_are_refused(tmp_path: Path) -> None:
    assert refused(tmp_path, "port: \"{{ env_var('PORT', '443') | int }}\"") == (
        "SST-CFG049",
        "target 'x': 'port' holds a template other than env_var()",
    )
    assert refused(tmp_path, 'role: "{% if true %}R{% endif %}"')[1] == (
        "target 'x': 'role' holds a template other than env_var()"
    )
    assert refused(tmp_path, "port: many")[1] == "target 'x': 'port' must be a whole number"
    assert refused(tmp_path, "insecure_mode: sometimes")[1] == "target 'x': 'insecure_mode' must be true or false"
    assert refused(tmp_path, "oauth_client_id: id\noauth_client_secret: s\ntoken: refresh") == (
        "SST-CFG050",
        "target 'x': oauth_client_id and oauth_client_secret ask dbt to exchange a refresh token, which SST does not do",
    )
    assert refused(tmp_path, "private_key: abc\nprivate_key_path: /k.p8")[1] == (
        "target 'x': private_key and a private key file are both set"
    )
    assert refused(tmp_path, "private_key_path: /a.p8\nprivate_key_file: /b.p8")[1] == (
        "target 'x': private_key_path and private_key_file name different files"
    )


def test_an_inline_private_key_becomes_der_bytes_and_is_never_echoed(tmp_path: Path) -> None:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    der = key.private_bytes(serialization.Encoding.DER, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    plain = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    ).decode()
    encrypted = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.BestAvailableEncryption(b"pass"),
    ).decode()

    def inline(pem: str, extra: str = "") -> str:
        return "private_key: |\n" + "".join(f"  {line}\n" for line in pem.strip().splitlines()) + extra

    assert load_profile_target(target_with(tmp_path, inline(plain))).connection_params["private_key"] == der
    decrypted = load_profile_target(target_with(tmp_path, inline(encrypted, "private_key_passphrase: pass")))
    assert decrypted.connection_params["private_key"] == der
    assert decrypted.authentication == "key pair (private_key)"
    encoded = base64.b64encode(der).decode()
    assert load_profile_target(target_with(tmp_path, f"private_key: {encoded}")).connection_params["private_key"] == der
    # dbt accepts base64 DER wrapped across lines.
    wrapped = "".join(f"  {encoded[index:index + 64]}\n" for index in range(0, len(encoded), 64))
    assert (
        load_profile_target(target_with(tmp_path, "private_key: |\n" + wrapped)).connection_params["private_key"] == der
    )

    def refused_on_connect(fields: str) -> tuple[str, str]:
        # The key is decoded only when a connection opens, so loading succeeds.
        target = load_profile_target(target_with(tmp_path, fields))
        assert target.authentication == "key pair (private_key)"
        with pytest.raises(ProjectError) as caught:
            target.connection_params
        diagnostic = caught.value.diagnostics[0]
        return diagnostic.code, diagnostic.message

    code, message = refused_on_connect(inline(encrypted, "private_key_passphrase: wrong"))
    assert (code, message) == ("SST-CFG050", "target 'x': private_key cannot be read (ValueError)")
    code, message = refused_on_connect("private_key: not-base64!")
    assert code == "SST-CFG050" and "not-base64" not in message


def test_dbt_filters_secrets_with_braces_and_token_precedence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PORT", "8443")
    monkeypatch.setenv("KEEP", "True")
    monkeypatch.setenv("SECRET", "hunter{{2")
    target = load_profile_target(
        target_with(
            tmp_path,
            "port: \"{{ env_var('PORT') | as_number }}\"\n"
            "client_session_keep_alive: \"{{ env_var('KEEP', 'false') | as_bool }}\"\n"
            "password: \"{{ env_var('SECRET') }}\"\n"
            "token: abc",
        )
    )
    params = target.connection_params
    assert (params["port"], params["client_session_keep_alive"]) == (8443, True)
    # A rendered secret may contain `{{`; only the authored value is checked for templates.
    assert params["password"] == "hunter{{2"
    # With a password present, a token does not switch the login to OAuth.
    assert "authenticator" not in params and target.authentication == "password"
