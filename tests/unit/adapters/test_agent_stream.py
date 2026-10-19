"""The Cortex Agents REST adapter: headers, renewing an expired session once, threads, streams, cancel."""

from __future__ import annotations

import io
import itertools
import json
import urllib.error
import urllib.request
from collections.abc import Iterator
from email.message import Message
from typing import Any

import pytest

from snowflake_semantic_tools.adapters.snowflake.connector import SnowflakeConnector
from snowflake_semantic_tools.adapters.snowflake.connector.agent_stream import (
    MAX_LINE_BYTES,
    RestClient,
    RestFailure,
    _token,
    events,
)
from snowflake_semantic_tools.domain.ask.events import Done, Finished, MessageIds, RunError, TextDelta
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError, SnowflakeTransientError


class _Response(io.BytesIO):
    def __init__(self, body: bytes, kind: str = "application/json", status: int = 200) -> None:
        super().__init__(body)
        self.headers = Message()
        self.headers["Content-Type"] = kind
        self.status = status


def _error(status: int, body: dict[str, object] | str) -> urllib.error.HTTPError:
    raw = (json.dumps(body) if isinstance(body, dict) else body).encode()
    return urllib.error.HTTPError("https://x", status, "error", Message(), io.BytesIO(raw))


class _Opener:
    """Answer each request with the next scripted response or error; record what was sent."""

    def __init__(self, *answers: _Response | BaseException) -> None:
        self.answers = list(answers)
        self.sent: list[urllib.request.Request] = []

    def __call__(self, request: urllib.request.Request, timeout: int) -> Any:
        self.sent.append(request)
        answer = self.answers.pop(0)
        if isinstance(answer, BaseException):
            raise answer
        return answer


def _client(*answers: _Response | BaseException) -> tuple[RestClient, _Opener, list[str]]:
    opener = _Opener(*answers)
    renewed: list[str] = []
    tokens = itertools.chain(("first", "second"), itertools.repeat("later"))
    client = RestClient("https://acct", lambda: next(tokens), lambda: renewed.append("renewed"), opener=opener)
    return client, opener, renewed


def test_a_call_carries_the_session_token_and_no_role_header() -> None:
    client, opener, _ = _client(_Response(b'{"thread_id": 5}'))
    assert client.call("POST", "/api/v2/cortex/threads", {"origin_application": "sst_ask"}) == {"thread_id": 5}
    [request] = opener.sent
    headers = {key.casefold(): value for key, value in request.header_items()}
    assert headers["authorization"] == 'Snowflake Token="first"'
    assert "x-snowflake-role" not in headers and request.get_method() == "POST"
    assert json.loads(request.data or b"") == {"origin_application": "sst_ask"}  # type: ignore[arg-type]
    empty, _, _ = _client(_Response(b"  "))
    assert empty.call("DELETE", "/x") is None


@pytest.mark.parametrize(
    "expired",
    [
        pytest.param(_error(401, {"code": "390104", "message": "User must login again"}), id="401"),
        pytest.param(_Response(b'{"code": "390112", "message": "Your session has expired."}'), id="200-with-an-error"),
    ],
)
def test_an_expired_session_is_renewed_and_the_call_sent_once_more(expired: _Response | BaseException) -> None:
    client, opener, renewed = _client(expired, _Response(b'{"ok": true}'))
    assert client.call("GET", "/x") == {"ok": True}
    assert renewed == ["renewed"] and len(opener.sent) == 2
    assert dict(opener.sent[1].header_items())["Authorization"] == 'Snowflake Token="second"'


def test_a_session_that_stays_expired_or_another_error_is_raised() -> None:
    stuck, _, _ = _client(_Response(b'{"code": "390112"}'), _Response(b'{"code": "390112", "message": "expired"}'))
    with pytest.raises(RestFailure) as raised:
        stuck.call("GET", "/x")
    assert (raised.value.status, raised.value.code) == (401, "390112")
    refused, _, renewed = _client(_error(403, {"code": "399569", "message": "no tool", "request_id": "r"}))
    with pytest.raises(RestFailure) as raised:
        refused.call("GET", "/x")
    assert (raised.value.code, raised.value.request_id, raised.value.detail, renewed) == ("399569", "r", "no tool", [])
    for body in ("not json", '["a list"]', ""):
        odd, _, _ = _client(_error(500, body))
        with pytest.raises(RestFailure, match="HTTP 500"):
            odd.call("GET", "/x")
    unreachable, _, _ = _client(urllib.error.URLError("no route"))
    with pytest.raises(SnowflakeTransientError, match="could not reach Snowflake"):
        unreachable.call("GET", "/x")


def _sse(*lines: str) -> bytes:
    return ("\n".join(lines) + "\n").encode()


def test_a_stream_yields_its_lines_and_refuses_a_json_answer_or_an_unbounded_line() -> None:
    client, _, _ = _client(_Response(_sse("event: done", "data: [DONE]", ""), "text/event-stream"))
    assert list(client.stream("POST", "/run", {})) == ["event: done", "data: [DONE]", ""]
    expired, opener, renewed = _client(
        _Response(b'{"code": "390112", "message": "expired"}'),
        _Response(_sse("data: x"), "text/event-stream"),
    )
    assert list(expired.stream("POST", "/run", {})) == ["data: x"] and renewed == ["renewed"]
    huge, _, _ = _client(_Response(b"x" * (MAX_LINE_BYTES + 2), "text/event-stream"))
    with pytest.raises(SnowflakePortError, match="ran past"):
        list(huge.stream("GET", "/run"))

    class _Broken(_Response):
        def readline(self, size: int | None = -1) -> bytes:
            raise OSError("reset")

    broken, _, _ = _client(_Broken(b"", "text/event-stream"))
    with pytest.raises(SnowflakeTransientError, match="broke off"):
        list(broken.stream("GET", "/run"))


def test_events_decode_lines_and_a_last_event_without_a_blank_line() -> None:
    lines: Iterator[str] = iter(
        ["event: response.text.delta", 'data: {"text": "hi"}', "", "event: done", "data: [DONE]"]
    )
    assert list(events(lines)) == [TextDelta("hi"), Done()]


def _connector(*answers: _Response | BaseException) -> tuple[SnowflakeConnector, _Opener]:
    port = object.__new__(SnowflakeConnector)
    client, opener, _ = _client(*answers)
    port._client = client  # noqa: SLF001
    return port, opener


def test_threads_are_created_listed_and_deleted_and_a_gone_one_is_no_error() -> None:
    port, opener = _connector(
        _Response(b'{"thread_id": 9}'),
        _Response(b'[{"thread_id": 9, "origin_application": "sst_ask", "updated_on": 5}, {"other": 1}, "odd"]'),
        _Response(b""),
        _error(404, {"code": "399553", "message": "gone"}),
    )
    assert port.create_thread() == 9
    assert [(item.thread_id, item.updated_on) for item in port.list_threads()] == [(9, 5)]
    assert port.delete_thread(9) is True and port.delete_thread(9) is False
    assert opener.sent[1].full_url == "https://acct/api/v2/cortex/threads?origin_application=sst_ask"
    bad, _ = _connector(_Response(b'{"no": "id"}'), _error(500, {"code": "1"}))
    with pytest.raises(SnowflakePortError, match="no thread id"):
        bad.create_thread()
    with pytest.raises(RestFailure):
        bad.delete_thread(1)


def test_a_run_is_a_background_stream_and_a_refusal_is_one_error_event() -> None:
    stream = _sse(
        "event: metadata",
        'data: {"metadata": {"role": "user", "message_id": 2, "run_id": "9-2"}}',
        "",
        "event: response",
        'data: {"status": "completed", "metadata": {"run_id": "9-2", "assistant_message_id": 3}}',
        "",
    )
    port, opener = _connector(_Response(stream, "text/event-stream"), _error(403, {"code": "399569", "message": "no"}))
    assert list(port.run({"thread_id": 9})) == [MessageIds("user", 2, "9-2"), Finished("completed", "9-2", 3)]
    assert json.loads(opener.sent[0].data or b"") == {"thread_id": 9, "stream": True, "background": True}  # type: ignore[arg-type]
    assert list(port.run({})) == [RunError("no", "399569", "")]


def test_a_run_resumes_after_a_sequence_and_cancel_returns_the_saved_reply() -> None:
    port, opener = _connector(
        _Response(_sse("event: done", "data: [DONE]", ""), "text/event-stream"),
        _error(409, {"code": "399528", "message": "already completed"}),
        _Response(b'{"metadata": {"assistant_message_id": 11}}'),
        _error(409, {"code": "399528", "message": "already completed"}),
        _Response(b'{"metadata": "odd"}'),
        _error(500, {"message": "boom"}),
    )
    assert list(port.resume("9-2", 5)) == [Done()]
    assert opener.sent[0].full_url.endswith("/api/v2/cortex/agent/runs/9-2?starting_after=5")
    assert list(port.resume("9-2", 5)) == [RunError("already completed", "399528", "")]
    assert port.cancel("9-2") == 11 and port.cancel("9-2") is None and port.cancel("9-2") is None
    with pytest.raises(RestFailure):
        port.cancel("9-2")
    with pytest.raises(SnowflakePortError, match="not a run id"):
        port.cancel("9/../x")


def test_the_client_is_made_from_the_connection_and_a_missing_token_is_refused() -> None:
    port = object.__new__(SnowflakeConnector)

    class _Rest:
        token = "tok"

    class _Connection:
        host = "acct_name.snowflakecomputing.com"
        rest = _Rest()

    port._connection = _Connection()  # type: ignore[assignment]  # noqa: SLF001
    client = port._rest()  # noqa: SLF001
    assert client is port._rest() and client._base == "https://acct-name.snowflakecomputing.com"  # noqa: SLF001
    assert _token(_Rest()) == "tok"
    with pytest.raises(SnowflakePortError, match="no token"):
        _token(object())


def test_the_client_reaches_snowflake_over_https_only() -> None:
    with pytest.raises(SnowflakePortError, match="HTTPS only"):
        RestClient("http://acct", lambda: "t", lambda: None)
    assert RestClient("https://acct", lambda: "t", lambda: None)._open is not None  # noqa: SLF001
