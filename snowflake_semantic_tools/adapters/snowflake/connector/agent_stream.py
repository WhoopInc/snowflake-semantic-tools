"""The Cortex Agents REST API on the connector's own session: threads, streamed runs, cancel.

Built to the Run API and Threads API pages and to what was measured on a live account. The
request authenticates with the session's own token, `Authorization: Snowflake
Token="<token>"`, against the account host with `_` spelled `-`, as the Cortex Agent SDK's
session-token authentication does; Snowflake then runs the call as the session's role and
ignores a role header, so none is sent. A session that expired -- measured: after 72 idle
minutes Snowflake answers HTTP 200 with a JSON body, code 390112, not an event stream, and a
401 with 390104 for a token it does not know -- is renewed by a cheap statement on the
connector's session, and the request is sent once more. Requests go
through the standard library with certifi's CA bundle, and honor `HTTPS_PROXY` as the
connector does.

A turn is a background run, streamed: that is what makes cancel work, and its events resume
after a sequence number. Each server-sent event line is bounded, so a payload the decoder
parses can never be unbounded.
"""

from __future__ import annotations

import json
import re
import ssl
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterator, Mapping
from typing import IO

import certifi

from snowflake_semantic_tools.adapters.snowflake.connector.session import Session, _as_port_errors, _execute
from snowflake_semantic_tools.domain.ask.events import AgentEvent, RunError, SseDecoder, decode
from snowflake_semantic_tools.domain.ports.snowflake.agent_stream import ORIGIN, AgentStreamPort, ThreadSummary
from snowflake_semantic_tools.domain.ports.snowflake.errors import SnowflakePortError, SnowflakeTransientError
from snowflake_semantic_tools.domain.sql import sql

# Error codes of a 401 that renewing the session cures: the token expired, or must be renewed.
_RENEWABLE = frozenset(("390104", "390112", "390114"))
# Thread deleted already, or never the user's.
_NO_THREAD = "399553"
# The longest server-sent event line read: a result set travels in one, so the bound is generous.
MAX_LINE_BYTES = 32 * 1024 * 1024
# A background run may stream for as long as its 15-minute budget; reads wait a little longer.
_READ_TIMEOUT_SECONDS = 960
_CALL_TIMEOUT_SECONDS = 60
_RUN_ID = re.compile(r"\d+-\d+")


class RestFailure(SnowflakePortError):
    """Snowflake answered a REST call with an error status.

    Attributes:
        status: The HTTP status.
        code: Snowflake's error code from the body; empty when it sent none.
        request_id: Snowflake's request id; empty when it sent none.
    """

    def __init__(self, status: int, message: str, code: str = "", request_id: str = "") -> None:
        super().__init__(f"HTTP {status}{f' {code}' if code else ''}: {message}")
        self.status = status
        self.code = code
        self.request_id = request_id
        self.detail = message


class RestClient:
    """Send REST calls to one account with a session token, renewing the session once when it expired.

    Args:
        base: The account's URL, `https://<host>`.
        token: Returns the session's current token.
        renew: Runs a statement on the session, which renews its token when it expired.
        opener: Opens a request, as `urllib.request.urlopen` does; tests replace it.
    """

    def __init__(
        self,
        base: str,
        token: Callable[[], str],
        renew: Callable[[], None],
        opener: Callable[..., IO[bytes]] | None = None,
    ) -> None:
        if not base.startswith("https://"):
            raise SnowflakePortError(f"the Cortex REST API is reached over HTTPS only, not {base!r}")
        self._base = base
        self._token = token
        self._renew = renew
        # build_opener keeps the default handlers, the proxy one among them, so HTTPS_PROXY applies.
        handler = urllib.request.HTTPSHandler(context=ssl.create_default_context(cafile=certifi.where()))
        self._open = opener or urllib.request.build_opener(handler).open

    def call(self, method: str, path: str, body: Mapping[str, object] | None = None) -> object:
        """Send one call and return its JSON answer; None for an empty one.

        An answer that is an expired session's error, though its status says success, renews
        the session and sends the call once more, as an error status does.

        Raises:
            RestFailure: Snowflake answered with an error status, or an error body.
            SnowflakeTransientError: the call could not be sent or answered.
        """
        raw, answer = self._answer(method, path, body)
        if _expired(answer):
            self._renew()
            raw, answer = self._answer(method, path, body)
            if _expired(answer):
                raise _failure(401, raw)
        return answer

    def _answer(self, method: str, path: str, body: Mapping[str, object] | None) -> tuple[bytes, object]:
        with self._opened(method, path, body, "application/json", _CALL_TIMEOUT_SECONDS) as response:
            raw = response.read()
        return raw, json.loads(raw) if raw.strip() else None

    def stream(self, method: str, path: str, body: Mapping[str, object] | None = None) -> Iterator[str]:
        """Send one call and yield each line of its event stream, line endings stripped.

        Raises:
            RestFailure: Snowflake answered with an error status before the stream began.
            SnowflakeTransientError: the call could not be sent, or the stream broke off.
            SnowflakePortError: a line ran past `MAX_LINE_BYTES`.
        """
        with self._opened(method, path, body, "text/event-stream", _READ_TIMEOUT_SECONDS) as response:
            while True:
                try:
                    line = response.readline(MAX_LINE_BYTES + 1)
                except OSError as exc:
                    raise SnowflakeTransientError(f"the event stream broke off: {exc}") from None
                if not line:
                    return
                if len(line) > MAX_LINE_BYTES:
                    raise SnowflakePortError(f"an event stream line ran past {MAX_LINE_BYTES} bytes")
                yield line.decode("utf-8").rstrip("\r\n")

    def _opened(
        self, method: str, path: str, body: Mapping[str, object] | None, accept: str, timeout: int
    ) -> IO[bytes]:
        """Open the call, renewing the session and sending it again once when its token expired."""
        try:
            return self._checked(self._send(method, path, body, accept, timeout), accept)
        except RestFailure as failure:
            if failure.code not in _RENEWABLE:
                raise
        self._renew()
        return self._checked(self._send(method, path, body, accept, timeout), accept)

    def _checked(self, response: IO[bytes], accept: str) -> IO[bytes]:
        """Return the response; refuse one that answers an event stream with a JSON error instead.

        Raises:
            RestFailure: the body is a JSON error, such as an expired session's 390112.
        """
        kind = str(getattr(response, "headers", {}).get("Content-Type", ""))
        if accept != "text/event-stream" or not kind.startswith("application/json"):
            return response
        with response:
            raw = response.read(MAX_LINE_BYTES)
        raise _failure(int(getattr(response, "status", 200)), raw)

    def _send(self, method: str, path: str, body: Mapping[str, object] | None, accept: str, timeout: int) -> IO[bytes]:
        data = None if body is None else json.dumps(body).encode("utf-8")
        request = urllib.request.Request(
            self._base + path,
            data=data,
            method=method,
            headers={
                "Authorization": f'Snowflake Token="{self._token()}"',
                "Content-Type": "application/json",
                "Accept": accept,
                "User-Agent": "snowflake-semantic-tools",
            },
        )
        try:
            return self._open(request, timeout=timeout)
        except urllib.error.HTTPError as exc:
            raise _failure(exc.code, exc.read()) from None
        except OSError as exc:
            raise SnowflakeTransientError(f"{method} {path} could not reach Snowflake: {exc}") from None


def _expired(answer: object) -> bool:
    """Report whether an answer is an expired session's error, whatever its status said."""
    return isinstance(answer, dict) and str(answer.get("code") or "") in _RENEWABLE


def _failure(status: int, raw: bytes) -> RestFailure:
    """Read Snowflake's error body: its message, code and request id, or the raw text."""
    text = raw.decode("utf-8", "replace")[:2000]
    try:
        body = json.loads(text)
    except ValueError:
        return RestFailure(status, text.strip() or "no message")
    if not isinstance(body, dict):
        return RestFailure(status, text.strip() or "no message")
    return RestFailure(
        status,
        str(body.get("message") or "no message"),
        str(body.get("code") or body.get("error_code") or ""),
        str(body.get("request_id") or ""),
    )


def events(lines: Iterator[str]) -> Iterator[AgentEvent]:
    """Decode an event stream's lines into events, in order; the last event is yielded at the end too."""
    decoder = SseDecoder()
    for line in lines:
        event = decoder.feed(line)
        if event is not None:
            yield decode(event)
    last = decoder.feed("")
    if last is not None:
        yield decode(last)


class AgentStreamMethods(Session, AgentStreamPort):
    """Run an agent from a specification over REST, on this session's token and role."""

    _client: RestClient | None = None

    def create_thread(self, origin: str = ORIGIN) -> int:
        """Open a thread named for `origin`; return its id."""
        answer = self._rest().call("POST", "/api/v2/cortex/threads", {"origin_application": origin})
        thread_id = answer.get("thread_id") if isinstance(answer, dict) else None
        if not isinstance(thread_id, int):
            raise SnowflakePortError("creating a thread answered no thread id")
        return thread_id

    def delete_thread(self, thread_id: int) -> bool:
        """Delete a thread; False when Snowflake says it is already gone."""
        try:
            self._rest().call("DELETE", f"/api/v2/cortex/threads/{int(thread_id)}")
        except RestFailure as failure:
            if failure.status == 404 or failure.code == _NO_THREAD:
                return False
            raise
        return True

    def list_threads(self, origin: str = ORIGIN) -> tuple[ThreadSummary, ...]:
        """Return the user's threads opened for `origin`."""
        query = urllib.parse.urlencode({"origin_application": origin})
        answer = self._rest().call("GET", f"/api/v2/cortex/threads?{query}")
        rows = answer if isinstance(answer, list) else []
        return tuple(
            ThreadSummary(row["thread_id"], str(row.get("origin_application") or ""), int(row.get("updated_on") or 0))
            for row in rows
            if isinstance(row, dict) and isinstance(row.get("thread_id"), int)
        )

    def run(self, body: Mapping[str, object]) -> Iterator[AgentEvent]:
        """Stream one background turn; an error status before the stream is one `RunError`."""
        sent = {**body, "stream": True, "background": True}
        try:
            yield from events(self._rest().stream("POST", "/api/v2/cortex/agent:run", sent))
        except RestFailure as failure:
            yield RunError(failure.detail, failure.code, failure.request_id)

    def resume(self, run_id: str, starting_after: int) -> Iterator[AgentEvent]:
        """Stream a run's events after `starting_after`."""
        path = f"/api/v2/cortex/agent/runs/{_checked(run_id)}?starting_after={int(starting_after)}"
        try:
            yield from events(self._rest().stream("GET", path))
        except RestFailure as failure:
            yield RunError(failure.detail, failure.code, failure.request_id)

    def cancel(self, run_id: str) -> int | None:
        """Cancel a run; the saved partial reply's id, or None when it had already ended (409)."""
        try:
            answer = self._rest().call("POST", f"/api/v2/cortex/agent/runs/{_checked(run_id)}/cancel")
        except RestFailure as failure:
            if failure.status == 409:
                return None
            raise
        meta = answer.get("metadata") if isinstance(answer, dict) else None
        found = meta.get("assistant_message_id") if isinstance(meta, dict) else None
        return found if isinstance(found, int) else None

    def _rest(self) -> RestClient:
        """Return the REST client for this session's account, made on first use."""
        if self._client is None:
            connection = self._connection
            base = f"https://{str(connection.host).replace('_', '-')}"
            self._client = RestClient(base, lambda: _token(connection.rest), self._renew)
        return self._client

    def _renew(self) -> None:
        """Run a statement, so the driver renews the session's token when it expired."""
        with _as_port_errors(), self._cursor() as cursor:
            _execute(cursor, sql("SELECT 1"))


def _token(rest: object) -> str:
    """Return the driver's session token, which the REST calls authenticate with.

    Raises:
        SnowflakePortError: the connection holds no session token, as a closed one does not.
    """
    token = getattr(rest, "token", None)
    if not isinstance(token, str) or not token:
        raise SnowflakePortError("the Snowflake session holds no token to call the REST API with")
    return token


def _checked(run_id: str) -> str:
    """Return a run id as the Run API spells one, `<thread>-<message>`; refuse anything else.

    Raises:
        SnowflakePortError: `run_id` is not two numbers joined by `-`.
    """
    if not _RUN_ID.fullmatch(run_id):
        raise SnowflakePortError(f"not a run id: {run_id!r}")
    return run_id
