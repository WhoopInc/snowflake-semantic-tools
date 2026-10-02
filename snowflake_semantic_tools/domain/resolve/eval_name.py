"""Eval object names: the closed template grammar, and the probe render validators check shapes with.

Dataset, source table and run names are templates over four tokens -- `agent` (optionally
`| upper`), `sha7`, `variant` and `ts` -- resolved here without Jinja, so a template can
name nothing else. Before any real commit or timestamp exists, validators render a
template at fixed probe values of the real widths, so a probed name is as long as a real
one.
"""

from __future__ import annotations

import re

_EVAL_NAME_TOKEN = re.compile(r"{{\s*(agent(?:\s*\|\s*upper)?|sha7|variant|ts)\s*}}")
_AGENT_TOKEN = re.compile(r"{{\s*agent(?:\s*\|\s*upper)?\s*}}")
PROBE_SHA7 = "0000000"
PROBE_VARIANT = "ci"
PROBE_TS = "20000101T000000Z"
# The longest dataset or source table name SST accepts (SST-VAL702, SST-PRS010).
NAME_LIMIT = 128


def render_eval_name_template(
    template: str,
    *,
    agent: str,
    sha7: str,
    variant: str | None = None,
    ts: str | None = None,
) -> str:
    """Resolve the closed eval-name token grammar without invoking Jinja.

    Raises:
        ValueError: The template uses a token given no value, or any other `{{ }}` expression.
    """

    values = {"agent": agent, "sha7": sha7, "variant": variant, "ts": ts}

    def replace(match: re.Match[str]) -> str:
        token = match.group(1)
        if "|" in token:
            return agent.upper()
        value = values[token]
        if value is None:
            raise ValueError(f"eval name token {token!r} has no value")
        return value

    rendered = _EVAL_NAME_TOKEN.sub(replace, template)
    if "{{" in rendered or "}}" in rendered:
        raise ValueError("eval name template contains an unsupported expression")
    return rendered


def has_agent_token(template: str) -> bool:
    """Report whether a template names its agent, as `{{ agent }}` or `{{ agent | upper }}`."""
    return _AGENT_TOKEN.search(template) is not None


def probe_name(template: str, agent: str, variant: str | None = None) -> str | None:
    """Render a name template at the probe values to check its shape; None if it cannot render.

    A template that cannot render is reported where it is parsed, so its shape is not
    checked. `variant` falls back to `ci`, as it does for a real run.
    """
    try:
        return render_eval_name_template(
            template, agent=agent, sha7=PROBE_SHA7, variant=variant or PROBE_VARIANT, ts=PROBE_TS
        )
    except ValueError:
        return None
