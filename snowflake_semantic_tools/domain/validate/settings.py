"""Check an artifact's `config:` block: each key it sets must be a setting of its block, with a value.

A semantic view's or an agent's `config:` block is the only place the artifact overrides the
defaults its block and folder routes give it. Its keys are exactly the block's `+` keys,
written without the `+`, and a value is checked by the rules the configuration file applies
to that `+` key, so the two cannot drift.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping

from snowflake_semantic_tools.domain.diagnostics import D, Diagnostic, Origin
from snowflake_semantic_tools.domain.model.config_schema import block_settings
from snowflake_semantic_tools.domain.validate.config import value_problem


def item_config_diagnostics(
    block: str, config: object, *, subject: str, origin_of: Callable[[str], Origin]
) -> tuple[Diagnostic, ...]:
    """Report each key of one artifact's `config:` block that its block does not take, in written order.

    An absent or empty `config:` sets nothing. A block that is not a mapping is reported as a
    value of the wrong type, and nothing in it is read.

    Args:
        block: The routed block whose `+` keys the artifact may set, such as `agents`.
        config: The `config:` value as parsed; None when the artifact writes none.
        subject: The artifact's key, which every diagnostic names.
        origin_of: Where a key is written, given its name; `""` names the `config:` block.

    Diagnostics:
        SST-CFG004: `config:` is not a mapping, or a value is not of its key's type.
        SST-CFG053: a key is not one of the block's settings, or is written with its `+`.
        SST-CFG054: a key is written with no value.
        SST-CFG055: a key is one only the block root may set.
        SST-CFG008, SST-CFG023, and the other codes `value_problem` reports for a value outside
            its key's choices or bounds.
    """
    if config is None:
        return ()
    if not isinstance(config, Mapping):
        found = type(config).__name__
        return (D("SST-CFG004", origin=origin_of(""), subject=subject, key="config", expected="block", found=found),)
    settings = block_settings(block)
    allowed = ", ".join(name for name, spec in settings.items() if not spec.root_only)
    diagnostics: list[Diagnostic] = []
    for raw_key, value in config.items():
        name = str(raw_key)
        key = f"config.{name}"
        origin = origin_of(name)
        spec = settings.get(name)
        if spec is None:
            diagnostics.append(D("SST-CFG053", origin=origin, subject=subject, key=key, block=block, expected=allowed))
        elif spec.root_only:
            diagnostics.append(
                D("SST-CFG055", origin=origin, subject=subject, key=key, block=block, expected=spec.path)
            )
        elif value is None:
            diagnostics.append(D("SST-CFG054", origin=origin, subject=subject, key=key))
        else:
            problem = value_problem(spec, value, key=key, origin=origin, subject=subject)
            if problem is not None:
                diagnostics.append(problem)
    return tuple(diagnostics)
