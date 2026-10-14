"""Check a parsed `sst_config.yml` tree against the key table in `keys.py`.

The walk visits each block once, in document order. Every entry resolves to its
declared key, or to the `<name>` or `<route>` slot its block's child policy allows;
anything else is reported, never ignored.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from snowflake_semantic_tools.domain.diagnostics import (
    ERROR_REGISTRY,
    D,
    Diagnostic,
    DiagnosticBag,
    Origin,
    Severity,
    override_refusal,
)
from snowflake_semantic_tools.domain.model.config_schema.keys import (
    CHILDREN,
    CONFIG_FILE,
    CONFIG_KEYS,
    TOP_LEVEL_KEYS,
    ChildPolicy,
    ConfigKey,
    KeyKind,
    KeyStatus,
)
from snowflake_semantic_tools.domain.model.tool import ToolCatalog, vocabulary_key
from snowflake_semantic_tools.domain.parse.template import TemplateSyntaxError, scan_template_calls

_TYPES: Mapping[KeyKind, tuple[type, ...]] = MappingProxyType(
    {
        KeyKind.BLOCK: (dict,),
        KeyKind.MAP: (dict,),
        KeyKind.STRING: (str,),
        KeyKind.BOOLEAN: (bool,),
        KeyKind.INTEGER: (int,),
        KeyKind.LIST: (list,),
        KeyKind.ENUM: (str,),
    }
)

Positions = Mapping[tuple[str | int, ...], tuple[int, int]]
_SEVERITIES: Mapping[str, Severity] = MappingProxyType(
    {"error": Severity.ERROR, "warning": Severity.WARNING, "info": Severity.INFO}
)


def validate_config(
    tree: Mapping[Any, object], *, positions: Positions | None = None, file: str = CONFIG_FILE
) -> DiagnosticBag:
    """Check every key of a parsed `sst_config.yml` against the declared schema.

    Args:
        positions: The (line, column) of each key path; a path without one is reported
            against the file alone.
        file: How each diagnostic's origin names the configuration file.

    Returns:
        One diagnostic per problem, in document order; what a block leaves out is
        reported after the block's own entries.

    Diagnostics:
        SST-CFG003: a key is not declared where it is written.
        SST-CFG004: a value has the wrong type, or an entry of `dbt.manifest_schema_versions` is
            not an integer.
        SST-CFG007: an unknown top-level key is within two edits of a declared one.
        SST-CFG008: a value is outside its choices or bounds;
            `enrichment.sample_values_display_limit` exceeds `enrichment.distinct_limit`; or
            `dbt.manifest_schema_versions` is an empty list.
        SST-CFG015: `evals` sets `+database` or `+schema`.
        SST-CFG040: `vars` declares `sha_version`, which SST supplies.
        SST-CFG042: a block sets a key it no longer takes, which another block now holds.
        SST-CFG043: a removed key is set; the message says what replaced it.
        SST-CFG023: `semantic_views.+max_staleness` is below 120 seconds.
        SST-CFG054: a `+` key is written with no value.
        SST-CFG055: a folder route sets a `+` key only the block root may set.
        SST-CFG200: a deprecated key is set; it is checked as the key it is read as.
        SST-CFG025: `agents.+orchestration_model`, or a folder route's, is not in
            `snowflake.orchestration_models`.
        SST-CFG033: a severity override demotes a non-demotable code, an error below warning, or
            promotes an info code to error.
    """
    diagnostics: list[Diagnostic] = []
    located = _Located(positions or {}, file)
    _walk("", ChildPolicy.DECLARED, tree, (), located, diagnostics)
    diagnostics.extend(_allowlist_problems(tree, located))
    diagnostics.extend(_override_problems(tree, located))
    return DiagnosticBag(diagnostics)


def _overrides(tree: Mapping[Any, object]) -> dict[str, object]:
    block = tree.get("diagnostics")
    overrides = block.get("severity_overrides") if isinstance(block, Mapping) else None
    return {str(code): value for code, value in overrides.items()} if isinstance(overrides, Mapping) else {}


def _override_refusal(code: str, value: object) -> str | None:
    """Return why an override of a registered code is not permitted; None when it is, or is no severity."""
    if value not in _SEVERITIES:
        return None
    return override_refusal(code, _SEVERITIES[str(value)])


def _override_problems(tree: Mapping[Any, object], positions: _Located) -> list[Diagnostic]:
    """Report each override of an unregistered code, and each that breaks the demotion floor."""
    problems: list[Diagnostic] = []
    for code, value in _overrides(tree).items():
        path = ("diagnostics", "severity_overrides", code)
        if code not in ERROR_REGISTRY:
            problems.append(_diagnostic("SST-CFG003", path, positions, key=".".join(path)))
            continue
        reason = _override_refusal(code, value)
        if reason is not None:
            problems.append(_diagnostic("SST-CFG033", path, positions, code=code, found=str(value), reason=reason))
    return problems


def severity_overrides(tree: Mapping[Any, object]) -> dict[str, Severity]:
    """Return each legal `diagnostics.severity_overrides` entry as the severity its code now reports at.

    An entry that is not a severity, names no registered code, or breaks the demotion floor is
    left out: `validate_config` reports it.
    """
    return {
        code: _SEVERITIES[str(value)]
        for code, value in _overrides(tree).items()
        if code in ERROR_REGISTRY and value in _SEVERITIES and _override_refusal(code, value) is None
    }


def _allowlist_problems(tree: Mapping[Any, object], positions: _Located) -> list[Diagnostic]:
    """Report each `agents:` default orchestration model, routed or not, that the allowlist omits."""
    snowflake = tree.get("snowflake")
    allowed = snowflake.get("orchestration_models") if isinstance(snowflake, Mapping) else None
    names = [str(item) for item in allowed] if isinstance(allowed, list) else ["auto"]
    return [
        _diagnostic(
            "SST-CFG025",
            (*path, "+orchestration_model"),
            positions,
            kind="orchestration model",
            found=model,
            key="snowflake.orchestration_models",
        )
        for path, block in _route_blocks(tree.get("agents"), ("agents",))
        if isinstance(model := block.get("+orchestration_model"), str) and model not in names
    ]


def _route_blocks(block: object, path: tuple[str, ...]) -> list[tuple[tuple[str, ...], Mapping[Any, object]]]:
    """Return a routed block and each folder route below it, with its path, depth first."""
    if not isinstance(block, Mapping):
        return []
    found: list[tuple[tuple[str, ...], Mapping[Any, object]]] = [(path, block)]
    for key, child in block.items():
        if not str(key).startswith("+"):
            found.extend(_route_blocks(child, (*path, str(key))))
    return found


def unreferenced_tool_members(catalog: ToolCatalog, referenced: Iterable[str]) -> DiagnosticBag:
    """Report each declared tool member nothing references.

    Args:
        referenced: The member name of every reference to one, as `ToolCatalog.resolve` takes
            it. An agent tool's `{{ tool('<name>') }}`, an `agent` tool that names a member, and a
            configuration value's `tool()` each count.

    Diagnostics:
        SST-CFG018: a declared tool member is referenced by nothing.
    """
    names = {name.casefold() for name in referenced}
    return DiagnosticBag(
        D(
            "SST-CFG018",
            origin=member.origin,
            subject=f"tool:{member.name.casefold()}",
            name=member.name,
        )
        for member in catalog.members
        if member.name.casefold() not in names
    )


def config_tool_references(tree: Mapping[str, Any], catalog: ToolCatalog, *, file: str = CONFIG_FILE) -> DiagnosticBag:
    """Report each `{{ tool('<name>') }}` in a configuration value that names no declared member.

    Values inside lists are read too; an item's subject is its list's key with the index, `key[0]`.

    Diagnostics:
        SST-REF015: a configuration value's `tool()` does not take exactly one name.
        SST-CFG017: a configuration value's `tool()` names a member that is not declared.
    """
    declared = {member.name.casefold() for member in catalog.members}
    found: list[Diagnostic] = []
    for path, args in _config_tool_calls(tree):
        subject = f"config:{'.'.join(path)}"
        if len(args) != 1:
            found.append(
                D(
                    "SST-REF015",
                    origin=Origin(file),
                    subject=subject,
                    ref_function="tool",
                    expected="1",
                    found=len(args),
                )
            )
        elif args[0].casefold() not in declared:
            found.append(D("SST-CFG017", origin=Origin(file), subject=subject, name=args[0]))
    return DiagnosticBag(found)


def config_tool_calls(tree: Mapping[str, Any]) -> tuple[str, ...]:
    """Return the member each configuration value's one-name `tool()` call names, in document order."""
    return tuple(args[0] for _, args in _config_tool_calls(tree) if len(args) == 1)


def _config_tool_calls(tree: Mapping[str, Any]) -> list[tuple[tuple[str, ...], tuple[str, ...]]]:
    """Return each configuration value's `tool()` calls' arguments, with its path; a list item is `key[0]`."""
    calls: list[tuple[tuple[str, ...], tuple[str, ...]]] = []

    def walk(path: tuple[str, ...], value: object) -> None:
        if isinstance(value, Mapping):
            for key, item in value.items():
                walk((*path, str(key)), item)
        elif isinstance(value, (list, tuple)):
            # The root is a mapping, so a list always sits below a key.
            for index, item in enumerate(value):
                walk((*path[:-1], f"{path[-1]}[{index}]"), item)
        elif isinstance(value, str):
            calls.extend((path, args) for args in _tool_calls_in(value))

    walk((), tree)
    return calls


def _tool_calls_in(text: str) -> tuple[tuple[str, ...], ...]:
    """Return the arguments of each `tool()` call in `text`; none when its templates do not parse.

    A template that does not parse is reported when the value is validated, not here.
    """
    try:
        calls = scan_template_calls(text)
    except TemplateSyntaxError:
        return ()
    return tuple(call.args for call in calls if call.function == "tool")


def empty_is_block(path: tuple[str | int, ...]) -> bool:
    """Report whether an empty value at a configuration key path is an empty block, as documented.

    A block or name map left empty is read as one with no entries -- a consumed extension with no
    `fqn:` takes `default_prefix` -- so its null is meant; quoting it would make it a string.
    A path through a list, or one the schema does not declare, has no such meaning.
    """
    entry, policy = "", ChildPolicy.DECLARED
    spec: ConfigKey | None = None
    for key in path:
        if not isinstance(key, str):
            return False
        spec, route = _child_spec(CHILDREN.get(entry, {}), policy, key)
        if spec is None:
            return False
        if spec.status is KeyStatus.DEPRECATED:
            spec = CONFIG_KEYS[str(spec.replacement)]
        if not route:
            entry, policy = spec.path, spec.children
    return spec is not None and spec.kind in (KeyKind.BLOCK, KeyKind.MAP)


@dataclass(frozen=True, slots=True)
class _Located:
    """The key positions of one configuration file, and how its diagnostics name it."""

    positions: Positions
    file: str


def unstated_policy(tree: Mapping[Any, object], *, file: str = CONFIG_FILE) -> DiagnosticBag:
    """Report each policy key a configuration file must state although it has a default.

    Whether expressions are compiled against Snowflake decides what a green validate proves,
    so the file must say so; a block or value of the wrong type is reported by `validate_config`.

    Diagnostics:
        SST-CFG031: `validation.snowflake_syntax_check` is absent.
    """
    validation = tree.get("validation")
    if isinstance(validation, Mapping) and "snowflake_syntax_check" in validation:
        return DiagnosticBag()
    path = ("validation", "snowflake_syntax_check") if isinstance(validation, Mapping) else ("validation",)
    return DiagnosticBag((D("SST-CFG031", origin=Origin(file), subject=f"config:{'.'.join(path)}"),))


def _origin(path: tuple[str, ...], positions: _Located) -> Origin:
    position = positions.positions.get(path)
    return Origin(positions.file, position[0], position[1]) if position is not None else Origin(positions.file)


def _diagnostic(code: str, path: tuple[str, ...], positions: _Located, /, **context: Any) -> Diagnostic:
    return D(code, origin=_origin(path, positions), subject=f"config:{'.'.join(path)}", **context)


def _walk(
    entry_path: str,
    policy: ChildPolicy,
    value: Mapping[Any, object],
    actual: tuple[str, ...],
    positions: _Located,
    diagnostics: list[Diagnostic],
) -> None:
    """Check each entry of one block, then what its values imply together.

    `entry_path` is the declared path whose children apply, which for a folder route
    is the routed block's own path; `actual` is the path as written in the file.
    """
    declared = CHILDREN.get(entry_path, {})
    # A folder route is walked as its routed block, so it is written deeper than the block's path.
    in_route = len(actual) > (len(entry_path.split(".")) if entry_path else 0)
    for raw_key, child in value.items():
        key = str(raw_key)
        path = (*actual, key)
        spec, route = _child_spec(declared, policy, key)
        if spec is None:
            diagnostics.append(_unknown(path, positions))
            continue
        if spec.root_only and in_route:
            block = entry_path
            diagnostics.append(
                _diagnostic("SST-CFG055", path, positions, key=".".join(path), block=block, expected=spec.path)
            )
            continue
        _check(spec, child, path, positions, diagnostics, route_of=entry_path if route else None)
    diagnostics.extend(_block_problems(entry_path, value, actual, positions))


def _child_spec(declared: Mapping[str, ConfigKey], policy: ChildPolicy, key: str) -> tuple[ConfigKey | None, bool]:
    """Return the declared key an entry matches, and whether it matched as a folder route."""
    spec = declared.get(key)
    if spec is None and policy is ChildPolicy.NAMES:
        spec = declared.get("<name>")
    if spec is None and policy is ChildPolicy.ROUTES and not key.startswith("+"):
        spec = declared.get("<route>")
        return spec, spec is not None
    return spec, False


def _block_problems(
    entry_path: str, value: Mapping[Any, object], actual: tuple[str, ...], positions: _Located
) -> list[Diagnostic]:
    """Diagnose what one block's values imply together: the enrichment limits, the manifest versions."""
    if entry_path == "enrichment":
        return _enrichment_limit_problems(value, actual, positions)
    if entry_path == "dbt":
        return _schema_version_problems(value, actual, positions)
    return []


def _schema_version_problems(
    value: Mapping[Any, object], actual: tuple[str, ...], positions: _Located
) -> list[Diagnostic]:
    """Diagnose what `dbt.manifest_schema_versions` would drop: an entry that is not an integer.

    The reader keeps only integers and reads an empty result as the default set, so an empty
    list, or one of only strings or booleans, would silently accept the defaults. A value that
    is not a list is reported by its type check, and is not looked into.
    """
    versions = value.get("manifest_schema_versions")
    if not isinstance(versions, list):
        return []
    path = (*actual, "manifest_schema_versions")
    key = ".".join(path)
    if not versions:
        return [
            _diagnostic("SST-CFG008", path, positions, key=key, found="[]", expected="a list of one or more versions")
        ]
    return [
        _diagnostic("SST-CFG004", path, positions, key=f"{key}[{index}]", expected="integer", found=type(item).__name__)
        for index, item in enumerate(versions)
        if not isinstance(item, int) or isinstance(item, bool)
    ]


def _enrichment_limit_problems(
    value: Mapping[Any, object], actual: tuple[str, ...], positions: _Located
) -> list[Diagnostic]:
    """Diagnose a display limit above the distinct limit: no more values than that are sampled.

    An absent limit takes its default. A limit that is not an integer, or is outside its own
    bounds, is reported on its own and not compared.
    """
    limits = []
    for name in ("distinct_limit", "sample_values_display_limit"):
        spec = CHILDREN["enrichment"][name]
        written = value.get(name, int(spec.default or 0))
        if not isinstance(written, int) or isinstance(written, bool) or _out_of_bounds(spec, written):
            return []
        limits.append(written)
    distinct, display = limits
    if display <= distinct:
        return []
    path = (*actual, "sample_values_display_limit")
    return [
        _diagnostic(
            "SST-CFG008",
            path,
            positions,
            key=".".join(path),
            found=str(display),
            expected=f"1..{distinct} (enrichment.distinct_limit)",
        )
    ]


def _unknown(path: tuple[str, ...], positions: _Located) -> Diagnostic:
    key = ".".join(path)
    if len(path) == 1:
        suggestion = _near_miss(path[0])
        if suggestion is not None:
            return _diagnostic("SST-CFG007", path, positions, key=key, suggestion=suggestion)
    return _diagnostic("SST-CFG003", path, positions, key=key)


def _near_miss(key: str) -> str | None:
    candidates = sorted((distance, known) for known in TOP_LEVEL_KEYS if (distance := _edit_distance(key, known)) <= 2)
    return candidates[0][1] if candidates else None


def _edit_distance(left: str, right: str) -> int:
    previous = list(range(len(right) + 1))
    for row, left_character in enumerate(left, start=1):
        current = [row]
        for column, right_character in enumerate(right, start=1):
            current.append(
                min(
                    previous[column] + 1,
                    current[column - 1] + 1,
                    previous[column - 1] + (left_character != right_character),
                )
            )
        previous = current
    return previous[-1]


def _check(
    spec: ConfigKey,
    value: object,
    path: tuple[str, ...],
    positions: _Located,
    diagnostics: list[Diagnostic],
    *,
    route_of: str | None,
) -> None:
    """Check one entry against its declared key, then walk into it when it is a block.

    A removed key is reported without looking at its value, and a value
    that fails a check is not descended into. `route_of` names the routed block when the
    entry matched a folder route: the route's children are that block's own keys.
    """
    if spec.status is KeyStatus.DEPRECATED:
        # Checked as the block it is read as, so a mistake inside it is still reported.
        diagnostics.append(_diagnostic("SST-CFG200", path, positions, key=".".join(path), expected=spec.replacement))
        spec = CONFIG_KEYS[str(spec.replacement)]
    if spec.status is not KeyStatus.CURRENT:
        diagnostics.append(_status_problem(spec, path, positions))
        return
    if value is None and spec.name.startswith("+"):
        # A `+` key inherits when it is absent; written empty, it would read as a value it is not.
        diagnostics.append(_diagnostic("SST-CFG054", path, positions, key=".".join(path)))
        return
    if value is None:
        # An empty YAML value means unset: a scalar keeps its default, and a block
        # is checked as an empty block.
        if spec.kind not in (KeyKind.BLOCK, KeyKind.MAP):
            return
        value = {}
    problem = _value_problem(spec, value, path, positions)
    if problem is not None:
        diagnostics.append(problem)
        return
    if isinstance(value, dict) and spec.kind in (KeyKind.BLOCK, KeyKind.MAP):
        if route_of is not None:
            _walk(route_of, ChildPolicy.ROUTES, value, path, positions, diagnostics)
        else:
            _walk(spec.path, spec.children, value, path, positions, diagnostics)


def _status_problem(spec: ConfigKey, path: tuple[str, ...], positions: _Located) -> Diagnostic:
    """Diagnose a key that is set although SST no longer reads it."""
    if spec.code == "SST-CFG042":
        return _diagnostic(spec.code, path, positions, block=path[0], key=path[-1], reason=spec.replacement)
    if spec.code == "SST-CFG015":
        return _diagnostic(spec.code, path, positions, key=path[-1])
    if spec.code == "SST-CFG040":
        return _diagnostic(spec.code, path, positions)
    return _diagnostic("SST-CFG043", path, positions, key=".".join(path), reason=spec.replacement)


def _value_problem(spec: ConfigKey, value: object, path: tuple[str, ...], positions: _Located) -> Diagnostic | None:
    key = ".".join(path)
    return value_problem(spec, value, key=key, origin=_origin(path, positions), subject=f"config:{key}")


def value_problem(spec: ConfigKey, value: object, *, key: str, origin: Origin, subject: str) -> Diagnostic | None:
    """Return the first rule a declared key's value breaks: its type, its choices, then its bounds.

    The configuration file and an artifact's `config:` block check a value by the same rules,
    each reporting it where it is written.

    Args:
        key: The key as the diagnostic names it, such as `agents.+schema` or `config.schema`.

    Diagnostics:
        SST-CFG004: the value is not of the key's type; an integer key never takes a boolean.
        SST-CFG008: the value is outside the key's choices or bounds, unless the key names its own
            code for that, such as SST-CFG023.
    """
    expected = _TYPES.get(spec.kind)
    mistyped = expected is not None and not isinstance(value, expected)
    if mistyped or spec.kind is KeyKind.INTEGER and isinstance(value, bool):
        return D(
            "SST-CFG004", origin=origin, subject=subject, key=key, expected=spec.kind.value, found=type(value).__name__
        )
    code = spec.code or "SST-CFG008"
    if spec.choices and (vocabulary_key(str(value)) if spec.folded else value) not in spec.choices:
        return D(code, origin=origin, subject=subject, key=key, found=repr(value), expected=", ".join(spec.choices))
    if isinstance(value, int) and not isinstance(value, bool) and _out_of_bounds(spec, value):
        bounds = f"{spec.minimum}..{spec.maximum}"
        return D(code, origin=origin, subject=subject, key=key, found=str(value), expected=bounds)
    return None


def _out_of_bounds(spec: ConfigKey, value: int) -> bool:
    return (spec.minimum is not None and value < spec.minimum) or (spec.maximum is not None and value > spec.maximum)
