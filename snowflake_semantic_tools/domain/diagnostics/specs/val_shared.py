"""Validation codes 0xx (VAL): names, descriptions, prose, files and references, for every type."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Severity, spec

TITLE: str = "Validation"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-VAL001",
        Severity.ERROR,
        "Name is not unique within its type",
        "{type} '{name}' is declared more than once",
        "rename one of them",
    ),
    spec(
        "SST-VAL002",
        Severity.ERROR,
        "Name is not unique across the extension namespace",
        "{type} '{name}' collides with {other} in another namespace",
        "rename one of them",
    ),
    spec(
        "SST-VAL003",
        Severity.WARNING,
        "Description missing",
        "{type} '{name}' has no description",
        "add a description; it is how Analyst chooses between objects",
    ),
    spec(
        "SST-VAL004",
        Severity.WARNING,
        "Description shorter than the configured floor",
        "{type} '{name}' description is {size} chars, under {expected}",
        "expand the description",
    ),
    spec(
        "SST-VAL005",
        Severity.ERROR,
        "Description does not state when to invoke",
        "{type} '{name}' description describes what it is, not when to use it",
        "state the trigger condition",
    ),
    spec(
        "SST-VAL006",
        Severity.ERROR,
        "Hardcoded fully-qualified name in an authored file",
        "{type} '{name}': '{field}' hardcodes '{value}'",
        "resolve the object through sst_config.yml or a reference: block",
    ),
    spec(
        "SST-VAL007",
        Severity.WARNING,
        "Declared but referenced by nothing",
        "{type} '{name}' is referenced by nothing",
        "reference it, or delete it",
    ),
    spec(
        "SST-VAL008",
        Severity.ERROR,
        "Multi-line string uses a folded scalar",
        "{type} '{name}': '{field}' uses a folded scalar",
        "use |- so line breaks survive",
    ),
    spec(
        "SST-VAL009",
        Severity.WARNING,
        "File is not canonically formatted",
        "{path} is not canonically formatted",
        "run sst format",
    ),
    spec(
        "SST-VAL010",
        Severity.ERROR,
        "Reference graph contains a cycle",
        "{type} reference cycle: {cycle}",
        "break the cycle",
    ),
    spec(
        "SST-VAL011",
        Severity.ERROR,
        "Declared value would be dropped or altered by the renderer",
        "{type} '{name}': '{field}' would be {detail} by the renderer",
        "emit the value unmodified, or stop declaring it",
    ),
    spec(
        "SST-VAL012",
        Severity.WARNING,
        "Deprecated key spelling in use",
        "{type} '{name}' uses '{field}'; the current spelling is '{expected}'",
        "rename the key",
    ),
    spec(
        "SST-VAL013",
        Severity.ERROR,
        "Unmodelled key would be rendered unvalidated",
        "{type} '{name}': '{key}' is not modelled and would be rendered as-is",
        "promote the key, remove it, or set snowflake.allow_unknown_keys: true to render it with a warning instead",
    ),
    spec(
        "SST-VAL014",
        Severity.WARNING,
        "Unmodelled key reported",
        "{type} '{name}': {count} unmodelled keys rendered",
        "promote the keys to first-class fields if they are load-bearing",
    ),
    spec(
        "SST-VAL015",
        Severity.ERROR,
        "Deploy ordering violated",
        "{type} '{name}' would publish before {blocker}, which it depends on",
        "include the blocker in the selection, or break the dependency; the publish order is not authorable",
    ),
    spec(
        "SST-VAL016",
        Severity.ERROR,
        "Referenced object is not published by this project or declared external",
        "{type} '{name}' references '{value}', which is neither published nor declared",
        "publish it, or declare it under reference:",
    ),
    spec(
        "SST-VAL017",
        Severity.WARNING,
        "Two objects express the same thing",
        "{type} '{name}' duplicates {other}",
        "say it once, in the enforceable place",
    ),
    spec(
        "SST-VAL018",
        Severity.WARNING,
        "Composed prose surface exceeds the configured budget",
        "{artifact}: composed instruction surface is {size} chars, over {expected}",
        "move bulk content into a referenced file",
    ),
    spec(
        "SST-VAL019",
        Severity.WARNING,
        "Prose names an object that does not resolve",
        "{type} '{name}': prose names '{value}', which does not resolve",
        "correct the name, or remove the mention",
    ),
    spec(
        "SST-VAL020",
        Severity.INFO,
        "Rule skipped -- requirement unavailable",
        "{rule_id} skipped: {detail}",
        None,
    ),
)
