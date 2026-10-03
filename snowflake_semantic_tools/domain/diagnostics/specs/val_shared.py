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
        condition="two artifacts or members of one type share a name, across all files",
    ),
    spec(
        "SST-VAL002",
        Severity.ERROR,
        "Name is not unique across the extension namespace",
        "{type} '{name}' collides with {other} in another namespace",
        "rename one of them",
        condition="global uniqueness is required and two declarations collide",
    ),
    spec(
        "SST-VAL003",
        Severity.WARNING,
        "Description missing",
        "{type} '{name}' has no description",
        "add a description; it is how Analyst chooses between objects",
        condition="a describable object has no description",
    ),
    spec(
        "SST-VAL004",
        Severity.WARNING,
        "Description shorter than the configured floor",
        "{type} '{name}' description is {size} chars, under {expected}",
        "expand the description",
        condition="a description is present and below the length floor",
    ),
    spec(
        "SST-VAL005",
        Severity.ERROR,
        "Description does not state when to invoke",
        "{type} '{name}' description describes what it is, not when to use it",
        "state the trigger condition",
        condition="a description omits invocation guidance for a routed object",
        note=(
            "It is the only text the agent matches against, so a description that never states a "
            "trigger means the object is silently never invoked, which is indistinguishable from not "
            "publishing it."
        ),
    ),
    spec(
        "SST-VAL006",
        Severity.ERROR,
        "Hardcoded fully-qualified name in an authored file",
        "{type} '{name}': '{field}' hardcodes '{value}'",
        "resolve the object through sst_config.yml or a reference: block",
        condition="a literal three-part name appears in an authored artifact",
    ),
    spec(
        "SST-VAL007",
        Severity.WARNING,
        "Declared but referenced by nothing",
        "{type} '{name}' is referenced by nothing",
        "reference it, or delete it",
        condition="a declared object has no consumer anywhere in the project",
    ),
    spec(
        "SST-VAL008",
        Severity.ERROR,
        "Multi-line string uses a folded scalar",
        "{type} '{name}': '{field}' uses a folded scalar",
        "use |- so line breaks survive",
        condition="a multi-line value uses > or >-, which reflows prose the model reads",
    ),
    spec(
        "SST-VAL009",
        Severity.WARNING,
        "File is not canonically formatted",
        "{path} is not canonically formatted",
        "run sst format",
        condition="sst format would change the file",
    ),
    spec(
        "SST-VAL010",
        Severity.ERROR,
        "Reference graph contains a cycle",
        "{type} reference cycle: {cycle}",
        "break the cycle",
        condition="a declared reference graph of one kind is not acyclic",
    ),
    spec(
        "SST-VAL011",
        Severity.ERROR,
        "Declared value would be dropped or altered by the renderer",
        "{type} '{name}': '{field}' would be {detail} by the renderer",
        "emit the value unmodified, or stop declaring it",
        condition="a declared value is validated and would not reach the published object intact",
    ),
    spec(
        "SST-VAL012",
        Severity.ERROR,
        "Deprecated key spelling in use",
        "{type} '{name}' uses '{field}'; the current spelling is '{expected}'",
        "rename the key",
        condition="a superseded key spelling is present and still honoured",
    ),
    spec(
        "SST-VAL013",
        Severity.ERROR,
        "Unmodelled key would be rendered unvalidated",
        "{type} '{name}': '{key}' is not modelled and would be rendered as-is",
        "promote the key, remove it, or set `snowflake.allow_unknown_keys: true` to render it with a warning instead",
        condition=(
            "an off-spec key reaches the renderer without validation and `snowflake.allow_unknown_keys` is `false`"
        ),
    ),
    spec(
        "SST-VAL014",
        Severity.WARNING,
        "Unmodelled key reported",
        "{type} '{name}': {count} unmodelled keys rendered",
        "promote the keys to first-class fields if they are load-bearing",
        condition=("off-spec usage is present and visible and `snowflake.allow_unknown_keys` is `true`, its default"),
    ),
    spec(
        "SST-VAL015",
        Severity.ERROR,
        "Deploy ordering violated",
        "{type} '{name}' would publish before {blocker}, which it depends on",
        "include the blocker in the selection, or break the dependency -- the ORDER is not authorable",
        condition="a dependent artifact is ordered before its dependency",
        note=(
            "Publish order is the registry's type order, which is proven a valid topological sort, so "
            "this means the selection is missing the blocker, or the project has a dependency the "
            "registry's type order cannot satisfy."
        ),
    ),
    spec(
        "SST-VAL016",
        Severity.ERROR,
        "Referenced object is not published by this project or declared external",
        "{type} '{name}' references '{value}', which is neither published nor declared",
        "publish it, or declare it under reference:",
        condition="a reference names an object with no owner",
    ),
    spec(
        "SST-VAL017",
        Severity.WARNING,
        "Two objects express the same thing",
        "{type} '{name}' duplicates {other}",
        "say it once, in the enforceable place",
        condition="a filter and a custom instruction, or two near-duplicate descriptions, overlap",
    ),
    spec(
        "SST-VAL018",
        Severity.WARNING,
        "Composed prose surface exceeds the configured budget",
        "{artifact}: composed instruction surface is {size} chars, over {expected}",
        "move bulk content into a referenced file",
        condition="descriptions plus instructions plus tool text exceed the budget",
    ),
    spec(
        "SST-VAL019",
        Severity.WARNING,
        "Prose names an object that does not resolve",
        "{type} '{name}': prose names '{value}', which does not resolve",
        "correct the name, or remove the mention",
        condition="free text references a filter, metric, dimension, table or tool that is absent",
    ),
    spec(
        "SST-VAL020",
        Severity.INFO,
        "Rule skipped -- requirement unavailable",
        "{rule_id} skipped: {detail}",
        None,
        condition="a rule needs an observation, a connection or a cap that was unavailable",
    ),
)
