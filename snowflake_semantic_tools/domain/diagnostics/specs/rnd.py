"""Rendering codes (RND): a resolved artifact the renderer cannot turn into publishable output.

Some are warnings: the output renders, but publication will not check what it relies on.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Severity, spec

TITLE: str = "Rendering"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-RND001",
        Severity.ERROR,
        "Model is missing a field the dialect requires",
        "{artifact}: the {value} dialect requires '{field}'",
        "supply the field",
        condition="the model is valid and the renderer cannot produce DDL from it",
    ),
    spec(
        "SST-RND002",
        Severity.ERROR,
        "Identifier cannot be safely quoted",
        "{artifact}: '{name}' cannot be safely quoted",
        "rename the object",
        condition="an identifier contains characters no quoting scheme survives",
    ),
    spec(
        "SST-RND003",
        Severity.WARNING,
        "Rendered DDL exceeds the statement size limit",
        "{artifact}: rendered DDL is {size} bytes, over the {expected} guess",
        "split the view, or reduce the member count",
        condition="the rendered statement is larger than the assumed statement limit",
    ),
    spec(
        "SST-RND010",
        Severity.WARNING,
        "Agent has no tools",
        "agent '{artifact}' renders with no tools",
        "declare at least one tool",
        condition="the rendered agent spec has an empty tool list",
    ),
    spec(
        "SST-RND011",
        Severity.ERROR,
        "Agent spec contains a dollar-quote sequence",
        "agent '{artifact}': spec contains '$$' at {value}",
        "remove or escape the sequence",
        condition="the rendered spec would break dollar-quoting in the DDL",
    ),
    spec(
        "SST-RND012",
        Severity.ERROR,
        "Unknown agent tool type at render",
        "agent '{artifact}': tool type '{found}' is unknown to the renderer",
        "add the type to snowflake.tool_types",
        condition="a tool type reaches the renderer unrecognised",
    ),
    spec(
        "SST-RND013",
        Severity.WARNING,
        "generic tool resources emitted with no server-side check behind them",
        "agent '{artifact}': tool_resources for '{name}' is not validated by Snowflake",
        "confirm the key and the backing object yourself; `CREATE AGENT` will accept a wrong one",
        condition="a `generic` tool's resources are rendered, and nothing downstream validates them",
    ),
    spec(
        "SST-RND020",
        Severity.ERROR,
        "Unknown eval expectation kind",
        "eval '{artifact}': expectation kind '{found}' is unknown",
        "use a supported expectation kind",
        condition="an eval row declares an expectation the renderer cannot express",
    ),
    spec(
        "SST-RND021",
        Severity.WARNING,
        "Eval has no cases",
        "eval '{artifact}' renders with no cases",
        "add rows to the dataset",
        condition="the rendered eval has an empty case list",
    ),
    spec(
        "SST-RND022",
        Severity.WARNING,
        "SQL_MATCH expectation is brittle",
        "eval '{artifact}': row {index} uses SQL_MATCH",
        "prefer a result-set comparison",
        condition="a textual SQL match will break on any formatting change",
    ),
    spec(
        "SST-RND030",
        Severity.ERROR,
        "Skill references a path that does not exist",
        "skill '{artifact}': '{path}' does not exist at render",
        "add the file, or correct the reference",
        condition="a bundle path is unresolvable at render",
    ),
    spec(
        "SST-RND031",
        Severity.WARNING,
        "Skill body is empty",
        "skill '{artifact}': SKILL.md has no instructions after its frontmatter",
        "add content",
        condition="the rendered SKILL.md has no body",
    ),
    spec(
        "SST-RND032",
        Severity.WARNING,
        "Skill body exceeds the soft size limit",
        "skill '{artifact}': rendered body is {size} bytes, over {expected}",
        "move bulk content into a referenced file",
        condition="the rendered body is over the soft cap",
    ),
    spec(
        "SST-RND040",
        Severity.ERROR,
        "Unsupported tool kind at render",
        "tool '{artifact}': kind '{found}' has no renderer",
        "register a renderer, or change the kind",
        condition="a tool kind reaches the renderer with no implementation",
    ),
    spec(
        "SST-RND041",
        Severity.ERROR,
        "Tool source query is empty",
        "tool '{artifact}': rendered source query is empty",
        "supply on: or body_file: content",
        condition="the rendered tool body has no statement",
    ),
    spec(
        "SST-RND900",
        Severity.ERROR,
        "Rendered member count differs from the model's",
        "{artifact}: rendered {found} members, model carries {expected}",
        "report this as a bug",
        condition="render is not total over the model",
    ),
)
