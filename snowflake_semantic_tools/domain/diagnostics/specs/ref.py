"""Reference codes (REF): a reference that does not resolve, takes a rejected form, or forms a cycle."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Severity, spec

TITLE: str = "References"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-REF001",
        Severity.ERROR,
        "ref() model not in the dbt catalog",
        "{{{{ ref('{model}') }}}} is not a model in the dbt manifest",
        "run dbt compile, or correct the model name",
        condition="a single-argument ref names a model the manifest does not list",
    ),
    spec(
        "SST-REF002",
        Severity.ERROR,
        "ref() column not on the model",
        "{{{{ ref('{model}','{column}') }}}}: '{column}' is not a column on {model}",
        "correct the column name, or add it to the model's columns: block",
        condition="a two-argument ref names a column absent from the model",
    ),
    spec(
        "SST-REF003",
        Severity.ERROR,
        "Malformed template expression",
        "{file}:{line}:{col}: malformed template expression '{value}'",
        "correct the expression syntax",
        condition=(
            "a `{{ fn(...) }}` call's arguments do not parse, or a field that must be exactly one "
            "template call holds something else"
        ),
    ),
    spec(
        "SST-REF004",
        Severity.ERROR,
        "Unknown template function",
        "'{ref_function}' is not a template function",
        "use one of {expected}",
        condition="a `{{ }}` call names a function the 1.0 template dialect does not have",
    ),
    spec(
        "SST-REF005",
        Severity.ERROR,
        "metric() reference cycle",
        "metric reference cycle: {cycle}",
        "break the cycle",
        condition="metric() references among metrics form a cycle",
    ),
    spec(
        "SST-REF006",
        Severity.ERROR,
        "metric() target not found",
        "{{{{ metric('{name}') }}}} does not resolve",
        "correct the name, or declare the metric",
        condition="a `metric()` reference names no metric the project declares",
    ),
    spec(
        "SST-REF007",
        Severity.ERROR,
        "custom_instructions() target not found",
        "{{{{ custom_instructions('{name}') }}}} does not resolve",
        "declare the block, or correct the name",
        condition="a `custom_instructions()` reference names no custom instruction the project declares",
    ),
    spec(
        "SST-REF008",
        Severity.ERROR,
        "Ref used in a field that forbids it",
        "{artifact}: '{field}' does not accept template expressions",
        "move the reference, or inline the value",
        condition=(
            "a `{{` appears in a field that is published as written, or a template call in a field that accepts none"
        ),
    ),
    spec(
        "SST-REF009",
        Severity.ERROR,
        "Ref resolved to an empty string",
        "{{{{ {ref_function}('{name}') }}}} resolved to an empty string",
        "check the referenced object's name",
        condition="a template call resolves to an empty value, such as `var()` naming a variable whose value is empty",
    ),
    spec(
        "SST-REF010",
        Severity.ERROR,
        "tool() target not found",
        "{{{{ tool('{group}','{name}') }}}} does not resolve",
        "declare the member in the tools directory",
        condition=("a `tool()` reference names no tool member, or, by name alone, more than one"),
    ),
    spec(
        "SST-REF011",
        Severity.ERROR,
        "semantic_view() target not found",
        "{{{{ semantic_view('{name}') }}}} does not resolve",
        "declare the view, or correct the name",
        condition="a `semantic_view()` reference names no semantic view the project declares",
    ),
    spec(
        "SST-REF012",
        Severity.ERROR,
        "agent() target not found",
        "{{{{ agent('{name}') }}}} does not resolve",
        "declare the agent, or correct the name",
        condition=(
            "an `agent()` reference names no agent the project declares; also an eval whose "
            "dataset or config names another agent than the eval's, or than each other"
        ),
    ),
    spec(
        "SST-REF013",
        Severity.ERROR,
        "extension() target not found",
        "{{{{ extension('{name}') }}}} does not resolve",
        "declare the extension source, or correct the name",
        condition="an agent's `extension()` entry names no extension `skills.extensions` declares",
    ),
    spec(
        "SST-REF014",
        Severity.ERROR,
        "file() sidecar does not resolve",
        "{{{{ file('{path}') }}}} does not resolve to a file",
        "create the file, or correct the path",
        condition="an agent's or a verified query's `file()` reference names a file that does not exist",
    ),
    spec(
        "SST-REF015",
        Severity.ERROR,
        "Ref has the wrong arity",
        "{{{{ {ref_function}() }}}} takes {expected} arguments, found {found}",
        "supply the documented arguments",
        condition="a template function is called with the wrong number of arguments",
    ),
    spec(
        "SST-REF018",
        Severity.ERROR,
        "Ref resolves outside the declared target",
        "{{{{ {ref_function}('{name}') }}}} has no entry for target '{target}'",
        "declare a relation for every target you publish to",
        condition=(
            "a `reference:` tool member has no relation for the current target, or declares one "
            "for a target `profiles.yml` does not declare"
        ),
    ),
    spec(
        "SST-REF019",
        Severity.ERROR,
        "Relation is not a three-part name",
        "'{value}' is not a three-part fully-qualified name",
        "use database.schema.object",
        condition=(
            "a tool member's relation, a view table's dbt relation, or a resolved tag name is not a three-part name"
        ),
    ),
    spec(
        "SST-REF020",
        Severity.ERROR,
        "Ref resolves to an incompatible object type",
        "{{{{ {ref_function}('{name}') }}}} resolves to {found}, expected {expected}",
        "point the reference at a compatible object",
        condition="an agent tool's backing tool member is of a type that tool cannot use",
    ),
    spec(
        "SST-REF022",
        Severity.ERROR,
        "Agent reference graph has a cycle",
        "agent delegation cycle: {cycle}",
        "break the cycle",
        condition="agents delegate to one another, through `agent()` tools, in a cycle",
    ),
    spec(
        "SST-REF023",
        Severity.WARNING,
        "Ref target resolves to the same object across targets",
        "{{{{ {ref_function}('{name}') }}}} resolves to {value} for both dev and prod",
        "use per-target relations for anything writable",
        condition=(
            "a `reference:` tool member, in a group not declared immutable, names one object for "
            "both its `dev` and its `prod` target"
        ),
    ),
    spec(
        "SST-REF026",
        Severity.ERROR,
        "eval_metric() target not found",
        "{{{{ eval_metric('{name}') }}}} does not resolve",
        "declare the metric in the eval_metrics/ tree, or correct the name",
        condition=(
            "an eval config names a custom metric that no file under `eval_metrics/`, or "
            "`project.eval_metrics_dir`, declares"
        ),
    ),
    spec(
        "SST-REF027",
        Severity.ERROR,
        "file() path escapes the project root",
        "{{{{ file('{path}') }}}} resolves outside the project root",
        "use a path inside the project",
        condition=(
            "an agent's or a verified query's `file()` reference, or an eval's dataset or config path, "
            "resolves outside the project root"
        ),
    ),
    spec(
        "SST-REF028",
        Severity.ERROR,
        "tag() target not declared",
        "{{{{ tag('{name}') }}}} does not resolve",
        "declare the entry under tags: in sst_config.yml, or correct the name",
        condition="a tag reference names an entry absent from the tags: block",
    ),
    spec(
        "SST-REF029",
        Severity.ERROR,
        "relationship() target not declared",
        "{{{{ relationship('{name}') }}}} does not resolve",
        "declare the relationship, or correct the name",
        condition="a relationship reference names a relationship absent from the project",
    ),
    spec(
        "SST-REF030",
        Severity.ERROR,
        "filter() target not declared",
        "{{{{ filter('{name}') }}}} does not resolve",
        "declare the filter, or correct the name",
        condition="a filter reference names a filter absent from the project",
    ),
    spec(
        "SST-REF031",
        Severity.ERROR,
        "verified_query() target not declared",
        "{{{{ verified_query('{name}') }}}} does not resolve",
        "declare the verified query, or correct the name",
        condition="a verified-query reference names a query absent from the project",
    ),
    spec(
        "SST-REF032",
        Severity.ERROR,
        "skill() target not declared",
        "{{{{ skill('{name}') }}}} does not resolve",
        "declare the skill under skills/, or correct the name",
        condition="a `skill()` reference names no skill the project declares",
    ),
    spec(
        "SST-REF033",
        Severity.ERROR,
        "Template does not match the ref grammar",
        "{file}:{line}:{col}: '{text}' is not a valid reference: {detail}",
        "use `{{ fn('arg') }}`",
        condition="a `{{ ... }}` span does not match `{{ fn(args) }}`",
    ),
    spec(
        "SST-REF034",
        Severity.ERROR,
        "Legacy table() global is rejected",
        "{file}:{line}:{col}: '{{{{ table('{model}') }}}}' is not a reference in 1.0; use '{{{{ ref('{model}') }}}}'",
        "run `sst migrate refs`, which is a pure rename",
        condition="the legacy `table()` global appears anywhere in the project",
        note="`sst migrate refs` only reports by default; `--write` rewrites the files.",
    ),
    spec(
        "SST-REF035",
        Severity.ERROR,
        "Legacy column() global is rejected",
        "{file}:{line}:{col}: '{{{{ column('{model}','{column}') }}}}' is not a reference in 1.0; "
        "use '{{{{ ref('{model}','{column}') }}}}'",
        "run `sst migrate refs`, which is a pure rename",
        condition="the legacy `column()` global appears anywhere in the project",
        note="`sst migrate refs` only reports by default; `--write` rewrites the files.",
    ),
    spec(
        "SST-REF036",
        Severity.ERROR,
        "plugin() target not declared",
        "{{{{ plugin('{name}') }}}} does not resolve",
        "declare the plugin under the plugins directory, or correct the name",
        condition="an agent's `plugin()` entry names no plugin the project declares",
    ),
    spec(
        "SST-REF037",
        Severity.ERROR,
        "extension() names an extension this project publishes",
        "agent '{artifact}': extension('{name}') names a {kind} this project publishes",
        "reference it with skill() or plugin(), which pins the published version",
        condition="an agent's `extension()` entry names a skill or plugin this project publishes itself",
    ),
    spec(
        "SST-REF041",
        Severity.ERROR,
        "Template function not allowed in this field",
        "{artifact}: {function}() is not allowed in {field}",
        "remove the call; each field accepts only the functions its reference page lists",
        condition="a template call uses a function the field does not allow",
    ),
    spec(
        "SST-REF044",
        Severity.ERROR,
        "View table entry is not a ref()",
        "{artifact}: table entry {found} is not a {{{{ ref('<model>') }}}} call",
        "write each tables: entry as `{{ ref('<model>') }}`",
        condition="a view tables: entry is not exactly one `{{ ref('<model>') }}` call",
    ),
    spec(
        "SST-REF046",
        Severity.ERROR,
        "Relationship endpoint is not a ref() call",
        "{artifact}: {field} is '{found}'; it takes one {{{{ ref('<model>') }}}} call",
        "write the endpoint as {{ ref('<model>') }}, or run sst migrate refs --write",
        condition="a relationship left_table/right_table is anything but one one-argument {{ ref() }} call",
    ),
    spec(
        "SST-REF900",
        Severity.ERROR,
        "Resolver exceeded max depth",
        "resolution depth {found} exceeded the limit {expected}",
        "reduce the nesting of metric() references",
        condition="a chain of `metric()` references nests deeper than 32 metrics",
    ),
)
