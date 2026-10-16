"""Parsing codes (PRS): a file or field that does not have the shape, type, or value SST accepts."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Severity, spec

TITLE: str = "Parsing"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-PRS001",
        Severity.WARNING,
        "Unknown top-level key",
        "no registered type owns root key '{key}'",
        "remove the block, or register a type that owns it",
        condition=(
            "a document's root mapping holds a key no registered artifact type owns beside one that "
            "is owned; when none is owned, SST-LOD021 is reported instead"
        ),
    ),
    spec(
        "SST-PRS002",
        Severity.ERROR,
        "Required field missing",
        "{artifact}: required field '{field}' is missing",
        "add {field}",
        condition="a field with no default is absent from a known block",
    ),
    spec(
        "SST-PRS003",
        Severity.ERROR,
        "Field has the wrong type",
        "{artifact}: '{field}' expects {expected}, found {found}",
        "change the value to {expected}",
        condition="a field value fails its declared type",
    ),
    spec(
        "SST-PRS004",
        Severity.WARNING,
        "Unknown field in a known block",
        "{artifact}: unknown field '{field}'",
        "remove the field, or check the spelling",
        condition="a block contains a key the type does not model",
        note=(
            "Distinct from `SST-VAL013`/`SST-VAL014`: this is a key inside a block SST does model, so "
            "it is a typo or an SST-schema gap. Gated by `validation.strict`, not by "
            "`snowflake.allow_unknown_keys`."
        ),
    ),
    spec(
        "SST-PRS005",
        Severity.ERROR,
        "Name is not a valid identifier",
        "{artifact}: '{value}' is not a valid identifier",
        "use letters, digits and underscores, starting with a letter",
        condition="a name fails the identifier grammar",
        note=(
            "An agent's alias must be a plain identifier, rendered unquoted: one written quoted, or "
            "valid only once quoted, is reported too."
        ),
    ),
    spec(
        "SST-PRS006",
        Severity.ERROR,
        "Duplicate name within a type",
        "{type} '{name}' is declared more than once",
        "rename one of them",
        condition="two records of one type share a name",
    ),
    spec(
        "SST-PRS007",
        Severity.ERROR,
        "Name is not unique across the artifact set",
        "{type} '{name}' collides with {other}",
        "rename one of them",
        condition="a name declared in one file is declared again in another file, which the message names",
    ),
    spec(
        "SST-PRS008",
        Severity.ERROR,
        "Name collides with a derived member name",
        "metric '{name}' collides with a derived metric name",
        "rename one of them",
        condition="a derived (view-scoped) metric and a table-scoped metric share a name",
    ),
    spec(
        "SST-PRS009",
        Severity.ERROR,
        "Name is not a valid tool identifier",
        "'{name}' does not resolve to a 1-64 char tool identifier",
        "shorten or simplify the name",
        condition=(
            "a resolved agent tool name holds a character other than a letter, digit, `_` or `-`; a "
            "name that is empty or over 64 characters is SST-VAL513"
        ),
    ),
    spec(
        "SST-PRS010",
        Severity.ERROR,
        "Identifier exceeds the object name limit",
        "'{name}' is {size} chars, over the {expected} limit",
        "shorten the name",
        condition="an eval's rendered source table name is empty or longer than 128 characters",
    ),
    spec(
        "SST-PRS011",
        Severity.ERROR,
        "Identifier requires quoting and quoting is not declared",
        "'{name}' needs quoting to render safely",
        "rename it, or declare it quoted",
        condition="a name is written unquoted and is a valid identifier only once quoted",
    ),
    spec(
        "SST-PRS012",
        Severity.WARNING,
        "Non-ASCII identifier",
        "'{name}' contains non-ASCII characters",
        "use ASCII identifiers",
        condition="an identifier contains characters outside ASCII",
    ),
    spec(
        "SST-PRS013",
        Severity.ERROR,
        "Value outside the allowed set for a closed field",
        "{artifact}: '{field}' is '{found}', expected one of {expected}",
        "use one of {expected}",
        condition="a closed-enum field holds an unrecognised value",
    ),
    spec(
        "SST-PRS014",
        Severity.ERROR,
        "Mutually exclusive fields both present",
        "{artifact}: '{field}' and '{other}' are mutually exclusive",
        "declare exactly one",
        condition="two fields that exclude each other are both set",
    ),
    spec(
        "SST-PRS015",
        Severity.ERROR,
        "Paired fields not declared together",
        "{artifact}: '{field}' requires '{other}'",
        "declare both, or neither",
        condition="one half of a required pair is present",
    ),
    spec(
        "SST-PRS016",
        Severity.ERROR,
        "Numeric value out of range",
        "{artifact}: '{field}' is {found}, expected {expected}",
        "use a value in {expected}",
        condition="a numeric field is zero, negative or beyond its bound",
    ),
    spec(
        "SST-PRS017",
        Severity.ERROR,
        "Unparseable timestamp",
        "{artifact}: '{field}' value '{found}' is not an epoch or ISO timestamp",
        "use an integer epoch or an ISO-8601 string",
        condition="a timestamp field parses as neither form",
    ),
    spec(
        "SST-PRS018",
        Severity.ERROR,
        "Collection element has the wrong shape",
        "{artifact}: '{field}'[{index}] expects {expected}, found {found}",
        "correct the element",
        condition="a list element fails the element schema",
    ),
    spec(
        "SST-PRS019",
        Severity.ERROR,
        "Boolean field holds a non-boolean",
        "{artifact}: '{field}' is '{found}', expected a boolean",
        "use true or false",
        condition="a boolean field holds a string or number",
    ),
    spec(
        "SST-PRS020",
        Severity.ERROR,
        "Deprecated field spelling",
        "{artifact}: '{field}' is deprecated; use '{expected}'",
        "rename the field",
        condition="a key uses its 0.3 spelling, in place of its 1.0 key or beside it; beside it, it is not read",
    ),
    spec(
        "SST-PRS021",
        Severity.ERROR,
        "Deprecated relationship column shape",
        "{artifact}: relationship_columns / left_column / right_column is not supported",
        "use relationship_conditions",
        condition="the removed relationship shape is declared",
    ),
    spec(
        "SST-PRS022",
        Severity.ERROR,
        "Unknown field within edit distance of a known field",
        "{artifact}: unknown field '{field}'; did you mean '{expected}'?",
        "correct the spelling",
        condition="an unknown key is 1-2 edits from a modelled key, so it is a typo",
    ),
    spec(
        "SST-PRS023",
        Severity.ERROR,
        "Passthrough block collides with a modelled key",
        "{artifact}: passthrough key '{key}' is already rendered by SST",
        "remove the key from passthrough",
        condition="a passthrough or tool_spec_passthrough key duplicates a rendered key",
    ),
    spec(
        "SST-PRS024",
        Severity.WARNING,
        "Passthrough block is non-empty",
        "{artifact}: {count} passthrough keys will be rendered unvalidated",
        "promote the keys to first-class fields where possible",
        condition="a passthrough block carries values SST does not model",
    ),
    spec(
        "SST-PRS025",
        Severity.ERROR,
        "Reserved alias used",
        "{artifact}: alias '{value}' is reserved",
        "choose a different alias",
        condition=(
            "an agent's alias is `LIVE`, `FIRST`, `LAST` or `DEFAULT`, or a Snowflake reserved word "
            "such as `select` or `current`, ignoring case"
        ),
        note="SST never double-quotes an alias to rescue a reserved word: quoting would make it case-sensitive.",
    ),
    spec(
        "SST-PRS026",
        Severity.ERROR,
        "Tag value exceeds the length limit",
        "{artifact}: tag '{field}' value is {size} chars, over 256",
        "shorten the tag value",
        condition="a tag value is longer than 256 characters",
    ),
    spec(
        "SST-PRS027",
        Severity.ERROR,
        "Tags block has the wrong shape",
        "{artifact}: tags must be a list of name and value entries, found {found}",
        "correct the tags block",
        condition=(
            "the tags block fails its schema: it is not a list of name and value entries, or an entry "
            "lacks name or value"
        ),
        note="An entry whose name is not one `{{ tag('<name>') }}` call is `SST-REF050`.",
    ),
    spec(
        "SST-PRS028",
        Severity.ERROR,
        "Constraints block has the wrong shape",
        "{artifact}: constraints block is invalid: {detail}",
        "correct the constraints block",
        condition=(
            "a dbt model's `meta.sst.distinct_range` is not a mapping that names both a `start` and an `end` column"
        ),
    ),
    spec(
        "SST-PRS029",
        Severity.ERROR,
        "Synonyms block has the wrong type",
        "{artifact}: synonyms must be a list of strings, found {found}",
        "use a list of strings",
        condition="the synonyms value is not a list of strings",
    ),
    spec(
        "SST-PRS030",
        Severity.WARNING,
        "Synonym contains problematic characters",
        "{artifact}: synonym '{value}' contains {detail}",
        "remove the punctuation",
        condition=(
            "a synonym holds a quote or a control character; a synonym `sst enrich` proposes is also "
            "refused for template syntax, and is dropped rather than written"
        ),
    ),
    spec(
        "SST-PRS031",
        Severity.WARNING,
        "Name collides with a reserved word",
        "'{name}' is a reserved word",
        "rename it, or accept the quoted identifier",
        condition="a name, unquoted, is a Snowflake reserved word",
    ),
    spec(
        "SST-PRS032",
        Severity.ERROR,
        "Input schema property has a forbidden type",
        "{artifact}: input_schema property '{field}' has type '{found}'",
        "use string, number, integer, boolean or array",
        condition=(
            "a generic tool's input_schema property declares a type other than string, number, "
            "integer, boolean or array, or a tool member's signature has a parameter typed OBJECT"
        ),
    ),
    spec(
        "SST-PRS033",
        Severity.ERROR,
        "Required input-schema name is not a declared property",
        "{artifact}: input_schema.required names '{field}', absent from properties",
        "declare the property, or drop it from required",
        condition="a required entry has no matching property",
    ),
    spec(
        "SST-PRS034",
        Severity.ERROR,
        "Frontmatter missing a required key",
        "{artifact}: SKILL.md frontmatter omits '{field}'",
        "declare name and description in the frontmatter",
        condition="a skill's SKILL.md frontmatter has no non-blank string `name` or `description`; once for each",
    ),
    spec(
        "SST-PRS100",
        Severity.WARNING,
        "Name collides with a reserved identifier namespace",
        "'{name}' is in a reserved namespace",
        "rename it",
        condition="a name is `snowflake`, or starts with `sst_`, `sst$`, `snowflake_` or `snowflake$`, ignoring case",
    ),
    spec(
        "SST-PRS101",
        Severity.ERROR,
        "Required collection is empty",
        "{artifact}: '{field}' is present and empty",
        "declare at least one entry, or remove the key",
        condition="an eval config enables no system metric and names no custom metric",
    ),
    spec(
        "SST-PRS103",
        Severity.ERROR,
        "Value outside the allowed enum",
        "{artifact}: '{field}' is '{found}', expected one of {expected}",
        "use one of {expected}",
        condition=(
            "a metric's `access_modifier`, or a window `order_by` entry's `sort_direction` or "
            "`null_order`, holds an unrecognised value"
        ),
    ),
    spec(
        "SST-PRS104",
        Severity.ERROR,
        "Member declared under two owners",
        "{member} is declared under both {a} and {b}",
        "declare it once",
        condition="one member name is declared in the folders of two owning types",
    ),
    spec(
        "SST-PRS105",
        Severity.ERROR,
        "Member type not accepted by its owner",
        "{path}: {root}/ holds {type} members, not the {member_type} under '{key}:'",
        "move the {member_type} to the {member_type} folder, or out of {root}/",
        condition="a file in a member type's folder declares a member of another type, as one item or a list",
    ),
    spec(
        "SST-PRS106",
        Severity.ERROR,
        "Duplicate member name within an owner",
        "{artifact}: {member_type} '{name}' is declared twice",
        "rename one of them",
        condition="a member's name repeats in the file that first declares it",
    ),
    spec(
        "SST-PRS107",
        Severity.ERROR,
        "Member declares no distinguishing key",
        "{artifact}: {member} has no name",
        "name the member",
        condition=(
            "a member entry is not a mapping, or has no `name`; a single-item file's member is named "
            "`the <type>`, a list's by its index"
        ),
    ),
    spec(
        "SST-PRS110",
        Severity.ERROR,
        "Condition does not parse to one column pair",
        "{artifact}: condition '{value}' does not parse to one left/right pair",
        "express one pair per condition",
        condition="a relationship condition is not an equality, an ASOF comparison or a range",
    ),
    spec(
        "SST-PRS111",
        Severity.ERROR,
        "Range condition names one column twice",
        "{artifact}: range condition uses '{name}' for both start and end",
        "name two different columns",
        condition="a range condition names one column for both its start and its end",
    ),
    spec(
        "SST-PRS112",
        Severity.ERROR,
        "More than one asof condition",
        "{artifact}: {count} asof conditions declared",
        "declare at most one",
        condition="more than one of a relationship's conditions is an ASOF comparison",
    ),
    spec(
        "SST-PRS113",
        Severity.ERROR,
        "Expression field is not a string",
        "{artifact}: '{field}' expects a SQL string, found {found}",
        "supply the expression as a string",
        condition="a metric's `expr` is present and not a string, such as a list or a mapping",
    ),
    spec(
        "SST-PRS114",
        Severity.ERROR,
        "Score ranges are not contiguous",
        "{artifact}: score_ranges leave a gap or overlap at {value}",
        "make the ranges contiguous with min-inclusive, max-inclusive boundaries",
        condition="a judge metric's score band is inverted, or its bands leave a gap or overlap",
    ),
    spec(
        "SST-PRS115",
        Severity.ERROR,
        "threshold_default declares no usable bound inside the scale",
        "{artifact}: threshold_default {found} is not a usable bound within max_score {expected}",
        "declare min, optionally max, with min <= max and both inside max_score",
        condition=(
            "a custom metric's threshold_default map omits min, inverts its bounds, or places one "
            "outside the declared scale"
        ),
    ),
    spec(
        "SST-PRS116",
        Severity.ERROR,
        "Unsupported template placeholder in a judge prompt",
        "{artifact}: '{placeholder}' is not one of the 12 supported names",
        "use a supported placeholder",
        condition="a judge prompt placeholder is not in the supported set",
    ),
    spec(
        "SST-PRS117",
        Severity.ERROR,
        "Dataset row has no question or no expected field",
        "{artifact}: row {index} has {detail}",
        "give every row a question and at least one expectation",
        condition="an eval dataset row has no `question`, or no `ground_truth`; once for each",
    ),
    spec(
        "SST-PRS118",
        Severity.ERROR,
        "Sample question entry is a bare string",
        "{artifact}: sample_questions[{index}] is a string, expected a mapping",
        "write each entry as a mapping with a `question:` key",
        condition="an agent's sample question is not a mapping with a string `question`",
    ),
    spec(
        "SST-PRS119",
        Severity.ERROR,
        "Skill folder contains another skill folder",
        "{artifact}: nested skill folder at {path}",
        "flatten the layout; one skill per folder",
        condition="a skill folder contains another skill folder; once per nested folder",
    ),
    spec(
        "SST-PRS120",
        Severity.ERROR,
        "SKILL.md is not at the folder root",
        "{artifact}: SKILL.md found at {path}",
        "move SKILL.md to the folder root",
        condition=(
            "a folder holds files of its own and its SKILL.md sits in a subfolder, where Snowflake will not find it"
        ),
    ),
    spec(
        "SST-PRS123",
        Severity.ERROR,
        "Semantic view definition is malformed",
        "{artifact}: {detail}",
        "fix the field the message names; the semantic views guide lists each field's form",
        condition="a semantic view fails to build for a reason no more specific code reports",
    ),
    spec(
        "SST-PRS124",
        Severity.ERROR,
        "Window frame is not a frame clause",
        "{artifact}: window frame '{value}' is not ROWS or RANGE BETWEEN <bound> AND <bound>",
        "write each bound as UNBOUNDED PRECEDING, UNBOUNDED FOLLOWING, CURRENT ROW, or <n> or `INTERVAL '<n> "
        "<unit>'` followed by PRECEDING or FOLLOWING",
        condition="a window frame is not `ROWS/RANGE BETWEEN <bound> AND <bound>`",
    ),
    spec(
        "SST-PRS125",
        Severity.WARNING,
        "File formatting is not preserved",
        "{file}: writing it changes lines sst enrich did not edit",
        "review the change with sst enrich --dry-run; once the file is written, later runs keep its formatting exactly",
        condition="sst enrich would rewrite lines it did not edit while writing a file",
    ),
    spec(
        "SST-PRS126",
        Severity.ERROR,
        "Removed field",
        "{artifact}: '{field}' is no longer written: {reason}",
        "remove the field",
        condition="a key that SST now works out for itself, or reads from elsewhere, is written",
        note="`sst migrate refs --write` removes the member `tables:` and `derived:` keys.",
    ),
    spec(
        "SST-PRS127",
        Severity.ERROR,
        "View variable default does not fit its data type",
        "{artifact}: variable '{name}' of type {type} cannot default to {found}: it takes {expected}",
        "write the default as the message says, or change the variable's data_type",
        condition=(
            "a semantic view variable's `default_value` is not a value of its `data_type`'s family: a "
            "number for a numeric type, true or false for BOOLEAN, a string for text, ISO 8601 text "
            "for DATE, TIME or a timestamp; or the type is one no default can be written for"
        ),
        note="A temporal default renders as `'<text>'::<type>`; Snowflake reads a bare string as VARCHAR.",
    ),
    spec(
        "SST-PRS900",
        Severity.ERROR,
        "Parser returned a non-frozen record",
        "{type} parser returned {cls}, which is mutable",
        "return a frozen dataclass",
        condition="a parser returns a record that is not a frozen dataclass, nor a tuple of them",
    ),
)
