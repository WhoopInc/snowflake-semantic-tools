"""Loading codes (LOD): file bytes, YAML syntax, document shape, template syntax, and sidecars."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Severity, spec

TITLE: str = "Loading"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-LOD001",
        Severity.ERROR,
        "YAML syntax error",
        "{file}:{line}:{col}: {detail}",
        "fix the YAML syntax at the reported position",
        condition=(
            "a YAML file, or a skill's `SKILL.md` frontmatter, does not parse, nests collections deeper "
            "than the depth limit, or holds a value YAML cannot construct, such as an unknown tag, or a "
            "character YAML does not allow, such as a control character"
        ),
    ),
    spec(
        "SST-LOD002",
        Severity.ERROR,
        "Document root is not a mapping",
        "{file} root is {found}, expected a mapping",
        "make the document a top-level mapping",
        condition="the document root, or a skill's frontmatter, is a list, a scalar, or null",
    ),
    spec(
        "SST-LOD003",
        Severity.WARNING,
        "File is empty",
        "{file} is empty",
        "add content, or delete the file",
        condition="the file holds no YAML document: it is empty, or holds only whitespace and comments",
    ),
    spec(
        "SST-LOD004",
        Severity.ERROR,
        "Template expression is malformed",
        "{file}:{line}:{col}: malformed template: {reason}",
        "close the `}}`, or remove the nested `{{`",
        condition=(
            "a `{{` never closes, or another `{{` opens inside it; also a view's, metric's, filter's or "
            "verified query's `tables` reference that is malformed"
        ),
        note=(
            "Not an unquoted `{{`: unquoted is the majority spelling and is not valid YAML, so it is "
            "neutralised at the load boundary rather than rejected."
        ),
    ),
    spec(
        "SST-LOD005",
        Severity.ERROR,
        "Duplicate key in a YAML mapping",
        "{file}:{line}: duplicate key '{key}'",
        "remove one of the two keys",
        condition="a mapping declares one key twice, where stock YAML would silently keep the later one",
    ),
    spec(
        "SST-LOD006",
        Severity.ERROR,
        "File is not valid UTF-8",
        "{file}: invalid UTF-8 at byte {offset}",
        "re-save the file as UTF-8",
        condition=(
            "a file SST reads is not UTF-8: project YAML, a `file()` sidecar such as an instruction, a "
            "verified-query SQL file or a routine body, a `SKILL.md`, or a profile's `AGENTS.md` or rule"
        ),
    ),
    spec(
        "SST-LOD007",
        Severity.ERROR,
        "File exceeds the size limit",
        "{file} is {size} bytes, over the {expected} limit",
        "split the file",
        condition="a YAML file is larger than 4 MiB",
    ),
    spec(
        "SST-LOD008",
        Severity.ERROR,
        "Multi-document YAML stream",
        "{file} contains {count} documents",
        "keep one document per file",
        condition="the file holds more than one YAML document",
    ),
    spec(
        "SST-LOD009",
        Severity.ERROR,
        "Unquoted colon inside a plain scalar",
        "{file}:{line}: '{key}' value contains an unquoted ':'",
        "quote the value",
        condition="YAML stops at an unquoted `: ` inside a plain value on a key's line",
    ),
    spec(
        "SST-LOD010",
        Severity.ERROR,
        "Tab character used for indentation",
        "{file}:{line}: tab used for indentation",
        "indent with spaces",
        condition="a structural line is indented with a tab, which YAML forbids",
    ),
    spec(
        "SST-LOD011",
        Severity.WARNING,
        "Folded scalar used for a multi-line value",
        "{file}:{line}: '{key}' uses a folded scalar",
        "use |- so line breaks survive",
        condition="a value is a folded scalar (`>` or `>-`), which joins its lines",
    ),
    spec(
        "SST-LOD012",
        Severity.WARNING,
        "Trailing whitespace or CRLF line endings",
        "{file}: {detail}",
        "run sst format",
        condition="lines end in CRLF, or carry trailing whitespace; reported once for each",
    ),
    spec(
        "SST-LOD013",
        Severity.ERROR,
        "Anchor or alias used",
        "{file}:{line}: YAML anchors are not supported",
        "expand the anchor",
        condition="the document writes an anchor (`&`) or an alias (`*`); one per anchor and alias",
    ),
    spec(
        "SST-LOD014",
        Severity.ERROR,
        "Merge key used",
        "{file}:{line}: merge keys are not supported",
        "expand the merge",
        condition="a mapping uses a merge key (`<<`)",
    ),
    spec(
        "SST-LOD015",
        Severity.ERROR,
        "Non-string mapping key",
        "{file}:{line}: mapping key {found} is not a string",
        "quote the key",
        condition="a mapping key loads as something other than a string, such as an int, bool, date or null",
    ),
    spec(
        "SST-LOD016",
        Severity.WARNING,
        "Implicit boolean or null coercion",
        "{file}:{line}: '{key}' value {found} coerced to {expected}",
        "quote the value if it is meant as a string, or write {} if it is meant as an empty block",
        condition=(
            "a plain `yes`, `no`, `on`, `off`, `y` or `n` loads as a boolean, or a `~` or empty value "
            "loads as null where null has no documented meaning"
        ),
    ),
    spec(
        "SST-LOD017",
        Severity.ERROR,
        "Byte-order mark present",
        "{file} begins with a BOM",
        "re-save without a BOM",
        condition=(
            "the file begins with a UTF-8 byte-order mark (U+FEFF): project YAML, a `file()` sidecar "
            "(a hook script among them), a `SKILL.md`, or a profile's `AGENTS.md` or rule; the mark is "
            "refused rather than removed, since removing it would change what is published"
        ),
    ),
    spec(
        "SST-LOD018",
        Severity.ERROR,
        "Referenced or project file cannot be read",
        "{file}: could not read {path}",
        "create the file, correct the path, or make it readable",
        condition=(
            "a `file()` sidecar a document references exists and cannot be read; also an agent, eval, "
            "metric, or project YAML file that cannot be read"
        ),
    ),
    spec(
        "SST-LOD019",
        Severity.ERROR,
        "Sidecar file is empty",
        "{path}, referenced by {file}, is empty",
        "add content, or remove the reference",
        condition="a `file()` sidecar holds only whitespace",
    ),
    spec(
        "SST-LOD020",
        Severity.WARNING,
        "Unrecognised file extension accepted",
        "{file} uses '{found}'",
        "standardise on .yml",
        condition="a discovered file uses an extension SST tolerates other than `.yml` and `.yaml`, such as `.YML`",
    ),
    spec(
        "SST-LOD021",
        Severity.ERROR,
        "Document declares no recognised root key",
        "{file} declares no recognised root key",
        "add the artifact's root key",
        condition=(
            "the root mapping has keys and none is a registered artifact type's root key; when only "
            "some are unclaimed, SST-PRS001 names each"
        ),
    ),
    spec(
        "SST-LOD022",
        Severity.ERROR,
        "Text holds a character Snowflake cannot store",
        "{file}: '{field}' holds {found}, which Snowflake cannot store",
        "remove the character",
        condition=(
            "a file SST reads holds a NUL character, or a value read from one holds a NUL or a lone "
            "UTF-16 surrogate, such as a YAML `\\0` or `\\ud800` escape: project YAML, a `file()` "
            "sidecar, a `SKILL.md`, a profile's `AGENTS.md` or rule, or a dbt description, synonym "
            "or sample value a view uses"
        ),
        note="refused wherever it is written, an agent's specification included",
    ),
    spec(
        "SST-LOD200",
        Severity.INFO,
        ".yaml extension accepted",
        "{file} uses .yaml; accepted",
        None,
        condition="a discovered file uses `.yaml`, which SST reads as it does `.yml`",
    ),
    spec(
        "SST-LOD201",
        Severity.INFO,
        "File loaded from cache",
        "{file} served from the load cache",
        None,
        condition=(
            "a file whose bytes were already parsed in this invocation is served from the in-process "
            "load cache; reported only with --verbose"
        ),
    ),
    spec(
        "SST-LOD202",
        Severity.INFO,
        "Line-ending normalisation applied on read",
        "{file} normalised {count} line endings on read",
        None,
        condition="CRLF line endings were read as LF in memory; the file itself is left as it is",
    ),
)
