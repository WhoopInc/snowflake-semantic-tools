"""What a code that is no longer raised means: SST 0.3's codes, and the 1.0 numbers since retired.

Neither kind is ever raised. A 0.3 code survives as an alias so an old CI log or a suppression
comment stays readable: `ALIASES` records, for each, its 0.3 title, what became of it, and the 1.0
codes it now answers to. A retired 1.0 number is burned; `TOMBSTONES` keeps its title, why it was retired,
and the code that took its condition over when one did, so `sst explain` and the error reference
answer for it rather than calling it unknown. Both tables are data and import nothing but the standard library.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType


@dataclass(frozen=True, slots=True)
class Alias:
    """One SST 0.3 code, and the 1.0 codes it resolves to.

    Attributes:
        title: The code's 0.3 title.
        disposition: What 1.0 did with it: `KEPT`, `SPLIT` into several codes, `RETIRED`, or
            `KEPT, then SUPERSEDED`.
        targets: The 1.0 codes it now answers to, most direct first; empty
            for a code retired with no successor.
    """

    code: str
    title: str
    disposition: str
    targets: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Tombstone:
    """One retired 1.0 number: what it detected, and the code that detects it now.

    Attributes:
        superseded_by: The code that took the condition over; None when the condition is gone.
        reason: Why the number was retired, as `sst explain` and the error reference state it.
    """

    code: str
    title: str
    superseded_by: str | None
    reason: str


def _aliases(*rows: tuple[str, str, str, tuple[str, ...]]) -> Mapping[str, Alias]:
    return MappingProxyType({row[0]: Alias(*row) for row in rows})


def _tombstones(*rows: tuple[str, str, str | None, str]) -> Mapping[str, Tombstone]:
    return MappingProxyType({row[0]: Tombstone(*row) for row in rows})


ALIASES: Mapping[str, Alias] = _aliases(
    ("SST-V001", "Missing required field", "SPLIT", ("SST-PRS002", "SST-VAL201", "SST-PRS107")),
    ("SST-V002", "Unknown table reference", "SPLIT", ("SST-DBT002", "SST-REF001")),
    ("SST-V003", "Unknown column reference", "SPLIT", ("SST-REF002", "SST-DBT004", "SST-VAL020")),
    ("SST-V004", "Duplicate name", "KEPT", ("SST-PRS006", "SST-PRS106", "SST-VAL001")),
    (
        "SST-V005",
        "Invalid field type",
        "SPLIT",
        (
            "SST-PRS003",
            "SST-PRS018",
            "SST-PRS019",
            "SST-DBT004",
            "SST-VAL418",
            "SST-VAL113",
            "SST-PRS113",
            "SST-PRS029",
            "SST-MEM101",
        ),
    ),
    ("SST-V006", "Empty required field", "KEPT", ("SST-PRS101", "SST-PRS107")),
    ("SST-V007", "Invalid column_type", "KEPT", ("SST-PRS013",)),
    ("SST-V008", "Invalid data_type", "KEPT", ("SST-DBT004",)),
    ("SST-V010", "Missing primary_key", "SPLIT", ("SST-VAL311", "SST-VAL312", "SST-DBT003", "SST-MEM011")),
    ("SST-V011", "Primary key column not found", "KEPT", ("SST-VAL310", "SST-VAL210")),
    ("SST-V012", "Missing table description", "KEPT", ("SST-VAL003",)),
    ("SST-V013", "Invalid synonyms type", "SPLIT", ("SST-PRS029", "SST-MEM011", "SST-VAL020")),
    ("SST-V014", "Problematic characters in synonyms", "KEPT", ("SST-PRS030",)),
    ("SST-V015", "Invalid constraints configuration", "KEPT", ("SST-PRS028",)),
    ("SST-V016", "Invalid tags configuration", "KEPT", ("SST-PRS027",)),
    ("SST-V020", "Missing column description", "KEPT", ("SST-VAL003",)),
    ("SST-V021", "Missing column_type metadata", "KEPT", ("SST-VAL308",)),
    ("SST-V022", "Missing data_type metadata", "KEPT", ("SST-VAL309",)),
    ("SST-V023", "Fact column must be numeric", "KEPT", ("SST-VAL305",)),
    ("SST-V024", "Time dimension must be temporal", "KEPT", ("SST-VAL306",)),
    ("SST-V025", "Enum without sample_values", "SPLIT", ("SST-VAL314", "SST-VAL315", "SST-MEM103")),
    ("SST-V032", "Missing metric tables", "KEPT", ("SST-MEM002",)),
    ("SST-V033", "Invalid metric expression type", "KEPT", ("SST-PRS113", "SST-PRS002")),
    ("SST-V034", "Empty metric tables list", "KEPT", ("SST-PRS126",)),
    ("SST-V035", "Invalid visibility value", "SPLIT", ("SST-PRS013", "SST-VAL121", "SST-VAL122")),
    ("SST-V036", "Invalid non_additive_by configuration", "KEPT", ("SST-VAL118",)),
    ("SST-V037", "Invalid window metric configuration", "KEPT", ("SST-VAL102", "SST-PRS003")),
    ("SST-V038", "Invalid using_relationships type", "KEPT", ("SST-PRS003", "SST-VAL115")),
    ("SST-V039", "Cross-entity column reference in metric expression", "KEPT", ("SST-VAL112",)),
    ("SST-V040", "Missing relationship field", "SPLIT", ("SST-PRS002", "SST-VAL201")),
    ("SST-V041", "Relationship table not found", "KEPT", ("SST-VAL205", "SST-REF001")),
    ("SST-V042", "Relationship missing primary key", "KEPT", ("SST-VAL203",)),
    ("SST-V043", "Relationship column not found", "KEPT", ("SST-VAL204", "SST-REF002")),
    ("SST-V044", "Using relationship not found", "KEPT", ("SST-VAL214", "SST-VAL204")),
    ("SST-V045", "Derived metric must reference other metrics", "KEPT", ("SST-REF006",)),
    ("SST-V046", "Invalid field on derived metric", "KEPT", ("SST-PRS126",)),
    ("SST-V047", "Excluded column referenced in expression", "KEPT", ("SST-VAL318",)),
    ("SST-V048", "Primary key and unique keys overlap", "KEPT, then SUPERSEDED", ("SST-VAL223",)),
    ("SST-V049", "Unsupported multi-column expression in relationship condition", "KEPT", ("SST-VAL213", "SST-PRS110")),
    ("SST-V050", "Deprecated filters syntax", "RETIRED", ("SST-VAL403",)),
    ("SST-V051", "Invalid filter expression", "KEPT", ("SST-VAL401",)),
    ("SST-V052", "Deprecated custom instruction key names", "KEPT", ("SST-VAL012", "SST-PRS020")),
    ("SST-V060", "Missing verified query field", "SPLIT", ("SST-PRS002", "SST-VAL412", "SST-VAL020")),
    ("SST-V061", "VQR sql_file not found", "KEPT", ("SST-LOD018", "SST-LOD019")),
    ("SST-V062", "VQR mutual exclusivity violation", "KEPT", ("SST-PRS014", "SST-VAL412")),
    ("SST-V070", "Missing semantic view field", "SPLIT", ("SST-PRS002", "SST-VAL003")),
    ("SST-V071", "Semantic view references unknown table", "KEPT", ("SST-MEM008", "SST-REF001")),
    ("SST-V081", "Quoted template expression", "RETIRED", ()),
    ("SST-V090", "Circular dependency", "SPLIT", ("SST-REF005", "SST-VAL010", "SST-VAL215", "SST-PLN005")),
    ("SST-V091", "Duplicate metric expressions", "KEPT", ("SST-VAL124", "SST-PRS006")),
    ("SST-V092", "Metric references undeclared column", "KEPT", ("SST-VAL110",)),
    ("SST-P001", "YAML syntax error", "RETIRED", ("SST-LOD001",)),
    ("SST-P002", "YAML colon in description", "RETIRED", ("SST-LOD001", "SST-LOD009")),
    ("SST-P003", "Template parsing error", "KEPT", ("SST-REF003",)),
    ("SST-P004", "Invalid YAML structure", "RETIRED", ("SST-LOD002",)),
    ("SST-E001", "Snowflake connection failed", "SPLIT", ("SST-PRT001", "SST-PRT002", "SST-SNO013", "SST-SNO014")),
    ("SST-E002", "Schema not found", "RETIRED", ("SST-PLN010", "SST-SNO005")),
    ("SST-E003", "Permission denied", "KEPT", ("SST-PRT004", "SST-PLN008")),
    ("SST-E004", "Manifest not found", "KEPT", ("SST-PRT006", "SST-DBT001")),
    ("SST-E005", "Stale manifest", "RETIRED", ("SST-DBT005",)),
    ("SST-E006", "Source not found in manifest", "KEPT", ("SST-DBT011",)),
    ("SST-E007", "Source table not found in Snowflake", "KEPT", ("SST-PLN007", "SST-SNO003")),
    ("SST-E008", "No sources found", "KEPT", ("SST-DBT025", "SST-DIS003")),
    ("SST-E009", "Source YAML write failed", "KEPT", ("SST-PRT008",)),
    ("SST-E010", "Invalid source selector", "KEPT", ("SST-PRT105",)),
    ("SST-G001", "DDL execution failed", "SPLIT", ("SST-APL001", "SST-SNO001")),
    ("SST-G002", "Object name collision", "KEPT", ("SST-SNO002", "SST-PLN009")),
    ("SST-G003", "Missing metadata tables", "RETIRED", ("SST-APL027",)),
    ("SST-G004", "Table not found in Snowflake", "SPLIT", ("SST-SNO003", "SST-PLN007", "SST-PLN002")),
    ("SST-G005", "Insufficient privileges", "KEPT", ("SST-SNO004", "SST-PLN008")),
    ("SST-G006", "SQL output write failed", "KEPT", ("SST-PRT008",)),
    ("SST-G007", "SST manifest not found", "KEPT", ("SST-MAN001",)),
    ("SST-G008", "SST YAML change detection failed", "SPLIT", ("SST-MAN022", "SST-MAN023")),
    ("SST-C001", "Missing sst_config.yml", "RETIRED", ("SST-CFG001",)),
    ("SST-C002", "Missing dbt_project.yml", "RETIRED", ("SST-CFG002", "SST-DBT019")),
    ("SST-C003", "Invalid configuration", "RETIRED", ("SST-CFG003", "SST-CFG004", "SST-CFG008")),
    ("SST-C004", "Missing profiles.yml", "RETIRED", ("SST-CFG009", "SST-CFG004")),
    ("SST-C005", "Compile failed", "KEPT", ("SST-MAN008", "SST-CFG006")),
    ("SST-C006", "Manifest write failed", "KEPT", ("SST-MAN007", "SST-PRT008")),
    ("SST-C007", "Manifest load failed", "SPLIT", ("SST-MAN002", "SST-MAN003", "SST-MAN025")),
    ("SST-C008", "Manifest stale", "KEPT", ("SST-DBT005",)),
    ("SST-D001", "Diff connection failed", "SPLIT", ("SST-PRT001", "SST-PLN001")),
    ("SST-D002", "Diff manifest not found", "KEPT", ("SST-MAN001",)),
    ("SST-D003", "No views to compare", "RETIRED", ("SST-DIS003", "SST-PLN016")),
    ("SST-D004", "Describe view failed", "KEPT", ("SST-PLN001", "SST-SNO025")),
    ("SST-K001", "Target directory not found", "KEPT", ("SST-PRT009",)),
    ("SST-K002", "Could not remove artifact", "KEPT", ("SST-PRT010", "SST-PRT008")),
)

TOMBSTONES: Mapping[str, Tombstone] = _tombstones(
    (
        "SST-MEM003",
        "Declared table does not name a known dbt model",
        "SST-REF001",
        "a member's tables are the models its ref() calls name, so an unknown one is that ref(), SST-REF001",
    ),
    (
        "SST-MEM012",
        "Tables inferred for a member",
        None,
        "a member's tables are always the models its templates name, so there is nothing to report",
    ),
    (
        "SST-PRS102",
        "tables is explicitly empty",
        "SST-PRS126",
        "`tables:` is no longer written on a member; its tables are inferred",
    ),
    (
        "SST-REF043",
        "Expression references a table outside the member's tables:",
        None,
        "a member's tables are the models its expression names, so none can lie outside them",
    ),
    (
        "SST-REF045",
        "Relationship endpoint is written as a ref() call",
        "SST-REF046",
        "relationship endpoints take a `ref()` call; a bare model name is SST-REF046",
    ),
    (
        "SST-VAL104",
        "Derived metric references a physical column",
        None,
        "a metric whose expression names a column is table-scoped by definition, never derived",
    ),
    (
        "SST-VAL108",
        "Derived metric declares a table list",
        "SST-PRS126",
        "`tables:` is no longer written on a metric; SST-PRS126 names the key",
    ),
    (
        "SST-VAL109",
        "Table-scoped metric declares no table list",
        "SST-MEM002",
        "a metric's tables are inferred from its expression; one that names none is SST-MEM002",
    ),
    (
        "SST-APL101",
        "Rolled back after a smoke failure",
        None,
        "apply runs no smoke probes and never rolls back; smoke is its own suite, `sst test --suite smoke`",
    ),
    (
        "SST-APL102",
        "Smoke failed and the prior definition was not recoverable",
        None,
        "apply runs no smoke probes and never rolls back, so there is no prior definition to recover",
    ),
    (
        "SST-CFG021",
        "publish_via is not yaml or ddl",
        None,
        "`generation.publish_via` is removed: SST renders DDL directly",
    ),
    (
        "SST-CFG022",
        "publish_via incompatible with a declared feature",
        None,
        "`generation.publish_via` is removed, so there is one publish path for every feature",
    ),
    ("SST-CFG024", "spec_version is not recognised", None, "configuration carries no spec version to check"),
    (
        "SST-CFG026",
        "dependency_order is incomplete or has unknown entries",
        None,
        "publish order comes from the artifact registry and the dependency graph, never from a setting",
    ),
    ("SST-CFG027", "Smoke queries disabled", None, "apply runs no smoke probes, so there is nothing to disable"),
    (
        "SST-CFG028",
        "emit_relationship_type is enabled",
        None,
        "`generation.emit_relationship_type` is removed: relationship and join types are never rendered",
    ),
    ("SST-CFG030", "infer_is_enum is enabled", None, "there is no such setting; `sst enrich` writes `is_enum`"),
    (
        "SST-DBT007",
        "meta.sst location disagrees with the dbt config",
        None,
        "`meta.sst.database` and `meta.sst.schema` are refused outright (SST-DBT030), so no copy is left to disagree",
    ),
    (
        "SST-DBT008",
        "meta.sst location is hardcoded",
        None,
        "the keys it warned against are refused outright (SST-DBT030)",
    ),
    ("SST-MAN004", "Impact index incomplete", None, "the impact index it checked is no longer written"),
    (
        "SST-MAN021",
        "state.manifest_id does not match the current manifest",
        None,
        "no run is refused over a manifest mismatch: each artifact is compared with its own state entry",
    ),
    (
        "SST-MEM102",
        "Member attached only through a legacy reference",
        None,
        "legacy `table()` and `column()` references are refused at parse, so no member can attach through one",
    ),
    ("SST-PRS108", "Role-playing occurrence has no distinct name", None, "role-playing tables are not supported"),
    (
        "SST-PRT007",
        "dbt manifest schema version unsupported",
        "SST-DBT017",
        "the manifest schema check belongs with the other dbt checks",
    ),
    (
        "SST-REF016",
        "Two-argument ref used with a dbt cross-project idiom",
        None,
        "SST reads the dbt manifest, not model SQL, and in SST YAML a two-argument `ref` always names a model and "
        "a column, so a misread one is an unknown column (SST-REF002)",
    ),
    (
        "SST-REF017",
        "Hardcoded fully-qualified name",
        "SST-VAL006",
        "SST-VAL006 already reported the same condition, at validation, which sees the literal text",
    ),
    (
        "SST-REF021",
        "Ref target is declared under both define and reference",
        "SST-CFG019",
        "SST-CFG019 already reported the same condition, when configuration is validated",
    ),
    (
        "SST-REF024",
        "Legacy table() reference does not resolve",
        "SST-REF034",
        "`table()` is refused at parse, so it never reaches resolution",
    ),
    (
        "SST-REF025",
        "Legacy column() reference does not resolve",
        "SST-REF035",
        "`column()` is refused at parse, so it never reaches resolution",
    ),
    (
        "SST-REF038",
        "Unknown project variable",
        "SST-CFG029",
        "SST-CFG029 already reported an undeclared `var()`, wherever it appears",
    ),
    (
        "SST-REF200",
        "table() is deprecated",
        "SST-REF034",
        "`table()` is refused, not deprecated; a warning would say it still works",
    ),
    (
        "SST-REF201",
        "column() is deprecated",
        "SST-REF035",
        "`column()` is refused, not deprecated; a warning would say it still works",
    ),
    (
        "SST-SNO021",
        "Unsupported clause for the publish path",
        None,
        "`generation.publish_via` is removed, so there is one publish path",
    ),
    (
        "SST-VAL015",
        "Deploy ordering violated",
        "SST-PLN038",
        "raised by plan, so it moved to the PLN area",
    ),
    (
        "SST-VAL313",
        "Primary key and unique keys overlap",
        "SST-VAL223",
        "SST-VAL223 reports a primary-key column that is also declared unique",
    ),
    (
        "SST-VAL501",
        "create_mode holds an unrecognised value",
        None,
        "`create_mode` is removed: an artifact is created when absent and updated by its type's rule",
    ),
    (
        "SST-VAL502",
        "create_mode and copy_grants combination is illegal",
        None,
        "`create_mode` and `copy_grants` are removed",
    ),
    ("SST-VAL503", "copy_grants is a no-op for this create mode", None, "`create_mode` and `copy_grants` are removed"),
    (
        "SST-VAL504",
        "Replace would drop live grants",
        None,
        "how grants survive a replace is each artifact type's registered rule, not a setting to get wrong",
    ),
    (
        "SST-VAL505",
        "Replace would run without COPY GRANTS",
        None,
        "how grants survive a replace is each artifact type's registered rule, not a setting to get wrong",
    ),
    (
        "SST-VAL620",
        "Publishing an artifact type whose grant preservation is unprobed",
        None,
        "the artifact registry refuses a type that replaces its object without declaring grant handling (SST-REG021)",
    ),
    (
        "SST-VAL736",
        "Selected verified queries are removed for the duration of a run",
        None,
        "evals evaluate agents only; the Cortex Analyst evaluation this guarded is not part of SST",
    ),
    (
        "SST-VAL749",
        "Analyst eval declares the wrong subject or source",
        None,
        "evals evaluate agents only; the Cortex Analyst evaluation this guarded is not part of SST",
    ),
    (
        "SST-VAL750",
        "Analyst eval declares more than one semantic view",
        None,
        "evals evaluate agents only; the Cortex Analyst evaluation this guarded is not part of SST",
    ),
    (
        "SST-VAL751",
        "Analyst eval requests a metric other than sql_correctness",
        None,
        "evals evaluate agents only; the Cortex Analyst evaluation this guarded is not part of SST",
    ),
    (
        "SST-VAL752",
        "min_verified_queries resolved per file instead of per view",
        None,
        "evals evaluate agents only; the Cortex Analyst evaluation this guarded is not part of SST",
    ),
    (
        "SST-VAL753",
        "Analyst gate is an absolute pass rate",
        None,
        "evals evaluate agents only; the Cortex Analyst evaluation this guarded is not part of SST",
    ),
    (
        "SST-VAL754",
        "A verified query was removed since the last run",
        None,
        "evals evaluate agents only; the Cortex Analyst evaluation this guarded is not part of SST",
    ),
    (
        "SST-VAL756",
        "Analyst eval run overlaps a regenerate of the same view",
        None,
        "evals evaluate agents only; the Cortex Analyst evaluation this guarded is not part of SST",
    ),
    (
        "SST-VAL757",
        "Analyst evals cannot colocate with their ground truth",
        None,
        "evals evaluate agents only; the Cortex Analyst evaluation this guarded is not part of SST",
    ),
)
