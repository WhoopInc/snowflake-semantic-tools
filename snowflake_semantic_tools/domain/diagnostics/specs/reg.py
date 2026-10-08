"""Registry codes (REG): the codes and artifact types SST is built from contradict themselves.

Every one is raised as `RegistryIntegrityError` while the package is imported or a registry is
built, except SST-REG015, which a source scan in the test suite raises; none is ever reported
for a user's project, and every one is non-demotable: a registry that contradicts itself cannot
be trusted to report its own contradiction.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Severity, spec

TITLE: str = "Registry"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-REG001",
        Severity.ERROR,
        "Registration missing a required field",
        "registration for {type} omits required field '{field}'",
        "add {field} to the register() call",
        demotable=False,
        condition=(
            "an artifact or member type leaves a required field empty, an object-lifecycle type names no "
            "object type, a type pins versions of a type outside its `dependency_types`, a member's owner "
            "omits it from `member_types`, or a diagnostic code has an empty title or message template"
        ),
    ),
    spec(
        "SST-REG002",
        Severity.ERROR,
        "Duplicate registration",
        "{type} is already registered",
        "choose a different name, or delete the duplicate registration",
        demotable=False,
        condition="two artifact types, two member types, or two diagnostic codes are registered under one name",
    ),
    spec(
        "SST-REG003",
        Severity.ERROR,
        "Two types claim one root_key",
        "root_key '{root_key}' is claimed by {types}",
        "give each artifact type a distinct root_key",
        demotable=False,
        condition="two artifact or member types declare the same YAML root key",
    ),
    spec(
        "SST-REG004",
        Severity.ERROR,
        "Duplicate ddl_position or clause_position",
        "position {position} is claimed by {types}",
        "renumber one of the colliding positions",
        demotable=False,
        condition=(
            "two artifact types share a `ddl_position`, or two member types of one owner share a `clause_position`"
        ),
    ),
    spec(
        "SST-REG005",
        Severity.ERROR,
        "Two types claim one ref_function",
        "ref_function '{ref_function}' is claimed twice",
        "give each referenceable type its own template function",
        demotable=False,
        condition='two artifact or member types register the same "{{ }}" function name',
    ),
    spec(
        "SST-REG006",
        Severity.ERROR,
        "Type names an unregistered rule",
        "{type} names rule '{rule_id}', which is not registered",
        "register the rule, or remove it from the type",
        demotable=False,
        condition="a type's `validation_rules` names a rule set the diagnostics registry does not define",
    ),
    spec(
        "SST-REG007",
        Severity.ERROR,
        "Type names an unregistered member type or owner",
        "{type} names '{member_type}', which is not registered",
        "register the member type, or correct the name",
        demotable=False,
        condition=(
            "a member's owner type, or an artifact type's member or dependency type, is not registered; "
            "or an artifact type lists a member type whose `owner_type` is not that artifact type"
        ),
    ),
    spec(
        "SST-REG010",
        Severity.ERROR,
        "ddl_position contradicts dependency_types",
        "{type} is ordered at {position} but depends on {blocker}, ordered later",
        "renumber ddl_position so dependents follow their dependencies",
        demotable=False,
        condition="an artifact type's `ddl_position` is not after that of every type in its `dependency_types`",
    ),
    spec(
        "SST-REG011",
        Severity.ERROR,
        "Type dependency graph has a cycle",
        "type dependency cycle: {cycle}",
        "break the cycle by removing one entry from a type's dependency_types",
        demotable=False,
        condition="the graph the artifact types' `dependency_types` declare has a cycle",
    ),
    spec(
        "SST-REG012",
        Severity.ERROR,
        "Code registered with a malformed identifier",
        "'{code}' does not match SST-<AREA><NNN>",
        "use three uppercase letters and three digits",
        demotable=False,
        condition="a registered diagnostic code is not `SST-` followed by three uppercase letters and three digits",
    ),
    spec(
        "SST-REG013",
        Severity.ERROR,
        "Code area disagrees with its declared subsystem",
        "'{code}' declares subsystem {subsystem}",
        "make the area letters and the subsystem field agree",
        demotable=False,
        condition="a diagnostic code's `subsystem` field is not the three letters after `SST-` in the code",
    ),
    spec(
        "SST-REG014",
        Severity.ERROR,
        "Internal-band code is not a non-demotable ERROR",
        "'{code}' is in the 900 band with severity {severity}",
        "declare 900-band codes ERROR, and non_demotable in INT and REG",
        demotable=False,
        condition=(
            "a 900-band code is registered WARNING or INFO, or an INT or REG 900-band code is registered demotable"
        ),
    ),
    spec(
        "SST-REG015",
        Severity.ERROR,
        "Severity comparison outside the diagnostics module",
        "{module} compares severities outside diagnostics/",
        "route the decision through the resolved severity on the diagnostic",
        demotable=False,
        condition=(
            "a package module outside `domain/diagnostics/` compares a `.severity` attribute or a "
            "`Severity` member instead of reading `Diagnostic.blocks`; the test suite scans for this, "
            "not the import"
        ),
    ),
    spec(
        "SST-REG016",
        Severity.ERROR,
        "Message template placeholder is not supplied",
        "'{code}' template names {placeholder}, absent from declared params",
        "add the placeholder to the code's params, or correct the template",
        demotable=False,
        condition="a message template names a placeholder outside the shared placeholder vocabulary",
    ),
    spec(
        "SST-REG017",
        Severity.ERROR,
        "Retired number re-registered",
        "'{code}' was retired in {version} and cannot be reused",
        "allocate the next free number in the band",
        demotable=False,
        condition="a diagnostic code is registered under a number the 1.0 catalog retired",
    ),
    spec(
        "SST-REG018",
        Severity.ERROR,
        "Deprecation without a successor",
        "'{code}' is deprecated_in {version} with no superseded_by",
        "name the successor code, or do not deprecate",
        demotable=False,
        condition="a diagnostic code sets `deprecated_in` and leaves `superseded_by` unset",
    ),
    spec(
        "SST-REG019",
        Severity.ERROR,
        "internal_detail declared outside INT and SNO",
        "'{code}' declares internal_detail but its area is {area}",
        "drop internal_detail, or move the raw text into params",
        demotable=False,
        condition="a code outside the INT and SNO areas sets `internal_detail`",
    ),
    spec(
        "SST-REG020",
        Severity.ERROR,
        "Resolver set and registry `ref_function`s disagree",
        "'{ref_function}' -- {direction}",
        "register the type, or remove the resolver",
        demotable=False,
        condition=(
            "a resolver addresses no registered type, a type's `ref_function` has no resolver, or an "
            "artifact type declares no `ref_function` without being listed as deliberately unreferenced"
        ),
    ),
    spec(
        "SST-REG021",
        Severity.ERROR,
        "`grant_preservation` contradicts the type's other fields",
        "type '{artifact}' -- {reason}",
        "correct the registration against the probe result",
        demotable=False,
        condition=(
            "a type that replaces its object declares `NONE` grant preservation, a type that never "
            "replaces declares some, or a composite type declares an object type or is prunable or "
            "replaced generically"
        ),
    ),
    spec(
        "SST-REG022",
        Severity.ERROR,
        "Non-`semantic_view` artifact type declares `member_types`",
        "artifact type '{artifact}' declares member_types {member_types}; only semantic_view may",
        "leave `member_types` empty, or register the members as their own top-level type",
        demotable=False,
        condition="an artifact type other than `semantic_view` registers a non-empty `member_types` tuple",
    ),
    spec(
        "SST-REG023",
        Severity.ERROR,
        "`MemberIndex` does not cover every declared member type",
        "MemberIndex covers {covered}; semantic_view declares {declared}",
        "index every declared member type",
        demotable=False,
        condition=("the set of member types in `MemberIndex` is not the set named by `semantic_view.member_types`"),
    ),
    spec(
        "SST-REG024",
        Severity.ERROR,
        "A `ref_field` policy is undeclared, or a declared policy is neither bound nor explained",
        "ref policy '{policy}': {detail}",
        "declare the policy, bind it to a field, or state why it is unbound",
        demotable=False,
        condition=(
            "a policy named by `ref_field(...)` is not declared, or a declared policy is bound to no "
            "field and carries no stated reason, or is both bound and explained as unbound"
        ),
    ),
    spec(
        "SST-REG900",
        Severity.ERROR,
        "Registry mutated after freeze",
        "registry mutated after freeze: {detail}",
        "move the registration to import time",
        demotable=False,
        condition="an artifact or member type is registered, or the type registry frozen again, after it is frozen",
    ),
)
