"""Configuration codes (CFG): `sst_config.yml` keys and values, folder routes, and dbt profiles."""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Severity, spec

TITLE: str = "Configuration"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-CFG001",
        Severity.ERROR,
        "No config file found",
        "no sst_config.yml at {path}",
        "create sst_config.yml at the project root, or pass --project-dir",
        condition="the explicit project root has no config file",
    ),
    spec(
        "SST-CFG002",
        Severity.ERROR,
        "Config is not valid YAML",
        "{path} is not valid YAML: {detail}",
        "fix the YAML syntax at the reported position",
        condition="the config file fails to parse",
    ),
    spec(
        "SST-CFG003",
        Severity.WARNING,
        "Unknown config key",
        "unknown config key '{key}'",
        "remove the key, or check it against sst docs config",
        condition="a config key is not in the known key set",
    ),
    spec(
        "SST-CFG004",
        Severity.ERROR,
        "Config value has the wrong type",
        "config key '{key}' expects {expected}, found {found}",
        "correct the value type",
        condition="a config value fails its declared type",
    ),
    spec(
        "SST-CFG005",
        Severity.ERROR,
        "Multiple config files discoverable",
        "{count} candidate config files found; used {used}, shadowed {shadowed}",
        "delete the shadowed files, or pass --project-dir",
        condition="more than one config file is discoverable from the project root",
    ),
    spec(
        "SST-CFG006",
        Severity.ERROR,
        "Required config key missing",
        "required config key '{key}' is absent",
        "add {key} to sst_config.yml",
        condition="a key with no default is absent",
    ),
    spec(
        "SST-CFG007",
        Severity.ERROR,
        "Unknown top-level key near a known key",
        "unknown top-level key '{key}'; did you mean '{suggestion}'?",
        "correct the spelling",
        condition="an unknown top-level key is within edit distance 1-2 of a known key",
    ),
    spec(
        "SST-CFG008",
        Severity.ERROR,
        "Config value outside its allowed domain",
        "config key '{key}' value {found} is outside {expected}",
        "use one of {expected}",
        condition="a value passes its type and fails its domain",
    ),
    spec(
        "SST-CFG009",
        Severity.ERROR,
        "profiles.yml not found",
        "no profiles.yml at any searched location",
        "create profiles.yml, or set DBT_PROFILES_DIR",
        condition="the dbt profile file is absent everywhere searched",
    ),
    spec(
        "SST-CFG010",
        Severity.ERROR,
        "Profile or target not found",
        "target '{target}' is absent from profile '{profile}'",
        "add the target, or pass --target with a declared name",
        condition="the requested target is not in profiles.yml",
    ),
    spec(
        "SST-CFG011",
        Severity.ERROR,
        "Profile type is not snowflake",
        "profile '{profile}' declares type '{found}'",
        "point SST at a Snowflake profile",
        condition="the resolved dbt profile is for another adapter",
    ),
    spec(
        "SST-CFG012",
        Severity.ERROR,
        "Credential field missing from the resolved profile",
        "profile '{profile}' has no {field}",
        "set {field} in profiles.yml or its env var",
        condition="account or user is absent or empty in the resolved target",
    ),
    spec(
        "SST-CFG013",
        Severity.ERROR,
        "env_var() resolved to an empty string",
        "env_var('{var}') is unset and has no default",
        "set {var}, or give env_var() a default",
        condition="a profile env_var() has no value and no default",
    ),
    spec(
        "SST-CFG014",
        Severity.WARNING,
        "Profile field silently empty",
        "profile '{profile}' leaves {field} empty",
        "set {field} explicitly rather than relying on account defaults",
        condition="role, warehouse, database or schema is absent from the resolved target",
    ),
    spec(
        "SST-CFG015",
        Severity.ERROR,
        "evals block declares a database or schema",
        "evals: declares {key}, which is structurally invalid",
        "remove +database and +schema from evals:",
        condition="eval targeting keys are set; eval objects resolve to the agent schema",
    ),
    spec(
        "SST-CFG016",
        Severity.ERROR,
        "Targeting value does not resolve for the current target",
        "+{key} does not resolve for target '{target}'",
        "declare the value for every target you publish to",
        condition="a +database or +schema resolves for some target and not the current one",
    ),
    spec(
        "SST-CFG017",
        Severity.ERROR,
        "tool() reference does not resolve from config",
        "{{ tool('{group}','{name}') }} does not resolve",
        "declare the group and member in the tools directory",
        condition="a config tool() ref names an undeclared group or member",
    ),
    spec(
        "SST-CFG018",
        Severity.WARNING,
        "Declared tool member referenced by nothing",
        "tool member '{group}.{name}' is referenced by nothing",
        "reference it from an agent, or delete it",
        condition="a declared tool member has no consumer",
    ),
    spec(
        "SST-CFG019",
        Severity.ERROR,
        "Tool member declared under both define and reference",
        "'{name}' appears under both define: and reference:",
        "choose one; a member is owned or referenced, never both",
        condition="a member is listed in both blocks",
    ),
    spec(
        "SST-CFG020",
        Severity.ERROR,
        "Per-group tools override names an unknown or reference-only group",
        "tools: override names group '{group}', which {reason}",
        "target a group that exists and contains define: members",
        condition="a per-group override names a missing group, or a reference-only group",
    ),
    spec(
        "SST-CFG023",
        Severity.ERROR,
        "max_staleness below the accepted floor",
        "+max_staleness is {found}; the minimum is 120",
        "raise max_staleness to at least 120",
        condition="a target-level max_staleness is set below 120 seconds",
    ),
    spec(
        "SST-CFG025",
        Severity.WARNING,
        "Value absent from an allowlist",
        "{kind} '{found}' is absent from {key}",
        "add it to {key}, or use an allowed value",
        condition="a tool type or model is used that neither the shipped default nor the allowlist names",
    ),
    spec(
        "SST-CFG029",
        Severity.ERROR,
        "var() reference has no declaration",
        "{{ var('{var}') }} is not declared in config",
        "declare {var} under vars:",
        condition="a var() reference resolves to nothing",
    ),
    spec(
        "SST-CFG031",
        Severity.ERROR,
        "snowflake_syntax_check is not set explicitly",
        "validation.snowflake_syntax_check is unset",
        "set it true or false explicitly",
        condition="the key is absent",
        note=(
            "1.0 defaults it true, but whether expressions are compiled decides what a green validate "
            "proves, so intent must be stated."
        ),
    ),
    spec(
        "SST-CFG032",
        Severity.WARNING,
        "Config file shadows another config file",
        "using {used}; shadowed {shadowed}",
        "delete the shadowed file",
        condition="discovery found a usable file and at least one shadowed candidate",
    ),
    spec(
        "SST-CFG033",
        Severity.ERROR,
        "Illegal severity override",
        "severity_overrides {code}: {found} is not permitted ({reason})",
        "promote instead of demoting, or remove the override",
        demotable=False,
        condition="an override breaks the demotion floor, or --strict and strict: disagree",
    ),
    spec(
        "SST-CFG034",
        Severity.WARNING,
        "Strict flag and config key disagree",
        "--strict {flag} disagrees with validation.strict {config}; the flag wins",
        "remove one of the two",
        condition="both are set and they disagree",
    ),
    spec(
        "SST-CFG035",
        Severity.WARNING,
        "Baseline nearing expiry",
        "baseline expires on {date}; {count} entries remain",
        "fix the baselined diagnostics, or run sst baseline renew --reason",
        condition="the baseline file expires within 30 days",
    ),
    spec(
        "SST-CFG036",
        Severity.ERROR,
        "Name-map entry cannot be qualified",
        "{block}: '{name}' cannot be qualified -- no fqn: and {reason}",
        "set fqn: on the entry, or set default_prefix on the block",
        condition=(
            "a `tags:` or `skills.extensions:` entry has no `fqn:` and either the block sets no "
            "`default_prefix` or the entry key contains a dot, which would render a four-part name; "
            "also two entries in one block that qualify to the same name"
        ),
    ),
    spec(
        "SST-CFG037",
        Severity.WARNING,
        "validation.strict is enforced from 1.0 and no baseline exists",
        "validation.strict: true is enforced from 1.0; {count} warnings now block",
        "run sst baseline add to hold the current warning count, then fix them",
        condition=(
            "the first 1.0 `validate`, `plan` or `apply` in a project that declares `strict: true` "
            "and has no baseline file"
        ),
        note="Once only, per project.",
    ),
    spec(
        "SST-CFG038",
        Severity.ERROR,
        "Sample-value collection requested but the project refuses it",
        "--include {components} reads row data, and enrichment.allow_sample_value_collection is false",
        "remove --include sample-values, or change the key in sst_config.yml",
        demotable=False,
        condition=(
            "`sst enrich` is invoked with `--include sample-values` or `--all` while "
            "`enrichment.allow_sample_value_collection: false`"
        ),
        note=("Fires on COLLECTION only -- an authored `sample_values` in project YAML is legal and must not trip it."),
    ),
    spec(
        "SST-CFG039",
        Severity.ERROR,
        "Baseline is past its expiry",
        "baseline expired on {date}; {count} entries resume blocking",
        "fix the baselined diagnostics, or run sst baseline renew --reason",
        condition="the baseline file is past its `expires_on`, so baselined diagnostics resume blocking",
    ),
    spec(
        "SST-CFG040",
        Severity.ERROR,
        "`sha_version` is declared in `vars:`",
        "vars.sha_version is supplied by SST and must not be declared",
        "remove it from `vars:`; SST resolves it from the commit being published",
        condition="`sha_version` is declared in `vars:`",
        note=(
            "`sha_version` pins an agent's Cortex Extension reference, so it is the published skill's "
            "version identity. Resolved from the commit it agrees by construction; typed by hand it "
            "goes stale silently, and `CREATE AGENT` accepts a nonexistent version alias, so a stale "
            "value produces an agent that reports success and has no skill."
        ),
    ),
    spec(
        "SST-CFG041",
        Severity.ERROR,
        "Folder route names a directory that does not exist",
        "config key '{key}' in block '{block}' names no directory under '{root}'",
        "create the directory, or remove the key",
        condition=("an unprefixed key inside an artifact block does not match a directory under that block's root"),
    ),
    spec(
        "SST-CFG042",
        Severity.ERROR,
        "Folder route declared under `evals:` or `skills:`",
        "{block}: declares a folder route '{key}'",
        "remove it -- `evals:` location is structural, and `skills:`'s unprefixed keys are its `catalog`/`stage` "
        "sub-blocks",
        condition="an unprefixed path-segment key appears inside `evals:`",
    ),
    spec(
        "SST-CFG043",
        Severity.ERROR,
        "Config key was removed",
        "config key '{key}' was removed: {reason}",
        "delete the key",
        condition="a key the schema records as removed is set; the message names what replaced it",
    ),
    spec(
        "SST-CFG044",
        Severity.ERROR,
        "Config key is not supported in this release",
        "config key '{key}' is not supported in this release",
        "delete the key",
        condition="a key reserved for a later release is set",
    ),
    spec(
        "SST-CFG046",
        Severity.ERROR,
        "Configuration requires a dbt project",
        "{key} requires a dbt project, and the project has no dbt_project.yml",
        "add dbt_project.yml, or remove the configuration; a project without dbt publishes skills, plugins, "
        "and profiles only",
        condition=(
            "configuration that needs dbt (semantic_views, evals, ...) is present and dbt_project.yml is absent"
        ),
    ),
    spec(
        "SST-CFG047",
        Severity.ERROR,
        "Configured directory does not exist",
        "{key} is {value}, which is not a directory in the project",
        "fix the path, or remove the key to use the default; otherwise SST finds nothing there, and "
        "--prune would remove everything that directory published",
        condition="a project.*_dir key names a path that is not a directory",
    ),
    spec(
        "SST-CFG048",
        Severity.WARNING,
        "profiles.yml field is not used by SST",
        "target '{target}': '{key}' is not a setting SST reads, so it is ignored",
        "check the spelling; SST passes only connection settings to Snowflake, so a misspelled credential "
        "field would otherwise be dropped silently",
        condition="a profiles.yml target carries a field SST does not read",
    ),
    spec(
        "SST-CFG049",
        Severity.ERROR,
        "profiles.yml value cannot be used",
        "target '{target}': '{key}' {problem}",
        "SST renders {{ env_var('NAME') }} and {{ env_var('NAME', 'default') }} anywhere in a value, and no "
        "other template; write numbers and booleans without filters such as as_number",
        condition=(
            "a profiles.yml value holds a template other than env_var(), or a whole-number/boolean "
            "field holds another value"
        ),
    ),
    spec(
        "SST-CFG050",
        Severity.ERROR,
        "Unsupported authentication configuration",
        "target '{target}': {detail}",
        "authenticate with a key pair, a password, SSO (authenticator), or an OAuth access token (token)",
        condition=(
            "the profile asks for an unsupported auth mode (refresh-token exchange, OAuth client "
            "credentials, two private keys) or an unreadable private_key"
        ),
    ),
    spec(
        "SST-CFG051",
        Severity.WARNING,
        "profiles.yml disables certificate revocation checks",
        "target '{target}': insecure_mode is true, so OCSP certificate revocation checks are off for this connection",
        "remove insecure_mode, or set it to false; it is a debugging switch, and with it on a revoked certificate "
        "is still accepted",
        condition="a profiles.yml target sets insecure_mode: true",
    ),
    spec(
        "SST-CFG200",
        Severity.ERROR,
        "Deprecated config key",
        "config key '{key}' is deprecated; use '{expected}'",
        "rename the key",
        condition="a deprecated key is present",
    ),
)
