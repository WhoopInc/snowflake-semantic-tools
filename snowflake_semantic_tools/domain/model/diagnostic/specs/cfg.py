"""Configuration codes (CFG): `sst_config.yml` keys and values, folder routes, and dbt profiles."""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.diagnostic.core import ErrorSpec, Severity, spec

TITLE: str = "Configuration"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-CFG003",
        Severity.WARNING,
        "Unknown config key",
        "unknown config key '{key}'",
        "remove the key, or check it against the configuration reference",
    ),
    spec(
        "SST-CFG004",
        Severity.ERROR,
        "Config value has the wrong type",
        "config key '{key}' expects {expected}, found {found}",
        "correct the value type",
    ),
    spec(
        "SST-CFG006",
        Severity.ERROR,
        "Required config key missing",
        "required config key '{key}' is absent",
        "add the key to sst_config.yml",
    ),
    spec(
        "SST-CFG007",
        Severity.ERROR,
        "Unknown top-level key near a known key",
        "unknown top-level key '{key}'; did you mean '{suggestion}'?",
        "correct the spelling",
    ),
    spec(
        "SST-CFG008",
        Severity.ERROR,
        "Config value outside its allowed domain",
        "config key '{key}' value {found} is outside {expected}",
        "use an allowed value",
    ),
    spec(
        "SST-CFG010",
        Severity.ERROR,
        "Profile or target not found",
        "target '{target}' is absent from profile '{profile}'",
        "add the target, or pass --target with a declared name",
    ),
    spec(
        "SST-CFG015",
        Severity.ERROR,
        "evals block declares a database or schema",
        "evals: declares {key}, which is structurally invalid",
        "remove +database and +schema from evals:; eval objects resolve to the agent's schema",
    ),
    spec(
        "SST-CFG036",
        Severity.ERROR,
        "Name-map entry cannot be qualified",
        "{block}: '{name}' cannot be qualified -- no fqn: and {reason}",
        "set fqn: on the entry, or set default_prefix on the block",
    ),
    spec(
        "SST-CFG038",
        Severity.ERROR,
        "Sample-value collection is disabled",
        "--include {components} reads row data, and enrichment.allow_sample_value_collection is false",
        "leave sample-values and enums out of --include; authored sample_values are still read",
        demotable=False,
    ),
    spec(
        "SST-CFG040",
        Severity.ERROR,
        "sha_version is declared in vars",
        "vars.sha_version is supplied by SST and must not be declared",
        "remove it from vars:",
    ),
    spec(
        "SST-CFG041",
        Severity.ERROR,
        "Folder route names a directory that does not exist",
        "config key '{key}' in block '{block}' names no directory under '{root}'",
        "create the directory, or remove the key",
    ),
    spec(
        "SST-CFG042",
        Severity.ERROR,
        "Folder route declared under evals or skills",
        "{block}: declares a folder route '{key}'",
        "remove it; evals: location is structural, and skills: keeps only its catalog, stage, and extensions blocks",
    ),
    spec(
        "SST-CFG043",
        Severity.ERROR,
        "Config key was removed",
        "config key '{key}' was removed: {reason}",
        "delete the key",
    ),
    spec(
        "SST-CFG044",
        Severity.ERROR,
        "Config key is not supported in this release",
        "config key '{key}' is not supported in this release",
        "delete the key",
    ),
    spec(
        "SST-CFG046",
        Severity.ERROR,
        "Configuration requires a dbt project",
        "{key} requires a dbt project, and the project has no dbt_project.yml",
        "add dbt_project.yml, or remove the configuration; a project without dbt publishes skills, plugins, "
        "and profiles only",
    ),
    spec(
        "SST-CFG047",
        Severity.ERROR,
        "Configured directory does not exist",
        "{key} is {value}, which is not a directory in the project",
        "fix the path, or remove the key to use the default; otherwise SST finds nothing there, and "
        "--prune would remove everything that directory published",
    ),
    spec(
        "SST-CFG048",
        Severity.WARNING,
        "profiles.yml field is not used by SST",
        "target '{target}': '{key}' is not a setting SST reads, so it is ignored",
        "check the spelling; SST passes only connection settings to Snowflake, so a misspelled credential "
        "field would otherwise be dropped silently",
    ),
    spec(
        "SST-CFG049",
        Severity.ERROR,
        "profiles.yml value cannot be used",
        "target '{target}': '{key}' {problem}",
        "SST renders {{ env_var('NAME') }} and {{ env_var('NAME', 'default') }} anywhere in a value, and no "
        "other template; write numbers and booleans without filters such as as_number",
    ),
    spec(
        "SST-CFG050",
        Severity.ERROR,
        "Unsupported authentication configuration",
        "target '{target}': {detail}",
        "authenticate with a key pair, a password, SSO (authenticator), or an OAuth access token (token)",
    ),
)
