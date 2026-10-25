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
        condition=(
            "the file `--config` or `$SST_CONFIG` names does not exist, or a command that needs a "
            "configuration file finds neither `sst_config.yml` nor `sst_config.yaml` at the project root"
        ),
    ),
    spec(
        "SST-CFG002",
        Severity.ERROR,
        "Config is not valid YAML",
        "{path} is not valid YAML: {detail}",
        "fix the YAML syntax at the reported position",
        condition="`sst_config.yml` or `dbt_project.yml` is not valid YAML; an invalid `profiles.yml` is SST-DBT019",
    ),
    spec(
        "SST-CFG003",
        Severity.WARNING,
        "Unknown config key",
        "unknown config key '{key}'",
        "remove the key, or check it against docs/reference/config.md (`sst docs --only config`)",
        condition=(
            "a config key is not declared where it is written, including a "
            "`diagnostics.severity_overrides` entry that names no registered code"
        ),
    ),
    spec(
        "SST-CFG004",
        Severity.ERROR,
        "Config value has the wrong type",
        "config key '{key}' expects {expected}, found {found}",
        "correct the value type",
        condition=(
            "a config value is not of its declared type, or an `SST_*` environment variable a command "
            "option reads is not of the option's type"
        ),
    ),
    spec(
        "SST-CFG005",
        Severity.ERROR,
        "Multiple config files discoverable",
        "{count} config files found at the project root: {files}",
        "delete one of the two, or pass --config naming the one to use",
        condition=(
            "both `sst_config.yml` and `sst_config.yaml` exist at the project root and neither "
            "`--config` nor `$SST_CONFIG` names the file to use"
        ),
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
        condition=(
            "a value of the right type is outside its choices or bounds or is not its fixed value, "
            "`enrichment.sample_values_display_limit` exceeds `enrichment.distinct_limit`, or "
            "`skills.+version_prefix` cannot start a version alias"
        ),
    ),
    spec(
        "SST-CFG009",
        Severity.ERROR,
        "profiles.yml not found",
        "no profiles.yml at any searched location",
        "create profiles.yml, or set DBT_PROFILES_DIR",
        condition=(
            "no `profiles.yml` exists in the `--profiles-dir` directory, `$DBT_PROFILES_DIR`, the "
            "project root, or `~/.dbt`, searched in that order"
        ),
    ),
    spec(
        "SST-CFG010",
        Severity.ERROR,
        "dbt profile or target not found",
        "target '{target}' is absent from dbt profile '{profile}'",
        "add the target, or pass --target with a declared name",
        condition=(
            "`profiles.yml` does not declare the project's dbt profile, or the profile's outputs do not "
            "declare the selected target or the `defer.target`"
        ),
    ),
    spec(
        "SST-CFG011",
        Severity.ERROR,
        "dbt profile type is not snowflake",
        "dbt profile '{profile}' declares type '{found}'",
        "point SST at a Snowflake dbt profile",
        condition="the selected `profiles.yml` target declares a `type` other than `snowflake`",
    ),
    spec(
        "SST-CFG012",
        Severity.ERROR,
        "Required field missing from the resolved dbt profile",
        "dbt profile '{profile}' has no {field}",
        "set {field} in profiles.yml or its env var",
        condition=(
            "account or user is absent or empty in the resolved target when a connection opens, or "
            "the target sets no `database` or no `schema`, which SST publishes into and keeps its "
            "state table in"
        ),
    ),
    spec(
        "SST-CFG013",
        Severity.ERROR,
        "env_var() names an unset variable with no default",
        "env_var('{var}') is unset and has no default",
        "set {var}, or give env_var() a default",
        condition="a `profiles.yml` field SST reads calls `env_var()` on an unset variable with no default",
    ),
    spec(
        "SST-CFG014",
        Severity.WARNING,
        "dbt profile field silently empty",
        "dbt profile '{profile}' leaves {field} empty",
        "set {field} explicitly rather than relying on account defaults",
        condition="the selected `profiles.yml` target sets no `role`, or no `warehouse`",
    ),
    spec(
        "SST-CFG015",
        Severity.ERROR,
        "evals block declares a database or schema",
        "evals: declares {key}, which is structurally invalid",
        "remove +database and +schema from evals:",
        condition="`evals:` sets `+database` or `+schema`",
        note="Eval objects resolve to their agent's schema, so neither key can apply.",
    ),
    spec(
        "SST-CFG016",
        Severity.ERROR,
        "Targeting value does not resolve for the current target",
        "+{key} does not resolve for target '{target}'",
        "declare the value for every target you publish to",
        condition=(
            "a whole-value target conditional in `sst_config.yml` has no arm for the current target and no final `else`"
        ),
    ),
    spec(
        "SST-CFG017",
        Severity.ERROR,
        "tool() reference does not resolve from config",
        "{{{{ tool('{name}') }}}} does not resolve",
        "declare the member in the tools directory, or correct the name",
        condition="a `tool()` call in a config value names no member the tools directory declares",
    ),
    spec(
        "SST-CFG018",
        Severity.WARNING,
        "Declared tool member referenced by nothing",
        "tool member '{name}' is referenced by nothing",
        "reference it from an agent, or delete it",
        condition="a declared tool member is referenced by no agent and no config value",
    ),
    spec(
        "SST-CFG023",
        Severity.ERROR,
        "max_staleness below the accepted floor",
        "{key} is {found}; the minimum is 120",
        "raise max_staleness to at least 120",
        condition=(
            "`semantic_views.+max_staleness`, a folder route's, or a view's `config.max_staleness` is set "
            "below 120 seconds, Snowflake's minimum"
        ),
    ),
    spec(
        "SST-CFG025",
        Severity.WARNING,
        "Value absent from an allowlist",
        "{kind} '{found}' is absent from {key}",
        "add it to {key}, or use an allowed value",
        condition=(
            "`agents.+orchestration_model` or `cowork_profiles.+orchestration_model`, or a folder "
            "route's, names a model "
            "`snowflake.orchestration_models` does not list; unset, that list holds only `auto`"
        ),
    ),
    spec(
        "SST-CFG029",
        Severity.ERROR,
        "var() reference has no declaration",
        "{{{{ var('{var}') }}}} is not declared in config",
        "declare {var} under vars:",
        condition=(
            "a `var()` call, in `sst_config.yml` or in project YAML, names a variable `vars:` does not "
            "declare; a declared variable whose value is empty is SST-REF009"
        ),
    ),
    spec(
        "SST-CFG031",
        Severity.ERROR,
        "snowflake_syntax_check is not set explicitly",
        "validation.snowflake_syntax_check is unset",
        "set it true or false explicitly",
        condition="`sst_config.yml` does not set `validation.snowflake_syntax_check`",
        note=(
            "Whether expressions are compiled decides what a green validate proves, so intent must be "
            "stated: unset, this error blocks the run and nothing is compiled against Snowflake; demoted "
            "with `diagnostics.severity_overrides`, the check runs as if the key were `true`."
        ),
    ),
    spec(
        "SST-CFG032",
        Severity.WARNING,
        "Config file shadows another config file",
        "using {used}; shadowed {shadowed}",
        "delete the shadowed file",
        condition=(
            "`--config` or `$SST_CONFIG` names a config file while the project root also holds a "
            "discoverable one, which is ignored"
        ),
    ),
    spec(
        "SST-CFG033",
        Severity.ERROR,
        "Illegal severity override",
        "severity_overrides {code}: {found} is not permitted ({reason})",
        "remove the override, or give the code a severity the floor permits",
        demotable=False,
        condition="a `diagnostics.severity_overrides` entry breaks the demotion floor",
        note=(
            "The floor: a `diagnostics.severity_overrides` entry may not lower a non-demotable code, "
            "demote an error to info, or promote an info code to error. A refused override is not "
            "applied. A `--strict` flag that disagrees with `validation.strict` is SST-CFG034, a "
            "warning, not this code."
        ),
    ),
    spec(
        "SST-CFG034",
        Severity.WARNING,
        "Strict flag and config key disagree",
        "--strict {flag} disagrees with validation.strict {config}; the flag wins",
        "remove one of the two",
        condition="`--strict` or `--no-strict` is given and `validation.strict` is set to the other value",
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
        "set fqn: on the entry under `extensions:` or `tags:`, or set default_prefix on that block",
        condition=(
            "an `extensions` entry, or a `tags` entry a view's `tag()` names, has no `fqn:` and "
            "either the block sets no `default_prefix` or the entry key contains a dot, which would "
            "render a four-part name; or the name it qualifies to does not parse"
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
            "and has no baseline file promotes at least one warning, unless `--no-strict` is given"
        ),
        note="Given until the project has run under 1.0 once; never when no warning is promoted.",
    ),
    spec(
        "SST-CFG038",
        Severity.ERROR,
        "Sample-value collection requested but the project refuses it",
        "--include {components} reads row data, and enrichment.allow_sample_value_collection is false",
        "leave sample-values and enums out of the run, or set enrichment.allow_sample_value_collection to true",
        demotable=False,
        condition=(
            "`sst enrich` includes a component that reads row data (`sample-values` or `enums`, "
            "alone or through a group such as `all`) while "
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
        "Block key moved to another block",
        "{block}: '{key}' is no longer read here; {reason}",
        "move its settings where the message says, and delete the key",
        condition=(
            "a block sets a key it no longer takes because another block now holds its settings: "
            "`skills.catalog`, `skills.stage`, or `skills.extensions`"
        ),
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
        condition="an eval config file declares a `sweep:` block, which this release does not run",
    ),
    spec(
        "SST-CFG046",
        Severity.ERROR,
        "Configuration requires a dbt project",
        "{key} requires a dbt project, and the project has no dbt_project.yml",
        "add dbt_project.yml, or remove the configuration; a project without dbt publishes skills, plugins, "
        "and CoCo Desktop profiles only",
        condition=(
            "configuration that needs dbt (semantic_views, evals, ...) is present and dbt_project.yml is "
            "absent, or `sst init` runs in a directory with no dbt_project.yml"
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
        "other template; dbt's as_number, as_bool, as_native and as_text filters are allowed after env_var()",
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
            "the dbt profile asks for an unsupported auth mode (refresh-token exchange, OAuth client "
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
        "SST-CFG052",
        Severity.ERROR,
        "Agent kit folder overlaps a published directory",
        "{path} overlaps project.{key} ({value}), a directory SST publishes from",
        "point project.{key} at a folder outside {path}; the agent kit's skills are for a coding "
        "agent, and SST would publish them to Snowflake",
        demotable=False,
        condition=(
            "`sst agent-kit` would install into a folder that is, contains, or lies inside a "
            "directory a `project.*_dir` key names, configured or by default"
        ),
    ),
    spec(
        "SST-CFG053",
        Severity.ERROR,
        "Unknown setting under config:",
        "'{key}' is not a setting of {block}",
        "use one of {expected}, written without its `+`; any other key belongs outside `config:`",
        condition=(
            "an artifact's `config:` block sets a key its block does not declare as a `+` key, or writes a "
            "key with its `+`"
        ),
    ),
    spec(
        "SST-CFG054",
        Severity.ERROR,
        "Setting written with no value",
        "'{key}' is set to null",
        "delete the key to inherit its value from the folder routes, the block root, or the default, or "
        "give it a value",
        condition=(
            "a key of an artifact's `config:` block, or a `+` key in `sst_config.yml`, "
            "is written with no value or as `null`; an absent key inherits, and null is not a value"
        ),
    ),
    spec(
        "SST-CFG055",
        Severity.ERROR,
        "Root-only setting set below the block root",
        "'{key}' can be set only at the root of {block}",
        "set {expected} once, at the root of {block}, and remove it here",
        condition=(
            "a `+` key the configuration schema marks root-only, such as `skills.+threads`, is set in a folder "
            "route, or in an artifact's `config:` block"
        ),
    ),
    spec(
        "SST-CFG056",
        Severity.ERROR,
        "Project names no profiles.yml profile",
        "no profiles.yml profile is named: {reason}",
        "set `profile:` in dbt_project.yml, or project.target_profile in sst_config.yml for a project "
        "without dbt; when both are set they must agree",
        condition=(
            "dbt_project.yml declares no `profile:`, the project has no dbt_project.yml and sets no "
            "`project.target_profile`, or the two name different dbt profiles"
        ),
    ),
    spec(
        "SST-CFG057",
        Severity.ERROR,
        "Sensitive tag entry names no tag",
        "'{key}' is {found}, which names no tag",
        "write the entry as `<database>.<schema>.<tag>`, or `<database>.<schema>.*` for every tag in the schema",
        demotable=False,
        condition=(
            "an entry of `governance.sensitive_tags` is not text, or is neither a three-part tag name nor a "
            "database and schema followed by `.*`"
        ),
        note=(
            "Such an entry would be read as naming nothing, so a column carrying the tag it meant would be "
            "sampled; no setting lowers it, and `sst enrich` stops on it before any read."
        ),
    ),
    spec(
        "SST-CFG058",
        Severity.ERROR,
        "Target conditional is not one SST evaluates",
        "'{key}' is a target conditional SST cannot read, from `{text}`",
        "write each arm as `'<value>' if target.name == '<target>'` (or `!=`), or "
        "`'<value>' if target.name in ('<target>', ...)` (or `not in`), chained with `else`",
        demotable=False,
        condition=(
            "a whole-value target conditional in `sst_config.yml` has an arm, or text after its arms, "
            "outside the grammar SST evaluates"
        ),
        note=(
            "The configuration evaluates only that grammar, never general Jinja, so a value is never resolved "
            "by a guess; the key is read as unset and the run fails rather than falling back to another location."
        ),
    ),
    spec(
        "SST-CFG059",
        Severity.ERROR,
        "Variable is the wrong shape for where it is used",
        "var('{name}') holds {found}",
        "after `target.name in` or `not in`, use a variable holding a non-empty list of quoted target "
        "names; everywhere else, a variable holding text",
        demotable=False,
        condition=(
            "in `sst_config.yml`, a `target.name in var('<name>')` arm names a variable that is not a non-empty "
            "list of target names, or a `{{ var('<name>') }}` used as text names a list or mapping"
        ),
        note="A list substituted as text would write its Python spelling into the value, so it is refused.",
    ),
    spec(
        "SST-CFG060",
        Severity.ERROR,
        "Target conditional is part of a longer value",
        "'{key}' has a target conditional inside a longer value, which never resolves",
        "make the conditional the whole value, with the full value in each arm, or hold the full value "
        "in a variable and make the key `{{ var('<name>') }}`",
        demotable=False,
        condition=(
            "a value in `sst_config.yml`, after its `var()` calls are substituted, holds a "
            "`{{ ... if target.name ... }}` template with other text beside it"
        ),
        note="Only a whole-value conditional is evaluated; any other would reach its reader as template text.",
    ),
    spec(
        "SST-CFG200",
        Severity.ERROR,
        "Deprecated config key",
        "config key '{key}' is deprecated; use '{expected}'",
        "rename the key",
        condition="a deprecated config key is set; it is still read as the key that replaced it",
    ),
)
