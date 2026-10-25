"""Validation codes 801-869 (VAL): skills, plugins, and CoCo Desktop profiles.

Skill and plugin bundles, their publication channels and stages, and CoCo Desktop
profiles with the hooks, MCP configs, and commands they name.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Precheck, Severity, spec

TITLE: str = "Validation"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-VAL801",
        Severity.ERROR,
        "Skill, plugin, CoCo Desktop or CoWork profile, or agent folder is misnamed",
        "'{artifact}': {detail}",
        "rename the folder, keep one manifest spelling, or fix the manifest or SKILL.md name the message names",
        condition=(
            "a skill, plugin, CoCo Desktop profile or CoWork profile folder is misnamed or its manifest is wrong: a "
            "skill or plugin "
            "name is not kebab-case or would publish as an extension name that does not start with a "
            "letter; a profile name is not lowercase letters, digits, '-' or '_', or is 'shared'; a "
            "plugin or profile folder has no manifest, or both the .yml and .yaml spellings; an agent "
            "folder holds both agent.yml and agent.yaml, or a CoWork profile folder both "
            "cowork_profile.yml and cowork_profile.yaml; or the SKILL.md frontmatter or manifest "
            "`name` is not the folder's name"
        ),
        note=(
            "The message names which; rename the folder, keep one manifest spelling, or correct the "
            "manifest or frontmatter."
        ),
    ),
    spec(
        "SST-VAL802",
        Severity.ERROR,
        "Skill is not published as a typed CORTEX EXTENSION",
        "skill '{artifact}': TYPE is {found}",
        "publish with an explicit TYPE of SKILL or PLUGIN",
        condition=(
            "a plan finds a CORTEX EXTENSION already at the target with no TYPE, or one SST does not "
            "recognize; the release is blocked"
        ),
        note=("SST did not create that object. Drop or rename it, or publish the skill under another name."),
    ),
    spec(
        "SST-VAL803",
        Severity.ERROR,
        "Extension version is not immutable and SHA-derived",
        "skill '{artifact}': version '{value}' is not SHA-derived",
        "derive the version from the content hash, using +version_prefix",
        condition=(
            "an extension's version alias is not its version prefix followed by the first 12 "
            "characters of the bundle's content digest, uppercased"
        ),
        note="A version that is not content-derived could change content under a pinned reference.",
    ),
    spec(
        "SST-VAL804",
        Severity.INFO,
        "Extension version referenced by no agent",
        "{artifact}: version {value} is referenced by no agent",
        "reference it, or stop publishing it",
        condition=(
            "the project defines agents, and a skill or plugin it publishes is pinned by none of them "
            "and included in no plugin, CoCo Desktop profile or the shared layer"
        ),
        note=(
            "A skill people use directly from the catalog has a consumer SST cannot see, so this is "
            "information, not a fault; raise it with `diagnostics.severity_overrides` to make an "
            "unconsumed extension a warning."
        ),
    ),
    spec(
        "SST-VAL806",
        Severity.WARNING,
        "Folder-to-schema split introduced",
        "skill '{artifact}' would land in schema '{value}'",
        "split for confidentiality, not tidiness; the agent owner then needs USAGE per schema",
        condition=(
            "a folder route or a skill's or plugin's own `config:` places its extension outside the "
            "database and schema its block's root gives (`skills.+database`, `+schema`, or `plugins:`'s)"
        ),
        note="A new skill schema has to be provisioned before any agent can use it.",
    ),
    spec(
        "SST-VAL807",
        Severity.ERROR,
        "Certification attempted by moving schemas",
        "skill '{artifact}': certification would change its schema",
        "certify with the SNOWFLAKE.CORE.CERTIFICATION_STATUS tag",
        condition=(
            "a certified release targets another schema than state recorded for the extension; the release is blocked"
        ),
        note="A schema move changes the extension's name, which breaks every pinned reference.",
    ),
    spec(
        "SST-VAL808",
        Severity.ERROR,
        "Markdown link names no file of the skill",
        "skill '{artifact}': the link to '{path}' names no file or folder of the skill",
        "add the file, or correct the link; it resolves from the linking file's folder or the skill folder",
        condition="a Markdown link, image, or reference definition in the skill names no file or folder of the skill",
        note=(
            "A path in prose or a code block is not a link: it is an instruction or an example, and is never reported."
        ),
    ),
    spec(
        "SST-VAL810",
        Severity.WARNING,
        "Path names a skill file from the repository root",
        "skill '{artifact}': '{path}' names '{target}' by its path from the repository root",
        "write '{target}', the path from the skill folder, which is how the published skill holds it",
        condition="a path in the skill's Markdown walks down a `skills/` tree to one of the skill's own files",
    ),
    spec(
        "SST-VAL811",
        Severity.WARNING,
        "Bundle exceeds the size budget",
        "'{artifact}': bundle is {size} bytes, over {expected}",
        "move bulk content out of the bundle",
        condition=(
            "a bundle is over the 1 MiB size budget and within the 10 MiB scan limit "
            "(SST-VAL834 reports a bundle over it)"
        ),
        note="Skills are read on demand, so the cost is invocation latency and tokens.",
    ),
    spec(
        "SST-VAL812",
        Severity.WARNING,
        "SKILL.md exceeds the size budget",
        "skill '{artifact}': SKILL.md is {size} bytes, over {expected}",
        "move bulk content into a referenced file",
        condition="a skill's SKILL.md, as authored, is over the 25 KiB size budget",
        note="The skill's main file is read on every orchestration turn, so its cost is paid continuously.",
    ),
    spec(
        "SST-VAL813",
        Severity.WARNING,
        "Bundled file is never referenced",
        "skill '{artifact}': '{path}' is never referenced from the skill's Markdown",
        "reference it, or remove it",
        condition=(
            "a file of the skill, outside any nested skill's folder, is named by no path in the "
            "skill's Markdown, by its path or by a file name no other file shares"
        ),
        note="Dead payload still ships, still counts against the size budget, and still has to be reviewed.",
    ),
    spec(
        "SST-VAL814",
        Severity.WARNING,
        "Bundle carries a script with no consumer that can run it",
        "skill '{artifact}': '{path}' is executable and no consuming agent enables code_execution",
        "enable code_execution, or drop the script",
        condition=(
            "an extension agents pin ships an executable script, and none of those agents declares a "
            "code_execution tool"
        ),
        note="The skill loads and is selected; the script cannot run.",
    ),
    spec(
        "SST-VAL815",
        Severity.WARNING,
        "Bundled script reads a credential or an absolute local path",
        "skill '{artifact}': '{path}' contains {detail}",
        "parameterize it; a path that resolves locally will not resolve in the sandbox",
        condition="a bundled script holds an absolute local path, or any bundled text file a credential literal",
    ),
    spec(
        "SST-VAL816",
        Severity.INFO,
        "SKILL.md and bundle byte split",
        "skill '{artifact}': {value}",
        None,
        condition="any skill bundle builds; reports how its bytes split between SKILL.md and the rest",
    ),
    spec(
        "SST-VAL820",
        Severity.ERROR,
        "Stage PUT target does not end in a separator",
        "skill '{artifact}': PUT target '{path}' does not end in '/'",
        "append a trailing slash",
        condition="a bundle's or CoCo Desktop profile tree's stage upload location does not end in '/'",
    ),
    spec(
        "SST-VAL822",
        Severity.ERROR,
        "Uploaded artifact has no registry pointer",
        "skill '{artifact}': '{path}' was uploaded with no PROFILE_REGISTRY pointer",
        "write the pointer as part of the same publish",
        condition="no pointer in a CoCo Desktop profile's registry row reaches a tree the profile uploads",
    ),
    spec(
        "SST-VAL823",
        Severity.ERROR,
        "CoCo Desktop profile content hash does not cover every shipped tree",
        "skill '{artifact}': hash omits {value}",
        "include every shipped tree in the hash",
        condition=(
            "a CoCo Desktop profile ships a tree whose digest-named prefix is absent from what its VERSION is the "
            "digest of, so a change there would produce no new version"
        ),
    ),
    spec(
        "SST-VAL824",
        Severity.ERROR,
        "Deleted file survives into the next version",
        "skill '{artifact}': '{path}' was deleted and is still published",
        "build each version from empty, or issue explicit removals",
        condition=(
            "a plan finds a file under the bundle's stage prefix that the bundle no longer has, before "
            "the version exists, so ADD VERSION would publish it; the release is blocked"
        ),
        note="Remove the leftover file from the stage prefix, then plan again.",
    ),
    spec(
        "SST-VAL825",
        Severity.ERROR,
        "Version name is not collision-free under parallel deploys",
        "skill '{artifact}': version name '{value}' is not collision-free",
        "derive the version from the content SHA",
        condition=(
            "two releases name one extension's version alike for different bundle content; the later "
            "one does not compile"
        ),
    ),
    spec(
        "SST-VAL826",
        Severity.ERROR,
        "Grant sent before certification succeeded, or naming no role type",
        "'{artifact}': a GRANT would be sent {detail}",
        "grant only after certification succeeds, and name the role TYPE explicitly",
        condition=(
            "a GRANT would run while the publish's certification has not yet succeeded, or names no "
            "role type (ROLE or DATABASE ROLE)"
        ),
    ),
    spec(
        "SST-VAL827",
        Severity.ERROR,
        "Identifier interpolated into DDL without validation",
        "skill '{artifact}': '{value}' reaches DDL unvalidated",
        "validate every interpolated identifier at parse time",
        condition="a publisher would send a statement built as raw text, so nothing it interpolates was validated",
    ),
    spec(
        "SST-VAL828",
        Severity.ERROR,
        "Partial deploy reported as aggregate success",
        "skill '{artifact}': {detail}",
        "report per-channel status separately",
        condition=(
            "apply reports no outcome, or several, for a skill, plugin or CoCo Desktop profile change, so the "
            "result would not show that channel's state"
        ),
    ),
    spec(
        "SST-VAL829",
        Severity.WARNING,
        "Channel content hashes diverge",
        "skill '{artifact}': catalog and stage hashes diverge by {value}",
        "compare the two bundles",
        condition="a skill's catalog and stage file sets differ",
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-VAL830",
        Severity.WARNING,
        "Skill reaches neither channel",
        "skill '{artifact}' is published nowhere",
        "reference it from a CoCo Desktop profile, or give it a +bundle_stage",
        condition=(
            "no `+bundle_stage` resolves for the skill, and no CoCo Desktop profile, plugin a profile names, or the "
            "shared layer includes it"
        ),
    ),
    spec(
        "SST-VAL831",
        Severity.INFO,
        "CORTEX EXTENSION DDL surface used",
        "skill '{artifact}': {value}",
        None,
        condition=(
            "any skill or plugin compiles; it publishes through the CORTEX EXTENSION statements, "
            "which are absent from the public SQL reference and so carry upgrade risk"
        ),
    ),
    spec(
        "SST-VAL832",
        Severity.ERROR,
        "Skill name is not globally unique across extensions",
        "'{artifact}' collides with '{other}' as one extension name",
        "rename one of them",
        condition="a skill or plugin would publish under an extension name an earlier skill or plugin already takes",
        note="Global uniqueness is what makes one flat extension schema viable.",
    ),
    spec(
        "SST-VAL834",
        Severity.ERROR,
        "Bundle exceeds the extension scan limits",
        "'{artifact}': {detail}",
        "split the bundle; Snowflake scans at most 50 files, 2 MiB per file, 10 MiB per version",
        condition="a bundle breaks a Snowflake scan limit (50 files, 2 MiB/file, 10 MiB/version)",
    ),
    spec(
        "SST-VAL835",
        Severity.ERROR,
        "Plugin member is not a project skill",
        "plugin '{artifact}': member '{name}' is not a skill in this project",
        "add the skill under the skills directory, or remove it from the plugin",
        condition="a plugin lists a member that is not a skill in this project",
    ),
    spec(
        "SST-VAL836",
        Severity.ERROR,
        "Plugin member cannot be bundled",
        "plugin '{artifact}': member '{name}' has errors, so the plugin cannot be bundled",
        "fix the member skill's errors",
        condition="a plugin member skill has errors, so the plugin cannot be bundled",
    ),
    spec(
        "SST-VAL837",
        Severity.WARNING,
        "Plugin lists a member twice",
        "plugin '{artifact}': lists '{name}' more than once",
        "list each member once",
        condition="a plugin lists one member more than once",
    ),
    spec(
        "SST-VAL838",
        Severity.ERROR,
        "Version set on a project-published extension",
        "agent '{artifact}': skill source '{name}' sets version, which SST manages for {kind}('{path}')",
        "delete version; SST pins the published content-hash alias",
        condition="an agent skill source for a project-published extension sets version",
    ),
    spec(
        "SST-VAL839",
        Severity.ERROR,
        "Skill version pinned to the commit SHA",
        "agent '{artifact}': skill source '{name}' pins var('sha_version'), which names no published version",
        "use skill() or plugin() with no version, or pin a real version of a consumed extension",
        condition="an agent skill source pins var('sha_version'), which names no published version",
    ),
    spec(
        "SST-VAL840",
        Severity.ERROR,
        "Plugin entry name is not one of its members",
        "agent '{artifact}': plugin entry name {{{{ skill('{name}') }}}} must be {expected}",
        "name a member as {{ skill('<member>') }}, or remove the name to load every member",
        condition="an agent's `plugin()` entry names a skill that is not a member of the plugin",
    ),
    spec(
        "SST-VAL841",
        Severity.WARNING,
        "Catalog serves a different version",
        "{artifact}: the catalog will serve {found} of {target}, not {value}, because {detail}",
        "agents pin their version and are unaffected; to serve this version in the catalog, "
        "set `+certified: true` and un-certify any later certified version in Snowsight",
        condition=(
            "another version of the extension is certified and outranks the one the release "
            "publishes, so the catalog serves it whatever the default version is"
        ),
        note=(
            "Without such a version, apply makes the published version the extension's default, "
            "so a revert or a default moved by hand is an update in the plan, not this warning."
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-VAL843",
        Severity.WARNING,
        "Two artifacts publish to one Snowflake name",
        "{a} and {b} both publish to {target}",
        "rename one of them; plans and reports that name the object become ambiguous",
        condition="two artifacts of different types publish to one Snowflake name",
    ),
    spec(
        "SST-VAL844",
        Severity.ERROR,
        "CoCo Desktop profile names a skill the project does not have",
        "CoCo Desktop profile '{artifact}': skill '{name}' is not a skill in this project",
        "add the skill under the skills directory, or remove it from the CoCo Desktop profile",
        condition="the shared layer or a CoCo Desktop profile names a skill the project does not have",
    ),
    spec(
        "SST-VAL845",
        Severity.WARNING,
        "CoCo Desktop profile repeats a shared skill or command",
        "CoCo Desktop profile '{artifact}': {kind} '{name}' already reaches every profile through shared/",
        "remove it from the CoCo Desktop profile",
        condition="a CoCo Desktop profile repeats a skill or command the shared layer already carries",
    ),
    spec(
        "SST-VAL846",
        Severity.ERROR,
        "CoCo Desktop profile names an unknown hook",
        "CoCo Desktop profile '{artifact}': hook '{name}' is not defined under the hooks directory",
        "add the hook, or remove it from the CoCo Desktop profile",
        condition="a CoCo Desktop profile names a hook not defined under the hooks directory",
    ),
    spec(
        "SST-VAL847",
        Severity.ERROR,
        "CoCo Desktop profile names an unknown MCP config",
        "CoCo Desktop profile '{artifact}': MCP config '{name}' is not defined under the MCP servers directory",
        "add the config, or remove it from the CoCo Desktop profile",
        condition="a CoCo Desktop profile names an MCP config not defined under the MCP servers directory",
    ),
    spec(
        "SST-VAL848",
        Severity.ERROR,
        "MCP server entry is not a configuration",
        "CoCo Desktop profile '{artifact}': MCP server '{name}' is not a configuration object",
        "replace the placeholder with a server definition, or stop referencing the config",
        condition="an MCP server a CoCo Desktop profile uses is not a configuration object",
    ),
    spec(
        "SST-VAL849",
        Severity.ERROR,
        "Two MCP configs define one server",
        "CoCo Desktop profile '{artifact}': MCP server '{name}' is defined by both '{a}' and '{b}'",
        "rename one server, or reference only one of the configs",
        condition="two MCP configs a CoCo Desktop profile uses define one server name",
    ),
    spec(
        "SST-VAL850",
        Severity.ERROR,
        "MCP config carries a literal credential",
        "MCP config '{artifact}': server '{name}' sets '{key}' to a literal value",
        "use a ${VARIABLE} placeholder; published configs are readable by every CoCo Desktop profile user",
        condition=(
            "an MCP config sets a credential-like key to a literal value, or sends an `Authorization`, "
            "`Proxy-Authorization` or `Cookie` header whose value holds no `${VARIABLE}` placeholder"
        ),
    ),
    spec(
        "SST-VAL851",
        Severity.ERROR,
        "CoCo Desktop profile key is not accepted",
        "CoCo Desktop profile '{artifact}': '{key}' is not accepted: {reason}",
        "delete the key",
        condition="a CoCo Desktop profile declares a key SST refuses",
    ),
    spec(
        "SST-VAL852",
        Severity.ERROR,
        "Hook definition is incomplete or ambiguous",
        "hook '{artifact}': {detail}",
        "declare event and command, and name the script with script: \"{{ file('<path>') }}\" "
        "when the folder holds several files",
        condition=(
            "a hook folder has no manifest or both spellings, is not exactly one event/command, or, with "
            "no `script:`, holds no script or more than one"
        ),
        note="A `script:` that names no file is `SST-REF014`; one that is not a `file()` call is `SST-REF050`.",
    ),
    spec(
        "SST-VAL853",
        Severity.ERROR,
        "MCP config is not valid",
        "MCP config '{artifact}': {detail}",
        'write mcp.json as `{"mcpServers": {"<name>": {...}}}`',
        condition="an MCP config folder has no mcp.json, or it is not UTF-8 JSON of the mcpServers shape",
    ),
    spec(
        "SST-VAL854",
        Severity.INFO,
        "CoCo Desktop profile registry is not the one Desktop reads",
        "CoCo Desktop profiles publish to {value}; CoCo Desktop reads only {expected}",
        "unset coco_desktop_profiles.+registry_table to publish where Desktop reads, or keep this for rehearsal",
        condition=(
            "CoCo Desktop profiles publish to a registry other than CORTEX_CODE.CONFIG.PROFILE_REGISTRY, the one "
            "CoCo Desktop reads (`coco_desktop_profiles.+registry_table`, or a profile's own `config:`)"
        ),
    ),
    spec(
        "SST-VAL855",
        Severity.ERROR,
        "CoCo Desktop profile includes a skill or plugin with errors",
        "CoCo Desktop profile '{artifact}': {kind} '{name}' has errors, so the profile cannot publish",
        "fix the errors reported for it",
        condition="a CoCo Desktop profile includes a skill or plugin that has errors",
    ),
    spec(
        "SST-VAL856",
        Severity.ERROR,
        "Agent references an extension that cannot publish",
        "agent '{artifact}': {kind}('{name}') has no version to pin because {reason}",
        "fix the diagnostic that names the cause; the reference resolves once the extension publishes",
        condition="an agent references a declared extension that has no version to pin",
    ),
    spec(
        "SST-VAL857",
        Severity.ERROR,
        "File name cannot be published to a stage",
        "{artifact}: {value} cannot be staged, because '{found}' is not made only of {expected}",
        "rename the file or folder; a stage rejects any other name, so apply would fail partway through",
        condition="a skill file, hook script or command path has a name a stage rejects",
    ),
    spec(
        "SST-VAL858",
        Severity.ERROR,
        "CoCo Desktop profile names an unknown command",
        "CoCo Desktop profile '{artifact}': command '{name}' is not a .md file under the commands directory",
        "name a command by its path below project.commands_dir without .md, such as sql/check",
        condition=(
            "the shared layer or a CoCo Desktop profile names a command that is not a .md file under the commands "
            "directory"
        ),
    ),
    spec(
        "SST-VAL859",
        Severity.ERROR,
        "Command file is invalid",
        "command '{artifact}': {detail}",
        "frontmatter is optional; when present it is a YAML mapping of description, allowed-tools, skill, and hidden",
        condition="a command file is not UTF-8, or its frontmatter is malformed or holds an unknown value",
    ),
    spec(
        "SST-VAL860",
        Severity.ERROR,
        "CoCo Desktop profile names an unknown plugin",
        "CoCo Desktop profile '{artifact}': plugin '{name}' is not a plugin under the plugins directory",
        "name a plugin folder that has a plugin.yml",
        condition="a CoCo Desktop profile names a plugin not under the plugins directory",
    ),
    spec(
        "SST-VAL861",
        Severity.INFO,
        "Deleted extension forgotten",
        "{artifact}: {value} is gone from the catalog, so apply --prune forgets its state entry",
        None,
        condition=(
            "a prune finds that a skill or plugin state records, whose source is gone from the project, is "
            "no longer in the catalog: `sst drop` or someone by hand dropped it"
        ),
        note=(
            "The apply drops nothing; it only removes the state entry, so a later extension of that name "
            "is never taken for SST's. An extension still in the catalog stays report-only (SST-PLN034), "
            "which names the `sst drop` that removes it."
        ),
        precheck=Precheck.OBSERVE,
    ),
)
