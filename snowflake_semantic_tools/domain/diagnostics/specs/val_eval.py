"""Validation codes 7xx (VAL): Cortex Agent evaluations.

Datasets and their rows, run names and pinned versions, system and custom metrics,
judge prompts and scoring ranges, thresholds, and baselines.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Precheck, Severity, spec

TITLE: str = "Validation"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-VAL701",
        Severity.ERROR,
        "Dataset name is not unique within the agent schema",
        "dataset '{artifact}' is declared twice in {value}",
        "rename one of them",
        condition=(
            "two evals' `dataset.name_template` render the same dataset name, compared ignoring case; "
            "the eval that claims it second is reported"
        ),
    ),
    spec(
        "SST-VAL702",
        Severity.ERROR,
        "Dataset name exceeds the object limit",
        "dataset '{artifact}' is {size} chars, over 128",
        "shorten the name",
        condition="the DATASET object name limit is exceeded",
    ),
    spec(
        "SST-VAL703",
        Severity.ERROR,
        "Dataset declares its own database or schema",
        "dataset '{artifact}' declares +{key}",
        "remove it; eval objects resolve to the agent's schema",
        condition="an eval's dataset or config file sets `+database`, `+schema` or `+enabled`",
    ),
    spec(
        "SST-VAL704",
        Severity.ERROR,
        "Resolved eval schema differs from the agent schema",
        "dataset '{artifact}' resolves to {found}; agent '{value}' resolves to {expected}",
        "align the eval with its agent, per target",
        condition=(
            "`dataset.name_template` or `dataset.source_table_template` renders a qualified name in "
            "another database or schema than the eval's agent; the eval does not compile"
        ),
    ),
    spec(
        "SST-VAL705",
        Severity.ERROR,
        "Dataset row is incomplete",
        "dataset '{artifact}': row {index} {detail}",
        "give every row a question and at least one expectation",
        condition="a row has no question, or no expected field",
    ),
    spec(
        "SST-VAL706",
        Severity.ERROR,
        "Relative date in an eval question or expected answer",
        "dataset '{artifact}': row {index} contains relative date '{value}'",
        "pin the date",
        condition=(
            "a row's question, or its expected output, required filters or tool inputs and outputs, "
            "contains a relative date such as 'last month'; the first one in the row is reported"
        ),
        note="A relative date makes the dataset silently change meaning over time.",
    ),
    spec(
        "SST-VAL707",
        Severity.WARNING,
        "Eval question duplicates an agent sample question",
        "dataset '{artifact}': row {index} is byte-identical to a sample_question",
        "choose questions that would catch the agent failing",
        condition="a row's question is identical to one of the agent's sample questions",
        note="A question the agent is primed with proves little about how it handles the rest.",
    ),
    spec(
        "SST-VAL708",
        Severity.ERROR,
        "ground_truth_invocations names a tool the agent does not have",
        "dataset '{artifact}': row {index} expects tool '{name}', absent from agent '{value}'",
        "correct the tool name, or add the tool",
        condition="an expectation names a nonexistent tool",
    ),
    spec(
        "SST-VAL709",
        Severity.ERROR,
        "web_search expectation uses a non-canonical name",
        "dataset '{artifact}': row {index} expects '{name}'",
        "use the literal name web_search",
        condition=(
            "the agent has a web_search tool and a row's expected tool name contains 'web' but is not "
            "exactly web_search, the trace name Snowflake reports, which is not configurable"
        ),
    ),
    spec(
        "SST-VAL710",
        Severity.WARNING,
        "Dataset has fewer rows than the configured floor",
        "dataset '{artifact}' has {count} rows, under {expected}",
        "add rows; a thin dataset cannot support range thresholds",
        condition="the dataset has fewer rows than `evals.+min_dataset_rows`",
    ),
    spec(
        "SST-VAL711",
        Severity.INFO,
        "Question-set tool coverage",
        "dataset '{artifact}': {value} of the agent's tools are exercised by no question",
        None,
        condition="some of the agent's tools are expected by no row's `ground_truth_invocations`",
        note="An untested tool is the likeliest place for a regression to hide.",
    ),
    spec(
        "SST-VAL712",
        Severity.INFO,
        "CREATE DATASET takes no properties",
        "dataset '{artifact}': versions and provenance are added by ALTER, not CREATE",
        None,
        condition=(
            "any eval dataset is checked; reported for every dataset so the authored YAML matches the "
            "DDL, which sets versions and provenance with ALTER"
        ),
    ),
    spec(
        "SST-VAL713",
        Severity.ERROR,
        "Dataset version requires ownership",
        "dataset '{artifact}': {value} holds {found}, not OWNERSHIP",
        "grant OWNERSHIP, or add the version as the owner",
        condition=(
            "a plan adds a version to an eval dataset that the session's role does not own, as SHOW "
            "DATASETS reports the owner; only the owner may add a version"
        ),
        note=(
            "SST mints a dataset as the session's role, so this means ownership moved since, by a "
            "future grant or by hand. Apply as the owning role, or transfer ownership back."
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-VAL714",
        Severity.WARNING,
        "Dataset metadata carries no commit provenance",
        "dataset '{artifact}': METADATA has no git SHA",
        "write the SHA into METADATA",
        condition=(
            "a plan adds a dataset version and SST could not read the project's git commit, so the "
            "METADATA it writes carries no `git_sha`"
        ),
        note=(
            "SST writes METADATA itself; run plan from a git checkout with a commit. The eval API "
            "cannot select a version, so METADATA is the only place provenance can live."
        ),
    ),
    spec(
        "SST-VAL715",
        Severity.ERROR,
        "Sensitive data in dataset metadata or comment",
        "dataset '{artifact}': {field} matches {detail}",
        "remove it; Snowflake treats both as metadata fields",
        condition=(
            "a dataset version's METADATA or COMMENT holds an email address, a social security or card "
            "number, or METADATA holds a key SST never writes; the eval is blocked"
        ),
        note=(
            "SST builds both from the agent, the source table and digests, so a match comes from one of "
            "those names: rename the agent or the source table template."
        ),
    ),
    spec(
        "SST-VAL716",
        Severity.ERROR,
        "dataset block present for an existing dataset",
        "dataset '{artifact}' emits a dataset: block and the object already exists",
        "remove the dataset: block once the object is minted",
        condition=(
            "the rendered run config has a top-level `dataset:` block while the dataset already exists, "
            "so every run would try to create it again and fail; the eval is blocked"
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-VAL717",
        Severity.ERROR,
        "run_name is not unique for the agent",
        "run_name '{value}' is already used for {artifact}",
        "choose a distinct run_name",
        condition=(
            "`run.name_template` renders a run name, compared ignoring case, that another eval of the "
            "same agent already renders; the eval that claims it second is reported"
        ),
        note="Database, schema, agent and run name together identify a run's result set.",
    ),
    spec(
        "SST-VAL718",
        Severity.ERROR,
        "run_name carries no commit SHA",
        "run_name '{value}' does not include the git SHA",
        "include the SHA so a score is attributable to a commit",
        condition="the run name `run.name_template` renders does not include the commit SHA",
    ),
    spec(
        "SST-VAL719",
        Severity.ERROR,
        "agent_version is LIVE or omitted",
        "eval config for '{artifact}': agent_version is '{found}'",
        "use committed or alias:<name>",
        condition=(
            "the eval's `agent_version`, else `evals.+agent_version`, is not `committed`, "
            "`alias:<name>` or `VERSION$<n>`; LIVE or an omitted version targets the default version"
        ),
        note=(
            "A pinned `VERSION$<n>` is also accepted. LIVE or no version would score whatever the "
            "default version is at run time: silent drift."
        ),
    ),
    spec(
        "SST-VAL720",
        Severity.ERROR,
        "Referenced agent version does not exist",
        "eval config for '{artifact}': version '{value}' does not exist or is dropped",
        "pin an existing version",
        condition=(
            "an eval run finds its `agent_version` does not resolve on the live agent: `committed` "
            "names its LAST alias, `alias:<name>` that alias, and neither points at a version"
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-VAL721",
        Severity.ERROR,
        "Metric is neither a system metric nor a declared custom metric",
        "eval config for '{artifact}': metric '{name}' is unknown",
        "declare the custom metric, or use a system metric",
        condition=(
            "a `metrics.system` entry is not one of the four system metrics, or a custom metric the "
            "config names is defined by no metric file"
        ),
    ),
    spec(
        "SST-VAL722",
        Severity.ERROR,
        "System metric version is not pinned",
        "eval config for '{artifact}': metric '{name}' declares no version",
        "pin the version",
        condition="a system metric sets no `version` and `evals.+metric_version` is unset",
        note=(
            "Set `version: v3` on the metric, or `evals.+metric_version: v3` once for the project; v3 is "
            "the only version SST accepts. Snowflake runs an unversioned metric at its own default "
            "version, which it changes on its own schedule, so scores would shift with no change here."
        ),
    ),
    spec(
        "SST-VAL723",
        Severity.WARNING,
        "System metric version pins a legacy judge",
        "eval config for '{artifact}': metric '{name}' pins legacy version '{found}'",
        "move to a current version, and record the score shift",
        condition=(
            "a system metric's version, else `evals.+metric_version`, is anything other than v3 "
            "or v1; SST-PRS013 reports the same value as an error, and SST-VAL764 reports v1"
        ),
        note=(
            "Pin v3. Scores from different metric versions are not comparable, so the eval's baseline "
            "no longer applies (SST-VAL759) and a new one must be captured."
        ),
    ),
    spec(
        "SST-VAL724",
        Severity.ERROR,
        "judge_model set for a system metric",
        "eval config for '{artifact}': metric '{name}' is a system metric and declares judge_model",
        "remove judge_model; the version carries the judge",
        condition="a `metrics.system` entry sets `judge_model`, which Snowflake ignores for a system metric",
    ),
    spec(
        "SST-VAL725",
        Severity.INFO,
        "Metric evaluation mechanics",
        "eval config for '{artifact}': metric '{name}' {detail}",
        None,
        condition="an eval config enables tool_selection_accuracy, which Snowflake computes without an LLM judge",
        note=(
            "Only tool_selection_accuracy is reported. A custom judge metric costs one model call per "
            "row, and every run executes the SQL the agent's text-to-SQL tool generates, so it also "
            "costs warehouse time."
        ),
    ),
    spec(
        "SST-VAL726",
        Severity.WARNING,
        "logical_consistency used as a gate",
        "eval config for '{artifact}': logical_consistency is a blocking metric",
        "report it rather than gate on it",
        condition="logical_consistency sets `gate: true`",
        note="It is reference-free: it judges the answer without the expected one, which makes it a weak gate.",
    ),
    spec(
        "SST-VAL727",
        Severity.ERROR,
        "Eval privileges are spread across secondary roles",
        "eval config for '{artifact}': {detail}",
        "put every eval privilege on one primary role",
        condition=(
            "the pre-run check finds the session's primary role lacks CREATE TASK, CREATE STAGE or "
            "CREATE FILE FORMAT on the agent's schema while a secondary role in the session holds it; "
            "no eval starts"
        ),
        note=(
            "A run executes as tasks, and a task runs as the primary role alone, so the privilege would "
            "work interactively and fail in the run. SST reads the grants on the schema, OWNERSHIP "
            "included, and follows the roles and database roles holding them up the role hierarchy, "
            "no further than 16 roles, to the primary role. Grant the privilege to the primary role, or "
            "to a role it inherits."
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-VAL728",
        Severity.ERROR,
        "Eval role lacks a required schema privilege",
        "eval config for '{artifact}': {value} lacks {detail}",
        "grant CREATE TASK, CREATE STAGE and CREATE FILE FORMAT",
        condition=(
            "the pre-run check finds no role in the session holds CREATE TASK, CREATE STAGE or CREATE "
            "FILE FORMAT on the agent's schema; no eval starts"
        ),
        note=(
            "Grant the missing privileges on the agent's schema to the primary role, or to a role it "
            "inherits. SST reads the grants on the schema itself, OWNERSHIP included, and follows the "
            "role hierarchy up from their holders; a grant at the database or account level is not "
            "seen."
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-VAL729",
        Severity.ERROR,
        "Config stage file format is wrong",
        "eval config for '{artifact}': stage file format is {found}",
        "use TYPE='CSV' with every delimiter and escape disabled",
        condition=(
            "the eval config stage that already exists in the agent's schema "
            "(`apply.eval_config_stage.stage`, default EVAL_CONFIGS) declares another file format, "
            "which EXECUTE_AI_EVALUATION cannot read the YAML through"
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-VAL730",
        Severity.ERROR,
        "Partial completion treated as a pass",
        "eval run for '{artifact}': status '{found}' is not a pass",
        "treat INVOCATION_PARTIALLY_COMPLETED and PARTIALLY_COMPLETED as failures",
        condition=(
            "an attempt ends INVOCATION_PARTIALLY_COMPLETED or PARTIALLY_COMPLETED and "
            "`run.accept_statuses` lists that status; reported even when a retry completed"
        ),
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-VAL731",
        Severity.WARNING,
        "Eval concurrency exceeds the configured ceiling",
        "eval config for '{artifact}': concurrency {found} exceeds {expected}",
        "lower concurrency; runs re-invoke the agent, so this is real load and real spend",
        condition="an eval's `run.concurrency` is greater than `evals.+concurrency`",
    ),
    spec(
        "SST-VAL732",
        Severity.INFO,
        "Unsupported tool types are skipped during evals",
        "eval config for '{artifact}': {value} will be skipped, not failed",
        None,
        condition=(
            "the agent has an `mcp` or `agent` tool, which an eval run skips; one per tool type, only for "
            "a config with a `run:` block"
        ),
        note="A skipped tool is never invoked, so the eval looks healthier than the agent is.",
    ),
    spec(
        "SST-VAL733",
        Severity.ERROR,
        "Blocking threshold declares no usable bound",
        "eval config for '{artifact}': metric '{name}' threshold has {detail}",
        "declare min, max, or both, with min <= max",
        condition=(
            "a gated system metric's threshold has no bound or min > max, so the gate cannot fire; or "
            "a threshold is set on a metric that does not gate (a system metric without `gate: true`, "
            "or a custom metric's `threshold_default` without `gate_default: true`)"
        ),
        note="For a threshold on an ungated metric, gate the metric or remove the threshold.",
    ),
    spec(
        "SST-VAL734",
        Severity.ERROR,
        "Threshold gates before the baseline runs complete",
        "eval config for '{artifact}': {count} baseline_runs completed, {expected} required",
        "complete the baseline first",
        condition=(
            "the eval gate finds the stored baseline holds fewer runs than the eval's `run.baseline_runs`, "
            "else `evals.+baseline_runs`, now requires; the gate has no signal"
        ),
        note=(
            'Capture a new baseline: `sst test --suite evals --capture-baseline --reason "<why>"`. '
            "An eval with no baseline at all is SST-VAL758."
        ),
    ),
    spec(
        "SST-VAL735",
        Severity.WARNING,
        "Threshold set with no baseline",
        "eval config for '{artifact}': metric '{name}' has a threshold and no baseline",
        "measure first, then set the threshold",
        condition=(
            "a system metric sets a threshold while the effective `run.baseline_runs` is unset or zero; "
            "one per such metric"
        ),
        note="A threshold set before any baseline is measured is a guess wearing the costume of a measurement.",
    ),
    spec(
        "SST-VAL737",
        Severity.ERROR,
        "Custom metric shadows a system metric name",
        "eval metric '{artifact}' shadows system metric '{name}'",
        "rename the custom metric",
        condition="a custom metric's name, compared ignoring case, is a system metric's",
    ),
    spec(
        "SST-VAL738",
        Severity.ERROR,
        "Custom metric declares no explicit judge model",
        "eval metric '{artifact}' names no judge model (model: {found})",
        "name the judge model explicitly",
        condition="a custom metric's `model`, else `evals.+judge_model`, is absent or `auto`",
        note=(
            "Set the metric file's `model:` key, or `evals.+judge_model` for every metric that sets "
            "none, to a named judge model. With `auto`, Snowflake picks "
            "the judge and may change it, so score movement cannot be attributed."
        ),
    ),
    spec(
        "SST-VAL739",
        Severity.ERROR,
        "Custom metric's judge model is not in the allowlist",
        "eval metric '{artifact}': model '{found}' is not in the allowlist",
        "add it to the config allowlist, or use an allowed model",
        condition=(
            "`snowflake.orchestration_models` is set and a custom metric's `model`, else "
            "`evals.+judge_model`, is not in it, compared ignoring case"
        ),
        note=(
            "Judge models share the allowlist with orchestration models. When "
            "`snowflake.orchestration_models` is unset, any named judge model is accepted."
        ),
    ),
    spec(
        "SST-VAL740",
        Severity.WARNING,
        "Custom metric duplicates a system metric's intent",
        "eval metric '{artifact}' duplicates '{name}'",
        "use the system metric; it is cheaper, versioned and comparable",
        condition=(
            "a custom metric's prompt uses a phrase that reads as a system metric's intent, such as "
            "'tool selection' or 'answer correctness'; the first such metric is named"
        ),
    ),
    spec(
        "SST-VAL741",
        Severity.ERROR,
        "Judge prompt declares no output contract",
        "eval metric '{artifact}': the prompt declares no scale or allowed values",
        "state the scale and the allowed values in the prompt",
        condition=(
            "a custom metric's prompt states no numeric scale ('1 to 5') together with an instruction "
            "to output a score or an anchor for every declared band, so its output cannot be "
            "reconciled with the scale and thresholds"
        ),
    ),
    spec(
        "SST-VAL742",
        Severity.WARNING,
        "Judge prompt asks for reasoning with no parseable score",
        "eval metric '{artifact}': the prompt has no parseable score instruction",
        "ask for a number in a fixed position",
        condition=(
            "a custom metric's prompt asks for reasoning or an explanation and has no output contract (SST-VAL741)"
        ),
        note="Unparseable judge output scores as a failure, not a skip.",
    ),
    spec(
        "SST-VAL743",
        Severity.WARNING,
        "Rubric has no tie-break or insufficient-information branch",
        "eval metric '{artifact}': the rubric has no {detail} branch",
        "add the branch; otherwise the judge invents one per call",
        condition=(
            "a custom metric's prompt mentions neither a tie nor insufficient, unclear or ambiguous information"
        ),
    ),
    spec(
        "SST-VAL744",
        Severity.ERROR,
        "Custom metric changed under the same name",
        "eval metric '{artifact}' changed since state recorded it under the same name",
        "publish the change under a new metric name, or revert it",
        condition=(
            "a plan finds a custom metric whose prompt, score bands or judge model changed since state "
            "recorded it under the same name; the eval is blocked"
        ),
        note=(
            "A custom metric has no Snowflake version: its name is the score's column, so an edited "
            "prompt under the same name would read as one continuous trend. Publish the change under "
            "a new metric name, or revert it."
        ),
    ),
    spec(
        "SST-VAL745",
        Severity.ERROR,
        "In-place edit to a metric a retained run references",
        "eval metric '{artifact}' is referenced by retained run '{value}'",
        "publish a new version instead",
        condition=(
            "the eval gate finds a custom metric's prompt, score bands or judge model differ from the "
            "definition the baseline's runs scored; the gate has no signal"
        ),
        note=(
            "Revert the edit, or publish it as a new metric; to keep the edit, capture a new baseline "
            "with a reason. Any other baseline mismatch is SST-VAL759."
        ),
    ),
    spec(
        "SST-VAL746",
        Severity.INFO,
        "System judge versions move on Snowflake's cadence",
        "eval metric '{artifact}': judge for version '{value}' recorded",
        None,
        condition=(
            "a baseline is captured; one per system metric, naming the version its judge was recorded "
            "under, so a later score shift can be explained"
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-VAL747",
        Severity.ERROR,
        "Scoring instruction produces a number outside the declared ranges",
        "eval metric '{artifact}': the prompt can produce {found}, outside {expected}",
        "align the prompt with score_ranges",
        condition=(
            "the numeric scale a custom metric's prompt states reaches below its lowest or above its "
            "highest `score_ranges` band"
        ),
    ),
    spec(
        "SST-VAL748",
        Severity.WARNING,
        "Judged range is wider than three bands for an invariant",
        "eval metric '{artifact}' declares {count} bands for an invariant",
        "narrow the scale",
        condition=(
            "a custom metric whose description or prompt says 'invariant' declares score bands "
            "spanning more than three values"
        ),
    ),
    spec(
        "SST-VAL755",
        Severity.WARNING,
        "Agent eval run overlaps a semantic view regenerate",
        "eval config for '{artifact}': the run overlaps a regenerate of view '{value}', which the agent uses",
        "serialise the run against the publish, or move one of the two",
        condition=(
            "an eval run is refused because an operation other than an eval run, such as an apply that "
            "may regenerate the view, holds the target's lock, and the eval's agent queries the view; "
            "one per eval and view"
        ),
        note=(
            "Snowflake does not coordinate the two, so SST serialises them through the target lock. "
            "Run the evals again once the operation holding the lock finishes."
        ),
        precheck=Precheck.OBSERVE,
    ),
    spec(
        "SST-VAL758",
        Severity.ERROR,
        "Eval baseline is absent",
        "eval '{artifact}' has no captured baseline",
        "capture a baseline explicitly with a reason",
        condition="the eval gate finds no baseline stored for the eval on this target; the gate has no signal",
        note='Capture one: `sst test --suite evals --capture-baseline --reason "<why>"`.',
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-VAL759",
        Severity.ERROR,
        "Eval baseline is incompatible",
        "eval '{artifact}' baseline is incompatible: {detail}",
        "capture a new baseline for the current dataset, agent version and metrics",
        condition=(
            "the eval gate finds the baseline was captured for another dataset, eval config, agent "
            "version, metric versions, thresholds, gating or tier, or the run scored another set of "
            "question/metric pairs; the gate has no signal"
        ),
        note=(
            "The run's agent version is part of the identity, so a new agent version needs a new "
            'baseline: `sst test --suite evals --capture-baseline --reason "<why>"`. An edited custom '
            "metric is SST-VAL745 instead."
        ),
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-VAL760",
        Severity.WARNING,
        "Eval baseline is nearing expiry",
        "eval '{artifact}' baseline expires on {date}",
        "capture a replacement baseline before it expires",
        condition=(
            "the eval gate finds the baseline expires within 7 days; a baseline expires 30 days after its capture"
        ),
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-VAL761",
        Severity.ERROR,
        "Eval baseline is expired",
        "eval '{artifact}' baseline expired on {date}",
        "capture a replacement baseline with a reason",
        demotable=False,
        condition=("the eval gate finds the baseline is 30 or more days old; the gate has no signal"),
        note='Capture a replacement: `sst test --suite evals --capture-baseline --reason "<why>"`.',
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-VAL762",
        Severity.ERROR,
        "Eval dataset template is missing",
        "eval '{artifact}': dataset.{field} is not set",
        "set dataset.name_template and dataset.source_table_template in the eval's config.yml",
        condition="dataset.name_template or dataset.source_table_template is not set in the eval config",
    ),
    spec(
        "SST-VAL763",
        Severity.ERROR,
        "Blocking eval regressed",
        "eval '{artifact}' regressed on {count} question/metric pair(s): {detail}",
        "fix what regressed, or capture a new baseline with a reason if the change is intended",
        demotable=False,
        condition=(
            "a blocking-tier eval regressed: a gated metric passed on a question in every baseline "
            "attempt and failed in some completed current attempt"
        ),
        note=(
            "A report-tier eval lists the same regressions without this error, so its run passes. The "
            "tier is `run.tier`, else `evals.+eval_tier`, else report."
        ),
        precheck=Precheck.RUNTIME,
    ),
    spec(
        "SST-VAL764",
        Severity.ERROR,
        "System metric pins version v1, which Snowflake refuses",
        "eval config for '{artifact}': metric '{name}' pins version '{found}', which Snowflake refuses",
        "pin `version: v3`",
        demotable=False,
        condition="a system metric's version, else `evals.+metric_version`, is v1",
        note=(
            "Snowflake refuses `version: v1` when the run starts, though its docs list it. Unlike "
            "SST-PRS013, which reports the same value, no setting lowers this."
        ),
    ),
    spec(
        "SST-VAL765",
        Severity.ERROR,
        "Two eval files of one kind share a name",
        "{path}: eval {kind} files {a} and {b} are both named '{name}'",
        "give each file its own name:",
        condition=(
            "two dataset files, or two config files, in one agent's `evals/` folder declare the same "
            "`name:`, compared casefolded, so a reference to it is ambiguous"
        ),
    ),
)
