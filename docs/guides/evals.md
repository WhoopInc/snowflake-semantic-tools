# Evals

An eval runs an agent against a fixed set of questions and scores the answers.
SST publishes the question set and run configuration with the agent, runs them
with `sst test --suite evals`, and gates a change on the scores.

## Layout

```text
agents/
  sales_analyst/
    agent.yml
    evals/
      dataset.yml
      config.yml
eval_metrics/
  answer_grounding.yml     # optional custom judges, shared by every eval
```

The agent points at its eval files:

```yaml
# agents/sales_analyst/agent.yml
evals:
  dataset: evals/dataset.yml
  config: evals/config.yml
```

Eval objects always publish into their agent's schema, so an eval and the agent
it evaluates move between targets together. `evals:` in `sst_config.yml` holds
defaults, never a location.

## The dataset

```yaml
agent: sales_analyst
description: Order and revenue questions the agent must route correctly.
questions:
  - question: How many orders were placed at each location from January to March 2026?
    ground_truth:
      ground_truth_invocations:
        - tool_name: SALES
          tool_input: Count orders grouped by location for January to March 2026.
          tool_output: An order count per location.
      ground_truth_output: |-
        The reply reports an order count for every location and names the
        months it covers.
```

- Each question needs an expected tool invocation or an expected output.
- `tool_name` is the name the agent actually exposes for the tool; an unknown
  name is an error (`SST-VAL708`).
- Dates are absolute. A relative date such as "last quarter" or "yesterday" in
  a question or an expected answer is an error (`SST-VAL706`): its meaning
  changes between runs, so scores from different days stop being comparable.
- A question byte-identical to one of the agent's `sample_questions` is a
  warning (`SST-VAL707`): the agent was tuned on it, so it catches nothing.
- A dataset smaller than `evals.+min_dataset_rows` is a warning.

## The run configuration

```yaml
agent: sales_analyst
agent_version: committed
dataset:
  name_template: EVAL_{{ agent | upper }}_{{ sha7 }}
  source_table_template: EVAL_SRC_{{ agent | upper }}_{{ sha7 }}
metrics:
  system:
    - name: tool_selection_accuracy
      version: v3
      gate: true
      threshold:
        min: 0.8
    - name: answer_correctness
      version: v3
      gate: false
  custom:
    - "{{ eval_metric('answer_grounding') }}"
run:
  name_template: EVAL_{{ agent | upper }}_{{ sha7 }}_{{ variant }}_{{ ts }}
  variant: ci
  retry: 1
  baseline_runs: 5
```

- `agent_version` is `committed`, `alias:<name>`, or `VERSION$<n>`. `LIVE` is
  refused: an eval must run against a version that cannot change under it.
- System metrics pin a `version`, so a scoring change on the Snowflake side
  cannot move a gate silently (`SST-VAL722`).
- `gate: true` makes a metric blocking, and a blocking metric needs a usable
  `threshold`: `min`, `max`, or both (`SST-VAL733`).
- Name templates take `{{ agent }}`, `{{ sha7 }}`, `{{ variant }}`, and `{{ ts }}`.

## Custom judges

A custom metric is an LLM judge with a prompt and score bands:

```yaml
# eval_metrics/answer_grounding.yml
name: answer_grounding
description: Whether the answer is grounded in the view it cites.
model: claude-sonnet-4-6
score_ranges:
  min_score: [0, 1]
  median_score: [2, 3]
  max_score: [4, 5]
prompt: |-
  You are scoring whether an analytical answer is grounded in the semantic
  view it claims to have used. ...
gate_default: true
threshold_default:
  min: 3
```

An eval includes it with `{{ eval_metric('answer_grounding') }}`.

## Running

```bash
sst compile
sst test --suite evals --target dev
```

The suite refuses to start unless the manifest is current and every eval it
would run has been applied, so a result always describes the published agent.
Each run is retried up to `retry` times, and every attempt is reported.

## Baselines

A gate compares scores with a **baseline**: the range of scores the published
agent produced across `baseline_runs` completed runs. Capture one on purpose,
with a reason that is recorded beside it:

```bash
sst test --suite evals --capture-baseline --reason "new tool for supply cost"
```

A baseline expires 30 days after it is captured. In its last 7 days each run warns
(`SST-VAL760`); after that the gate reports the expired baseline as an error
(`SST-VAL761`) until a new one is captured.

`evals.+eval_tier` decides what a regression does: `blocking` fails the run,
`report` only reports it.
