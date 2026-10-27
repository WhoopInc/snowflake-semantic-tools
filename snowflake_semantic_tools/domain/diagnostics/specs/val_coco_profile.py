"""Validation codes 890-899 (VAL): Cortex Code agent profiles.

Cortex Code agent profiles are a Snowflake private preview (PrPr) feature. A profile is an AGENT
in CORTEX_CODE.CONFIG whose specification holds only an orchestration model, response
instructions, and skills; these codes cover where it publishes, which profile a role is given,
and the CoCo Desktop profile layout it replaced.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.diagnostics.core import ErrorSpec, Severity, spec

TITLE: str = "Validation"

SPECS: tuple[ErrorSpec, ...] = (
    spec(
        "SST-VAL890",
        Severity.INFO,
        "Cortex Code profile relies on a Snowflake private preview",
        "coco profile '{artifact}' relies on Cortex Code agent profiles, a Snowflake private preview",
        "expect the agent profile contract to change before general availability, and recompile after each SST upgrade",
        condition="a Cortex Code agent profile compiles; reported once per compiled profile",
        note=(
            "Snowflake private preview. SST renders only the keys a Cortex Code profile accepts: models.orchestration, "
            "instructions.response, and skills."
        ),
    ),
    spec(
        "SST-VAL891",
        Severity.INFO,
        "Cortex Code profile publishes where Cortex Code does not list it",
        "coco profile '{artifact}' publishes to {target}, where Cortex Code does not list profiles",
        "publish to CORTEX_CODE.CONFIG on the target Cortex Code users reach",
        condition=(
            "a compiled Cortex Code profile's database and schema, compared as Snowflake resolves them, are not "
            "CORTEX_CODE.CONFIG"
        ),
        note=(
            "Snowflake private preview. The Cortex Code CLI, Desktop, and Snowsight list only the AGENT objects in "
            "CORTEX_CODE.CONFIG, so a Cortex Code profile anywhere else is a rehearsal, such as one a dev target "
            "publishes."
        ),
    ),
    spec(
        "SST-VAL892",
        Severity.ERROR,
        "Cortex Code profile shares a schema with an agent or CoWork profile",
        "coco profile '{artifact}' publishes to {target}, where '{other}' publishes too",
        "give coco_profiles: a schema of its own, such as CORTEX_CODE.CONFIG",
        condition=(
            "a compiled Cortex Code profile publishes to a schema, compared as Snowflake resolves it, that a "
            "compiled agent or CoWork profile publishes to"
        ),
        note=(
            "Cortex Code profiles, agents, and CoWork profiles are all AGENT objects. SST tells them apart by the "
            "schema each kind publishes to, so one schema holding two kinds would observe each object under two keys."
        ),
    ),
    spec(
        "SST-VAL893",
        Severity.INFO,
        "No Cortex Code profile is named default",
        "{count} coco profiles publish to CORTEX_CODE.CONFIG ({names}) and none is DEFAULT",
        "name one Cortex Code profile folder default, or grant each role USAGE on one Cortex Code profile only",
        condition=(
            "more than one compiled Cortex Code profile publishes to CORTEX_CODE.CONFIG, and none publishes as DEFAULT"
        ),
        note=(
            "Cortex Code applies a Cortex Code profile automatically only when the current role sees exactly one, "
            "or sees several and one is named DEFAULT. A role granted USAGE on two of them is given none until its "
            "user chooses one."
        ),
    ),
    spec(
        "SST-VAL894",
        Severity.ERROR,
        "Cortex Code profile model is not in the allowlist",
        "coco profile '{artifact}': model '{found}' is not in the allowlist",
        "add it to snowflake.orchestration_models, use auto, or leave model unset",
        condition=(
            "a Cortex Code profile's `model:` is not listed in `snowflake.orchestration_models`, which defaults to "
            "[auto]; compared exactly"
        ),
    ),
    spec(
        "SST-VAL895",
        Severity.ERROR,
        "CoCo Desktop profile folder is no longer read",
        "'{folder}/' holds CoCo Desktop profiles, which SST no longer publishes",
        "move each CoCo Desktop profile to coco_profiles/<name>/coco_profile.yml and delete '{folder}/'",
        condition="the project has a `coco_desktop_profiles/` folder beside the Cortex Code profiles directory",
        note=(
            "Cortex Code agent profiles, AGENT objects in CORTEX_CODE.CONFIG, replaced the CoCo Desktop profile "
            "registry. A Cortex Code profile carries skills and instructions only: no hooks, MCP servers, or "
            "commands."
        ),
    ),
)
