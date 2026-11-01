"""Which schema checks a project file: its kind, from where the project's configuration puts it.

The rules follow how SST discovers each kind: a semantic models file is any YAML file under
`project.semantic_models_dir`, an agent is `agent.yml` in a folder under `project.agents_dir`,
and so on. A file in an agent's or CoWork profile's `evals/` folder is a dataset when it lists
`questions:`, which the caller says, and a config otherwise.
"""

from __future__ import annotations

import posixpath
from collections.abc import Mapping

from snowflake_semantic_tools.domain.model.coco_profile import SHARED_PROFILE
from snowflake_semantic_tools.domain.model.config_schema import CONFIG_NAMES
from snowflake_semantic_tools.domain.model.skill.model import SKILL_FILE

# Each directory key, with the value it takes when the configuration does not set it.
DIRECTORY_DEFAULTS: Mapping[str, str] = {
    "semantic_models_dir": "semantic_models",
    "tools_dir": "tools",
    "agents_dir": "agents",
    "cowork_profiles_dir": "cowork_profiles",
    "eval_metrics_dir": "eval_metrics",
    "skills_dir": "skills",
    "plugins_dir": "plugins",
    "coco_profiles_dir": "coco_profiles",
}
_YAML = (".yml", ".yaml")
# The kinds each read from one named YAML file in a folder below their directory: by directory key,
# the file's name without its extension, and the kind.
_FOLDER_FILES = (
    ("agents_dir", "agent", "agent"),
    ("cowork_profiles_dir", "cowork_profile", "cowork_profile"),
    ("coco_profiles_dir", "coco_profile", "coco_profile"),
    ("plugins_dir", "plugin", "plugin"),
)


def schema_kind(path: str, directories: Mapping[str, str], *, lists_questions: bool = False) -> str | None:
    """Return the kind of schema that checks the project file at `path`; None when no schema does.

    Args:
        path: The file's path relative to the project, with `/` separators.
        directories: Each directory key's configured value; a key absent takes its default.
        lists_questions: The file's top level has `questions:`, which tells a dataset in an
            `evals/` folder from a config.
    """
    clean = posixpath.normpath(path)
    if clean in CONFIG_NAMES:
        return "sst_config"
    name = posixpath.basename(clean)
    yaml_file = name.endswith(_YAML)
    stem = name.rsplit(".", 1)[0]

    def under(key: str) -> tuple[str, ...] | None:
        root = posixpath.normpath(directories.get(key) or DIRECTORY_DEFAULTS[key])
        if not clean.startswith(root + "/"):
            return None
        return tuple(clean[len(root) + 1 :].split("/"))

    for key in ("agents_dir", "cowork_profiles_dir"):
        parts = under(key)
        if parts is not None and yaml_file and "evals" in parts[:-1]:
            return "eval_dataset" if lists_questions else "eval_config"
    for key, file_stem, kind in _FOLDER_FILES:
        parts = under(key)
        if parts is not None and len(parts) > 1 and stem == file_stem and yaml_file:
            shared = kind == "coco_profile" and parts[0] == SHARED_PROFILE
            return "coco_shared_profile" if shared else kind
    if under("skills_dir") is not None and name == SKILL_FILE:
        return "skill"
    if yaml_file and under("tools_dir") is not None:
        return "tool"
    if yaml_file and under("eval_metrics_dir") is not None:
        return "eval_metric"
    if yaml_file and under("semantic_models_dir") is not None:
        return "semantic_file"
    return None
