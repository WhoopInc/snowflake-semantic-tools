"""Presentation entries every family of kinds shares: tags, sort order, score bands, skills.

Each returns the entries for one place a shared node sits, by that place's path, so a node
written once in the authored tables is presented once here too.
"""

from __future__ import annotations

from snowflake_semantic_tools.domain.model.presentation.types import ListLayout, ListUi, Ui, Widget


def tags(path: str) -> tuple[dict[str, Ui], dict[str, ListUi]]:
    """Return the entries of a `config.tags` list at `path`: each tag's name and value."""
    fields = {
        path: Ui("Tags"),
        f"{path}.name": Ui("Tag", widget=Widget.REFERENCE),
        f"{path}.value": Ui("Value"),
    }
    return fields, {path: ListUi("tag", "name", ListLayout.TABLE, columns=("value",))}


def ordering(path: str) -> dict[str, Ui]:
    """Return the entries of a sort entry's direction and null placement under `path`."""
    return {
        f"{path}.sort_direction": Ui("Direction"),
        f"{path}.null_order": Ui("Nulls"),
    }


def threshold(path: str, label: str = "Pass band") -> dict[str, Ui]:
    """Return the entries of a score's pass band at `path`: its lowest and highest passing scores."""
    return {path: Ui(label), f"{path}.min": Ui("Lowest passing score"), f"{path}.max": Ui("Highest passing score")}


def skills(path: str) -> tuple[dict[str, Ui], dict[str, ListUi]]:
    """Return the entries of a `skills:` list at `path`: which extension each entry pins, and how."""
    source = f"{path}.source"
    fields = {
        path: Ui("Skills", group="skills"),
        f"{path}.name": Ui("Plugin member", widget=Widget.REFERENCE),
        source: Ui("Source"),
        f"{source}.type": Ui("Source type", advanced=True),
        f"{source}.path": Ui("Skill or extension", widget=Widget.REFERENCE),
        f"{source}.version": Ui("Version"),
    }
    return fields, {
        path: ListUi("skill", "source.path", ListLayout.TABLE, subtitle="name", columns=("source.version",))
    }


def instructions(path: str) -> tuple[dict[str, Ui], dict[str, ListUi]]:
    """Return the entries of an `instructions:` mapping at `path`: how it plans, answers, and is asked."""
    questions = f"{path}.sample_questions"
    fields = {
        path: Ui("Instructions", group="instructions"),
        f"{path}.orchestration": Ui("Planning instructions", widget=Widget.MARKDOWN),
        f"{path}.response": Ui("Answer instructions", widget=Widget.MARKDOWN),
        questions: Ui("Sample questions"),
        f"{questions}.question": Ui("Question", widget=Widget.PROSE),
    }
    return fields, {questions: ListUi("sample question", "question", ListLayout.CARDS)}


def display(path: str) -> dict[str, Ui]:
    """Return the entries of a published agent's display at `path`: its name, avatar and color."""
    return {
        path: Ui("Display", group="identity"),
        f"{path}.display_name": Ui("Display name"),
        f"{path}.avatar": Ui("Avatar"),
        f"{path}.color": Ui("Color"),
    }


def evals(path: str) -> dict[str, Ui]:
    """Return the entries of an `evals:` mapping at `path`: the dataset and config in its folder."""
    return {
        path: Ui("Evaluation", group="evaluation"),
        f"{path}.dataset": Ui("Dataset", widget=Widget.REFERENCE),
        f"{path}.config": Ui("Eval config", widget=Widget.REFERENCE),
    }


def retired(label: str) -> Ui:
    """Return the entry of a key SST no longer reads, or reads only to report: shown only when asked for."""
    return Ui(label, advanced=True)
