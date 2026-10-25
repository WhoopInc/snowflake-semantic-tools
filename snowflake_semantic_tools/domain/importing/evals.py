"""Map an eval input table -- questions and their ground truth -- to an agent's eval dataset file.

Snowflake's evaluations read a table of `INPUT_QUERY` and `GROUND_TRUTH`, the VARIANT holding
`ground_truth_output` and `ground_truth_invocations`. An import writes its rows as a dataset in the
agent's `evals/` folder, each ground truth as it is; the agent's own `evals:` block, which names
the dataset, is the user's to add, and the import says so (SST-IMP013).
"""

from __future__ import annotations

import posixpath
from collections.abc import Mapping, Sequence

from snowflake_semantic_tools.domain.diagnostics import D
from snowflake_semantic_tools.domain.file_names import file_name
from snowflake_semantic_tools.domain.importing.model import ImportedFile, ImportedObject, artifact_name
from snowflake_semantic_tools.domain.model.identifier import QualifiedName


def import_eval(table: QualifiedName, rows: Sequence[tuple[str, object]], folder: str) -> ImportedObject:
    """Map an eval input table's rows to `<folder>/evals/<name>.yml`, `folder` being an agent's.

    Diagnostics:
        SST-IMP013: the agent's `evals:` block must name the dataset; and a row whose ground
            truth is not an object, which is written without one.
    """
    name = artifact_name(table)
    key = f"eval:{name}"
    questions: list[dict[str, object]] = []
    skipped = 0
    for question, truth in rows:
        entry: dict[str, object] = {"question": question}
        if isinstance(truth, Mapping):
            entry["ground_truth"] = dict(truth)
        elif truth is not None:
            skipped += 1
        questions.append(entry)
    reference = "{{ eval_dataset('" + name + "') }}"
    detail = f'the agent in {folder} runs it once its `evals:` names it: dataset: "{reference}"'
    diagnostics = [D("SST-IMP013", subject=key, artifact=key, detail=detail)]
    if skipped:
        detail = f"{skipped} row(s) hold a ground truth that is not a JSON object; they are written without one"
        diagnostics.append(D("SST-IMP013", subject=key, artifact=key, detail=detail))
    document = {"name": name, "questions": questions}
    path = posixpath.join(folder, "evals", f"{file_name(name)}.yml")
    return ImportedObject("eval", table, name, (ImportedFile(path, document),), diagnostics=tuple(diagnostics))
