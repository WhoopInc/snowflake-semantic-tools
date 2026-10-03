"""Defer: resolve the relations dbt objects read to another target's, as `dbt --defer` does.

A deferred run compiles the project as it is now -- its models, columns and keys from the current
target's manifest -- but points every model and source the other target's manifest also holds at
the relation that target built. What the other target does not hold keeps its own relation, since
there is nothing there to defer to. Where artifacts publish is not changed.
"""

from __future__ import annotations

from dataclasses import replace

from snowflake_semantic_tools.domain.model.dbt import DbtCatalog


def defer_relations(current: DbtCatalog, deferred: DbtCatalog) -> DbtCatalog:
    """Return `current` with each model and source `deferred` also holds reading `deferred`'s relation.

    Nodes are matched by dbt unique id.
    """
    models = {model.unique_id: model for model in deferred.models}
    sources = {source.unique_id: source for source in deferred.sources}
    return replace(
        current,
        models=tuple(
            replace(
                model,
                relation_name=models[model.unique_id].relation_name,
                raw_relation_name=models[model.unique_id].raw_relation_name,
            )
            if model.unique_id in models
            else model
            for model in current.models
        ),
        sources=tuple(
            replace(source, relation_name=sources[source.unique_id].relation_name)
            if source.unique_id in sources
            else source
            for source in current.sources
        ),
    )
