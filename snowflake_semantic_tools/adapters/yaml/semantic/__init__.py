"""Load SST's semantic models into fully resolved `domain` semantic views.

Everything it produces is fully resolved: `{{ ref('products') }}` has become
`SST_REF_DEV.JAFFLE.PRODUCTS` and `{{ target.database }}` has been replaced from the
dbt target, so `domain` never needs to know either exists.

WHERE THE INFORMATION LIVES. A view declares its name, tables, and a few view-level
keys. Everything else in the rendered DDL arrives by ATTACHMENT from elsewhere in the
project:

    semantic_models/semantic_views/**/*.yml   the view
    target/manifest.json                      physical relation, model grain and
                                              resolved column metadata
    semantic_models/<member>/*.yml            metrics, filters, relationships,
                                              verified queries and custom
                                              instructions, attached by the tables
                                              they reference
    profiles.yml + dbt_project.yml            the target database and schema

Nothing here reads dbt: `adapters.dbt.project` loads the target and the manifest's
models, and the caller passes both in. Every authored key the loader does not read is
reported (`checks.authored_keys.AUTHORED_KEYS`), so no setting is dropped silently.

Modules, lowest first: `nodes` and `defs` (node iteration and the member records);
`readers`, `relationships`, `target` and `checks/` (reading or checking one concern);
`collect` (the parsed project); `build` (one view); `pipeline` (the whole load).
"""

from .pipeline import SemanticInputs, load_semantic_views_result, read_semantic_inputs

__all__ = ["SemanticInputs", "load_semantic_views_result", "read_semantic_inputs"]
