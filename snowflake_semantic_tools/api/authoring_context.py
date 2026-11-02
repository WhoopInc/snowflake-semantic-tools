"""What an editor offers as a project file is written: what each field takes, and what its calls name.

Names are listed without their descriptions, which hold most of a large project's bytes; an
editor that shows one reads it from the dbt docs.

`ProjectSession.authoring_context` returns it. Each field's vocabulary and every template
function are SST's own tables (`domain.render.field_vocabulary`); the names the functions take
that the catalog does not list -- dbt models and their columns, dbt sources, project variables
and tags -- are the project's, as the session read them. An artifact or member a call names is
one `catalog()` lists, by the type each function's `names` gives. Nothing is compiled or written.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from snowflake_semantic_tools.domain.model.config_schema import config_block
from snowflake_semantic_tools.domain.model.dbt import DbtCatalog
from snowflake_semantic_tools.domain.render.field_vocabulary import (
    SETTING_FUNCTIONS,
    TARGET_KEYS,
    FieldVocabulary,
    FileRoot,
    TemplateFunction,
    field_vocabularies,
    file_roots,
    template_functions,
)

# The variable SST supplies to a skill's version, which `vars:` may not declare (SST-CFG040).
SUPPLIED_VARIABLE = "sha_version"
# The key of `tags:` that is a default rather than a tag.
_TAG_DEFAULT = "default_prefix"


@dataclass(frozen=True, slots=True)
class ColumnName:
    """One column of a dbt model, as `ref('<model>', '<column>')` names it.

    Attributes:
        name: The column, as the manifest records it.
        data_type: Its data type, dbt's own or `meta.sst.data_type`; None when neither is written.
    """

    name: str
    data_type: str | None


@dataclass(frozen=True, slots=True)
class ModelName:
    """One dbt model, snapshot or seed `ref()` may name.

    Attributes:
        name: Its dbt name.
        relation: The relation it builds, as Snowflake SQL.
        columns: Its columns, in manifest order.
    """

    name: str
    relation: str
    columns: tuple[ColumnName, ...]


@dataclass(frozen=True, slots=True)
class SourceName:
    """One dbt source table, as `source('<source>', '<table>')` names it.

    Attributes:
        source: The source's name.
        table: The table's name.
        relation: The relation it reads; None when the manifest records none.
    """

    source: str
    table: str
    relation: str | None


@dataclass(frozen=True, slots=True)
class Variable:
    """One name `var()` may take.

    Attributes:
        name: The variable.
        value: Its value as `vars:` writes it, any YAML value; None for one SST supplies.
        supplied: SST supplies it -- `sha_version`, the commit being published -- so `vars:`
            does not declare it, and only a skill's version reads it.
    """

    name: str
    value: object
    supplied: bool = False


@dataclass(frozen=True, slots=True)
class AuthoringContext:
    """What each field takes and the names its calls may take, as the session read the project.

    Attributes:
        functions: Every template function a field may call, by name.
        fields: What each field that takes calls or values takes, by the path the JSON Schemas
            describe it under; a field not listed takes text of its schema's type.
        roots: The top-level keys each kind is declared under in a file that holds several, so
            a key path in such a file names its field: `snowflake_metrics[0].expr` is `metric.expr`.
        setting_functions: The functions a setting's value may call, in `sst_config.yml` or an
            artifact's `config:`, besides naming `{{ target.<key> }}`.
        target_keys: The keys `{{ target.<key> }}` names in a setting's value.
        models: Every dbt model, snapshot and seed `ref()` may name, by name.
        sources: Every dbt source table `source()` may name, by source, then table.
        variables: Every name `var()` may take: `vars:`, in its order, then what SST supplies.
        tags: Every tag `tag()` may name, as `tags:` declares them.
    """

    functions: tuple[TemplateFunction, ...]
    fields: tuple[FieldVocabulary, ...]
    roots: tuple[FileRoot, ...]
    setting_functions: tuple[str, ...]
    target_keys: tuple[str, ...]
    models: tuple[ModelName, ...]
    sources: tuple[SourceName, ...]
    variables: tuple[Variable, ...]
    tags: tuple[str, ...]


def authoring_context(config: Mapping[str, object], catalog: DbtCatalog) -> AuthoringContext:
    """Return what an editor offers for a project whose resolved configuration and dbt models these are."""
    declared = config_block(config.get("vars"))
    return AuthoringContext(
        functions=template_functions(),
        fields=field_vocabularies(config),
        roots=file_roots(),
        setting_functions=SETTING_FUNCTIONS,
        target_keys=TARGET_KEYS,
        models=tuple(
            sorted(
                (
                    ModelName(
                        name=model.name,
                        relation=model.relation_name,
                        columns=tuple(ColumnName(column.name, column.data_type) for column in model.columns),
                    )
                    for model in catalog.models
                ),
                key=lambda model: model.name,
            )
        ),
        sources=tuple(
            sorted(
                (SourceName(item.source_name, item.name, item.relation_name) for item in catalog.sources),
                key=lambda item: (item.source, item.table),
            )
        ),
        variables=(
            *(Variable(name, value) for name, value in declared.items() if name != SUPPLIED_VARIABLE),
            Variable(SUPPLIED_VARIABLE, None, supplied=True),
        ),
        tags=tuple(name for name in config_block(config.get("tags")) if name != _TAG_DEFAULT),
    )
