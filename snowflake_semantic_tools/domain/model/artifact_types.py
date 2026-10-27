"""The artifact and member types the registry is built from, and how plan and apply treat each.

Data types only: `domain.model.registry` registers them, checks them against one another, and
holds the closed registry. It re-exports every name here, so a reader imports them from either.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto


class MemberSource(Enum):
    """Where a member type is authored.

    FILES members are declared in SST's own YAML files, under their type's root key. DBT_META
    members are read from the `config.meta.sst` block of dbt model columns.
    """

    FILES = auto()
    DBT_META = auto()


class AttachRule(Enum):
    """How a member finds the semantic views it belongs to.

    TABLE_MEMBERSHIP attaches it to every view that holds all the tables it needs. VIEW_NAME
    attaches it to each view that names it.
    """

    TABLE_MEMBERSHIP = auto()
    VIEW_NAME = auto()


class GrantPreservation(Enum):
    """How an artifact type's grants survive an update.

    CLAUSE keeps them across a replace with `COPY GRANTS`; REPLAY re-applies them after a
    replace; NONE leaves them alone, because the object is never replaced.
    """

    CLAUSE = auto()
    REPLAY = auto()
    NONE = auto()


class ArtifactLifecycle(Enum):
    """How plan and apply reconcile an artifact type.

    An OBJECT type is one Snowflake object, listed by its object type and planned generically.
    A COMPOSITE type is several objects, planned and applied together by its own lifecycle
    handler.
    """

    OBJECT = auto()
    COMPOSITE = auto()


@dataclass(frozen=True, slots=True)
class ArtifactType:
    """One kind of artifact SST publishes, and how plan and apply treat it.

    `RegistryBuilder.freeze` refuses a set of types that contradict one another; it states the rules.

    Attributes:
        name: The type's name, which is the kind part of its artifacts' keys.
        root_key: The YAML key its declarations are listed under; None for a type authored some
            other way, such as one folder per artifact.
        singular_root: The YAML key a file declaring exactly one of them holds it under, as a
            mapping; None exactly when `root_key` is.
        ddl_position: Where its changes fall in plan order, lowest first; it is higher than the
            position of every type it depends on.
        ref_function: The template function another artifact names it with; None when none can.
        member_types: The member types it owns: exactly those whose `owner_type` names it.
        object_type: The Snowflake object type its objects are listed by; "" for a composite
            type, or for one that lists several `object_types` instead.
        prunable: Whether `--prune` drops its object once the object's source is deleted.
        replaces_on_update: Whether an update replaces the object rather than altering it in place.
        grant_preservation: How the type's grants survive an update; None for a type whose
            object kinds keep them differently, which lists them in `object_grant_preservation`.
        dependency_types: The types it may depend on, which it is planned after.
        pins_versions_of: The dependency types whose published version its payload names.
        object_types: The object types its objects may be, when there are several.
        object_grant_preservation: How each of `object_types` keeps its grants, when the kinds
            differ; each rendered object's `PublishShape` declares the same.
        validation_rules: The rule sets, by `RULE_SETS` name, whose codes validate it.
        dir_key: The `project.` configuration key naming the directory its files are under.
        nested_under: The type inside each of whose artifact folders its own files are; "" for
            a type whose directory is `dir_key` itself.
        routed_block: The `sst_config.yml` block whose `+` keys and folder routes its artifacts'
            settings resolve through; "" for a type routed some other way.
        route_dir: The directory below `dir_key` that `routed_block`'s folder routes start from;
            "" when they start from `dir_key` itself.
    """

    name: str
    root_key: str | None
    singular_root: str | None
    ddl_position: int
    ref_function: str | None
    member_types: tuple[str, ...]
    object_type: str
    prunable: bool = True
    replaces_on_update: bool = True
    grant_preservation: GrantPreservation | None = GrantPreservation.CLAUSE
    dependency_types: tuple[str, ...] = ()
    # Dependencies whose published version this type's payload names. A write of
    # this type is planned only together with them, so it cannot pin a version
    # that was never created.
    pins_versions_of: tuple[str, ...] = ()
    object_types: tuple[str, ...] = ()
    object_grant_preservation: tuple[tuple[str, GrantPreservation], ...] = ()
    lifecycle: ArtifactLifecycle = ArtifactLifecycle.OBJECT
    # Reference-page text; the generated artifact reference renders these rows.
    summary: str = ""
    authored_in: str = ""
    publishes: str = ""
    validation_rules: tuple[str, ...] = ()
    dir_key: str = ""
    nested_under: str = ""
    routed_block: str = ""
    route_dir: str = ""

    def grant_preservation_of(self, object_type: str) -> GrantPreservation | None:
        """Return how an object of `object_type` keeps its grants; None for a kind the type lacks."""
        return dict(self.object_grant_preservation).get(object_type, self.grant_preservation)


@dataclass(frozen=True, slots=True)
class MemberType:
    """One kind of semantic member, and how it is authored and attached to its owner's artifacts.

    Attributes:
        name: The type's name, which is the kind part of its members' keys.
        root_key: The YAML key its declarations are listed under; unique across the registry.
        singular_root: The YAML key a file declaring exactly one member holds it under, as a
            mapping; unique across the registry. None for a type not authored in files.
        clause_position: Orders its owner's member types, lowest first; unique per owner.
        owner_type: The artifact type whose artifacts it attaches to.
        ref_function: The template function that names a member of this type; None when none can.
        validation_rules: The rule sets, by `RULE_SETS` name, whose codes validate it.
    """

    name: str
    root_key: str
    singular_root: str | None
    source: MemberSource
    clause_position: int
    attaches_by: AttachRule
    owner_type: str
    ref_function: str | None
    validation_rules: tuple[str, ...] = ()
