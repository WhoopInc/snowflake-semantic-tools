"""Reuse a view built by an earlier load of the same session when nothing it reads has changed.

A long-lived session loads the project again after every edit, and most views read nothing an
edit touched. Building a view is a pure function of the arguments `_build_view` takes, so a view
whose arguments are the same as last time is the view built last time. "The same" is stricter
than `==`: `1`, `1.0` and `True` are equal in Python but are different YAML, and a dataclass may
leave a field out of its equality. `identical` compares type and every field, and compares two
references to one object at once, which is what an unchanged file's parse is.

Only views that built are kept; a failure is built again, so its exception is always new. The
cache holds the views of the last load only, so it never outgrows the project.
"""

from __future__ import annotations

import dataclasses
import math
from collections.abc import Mapping
from datetime import date, datetime, time
from enum import Enum
from pathlib import PurePath
from typing import Any

from snowflake_semantic_tools.domain.model.semantic_view import SemanticView

_ATOMS = (str, bytes, int, type(None), PurePath, date, datetime, time, Enum)


def identical(left: object, right: object) -> bool:
    """Report whether two values are the same value of the same type, field for field.

    A mapping's entries must be in the same order, and a float must match to its sign and
    NaN-ness. A type this does not know is never identical to anything but itself, so a value
    of a new type is rebuilt rather than reused wrongly.
    """
    if left is right:
        return True
    if type(left) is not type(right):
        return False
    if isinstance(left, _ATOMS):
        return bool(left == right)
    if isinstance(left, float):
        assert isinstance(right, float)
        if math.isnan(left) or math.isnan(right):
            return math.isnan(left) and math.isnan(right)
        return left == right and math.copysign(1.0, left) == math.copysign(1.0, right)
    if isinstance(left, (tuple, list)):
        assert isinstance(right, (tuple, list))
        return len(left) == len(right) and all(map(identical, left, right))
    if isinstance(left, Mapping):
        assert isinstance(right, Mapping)
        return len(left) == len(right) and all(
            identical(left_key, right_key) and identical(left_value, right_value)
            for (left_key, left_value), (right_key, right_value) in zip(left.items(), right.items(), strict=True)
        )
    if isinstance(left, (frozenset, set)):
        assert isinstance(right, (frozenset, set))
        return {(type(item), item) for item in left} == {(type(item), item) for item in right}
    if dataclasses.is_dataclass(left) and not isinstance(left, type):
        return all(
            identical(getattr(left, field.name), getattr(right, field.name)) for field in dataclasses.fields(left)
        )
    return False


class ViewBuilds:
    """The views one session's last load built, and the arguments each was built from.

    `begin` is given the arguments every view of a load shares; when any differs from the last
    load's, nothing is reused. `get` then returns a view built from identical arguments, and
    `put` records one just built. `end` forgets every view the load did not build.
    """

    def __init__(self) -> None:
        self._shared: tuple[Any, ...] | None = None
        self._views: dict[str, tuple[tuple[Any, ...], SemanticView]] = {}
        self._seen: set[str] = set()

    def begin(self, shared: tuple[Any, ...]) -> None:
        """Start a load whose views all read `shared`; a change in it forgets every view."""
        if self._shared is None or not identical(self._shared, shared):
            self._views.clear()
        self._shared = shared
        self._seen = set()

    def get(self, key: str, arguments: tuple[Any, ...]) -> SemanticView | None:
        """Return the view `key` built from identical `arguments` last load; None when there is none."""
        self._seen.add(key)
        held = self._views.get(key)
        if held is not None and identical(held[0], arguments):
            return held[1]
        return None

    def put(self, key: str, arguments: tuple[Any, ...], view: SemanticView) -> None:
        """Remember the view `key` built from `arguments`."""
        self._seen.add(key)
        self._views[key] = (arguments, view)

    def end(self) -> None:
        """Forget every view this load did not reach, so the cache holds one load's views."""
        for key in set(self._views) - self._seen:
            del self._views[key]
