"""SST-PRS900: a parser returned a record that is not immutable."""

from __future__ import annotations

from dataclasses import dataclass

from snowflake_semantic_tools.domain.diagnostics import Severity
from snowflake_semantic_tools.domain.parse.records import is_frozen_record, mutable_records


@dataclass
class _Loose:
    value: int = 1


@dataclass(frozen=True)
class _Frozen:
    value: int = 1


def test_sst_prs900_fires() -> None:
    [diagnostic] = mutable_records("metric", [_Loose(), _Frozen(), _Loose()])
    assert (diagnostic.code, diagnostic.severity) == ("SST-PRS900", Severity.ERROR)
    assert diagnostic.message == "metric parser returned _Loose, which is mutable"
    assert diagnostic.subject is None


def test_sst_prs900_silent() -> None:
    assert mutable_records("dimension", [_Frozen(), (_Frozen(), _Frozen())]) == ()
    assert not is_frozen_record(_Frozen) and not is_frozen_record({"value": 1})
