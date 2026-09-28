"""Persistence boundary for metadata-only evaluation baselines and gate state."""

from __future__ import annotations

from typing import Protocol

from ..model.eval import EvalBaselineRecord, EvalGateState


class EvalStateStore(Protocol):
    def read_baseline(self, target_name: str, eval_key: str) -> EvalBaselineRecord | None: ...

    def write_baseline(self, target_name: str, baseline: EvalBaselineRecord) -> None: ...

    def write_baselines(self, target_name: str, baselines: tuple[EvalBaselineRecord, ...]) -> None: ...

    def read_gate(self, target_name: str, eval_key: str) -> EvalGateState | None: ...

    def write_gate(self, target_name: str, gate: EvalGateState) -> None: ...
