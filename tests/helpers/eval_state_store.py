"""An in-memory eval state store for tests: baselines and gate state keyed by target and eval."""

from __future__ import annotations

from dataclasses import dataclass, field

from snowflake_semantic_tools.domain.model.eval import EvalBaselineRecord, EvalGateState


@dataclass
class InMemoryEvalStateStore:
    """Holds what `SnowflakeEvalStateStore` would persist, so eval gates run without Snowflake."""

    baselines: dict[tuple[str, str], EvalBaselineRecord] = field(default_factory=dict)
    gates: dict[tuple[str, str], EvalGateState] = field(default_factory=dict)

    def read_baseline(self, target_name: str, eval_key: str) -> EvalBaselineRecord | None:
        return self.baselines.get((target_name, eval_key))

    def write_baselines(self, target_name: str, baselines: tuple[EvalBaselineRecord, ...]) -> None:
        for baseline in baselines:
            self.baselines[(target_name, baseline.eval_key)] = baseline

    def write_gate(self, target_name: str, gate: EvalGateState) -> None:
        self.gates[(target_name, gate.eval_key)] = gate
