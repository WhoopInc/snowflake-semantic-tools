"""Persistence boundary for metadata-only evaluation baselines and gate state."""

from __future__ import annotations

from typing import Protocol

from snowflake_semantic_tools.domain.model.eval import EvalBaselineRecord, EvalGateState


class EvalStateStore(Protocol):
    """Keep each eval's baseline and gate state: one record of each per target and eval key.

    Records hold metadata only, such as fingerprints, run names, and pass flags, and never an
    input query, ground truth, or trace. A read never creates the store, so a read-only role
    can read it; a write creates it first. A write replaces the record it keys, so it is
    idempotent.
    """

    def read_baseline(self, target_name: str, eval_key: str) -> EvalBaselineRecord | None:
        """Return the baseline recorded for one eval on one target.

        Never writes. A stored baseline that does not decode raises rather than reading as absent.

        Returns:
            The baseline; None when the store does not exist or holds none for the eval.

        Raises:
            SnowflakePortError: the read failed, or more than one baseline matched.
        """
        ...

    def write_baseline(self, target_name: str, baseline: EvalBaselineRecord) -> None:
        """Insert or replace one eval's baseline on one target, keyed by its `eval_key`.

        Creates the store first unless it exists.

        Raises:
            SnowflakePortError: creating the store or the write failed.
        """
        ...

    def write_baselines(self, target_name: str, baselines: tuple[EvalBaselineRecord, ...]) -> None:
        """Insert or replace several baselines on one target in one transaction: all of them or none.

        An empty batch does nothing, and does not create the store.

        Raises:
            SnowflakePortError: creating the store or the transaction failed; the transaction is
                rolled back, so no baseline of the batch is written.
        """
        ...

    def read_gate(self, target_name: str, eval_key: str) -> EvalGateState | None:
        """Return the gate state the last gated run recorded for one eval on one target.

        Never writes. A stored gate state that does not decode raises rather than reading as absent.

        Returns:
            The gate state; None when the store does not exist or holds none for the eval.

        Raises:
            SnowflakePortError: the read failed, or more than one gate state matched.
        """
        ...

    def write_gate(self, target_name: str, gate: EvalGateState) -> None:
        """Insert or replace one eval's gate state on one target, keyed by its `eval_key`.

        Creates the store first unless it exists.

        Raises:
            SnowflakePortError: creating the store or the write failed.
        """
        ...
