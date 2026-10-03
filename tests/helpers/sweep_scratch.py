"""Drop the scratch schemas live runs left behind: one run's, or every one older than a window.

`python -m tests.helpers.sweep_scratch --run <id>` is a CI job's own teardown, run even when the job
failed; `--older-than-hours <n>` is the nightly sweep, the only cleanup that survives a cancelled job
or a killed runner. Either drops a schema only when `live_snowflake.sweepable` chooses it -- the
scratch prefix, a parseable stamp, and SST's marker comment -- so a schema anything else created is
out of reach. It prints each schema it drops, and a non-empty nightly report on a green pipeline is
worth investigating: it means some job died in a way nothing else noticed.

Exit status: 0 when every chosen schema was dropped (or none was chosen); 1 when a drop failed or no
account is configured.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

from snowflake_semantic_tools.domain.model.identifier import Identifier
from snowflake_semantic_tools.domain.ports.snowflake.execution import ExecutionPort
from snowflake_semantic_tools.domain.sql import ident, literal, sql
from tests.helpers.live_snowflake import (
    SCRATCH_PREFIX,
    LiveAccount,
    drop_scratch,
    run_token,
    scratch_scope,
    sweepable,
)


def listed_schemas(port: ExecutionPort, account: LiveAccount) -> list[tuple[str, str | None]]:
    """Every schema in the scratch database whose name starts with the prefix, with its comment."""
    shown = port.query(
        sql(
            "SHOW SCHEMAS LIKE {pattern} IN DATABASE {database}",
            pattern=literal(f"{SCRATCH_PREFIX}%"),
            database=ident(Identifier.parse(account.database)),
        )
    )
    columns = [column.lower() for column in shown.columns]
    name, comment = columns.index("name"), columns.index("comment")
    return [(str(row[name]), None if row[comment] in (None, "") else str(row[comment])) for row in shown.rows]


def sweep(
    port: ExecutionPort,
    account: LiveAccount,
    now: datetime,
    *,
    older_than: timedelta,
    run: str | None,
    dry_run: bool,
) -> tuple[list[str], list[str]]:
    """Drop what `sweepable` chooses; return the schemas dropped and the ones whose drop failed."""
    dropped: list[str] = []
    failed: list[str] = []
    for name in sweepable(listed_schemas(port, account), now, older_than=older_than, run=run):
        if dry_run or drop_scratch(port, scratch_scope(account, name)):
            dropped.append(name)
        else:
            failed.append(name)
    return dropped, failed


def main(argv: Sequence[str] | None = None, now: datetime | None = None) -> int:
    """Sweep the account `SST_TEST_SNOWFLAKE_*` names, by run or by age."""
    parser = argparse.ArgumentParser(prog="python -m tests.helpers.sweep_scratch", description=__doc__)
    chosen = parser.add_mutually_exclusive_group(required=True)
    chosen.add_argument("--run", help="Drop this run's schemas whatever their age (the raw SST_TEST_RUN_ID).")
    chosen.add_argument("--older-than-hours", type=float, help="Drop every scratch schema at least this old.")
    parser.add_argument("--dry-run", action="store_true", help="Report what would be dropped; drop nothing.")
    options = parser.parse_args(argv)
    account = LiveAccount.from_environment(os.environ)
    if account is None:
        print("sweep_scratch: SST_TEST_SNOWFLAKE_ACCOUNT is not set; nothing to sweep", file=sys.stderr)
        return 1
    from snowflake_semantic_tools.adapters.snowflake.connector import SnowflakeConnector

    connector = SnowflakeConnector(account.connection_params())
    try:
        dropped, failed = sweep(
            connector,
            account,
            now or datetime.now(UTC),
            older_than=timedelta(hours=options.older_than_hours or 0),
            run=None if options.run is None else run_token({"SST_TEST_RUN_ID": options.run}),
            dry_run=options.dry_run,
        )
    finally:
        connector.close()
    verb = "would drop" if options.dry_run else "dropped"
    for name in dropped:
        print(f"{verb} {account.database}.{name}")
    for name in failed:
        print(f"could not drop {account.database}.{name}", file=sys.stderr)
    print(f"sweep_scratch: {verb} {len(dropped)}, failed {len(failed)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
