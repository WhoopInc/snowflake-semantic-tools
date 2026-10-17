"""Every `sst` command, one module each, registered on the root group here.

Importing this package registers the commands on `cli`, in the order `sst` has always
registered them, and then gives each option declared without help text its shared
description, so `--help` and the generated CLI reference agree.
"""

from __future__ import annotations

from snowflake_semantic_tools.cli.commands import (
    agent_kit,
    apply,
    ask,
    baseline,
    clean,
    compile,
    debug,
    diff,
    docs,
    drop,
    enrich,
    explain,
    format,
    init,
    list,
    migrate,
    plan,
    state,
    test,
    validate,
)
from snowflake_semantic_tools.cli.globals import refuse_removed_options
from snowflake_semantic_tools.cli.group import cli
from snowflake_semantic_tools.cli.help_text import document_options

# The submodules stay bound under their own names, so `commands.apply` is a module to patch.
for _command in (
    init.init,
    debug.debug,
    compile.compile,
    validate.validate,
    plan.plan,
    apply.apply,
    list.list_command,
    clean.clean,
    migrate.migrate,
    enrich.enrich,
    test.test_command,
    ask.ask,
    docs.docs,
    explain.explain_command,
    format.format_command,
    baseline.baseline,
    diff.diff,
    drop.drop,
    agent_kit.agent_kit,
    state.state,
):
    cli.add_command(_command)
# After every command is registered, so a command's own option is never shadowed by a refusal.
refuse_removed_options(cli)
document_options(cli)
