"""Every `sst` command, one module each, registered on the root group here.

Importing this package registers the commands on `cli`, in the order `sst` has always
registered them, and then gives each option declared without help text its shared
description, so `--help` and the generated CLI reference agree.
"""

from __future__ import annotations

from snowflake_semantic_tools.cli.commands import (
    apply,
    baseline,
    clean,
    compile,
    debug,
    docs,
    enrich,
    explain,
    format,
    init,
    list,
    migrate,
    plan,
    test,
    validate,
)
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
    docs.docs,
    explain.explain_command,
    format.format_command,
    baseline.baseline,
):
    cli.add_command(_command)
document_options(cli)
