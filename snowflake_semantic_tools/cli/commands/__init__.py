"""Every `sst` command, one module each, registered on the root group here.

Importing this package registers the commands on `cli`, in the order `sst` has always
registered them, and then gives each option declared without help text its shared
description, so `--help` and the generated CLI reference agree.
"""

from __future__ import annotations

from ..group import cli
from ..help_text import document_options
from . import apply, clean, compile, debug, docs, init, list, migrate, plan, test, validate

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
    test.test_command,
    docs.docs,
):
    cli.add_command(_command)
document_options(cli)
