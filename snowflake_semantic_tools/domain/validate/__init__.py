"""Validate: check resolved artifacts against the rules Snowflake and SST impose, offline.

One module per subject: `shared` holds the building blocks every validator uses, `sql`
turns the SQL guards' refusals into diagnostics, and each other module checks one kind of
artifact or one rule shared across them. Validators return diagnostics and never raise for
a user's project. Importers name the submodule; nothing is re-exported here.
"""
