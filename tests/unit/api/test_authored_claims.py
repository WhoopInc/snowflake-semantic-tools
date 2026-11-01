"""SST reports what the authored-file tables claim, in every reference-project file of each kind.

For each field a table declares, at the first reference file holding it, a session overlay
writes one edit and checks that validation reports something it did not before: the field
removed when it is required, a value outside its choices when it is an enum, and a value of
the wrong kind. A field the table marks `loose` SST reads without checking its kind, so a value
of another kind there must be reported as SST-PRS131, not merely change something.
"""

from __future__ import annotations

import functools
from collections import Counter

import pytest

from snowflake_semantic_tools.adapters.errors import ProjectError
from snowflake_semantic_tools.api import ProjectSession
from snowflake_semantic_tools.domain.model.authored_schema import (
    FILE_KINDS,
    walk,
)
from tests.helpers.authored_claims import CLAIMS, FILES, Claim, edited
from tests.helpers.reference_project import DBT_MANIFEST, project_copy

# A loose field holding another kind is reported with this code; any other claim with any code.
_LOOSE_CODE = "SST-PRS131"


@pytest.fixture(scope="module")
def session(tmp_path_factory: pytest.TempPathFactory) -> ProjectSession:
    return ProjectSession.open(project_copy(tmp_path_factory.mktemp("claims")), manifest_path=DBT_MANIFEST)


@functools.cache
def _codes_of(session: ProjectSession, file: str, data: bytes) -> Counter[str]:
    try:
        return Counter(str(item["code"]) for item in session.with_overlay({file: data}).validate().diagnostics)
    except ProjectError as error:
        # A file SST cannot read stops the run, which is reported too.
        return Counter(item.code for item in error.diagnostics) or Counter(("raised",))


@pytest.mark.parametrize("claim", CLAIMS, ids=lambda claim: claim.id)
def test_sst_reports_what_the_table_claims(session: ProjectSession, claim: Claim) -> None:
    plain, changed = edited(session.project_dir, claim)
    new = _codes_of(session, claim.file, changed) - _codes_of(session, claim.file, plain)
    if claim.declared.loose and claim.claim == "kind":
        # What SST then reads (an unset description, say) may draw its own report too.
        assert _LOOSE_CODE in new, f"{claim.file}: {claim.id} reports {sorted(new)}"
    else:
        assert new, f"{claim.file}: SST reports nothing for {claim.id}"


def test_every_loose_field_is_probed_somewhere() -> None:
    """A field one node shares with another, such as a display block's, is the same field wherever it sits."""
    loose = {id(item) for name in FILES for _, _, item in walk(FILE_KINDS[name].node) if item.loose}
    probed = {id(claim.declared) for claim in CLAIMS if claim.declared.loose and claim.claim == "kind"}
    assert loose == probed
