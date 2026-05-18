"""
Clean Service

Removes SST-generated artifacts from the target directory.
"""

import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

from snowflake_semantic_tools.services.compile import MANIFEST_FILENAME
from snowflake_semantic_tools.shared.utils import get_logger

logger = get_logger("clean_service")

SST_ARTIFACTS = [
    MANIFEST_FILENAME,
    "semantic_views",
]


@dataclass
class CleanResult:
    success: bool = True
    removed: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    @property
    def file_count(self) -> int:
        return sum(1 for r in self.removed if "files)" not in r)

    @property
    def dir_count(self) -> int:
        return sum(1 for r in self.removed if "files)" in r)


def clean(target_dir: Path = Path("target")) -> CleanResult:
    result = CleanResult()

    if not target_dir.exists():
        result.errors.append(f"SST-K001: Target directory not found: {target_dir}")
        result.success = False
        return result

    for artifact in SST_ARTIFACTS:
        path = target_dir / artifact
        if not path.exists():
            continue

        try:
            if path.is_dir():
                count = sum(1 for _ in path.rglob("*") if _.is_file())
                shutil.rmtree(path)
                result.removed.append(f"{path}/ ({count} files)")
            else:
                path.unlink()
                result.removed.append(str(path))
        except OSError as e:
            result.errors.append(f"SST-K002: Could not remove {path}: {e}")
            result.success = False

    return result
