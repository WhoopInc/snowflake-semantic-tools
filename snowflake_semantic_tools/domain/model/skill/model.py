"""Skill, plugin, and bundle records, and the identities every publication channel derives from them.

A skill is one authored folder and a plugin one manifest naming skills; each publishes as a
Cortex Extension named by `extension_identifier`. A bundle is the exact file set one extension
version is built from, and `bundle_digest` is its content address: version aliases and Desktop
tree names are cut from it, so it must not change while the content does not.
"""

from __future__ import annotations

import json
import posixpath
from dataclasses import dataclass
from hashlib import sha256

from ..artifact_key import artifact_key
from ..diagnostic import DiagnosticBag, Origin

SKILL_FILE = "SKILL.md"
MARKDOWN_SUFFIXES = (".md", ".markdown")
ALIAS_HEX_CHARACTERS = 12
# Every extension version alias starts with this unless `+version_prefix` changes it.
DEFAULT_VERSION_PREFIX = "SST_"


@dataclass(frozen=True, slots=True)
class SkillFile:
    """One authored file, addressed relative to the skill folder."""

    path: str
    content: bytes

    @property
    def sha256(self) -> str:
        """Hash the content with SHA-256, as lowercase hex."""
        return sha256(self.content).hexdigest()

    @property
    def size(self) -> int:
        """Count the content's bytes."""
        return len(self.content)

    @property
    def is_markdown(self) -> bool:
        """Report whether the path ends in a Markdown suffix, in any letter case."""
        return self.path.casefold().endswith(MARKDOWN_SUFFIXES)

    @property
    def text(self) -> str | None:
        """Decode the content as UTF-8; None when it is not UTF-8, as for an image."""
        try:
            return self.content.decode("utf-8")
        except UnicodeDecodeError:
            return None


@dataclass(frozen=True, slots=True)
class Skill:
    """One authored skill folder. `name` is the folder name; the frontmatter must agree.

    Attributes:
        directory: The folder, relative to the project root.
        declared_name: The frontmatter `name`; None when the frontmatter declares none.
        body: The Markdown after the frontmatter; blank when the skill gives no instructions.
        files: Every published file of the folder, `SKILL.md` included, by path relative to it.
    """

    name: str
    directory: str
    declared_name: str | None
    description: str | None
    body: str
    files: tuple[SkillFile, ...]
    origin: Origin

    @property
    def key(self) -> str:
        """Name the skill as an artifact key, `skill:<name>`."""
        return artifact_key("skill", self.name)

    @property
    def extension_name(self) -> str:
        """Name the Cortex Extension the skill publishes as."""
        return extension_identifier(self.name)

    @property
    def source_files(self) -> tuple[str, ...]:
        """List the skill's files relative to the project root, in `files` order."""
        return tuple(posixpath.join(self.directory, item.path) for item in self.files)

    @property
    def scripts(self) -> tuple[SkillFile, ...]:
        """Select the files that are not Markdown: the ones an agent runs rather than reads."""
        return tuple(item for item in self.files if not item.is_markdown)

    def origin_of(self, path: str, line: int | None = None, col: int | None = None) -> Origin:
        """Locate one of the skill's files, given relative to the folder, for a diagnostic."""
        return Origin(posixpath.join(self.directory, path), line, col)


@dataclass(frozen=True, slots=True)
class Plugin:
    """One plugin manifest: a named group of project skills published as one extension.

    Attributes:
        directory: The plugin folder, relative to the project root; its name is the plugin's.
        members: The skill names the manifest lists, in its order and with any repeats.
    """

    name: str
    directory: str
    manifest_file: str
    description: str | None
    owner_team: str | None
    members: tuple[str, ...]
    origin: Origin

    @property
    def key(self) -> str:
        """Name the plugin as an artifact key, `plugin:<name>`."""
        return artifact_key("plugin", self.name)

    @property
    def extension_name(self) -> str:
        """Name the Cortex Extension the plugin publishes as."""
        return extension_identifier(self.name)


@dataclass(frozen=True, slots=True)
class SkillCatalog:
    """Every skill folder and plugin manifest of a project, as loaded.

    Attributes:
        diagnostics: What loading reported; validation reports these first, unchanged.
    """

    skills: tuple[Skill, ...] = ()
    plugins: tuple[Plugin, ...] = ()
    diagnostics: DiagnosticBag = DiagnosticBag()

    def skill(self, name: str) -> Skill | None:
        """Find the skill whose folder is named `name`; None when there is none."""
        return next((item for item in self.skills if item.name == name), None)


@dataclass(frozen=True, slots=True)
class BundleEntry:
    """One file of a bundle, at its published path."""

    path: str
    content: bytes

    @property
    def sha256(self) -> str:
        """Hash the content with SHA-256, as lowercase hex."""
        return sha256(self.content).hexdigest()

    @property
    def size(self) -> int:
        """Count the content's bytes."""
        return len(self.content)


@dataclass(frozen=True, slots=True)
class SkillBundle:
    """The exact file set one extension version is built from.

    Attributes:
        kind: The extension type, `SKILL` or `PLUGIN`.
        renames: (authored, published) for each nested path flattening renamed.
    """

    kind: str
    name: str
    entries: tuple[BundleEntry, ...]
    renames: tuple[tuple[str, str], ...] = ()

    @property
    def digest(self) -> str:
        """Address the bundle by its content; see `bundle_digest`."""
        return bundle_digest(self.entries)

    def alias(self, prefix: str) -> str:
        """Name the version: `prefix`, then the first 12 hex characters of the digest in uppercase."""
        return f"{prefix}{self.digest[:ALIAS_HEX_CHARACTERS].upper()}"

    def manifest(self, *, alias: str, target: str, comment: str, certified: bool = False) -> str:
        """Describe the version as canonical JSON: the payload plan and goldens compare."""
        document: dict[str, object] = {
            "alias": alias,
            "comment": comment,
            "digest": self.digest,
            "extension": target,
            "files": [{"path": entry.path, "sha256": entry.sha256, "size": entry.size} for entry in self.entries],
            "flattened": [{"authored": authored, "published": published} for authored, published in self.renames],
            "type": self.kind,
        }
        if certified:
            document["certified"] = True
        return json.dumps(document, indent=2, sort_keys=True) + "\n"


def extension_identifier(name: str) -> str:
    """Convert a kebab-case skill or plugin name to its UPPER_SNAKE extension name."""
    return name.replace("-", "_").upper()


def bundle_digest(entries: tuple[BundleEntry, ...]) -> str:
    """Hash a bundle's listing -- each path, size, and content hash, in path order -- with SHA-256.

    The order the entries arrive in does not matter, so a bundle and any copy of it agree.
    """
    listing = "".join(f"{entry.path}\0{entry.size}\0{entry.sha256}\n" for entry in sorted(entries, key=_entry_path))
    return sha256(listing.encode("utf-8")).hexdigest()


def _entry_path(entry: BundleEntry) -> str:
    return entry.path
