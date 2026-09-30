"""Skills and plugins: authored folders, path references, and flattened bundles.

A skill is authored nested, the way CoCo Desktop reads it, and flattened only for
the catalog channel, because a Cortex Agent reads supporting files beside
`SKILL.md` and never descends into a subdirectory. Flattening is three operations
in a fixed order: detect collisions in authored terms, join path components with a
double underscore, then rewrite every Markdown reference to the flattened name and
check the rewritten text again.
"""

from __future__ import annotations

import json
import posixpath
import re
from collections.abc import Mapping
from dataclasses import dataclass
from hashlib import sha256

from .artifact_key import artifact_key
from .diagnostic import D, Diagnostic, DiagnosticBag, Origin
from .stage_path import ALLOWED_DESCRIPTION, unsafe_segment

SKILL_FILE = "SKILL.md"
PLUGIN_MANIFEST = ".cortex-plugin/plugin.json"
MARKDOWN_SUFFIXES = (".md", ".markdown")
# Directories an author conventionally bundles. A bare path starting with one of
# them is a bundle reference even when the directory is absent, which is what
# catches a skill that names `scripts/x.sql` while shipping `x.sql` at its root.
CONVENTIONAL_DIRS = frozenset(("scripts", "reference", "references", "examples", "assets", "templates"))
# Snowflake scans an extension version only within these limits.
SCAN_MAX_FILES = 50
SCAN_MAX_FILE_BYTES = 2 * 1024 * 1024
SCAN_MAX_TOTAL_BYTES = 10 * 1024 * 1024
# Soft budgets: SKILL.md is read on every orchestration turn, the bundle on demand.
SKILL_MD_BUDGET_BYTES = 25 * 1024
BUNDLE_BUDGET_BYTES = 1024 * 1024
ALIAS_HEX_CHARACTERS = 12
IGNORE_MARKER = "sst: ignore SST-VAL808"

_KEBAB = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
_MAX_NAME = 64
# Every extension version alias starts with this unless `+version_prefix` changes it.
DEFAULT_VERSION_PREFIX = "SST_"
_LINK = re.compile(r"!?\[[^\]\n]*\]\(\s*<?([^)\s>]+)>?(?:\s+\"[^\"\n]*\")?\s*\)")
_DEFINITION = re.compile(r"^[ ]{0,3}\[[^\]\n]+\]:[ \t]*<?([^\s>]+)>?", re.MULTILINE)
_SEGMENT = r"(?:\.{1,2}|\.?[A-Za-z0-9_][A-Za-z0-9_.-]*)"
# A path's last segment never ends in punctuation, so a sentence-final period or
# comma stays outside the token.
_LAST = r"[A-Za-z0-9_](?:[A-Za-z0-9_.-]*[A-Za-z0-9_-])?"
_BARE = re.compile(
    rf"(?<![\w./:@~$-])((?:{_SEGMENT}/)+{_LAST}|[A-Za-z0-9_][A-Za-z0-9_-]*\.[A-Za-z][A-Za-z0-9]{{0,5}})"
    # A token that runs into a glob or template marker is a pattern, not a path.
    r"(?![\w/*{<$-])"
)
_ABSOLUTE = re.compile(r"(?<![\w./:-])((?:/(?:Users|home|opt)/|~/|[A-Za-z]:\\)[^\s`'\")\]]*)")
_URL = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")
_SECRET = re.compile(
    r"(?i)\b(?:password|passwd|secret|api[_-]?key|private[_-]?key|access[_-]?token|auth[_-]?token)\b\s*[:=]\s*['\"][^'\"\s]{4,}"
)


@dataclass(frozen=True, slots=True)
class SkillFile:
    """One authored file, addressed relative to the skill folder."""

    path: str
    content: bytes

    @property
    def sha256(self) -> str:
        return sha256(self.content).hexdigest()

    @property
    def size(self) -> int:
        return len(self.content)

    @property
    def is_markdown(self) -> bool:
        return self.path.casefold().endswith(MARKDOWN_SUFFIXES)

    @property
    def text(self) -> str | None:
        try:
            return self.content.decode("utf-8")
        except UnicodeDecodeError:
            return None


@dataclass(frozen=True, slots=True)
class Skill:
    """One authored skill folder. `name` is the folder name; the frontmatter must agree."""

    name: str
    directory: str
    declared_name: str | None
    description: str | None
    body: str
    files: tuple[SkillFile, ...]
    origin: Origin

    @property
    def key(self) -> str:
        return artifact_key("skill", self.name)

    @property
    def extension_name(self) -> str:
        return extension_identifier(self.name)

    @property
    def source_files(self) -> tuple[str, ...]:
        return tuple(posixpath.join(self.directory, item.path) for item in self.files)

    @property
    def scripts(self) -> tuple[SkillFile, ...]:
        return tuple(item for item in self.files if not item.is_markdown)


@dataclass(frozen=True, slots=True)
class Plugin:
    """One plugin manifest: a named group of project skills published as one extension."""

    name: str
    directory: str
    manifest_file: str
    description: str | None
    owner_team: str | None
    members: tuple[str, ...]
    origin: Origin

    @property
    def key(self) -> str:
        return artifact_key("plugin", self.name)

    @property
    def extension_name(self) -> str:
        return extension_identifier(self.name)


@dataclass(frozen=True, slots=True)
class SkillCatalog:
    skills: tuple[Skill, ...] = ()
    plugins: tuple[Plugin, ...] = ()
    diagnostics: DiagnosticBag = DiagnosticBag()

    def skill(self, name: str) -> Skill | None:
        return next((item for item in self.skills if item.name == name), None)


@dataclass(frozen=True, slots=True)
class PathReference:
    """A path written in a Markdown or script file, located by character offset."""

    file: str
    text: str
    anchor: str
    line: int
    col: int
    start: int
    end: int
    form: str
    suppressed: bool = False


@dataclass(frozen=True, slots=True)
class BundleEntry:
    path: str
    content: bytes

    @property
    def sha256(self) -> str:
        return sha256(self.content).hexdigest()

    @property
    def size(self) -> int:
        return len(self.content)


@dataclass(frozen=True, slots=True)
class SkillBundle:
    """The exact file set one extension version is built from."""

    kind: str
    name: str
    entries: tuple[BundleEntry, ...]
    renames: tuple[tuple[str, str], ...] = ()

    @property
    def digest(self) -> str:
        return bundle_digest(self.entries)

    def alias(self, prefix: str) -> str:
        return f"{prefix}{self.digest[:ALIAS_HEX_CHARACTERS].upper()}"

    def manifest(self, *, alias: str, target: str, comment: str, certified: bool = False) -> str:
        """Canonical JSON describing the version: the payload plan and goldens compare."""
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
    """The UPPER_SNAKE extension name of a kebab-case skill or plugin name."""
    return name.replace("-", "_").upper()


def bundle_digest(entries: tuple[BundleEntry, ...]) -> str:
    listing = "".join(f"{entry.path}\0{entry.size}\0{entry.sha256}\n" for entry in sorted(entries, key=_entry_path))
    return sha256(listing.encode("utf-8")).hexdigest()


def _entry_path(entry: BundleEntry) -> str:
    return entry.path


def flattened_name(path: str) -> str:
    """Join the components below the skill folder with a double underscore."""
    return "__".join(path.split("/"))


def scan_references(text: str, file: str) -> tuple[PathReference, ...]:
    """Every candidate path in one file, in source order."""
    found: list[PathReference] = []
    claimed: list[tuple[int, int]] = []
    for pattern, form in ((_LINK, "link"), (_DEFINITION, "definition")):
        for match in pattern.finditer(text):
            start, end = match.span(1)
            raw = match.group(1)
            if form == "link" and match.group(0).startswith("!"):
                form_name = "image"
            else:
                form_name = form
            claimed.append((start, end))
            found.append(_reference(text, file, raw, start, end, form_name))
    fences = _fence_spans(text)
    for match in _BARE.finditer(text):
        start, end = match.span(1)
        if any(left <= start < right for left, right in claimed):
            continue
        form = "fenced" if any(left <= start < right for left, right in fences) else "bare"
        found.append(_reference(text, file, match.group(1), start, end, form))
    return tuple(sorted(found, key=lambda item: item.start))


def _reference(text: str, file: str, raw: str, start: int, end: int, form: str) -> PathReference:
    path, _, anchor = raw.partition("#")
    line_start = text.rfind("\n", 0, start) + 1
    line_end = text.find("\n", start)
    line_text = text[line_start : line_end if line_end >= 0 else len(text)]
    return PathReference(
        file=file,
        text=path,
        anchor=f"#{anchor}" if anchor else "",
        line=text.count("\n", 0, start) + 1,
        col=start - line_start + 1,
        start=start,
        end=start + len(path),
        form=form,
        suppressed=IGNORE_MARKER in line_text,
    )


def _fence_spans(text: str) -> tuple[tuple[int, int], ...]:
    spans: list[tuple[int, int]] = []
    opened: int | None = None
    offset = 0
    for line in text.splitlines(keepends=True):
        if line.lstrip().startswith(("```", "~~~")):
            if opened is None:
                opened = offset
            else:
                spans.append((opened, offset + len(line)))
                opened = None
        offset += len(line)
    if opened is not None:
        spans.append((opened, len(text)))
    return tuple(spans)


@dataclass(frozen=True, slots=True)
class _Resolution:
    reference: PathReference
    target: str


def _resolve(reference: PathReference, paths: frozenset[str]) -> str | None:
    base = posixpath.dirname(reference.file)
    for candidate in (posixpath.join(base, reference.text), reference.text):
        normalized = posixpath.normpath(candidate)
        if normalized.startswith("../") or normalized == "..":
            continue
        if normalized in paths:
            return normalized
    return None


def _is_repo_anchored(reference: PathReference, skill_name: str) -> bool:
    """A path that walks down a `skills/` tree to this skill from the repository root."""
    parts = posixpath.normpath(reference.text).split("/")
    if reference.text.startswith(("./", "../")) or "skills" not in parts:
        return False
    return skill_name in parts[parts.index("skills") + 1 : -1]


def _anchored_target(reference: PathReference, skill_name: str, paths: frozenset[str]) -> tuple[str, ...]:
    """The bundle file a repo-anchored reference names: named, just not portably."""
    parts = posixpath.normpath(reference.text).split("/")
    below = parts[parts.index("skills") + 1 :]
    remainder = "/".join(below[below.index(skill_name) + 1 :])
    return (remainder,) if remainder in paths else ()


def _names_directory(reference: PathReference, paths: frozenset[str]) -> bool:
    """A directory reference cannot survive flattening: the directory no longer exists."""
    base = posixpath.dirname(reference.file)
    for candidate in (posixpath.join(base, reference.text), reference.text):
        prefix = posixpath.normpath(candidate) + "/"
        if any(path.startswith(prefix) for path in paths):
            return True
    return False


def _looks_bundled(reference: PathReference, top_level: frozenset[str], basenames: frozenset[str]) -> bool:
    if reference.form in ("link", "image", "definition"):
        return True
    if reference.text.startswith(("./", "../")):
        return True
    first = reference.text.split("/", 1)[0]
    return first in top_level or first in CONVENTIONAL_DIRS or posixpath.basename(reference.text) in basenames


def _skip(reference: PathReference) -> bool:
    return not reference.text or bool(_URL.match(reference.text)) or reference.text.startswith(("/", "~"))


def _subject(skill: Skill) -> str:
    return skill.key


def _origin(skill: Skill, file: str, line: int | None = None, col: int | None = None) -> Origin:
    return Origin(posixpath.join(skill.directory, file), line, col)


def validate_skill_catalog(catalog: SkillCatalog) -> DiagnosticBag:
    """Folder, naming, uniqueness, and plugin-membership rules across the catalog."""
    diagnostics: list[Diagnostic] = list(catalog.diagnostics)
    seen: dict[str, str] = {}
    for skill in sorted(catalog.skills, key=lambda item: item.directory):
        subject = _subject(skill)
        if not _KEBAB.fullmatch(skill.name) or len(skill.name) > _MAX_NAME:
            diagnostics.append(
                D(
                    "SST-VAL801",
                    origin=skill.origin,
                    subject=subject,
                    artifact=skill.name,
                    detail=f"folder name '{skill.name}' is not lowercase kebab-case of at most {_MAX_NAME} characters",
                )
            )
        if skill.declared_name is not None and skill.declared_name != skill.name:
            diagnostics.append(
                D(
                    "SST-VAL801",
                    origin=skill.origin,
                    subject=subject,
                    artifact=skill.name,
                    detail=f"frontmatter name '{skill.declared_name}' does not match the folder name",
                )
            )
        if not skill.body.strip():
            diagnostics.append(D("SST-RND031", origin=skill.origin, subject=subject, artifact=skill.name))
        for file in sorted(skill.files, key=lambda item: item.path):
            unsafe = unsafe_segment(file.path)
            if unsafe is not None:
                diagnostics.append(
                    D(
                        "SST-VAL857",
                        origin=_origin(skill, file.path),
                        subject=subject,
                        artifact=subject,
                        value=file.path,
                        found=unsafe,
                        expected=ALLOWED_DESCRIPTION,
                    )
                )
        other = seen.get(skill.extension_name)
        if other is not None:
            diagnostics.append(
                D("SST-VAL832", origin=skill.origin, subject=subject, artifact=skill.directory, other=other)
            )
        seen.setdefault(skill.extension_name, skill.directory)
    names = {skill.name for skill in catalog.skills}
    for plugin in sorted(catalog.plugins, key=lambda item: item.directory):
        subject = plugin.key
        if not _KEBAB.fullmatch(plugin.name) or len(plugin.name) > _MAX_NAME:
            diagnostics.append(
                D(
                    "SST-VAL801",
                    origin=plugin.origin,
                    subject=subject,
                    artifact=plugin.name,
                    detail=f"plugin name '{plugin.name}' is not lowercase kebab-case of at most {_MAX_NAME} characters",
                )
            )
        other = seen.get(plugin.extension_name)
        if other is not None:
            diagnostics.append(
                D("SST-VAL832", origin=plugin.origin, subject=subject, artifact=plugin.directory, other=other)
            )
        seen.setdefault(plugin.extension_name, plugin.directory)
        if not plugin.members:
            diagnostics.append(D("SST-PRS002", origin=plugin.origin, subject=subject, artifact=subject, field="skills"))
        repeated = sorted({member for member in plugin.members if plugin.members.count(member) > 1})
        for member in repeated:
            diagnostics.append(
                D("SST-VAL837", origin=plugin.origin, subject=subject, artifact=plugin.name, name=member)
            )
        for member in plugin.members:
            if member not in names:
                diagnostics.append(
                    D("SST-VAL835", origin=plugin.origin, subject=subject, artifact=plugin.name, name=member)
                )
    return DiagnosticBag(diagnostics)


def flatten_skill(skill: Skill) -> tuple[tuple[SkillFile, ...], tuple[tuple[str, str], ...], DiagnosticBag]:
    """Collision check, `__` join, and Markdown reference rewrite for one skill folder."""
    subject = _subject(skill)
    diagnostics: list[Diagnostic] = []
    by_name: dict[str, list[str]] = {}
    for item in skill.files:
        by_name.setdefault(flattened_name(item.path), []).append(item.path)
    for authored in sorted(paths for paths in by_name.values() if len(paths) > 1):
        diagnostics.append(
            D(
                "SST-VAL809",
                origin=_origin(skill, authored[0]),
                subject=subject,
                artifact=skill.name,
                a=authored[0],
                b=authored[1],
            )
        )
    if diagnostics:
        return (), (), DiagnosticBag(diagnostics)
    paths = frozenset(item.path for item in skill.files)
    top_level = frozenset(path.split("/", 1)[0] for path in paths)
    basenames = frozenset(posixpath.basename(path) for path in paths)
    referenced: set[str] = set()
    flattened: list[SkillFile] = []
    for item in skill.files:
        text = item.text
        if not item.is_markdown or text is None:
            flattened.append(SkillFile(flattened_name(item.path), item.content))
            continue
        resolutions: list[_Resolution] = []
        for reference in scan_references(text, item.path):
            if _skip(reference):
                continue
            target = _resolve(reference, paths)
            if target is not None:
                referenced.add(target)
                resolutions.append(_Resolution(reference, target))
                continue
            anchored = _is_repo_anchored(reference, skill.name)
            if anchored or _names_directory(reference, paths):
                if anchored:
                    referenced.update(_anchored_target(reference, skill.name, paths))
                diagnostics.append(
                    D(
                        "SST-VAL810",
                        origin=_origin(skill, item.path, reference.line, reference.col),
                        subject=subject,
                        artifact=skill.name,
                        path=reference.text,
                    )
                )
            elif _looks_bundled(reference, top_level, basenames) and not reference.suppressed:
                diagnostics.append(
                    D(
                        "SST-VAL808",
                        origin=_origin(skill, item.path, reference.line, reference.col),
                        subject=subject,
                        artifact=skill.name,
                        path=reference.text,
                    )
                )
        rewritten = _rewrite(text, resolutions)
        flattened.append(SkillFile(flattened_name(item.path), rewritten.encode("utf-8")))
    diagnostics.extend(_recheck(skill, tuple(flattened)))
    diagnostics.extend(_script_diagnostics(skill))
    for item in skill.files:
        if item.path != SKILL_FILE and item.path not in referenced:
            diagnostics.append(
                D("SST-VAL813", origin=_origin(skill, item.path), subject=subject, artifact=skill.name, path=item.path)
            )
    renames = tuple((item.path, flattened_name(item.path)) for item in skill.files if "/" in item.path)
    return tuple(flattened), renames, DiagnosticBag(diagnostics)


def _rewrite(text: str, resolutions: list[_Resolution]) -> str:
    rewritten = text
    for resolution in sorted(resolutions, key=lambda item: item.reference.start, reverse=True):
        published = flattened_name(resolution.target)
        reference = resolution.reference
        # Every flattened file sits beside SKILL.md, so a reference that already
        # resolves from the root to the published name needs no rewrite.
        if posixpath.normpath(reference.text) == published:
            continue
        rewritten = rewritten[: reference.start] + published + rewritten[reference.end :]
    return rewritten


def _recheck(skill: Skill, flattened: tuple[SkillFile, ...]) -> tuple[Diagnostic, ...]:
    """SST-VAL810: the rewritten Markdown must resolve against the flattened tree."""
    paths = frozenset(item.path for item in flattened)
    authored_paths = frozenset(item.path for item in skill.files)
    authored = {flattened_name(item.path): item for item in skill.files}
    diagnostics: list[Diagnostic] = []
    for item in flattened:
        text = item.text
        source = authored[item.path]
        if not item.is_markdown or text is None:
            continue
        resolvable = {
            reference.text
            for reference in scan_references(source.text or "", source.path)
            if not _skip(reference) and _resolve(reference, authored_paths) is not None
        }
        for reference in scan_references(text, item.path):
            if reference.text in resolvable and _resolve(reference, paths) is None:
                diagnostics.append(
                    D(
                        "SST-VAL810",
                        origin=_origin(skill, source.path, reference.line, reference.col),
                        subject=_subject(skill),
                        artifact=skill.name,
                        path=reference.text,
                    )
                )
    return tuple(diagnostics)


def _script_diagnostics(skill: Skill) -> tuple[Diagnostic, ...]:
    """SST-VAL815 and the flattening hazard, both for scripts, which are run and never rewritten.

    A literal credential is reported in any text file. An absolute or home-directory
    path is reported only in a script: in Markdown it is an instruction to the agent
    ("save the export to ~/Downloads"), which is how a skill is meant to be written.
    """
    nested = tuple(item.path for item in skill.files if "/" in item.path)
    diagnostics: list[Diagnostic] = []
    for item in skill.files:
        text = item.text
        if text is None:
            continue
        subject = _subject(skill)
        for match in () if item.is_markdown else _ABSOLUTE.finditer(text):
            line = text.count("\n", 0, match.start()) + 1
            diagnostics.append(
                D(
                    "SST-VAL815",
                    origin=_origin(skill, item.path, line),
                    subject=subject,
                    artifact=skill.name,
                    path=item.path,
                    detail=f"the absolute path '{match.group(1)}'",
                )
            )
        for match in _SECRET.finditer(text):
            line = text.count("\n", 0, match.start()) + 1
            diagnostics.append(
                D(
                    "SST-VAL815",
                    origin=_origin(skill, item.path, line),
                    subject=subject,
                    artifact=skill.name,
                    path=item.path,
                    detail="a credential literal",
                )
            )
        if item.is_markdown:
            continue
        for path in nested:
            relative = posixpath.relpath(path, posixpath.dirname(item.path) or ".")
            if path in text or (relative != path and relative in text):
                diagnostics.append(
                    D(
                        "SST-VAL833",
                        origin=_origin(skill, item.path),
                        subject=subject,
                        artifact=skill.name,
                        path=item.path,
                        target=path,
                    )
                )
    return tuple(diagnostics)


def _limit_diagnostics(subject: str, name: str, origin: Origin, entries: tuple[BundleEntry, ...]) -> list[Diagnostic]:
    diagnostics: list[Diagnostic] = []
    if len(entries) > SCAN_MAX_FILES:
        diagnostics.append(
            D(
                "SST-VAL834",
                origin=origin,
                subject=subject,
                artifact=name,
                detail=f"{len(entries)} files, over the limit of {SCAN_MAX_FILES}",
            )
        )
    for entry in entries:
        if entry.size > SCAN_MAX_FILE_BYTES:
            diagnostics.append(
                D(
                    "SST-VAL834",
                    origin=origin,
                    subject=subject,
                    artifact=name,
                    detail=f"'{entry.path}' is {entry.size} bytes, over the per-file limit of {SCAN_MAX_FILE_BYTES}",
                )
            )
    total = sum(entry.size for entry in entries)
    if total > SCAN_MAX_TOTAL_BYTES:
        diagnostics.append(
            D(
                "SST-VAL834",
                origin=origin,
                subject=subject,
                artifact=name,
                detail=f"{total} bytes in total, over the limit of {SCAN_MAX_TOTAL_BYTES}",
            )
        )
    elif total > BUNDLE_BUDGET_BYTES:
        diagnostics.append(
            D("SST-VAL811", origin=origin, subject=subject, artifact=name, size=total, expected=BUNDLE_BUDGET_BYTES)
        )
    return diagnostics


def build_skill_bundle(skill: Skill) -> tuple[SkillBundle | None, DiagnosticBag]:
    """The flattened `skills/<name>/...` bundle a SKILL-type version is built from."""
    files, renames, diagnostics = flatten_skill(skill)
    found: list[Diagnostic] = list(diagnostics)
    skill_md = next((item for item in skill.files if item.path == SKILL_FILE), None)
    if skill_md is not None and skill_md.size > SKILL_MD_BUDGET_BYTES:
        found.append(
            D(
                "SST-VAL812",
                origin=skill.origin,
                subject=_subject(skill),
                artifact=skill.name,
                size=skill_md.size,
                expected=SKILL_MD_BUDGET_BYTES,
            )
        )
    entries = tuple(
        sorted(
            (BundleEntry(f"skills/{skill.name}/{item.path}", item.content) for item in files),
            key=_entry_path,
        )
    )
    found.extend(_limit_diagnostics(_subject(skill), skill.name, skill.origin, entries))
    bag = DiagnosticBag(found)
    if bag.has_errors or not entries:
        return None, bag
    return SkillBundle("SKILL", skill.name, entries, renames), bag


def plugin_manifest_json(plugin: Plugin) -> str:
    document: dict[str, object] = {"name": plugin.name, "skills": "./skills/"}
    if plugin.description:
        document["description"] = plugin.description
    if plugin.owner_team:
        document["author"] = {"name": plugin.owner_team}
    return json.dumps(document, indent=2, sort_keys=True) + "\n"


def build_plugin_bundle(
    plugin: Plugin,
    members: Mapping[str, Skill],
) -> tuple[SkillBundle | None, DiagnosticBag]:
    """`.cortex-plugin/plugin.json` plus each member flattened under `skills/<member>/`."""
    diagnostics: list[Diagnostic] = []
    entries: list[BundleEntry] = [BundleEntry(PLUGIN_MANIFEST, plugin_manifest_json(plugin).encode("utf-8"))]
    renames: list[tuple[str, str]] = []
    for name in dict.fromkeys(plugin.members):
        skill = members.get(name)
        if skill is None:
            continue
        files, member_renames, member_diagnostics = flatten_skill(skill)
        if member_diagnostics.has_errors:
            diagnostics.append(
                D(
                    "SST-VAL836",
                    origin=plugin.origin,
                    subject=plugin.key,
                    artifact=plugin.name,
                    name=name,
                )
            )
            continue
        entries.extend(BundleEntry(f"skills/{name}/{item.path}", item.content) for item in files)
        renames.extend((f"{name}/{authored}", f"{name}/{published}") for authored, published in member_renames)
    ordered = tuple(sorted(entries, key=_entry_path))
    diagnostics.extend(_limit_diagnostics(plugin.key, plugin.name, plugin.origin, ordered))
    bag = DiagnosticBag(diagnostics)
    if bag.has_errors:
        return None, bag
    return SkillBundle("PLUGIN", plugin.name, ordered, tuple(renames)), bag
