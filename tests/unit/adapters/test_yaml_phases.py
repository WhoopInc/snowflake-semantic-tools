"""Discover/load phase contracts for semantic-layer YAML."""

from __future__ import annotations

from pathlib import Path

from snowflake_semantic_tools.adapters.yaml.documents import discover_yaml, load_documents
from snowflake_semantic_tools.adapters.yaml.loader import _parse_yaml_bytes


def test_discovery_sorts_paths_and_does_not_open_files(tmp_path: Path) -> None:
    root = tmp_path / "semantic_models"
    (root / "metrics").mkdir(parents=True)
    (root / "filters").mkdir()
    late = root / "metrics" / "z.yml"
    early = root / "filters" / "a.yml"
    late.write_text("snowflake_metrics: []\n", encoding="utf-8")
    early.write_text("snowflake_filters: []\n", encoding="utf-8")
    files = discover_yaml(tmp_path, "semantic_models")
    assert [item.path for item in files.files] == [
        "semantic_models/filters/a.yml",
        "semantic_models/metrics/z.yml",
    ]


def test_load_calls_the_document_reader_once_per_file(tmp_path: Path) -> None:
    root = tmp_path / "semantic_models" / "metrics"
    root.mkdir(parents=True)
    (root / "one.yml").write_text("snowflake_metrics: []\n", encoding="utf-8")
    (root / "two.yml").write_text("snowflake_metrics: []\n", encoding="utf-8")
    calls: list[Path] = []

    def counted(raw: bytes, path: str):
        calls.append(Path(path))
        return _parse_yaml_bytes(raw, path)

    documents = load_documents(discover_yaml(tmp_path, "semantic_models"), counted)
    assert len(calls) == 2
    assert len(documents.documents) == 2
    assert documents.failed == ()
    assert all(len(document.checksum) == 64 for document in documents.documents)


def test_root_key_owns_content_even_under_a_misleading_directory(tmp_path: Path) -> None:
    root = tmp_path / "semantic_models" / "metrics"
    root.mkdir(parents=True)
    path = root / "actually_filters.yml"
    path.write_text("snowflake_filters: []\n", encoding="utf-8")
    documents = load_documents(discover_yaml(tmp_path, "semantic_models"), _parse_yaml_bytes)
    assert documents.documents[0].hint_root == "metrics"
    assert documents.under(tmp_path / "semantic_models", "snowflake_filters") == documents.documents
    assert documents.under(tmp_path / "semantic_models", "snowflake_metrics") == ()


def test_load_reads_each_file_once(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "semantic_models" / "metrics"
    root.mkdir(parents=True)
    path = root / "one.yml"
    path.write_text("snowflake_metrics: []\n", encoding="utf-8")
    original = Path.read_bytes
    reads = 0

    def counted(self: Path) -> bytes:
        nonlocal reads
        reads += 1
        return original(self)

    monkeypatch.setattr(Path, "read_bytes", counted)
    load_documents(discover_yaml(tmp_path, "semantic_models"), _parse_yaml_bytes)
    assert reads == 1
