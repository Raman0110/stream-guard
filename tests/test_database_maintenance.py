from pathlib import Path

import pytest

from scripts import database


def test_default_backup_path_is_custom_archive():
    path = database.default_backup_path()
    assert path.parent == Path("backups")
    assert path.suffix == ".dump"


def test_verify_rejects_missing_or_empty_backup(tmp_path):
    with pytest.raises(ValueError, match="missing or empty"):
        database.verify_backup(tmp_path / "missing.dump")
    empty = tmp_path / "empty.dump"
    empty.touch()
    with pytest.raises(ValueError, match="missing or empty"):
        database.verify_backup(empty)


def test_create_backup_refuses_to_overwrite(tmp_path):
    existing = tmp_path / "existing.dump"
    existing.write_bytes(b"keep")

    with pytest.raises(FileExistsError, match="refusing to overwrite"):
        database.create_backup(existing)


def test_restore_requires_exact_database_confirmation(tmp_path, monkeypatch):
    backup = tmp_path / "valid.dump"
    backup.write_bytes(b"archive")
    monkeypatch.setattr(database, "verify_backup", lambda path: None)
    monkeypatch.setattr(database, "database_name", lambda: "streamguard")

    with pytest.raises(ValueError, match="exactly match"):
        database.restore_backup(backup, "wrong")
