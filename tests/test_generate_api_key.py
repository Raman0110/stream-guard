from pathlib import Path

import pytest

from scripts.generate_api_key import generate_api_key


def test_generate_api_key_creates_long_secret_without_overwrite(tmp_path: Path):
    path = tmp_path / "nested" / "api_key"

    assert generate_api_key(path) == path
    key = path.read_text(encoding="utf-8").strip()
    assert len(key) >= 64

    with pytest.raises(FileExistsError, match="refusing to overwrite"):
        generate_api_key(path)
