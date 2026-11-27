"""Generate a local API key file without printing the secret."""

import argparse
import secrets
from pathlib import Path


def generate_api_key(path: Path) -> Path:
    """Create a new key with restrictive permissions and refuse overwrites."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise FileExistsError(f"refusing to overwrite existing secret: {path}")
    path.write_text(secrets.token_urlsafe(48) + "\n", encoding="utf-8")
    path.chmod(0o600)
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path(".secrets/api_key"))
    args = parser.parse_args()
    path = generate_api_key(args.output)
    print(f"API key written to {path}; keep this file out of version control")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
