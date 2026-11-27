"""Create, verify, and explicitly restore PostgreSQL custom-format backups."""

import argparse
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import BinaryIO

POSTGRES_SERVICE = ("docker", "compose", "exec", "-T", "postgres")
BACKUP_COMMAND = (
    *POSTGRES_SERVICE,
    "sh",
    "-c",
    'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" --format=custom --no-owner --no-acl',
)
VERIFY_COMMAND = (*POSTGRES_SERVICE, "pg_restore", "--list")
RESTORE_COMMAND = (
    *POSTGRES_SERVICE,
    "sh",
    "-c",
    'pg_restore -U "$POSTGRES_USER" -d "$POSTGRES_DB" --clean --if-exists '
    "--no-owner --no-acl --exit-on-error --single-transaction",
)
DATABASE_NAME_COMMAND = (
    *POSTGRES_SERVICE,
    "sh",
    "-c",
    'printf %s "$POSTGRES_DB"',
)


def _run(
    command: tuple[str, ...],
    *,
    stdin: BinaryIO | None = None,
    stdout: BinaryIO | int | None = None,
) -> subprocess.CompletedProcess[bytes]:
    try:
        return subprocess.run(
            command,
            stdin=stdin,
            stdout=stdout,
            stderr=subprocess.PIPE,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        detail = exc.stderr.decode(errors="replace").strip()
        raise RuntimeError(detail or f"command failed with exit code {exc.returncode}") from exc


def verify_backup(path: Path) -> None:
    """Ask pg_restore to parse the archive table of contents."""
    if not path.is_file() or path.stat().st_size == 0:
        raise ValueError(f"backup is missing or empty: {path}")
    with path.open("rb") as source:
        _run(VERIFY_COMMAND, stdin=source, stdout=subprocess.DEVNULL)


def create_backup(output: Path) -> Path:
    """Write and verify a backup before atomically exposing the final file."""
    output.parent.mkdir(parents=True, exist_ok=True)
    partial = output.with_suffix(output.suffix + ".partial")
    if output.exists() or partial.exists():
        raise FileExistsError(f"refusing to overwrite backup path: {output}")
    try:
        with partial.open("wb") as destination:
            _run(BACKUP_COMMAND, stdout=destination)
        verify_backup(partial)
        partial.replace(output)
    finally:
        partial.unlink(missing_ok=True)
    return output


def database_name() -> str:
    result = _run(DATABASE_NAME_COMMAND, stdout=subprocess.PIPE)
    return result.stdout.decode().strip()


def restore_backup(path: Path, confirmation: str) -> None:
    """Replace the current schema from a verified backup after exact-name confirmation."""
    verify_backup(path)
    target = database_name()
    if confirmation != target:
        raise ValueError(f"restore confirmation must exactly match database name {target!r}")
    with path.open("rb") as source:
        _run(RESTORE_COMMAND, stdin=source)


def default_backup_path() -> Path:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return Path("backups") / f"streamguard-{stamp}.dump"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    backup = commands.add_parser("backup", help="create and verify a new backup")
    backup.add_argument("--output", type=Path, default=None)

    verify = commands.add_parser("verify", help="verify an existing backup archive")
    verify.add_argument("path", type=Path)

    restore = commands.add_parser("restore", help="replace the current schema from a backup")
    restore.add_argument("path", type=Path)
    restore.add_argument("--confirm-database", required=True)

    args = parser.parse_args()
    if args.command == "backup":
        path = create_backup(args.output or default_backup_path())
        print(f"Verified backup written to {path} ({path.stat().st_size} bytes)")
    elif args.command == "verify":
        verify_backup(args.path)
        print(f"Backup is readable: {args.path}")
    else:
        restore_backup(args.path, args.confirm_database)
        print(f"Database restored from {args.path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
