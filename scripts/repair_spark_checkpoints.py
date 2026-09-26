"""Quarantine incomplete trailing Spark checkpoint metadata safely.

Spark can leave zero-byte offset or commit files when a container is interrupted
during its atomic metadata-log write.  A later restart treats the highest file as
authoritative and fails before it can replay the prior committed micro-batch.

This command only accepts a contiguous invalid suffix.  It refuses an invalid
file followed by a valid file because that indicates an internal history hole,
not an interrupted tail.  Files are moved to a timestamped backup under the
checkpoint root, so the repair is reversible.
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from common.settings import DATA_DIR


def valid_metadata_log(path: Path) -> bool:
    """Return whether *path* is a complete Spark v1 metadata-log record."""
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError):
        return False
    if len(lines) < 2 or lines[0] != "v1":
        return False
    try:
        for line in lines[1:]:
            json.loads(line)
    except json.JSONDecodeError:
        return False
    return True


def invalid_suffix(log_dir: Path) -> list[Path]:
    """Return a corrupt numeric suffix, refusing corruption inside history."""
    records = sorted(
        (path for path in log_dir.iterdir() if path.name.isdigit()),
        key=lambda path: int(path.name),
    )
    invalid = [not valid_metadata_log(path) for path in records]
    if not any(invalid):
        return []
    first = invalid.index(True)
    if not all(invalid[first:]):
        raise RuntimeError(f"Internal checkpoint corruption in {log_dir}; refusing automatic repair")
    return records[first:]


def planned_repairs(checkpoint_root: Path) -> list[Path]:
    repairs: list[Path] = []
    if not checkpoint_root.exists():
        return repairs
    for query in sorted(path for path in checkpoint_root.iterdir() if path.is_dir()):
        if query.name == ".repair-backup":
            continue
        for name in ("offsets", "commits"):
            log_dir = query / name
            if log_dir.is_dir():
                repairs.extend(invalid_suffix(log_dir))
    return repairs


def repair(checkpoint_root: Path, apply: bool = False) -> dict:
    repairs = planned_repairs(checkpoint_root)
    result = {
        "checkpoint_root": str(checkpoint_root),
        "mode": "apply" if apply else "dry-run",
        "files": [str(path.relative_to(checkpoint_root)) for path in repairs],
    }
    if not apply or not repairs:
        return result

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = checkpoint_root / ".repair-backup" / stamp
    for path in repairs:
        relative = path.relative_to(checkpoint_root)
        destination = backup / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        os.replace(path, destination)
        checksum = path.parent / f".{path.name}.crc"
        if checksum.exists():
            checksum_destination = backup / checksum.relative_to(checkpoint_root)
            checksum_destination.parent.mkdir(parents=True, exist_ok=True)
            os.replace(checksum, checksum_destination)
    result["backup"] = str(backup)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="move the invalid suffix into a backup")
    parser.add_argument(
        "--checkpoint-root",
        type=Path,
        default=DATA_DIR / "checkpoints",
        help="checkpoint directory (default: DATA_DIR/checkpoints)",
    )
    args = parser.parse_args()
    print(json.dumps(repair(args.checkpoint_root, args.apply), indent=2))


if __name__ == "__main__":
    main()
