"""Versioned report exports and byte integrity, independent of the database."""

import hashlib
from pathlib import Path

ALGORITHM_VERSION = 4
REPORT_FORMATS = ("json", "csv", "html", "parquet")


def export_matches(path, expected_digest):
    if not expected_digest:
        return False
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest() == expected_digest
    except OSError:
        return False


def build_export_manifest(paths, root):
    """Build a digest manifest for every required report representation."""
    root = Path(root)
    by_format = {Path(path).suffix.lstrip("."): Path(path) for path in paths}
    missing = set(REPORT_FORMATS) - set(by_format)
    if missing:
        raise ValueError(f"Missing report formats: {sorted(missing)}")
    files = {}
    for report_format in REPORT_FORMATS:
        path = by_format[report_format]
        files[report_format] = {
            "path": path.relative_to(root).as_posix(),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
    return {"version": 1, "files": files}


def manifest_matches(root, manifest):
    """Require every promised report file to exist with its committed digest."""
    if not manifest or manifest.get("version") != 1:
        return False
    files = manifest.get("files") or {}
    if set(files) != set(REPORT_FORMATS):
        return False
    root = Path(root)
    for report_format in REPORT_FORMATS:
        item = files[report_format]
        relative = Path(item.get("path", ""))
        if (not relative.parts or relative.is_absolute() or ".." in relative.parts
                or relative.suffix != f".{report_format}"):
            return False
        if not export_matches(root / relative, item.get("sha256")):
            return False
    return True
