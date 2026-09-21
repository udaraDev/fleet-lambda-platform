"""Committed archive manifests: absence is not evidence of zero activity."""

import hashlib
from pathlib import Path


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def build_manifest(root, expected_rows):
    import pyarrow.parquet as pq

    files = []
    for path in sorted(root.glob("dt=*/*.parquet")):
        files.append({"path": path.relative_to(root).as_posix(), "sha256": digest(path),
                      "rows": pq.ParquetFile(path).metadata.num_rows})
    if sum(item["rows"] for item in files) != expected_rows:
        raise ValueError(f"Archive row count mismatch: {root}")
    return {"version": 1, "files": files, "rows": expected_rows}


def verified_paths(data_dir, batches, report_date):
    """Verify every expected file for this day, including missing directories."""
    output = []
    for batch in batches:
        manifest = batch.get("archive_manifest")
        if manifest is None:
            raise ValueError(f"Batch {batch['batch_id']} has no archive manifest; run migration")
        if manifest.get("version") != 1 or manifest.get("rows") != batch["rows_valid"]:
            raise ValueError(f"Invalid manifest for batch {batch['batch_id']}")
        if sum(f["rows"] for f in manifest["files"]) != manifest["rows"]:
            raise ValueError(f"Invalid manifest row total for batch {batch['batch_id']}")
        root = data_dir / "raw" / str(batch["batch_id"])
        for item in manifest["files"]:
            relative = Path(item["path"])
            if relative.is_absolute() or ".." in relative.parts:
                raise ValueError("Unsafe archive manifest path")
            if relative.parts[0] != f"dt={report_date}":
                continue
            path = root / relative
            if not path.is_file() or digest(path) != item["sha256"]:
                raise ValueError(f"Missing or changed committed archive: {path}")
            output.append(str(path))
    return output
