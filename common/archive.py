"""Committed archive manifests: absence is not evidence of zero activity.

Updated to use MinIO (S3) instead of local filesystem.
"""

import hashlib
from pathlib import Path
import s3fs
import pyarrow.parquet as pq

from common.settings import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET

def _get_fs():
    return s3fs.S3FileSystem(
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
    )

def digest(path, fs=None):
    fs = fs or _get_fs()
    value = hashlib.sha256()
    with fs.open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def build_manifest(batch_id, expected_rows):
    fs = _get_fs()
    root = f"{MINIO_BUCKET}/{batch_id}"
    files = []
    # s3fs glob returns full paths without protocol
    for path in sorted(fs.glob(f"{root}/dt=*/*.parquet")):
        # Get relative path by replacing the prefix
        relative = path.replace(f"{root}/", "")
        # Read metadata for rows
        with fs.open(path, "rb") as f:
            rows = pq.ParquetFile(f).metadata.num_rows
        files.append({"path": relative, "sha256": digest(path, fs), "rows": rows})
        
    if sum(item["rows"] for item in files) != expected_rows:
        raise ValueError(f"Archive row count mismatch: {root}")
    return {"version": 1, "files": files, "rows": expected_rows}


def verified_paths(batch_ids, report_date):
    """Verify every expected file for this day, including missing directories.
    Returns s3a:// paths so Spark can read them natively.
    """
    output = []
    fs = _get_fs()
    for batch in batch_ids:
        manifest = batch.get("archive_manifest")
        if manifest is None:
            raise ValueError(f"Batch {batch['batch_id']} has no archive manifest; run migration")
        if manifest.get("version") != 1 or manifest.get("rows") != batch["rows_valid"]:
            raise ValueError(f"Invalid manifest for batch {batch['batch_id']}")
        if sum(f["rows"] for f in manifest["files"]) != manifest["rows"]:
            raise ValueError(f"Invalid manifest row total for batch {batch['batch_id']}")
            
        root = f"{MINIO_BUCKET}/{batch['batch_id']}"
        for item in manifest["files"]:
            relative = Path(item["path"])
            if relative.is_absolute() or ".." in relative.parts:
                raise ValueError("Unsafe archive manifest path")
            if relative.parts[0] != f"dt={report_date}":
                continue
            path = f"{root}/{relative.as_posix()}"
            if not fs.exists(path) or digest(path, fs) != item["sha256"]:
                raise ValueError(f"Missing or changed committed archive: {path}")
            output.append(f"s3a://{path}")
    return output
