"""Apply additive schema and restore/baseline legacy archives while writers are stopped."""

from pathlib import Path
from psycopg2.extras import Json, execute_values
import pyarrow.parquet as pq
from streaming.sink import fingerprint

from common.archive import build_manifest, manifest_fingerprint
from common.db import connection, fetch_all
from common.settings import DATA_DIR


def _restore_legacy_files(batch_id, manifest):
    """Copy a pre-MinIO archive into object storage without changing its manifest."""
    import hashlib
    from common.archive import _get_fs
    from common.settings import MINIO_BUCKET

    fs = _get_fs()
    legacy_root = DATA_DIR / "raw" / str(batch_id)
    for item in manifest.get("files", []):
        destination = f"{MINIO_BUCKET}/{batch_id}/{item['path']}"
        if fs.exists(destination):
            remote_hash = hashlib.sha256()
            with fs.open(destination, "rb") as remote:
                for block in iter(lambda: remote.read(1024 * 1024), b""):
                    remote_hash.update(block)
            if remote_hash.hexdigest() == item["sha256"]:
                continue
        source = legacy_root / item["path"]
        if not source.is_file():
            raise ValueError(
                f"Batch {batch_id} is absent from MinIO and has no recoverable legacy file: {source}")
        if hashlib.sha256(source.read_bytes()).hexdigest() != item["sha256"]:
            raise ValueError(f"Legacy archive checksum mismatch: {source}")
        fs.makedirs(destination.rsplit("/", 1)[0], exist_ok=True)
        with source.open("rb") as src, fs.open(destination, "wb") as dst:
            for block in iter(lambda: src.read(1024 * 1024), b""):
                dst.write(block)


def migrate():
    with connection() as conn, conn.cursor() as cur:
        cur.execute((Path(__file__).resolve().parents[1] / "sql" / "002_integrity.sql").read_text())
        cur.execute((Path(__file__).resolve().parents[1] / "sql" / "003_completion.sql").read_text())
        cur.execute((Path(__file__).resolve().parents[1] / "sql" / "004_plan_tables.sql").read_text(encoding="utf-8-sig"))
        cur.execute((Path(__file__).resolve().parents[1] / "sql" / "005_retractable_metrics.sql").read_text(encoding="utf-8-sig"))
        cur.execute((Path(__file__).resolve().parents[1] / "sql" / "006_archive_offsets.sql").read_text(encoding="utf-8-sig"))
    batches = fetch_all("SELECT batch_id,rows_valid,archive_manifest FROM pipeline_batches ORDER BY batch_id")
    for index, batch in enumerate(batches, 1):
        manifest = batch.get("archive_manifest")
        if manifest:
            _restore_legacy_files(batch["batch_id"], manifest)
            with connection() as conn, conn.cursor() as cur:
                cur.execute("""INSERT INTO dq_archive_verification(batch_id,manifest_sha256)
                    VALUES (%s,%s) ON CONFLICT(batch_id) DO UPDATE SET
                    manifest_sha256=EXCLUDED.manifest_sha256,verified_at=now()""",
                    (batch["batch_id"], manifest_fingerprint(manifest)))
            if index % 500 == 0:
                print(f"Verified/restored {index}/{len(batches)} archive batches", flush=True)
            continue
        manifest = build_manifest(batch["batch_id"], batch["rows_valid"])
        with connection() as conn, conn.cursor() as cur:
            from common.archive import _get_fs
            from common.settings import MINIO_BUCKET
            fs = _get_fs()
            for item in manifest["files"]:
                path = f"{MINIO_BUCKET}/{batch['batch_id']}/{item['path']}"
                with fs.open(path, "rb") as f:
                    for chunk in pq.ParquetFile(f).iter_batches(batch_size=1000):
                        # Baseline event identities, not mutable serving state, on upgrade.
                        rows = {e["event_id"]: e for e in chunk.to_pylist()}
                        execute_values(cur, """INSERT INTO stream_event_keys
                            (event_id,fingerprint,event_ts,vehicle_id,trip_id,batch_id) VALUES %s
                            ON CONFLICT(event_id) DO NOTHING""", [
                            (e["event_id"], fingerprint(e), e["event_ts"], e["vehicle_id"],
                             e["trip_id"], batch["batch_id"])
                            for e in rows.values()])
            cur.execute("UPDATE pipeline_batches SET archive_manifest=%s WHERE batch_id=%s AND archive_manifest IS NULL",
                        (Json(manifest), batch["batch_id"]))
            cur.execute("""INSERT INTO dq_archive_verification(batch_id,manifest_sha256)
                VALUES (%s,%s) ON CONFLICT(batch_id) DO UPDATE SET
                manifest_sha256=EXCLUDED.manifest_sha256,verified_at=now()""",
                (batch["batch_id"], manifest_fingerprint(manifest)))
    print("Integrity migration complete; legacy archives restored/baselined against committed row counts.")


if __name__ == "__main__":
    migrate()
