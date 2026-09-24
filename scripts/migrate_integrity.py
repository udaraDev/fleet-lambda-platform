"""Apply additive schema and upgrade legacy archives/reports while writers are stopped."""

import csv
import io
import json
from pathlib import Path
from psycopg2.extras import Json, execute_values
import pyarrow as pa
import pyarrow.parquet as pq
from streaming.sink import fingerprint

from common.archive import build_manifest, manifest_fingerprint
from common.db import connection, fetch_all
from common.publication import build_export_manifest
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


def _upgrade_report_exports():
    """Create missing legacy representations and commit a four-file digest manifest."""
    report_dir = DATA_DIR / "reports"
    schema = pa.schema([
        pa.field("dt", pa.string()), pa.field("vehicle_id", pa.string()),
        pa.field("trips", pa.int32()), pa.field("revenue_cents", pa.int64()),
        pa.field("fuel_cents", pa.int64()), pa.field("maintenance_cents", pa.int64()),
        pa.field("profit_cents", pa.int64()), pa.field("margin_pct", pa.float64()),
        pa.field("is_unprofitable", pa.bool_()),
        pa.field("reconciliation_status", pa.string()), pa.field("run_id", pa.string()),
    ])
    reports = fetch_all("""SELECT dt,run_id FROM daily_report_status
                         WHERE export_status='published' AND export_manifest IS NULL ORDER BY dt""")
    for index, report in enumerate(reports, 1):
        report_date = report["dt"].isoformat()
        run_id = str(report["run_id"])
        json_path = report_dir / f"profitability_{report_date}.json"
        payload = json.loads(json_path.read_text(encoding="utf-8"))
        if payload.get("date") != report_date or str(payload.get("run_id")) != run_id:
            raise ValueError(f"Legacy report metadata mismatch: {json_path}")
        output = payload.get("vehicles") or []

        csv_path = report_dir / f"profitability_{report_date}.csv"
        if not csv_path.is_file():
            buffer = io.StringIO()
            if output:
                writer = csv.DictWriter(buffer, fieldnames=list(output[0]))
                writer.writeheader()
                writer.writerows(output)
            temporary = csv_path.with_suffix(".tmp")
            temporary.write_text(buffer.getvalue(), encoding="utf-8")
            temporary.replace(csv_path)

        html_path = report_dir / f"profitability_{report_date}.html"
        if not html_path.is_file():
            rows = "".join(
                f"<tr><td>{r['vehicle_id']}</td><td>{r['trips']}</td>"
                f"<td>{r['revenue_cents']}</td><td>{r.get('profit_cents','')}</td>"
                f"<td>{'Yes' if r.get('is_unprofitable') else 'No'}</td>"
                f"<td>{r['reconciliation_status']}</td></tr>" for r in output)
            temporary = html_path.with_suffix(".tmp")
            temporary.write_text(
                f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>Fleet Profitability {report_date}</title></head><body>
<h1>Fleet Profitability — {report_date}</h1>
<p>Currency: LKR cents &nbsp;|&nbsp; Algorithm version: {payload.get('algorithm_version')} &nbsp;|&nbsp; Run: {run_id}</p>
<table border='1' cellpadding='4'>
<tr><th>Vehicle</th><th>Trips</th><th>Revenue (c)</th><th>Profit (c)</th><th>Unprofitable</th><th>Status</th></tr>
{rows}</table></body></html>""", encoding="utf-8")
            temporary.replace(html_path)

        parquet_path = report_dir / f"profitability_{report_date}.parquet"
        if not parquet_path.is_file():
            rows = [{**r, "dt": report_date, "run_id": run_id} for r in output]
            arrays = {f.name: [r.get(f.name) for r in rows] for f in schema}
            table = pa.table({f.name: pa.array(arrays[f.name], type=f.type) for f in schema})
            temporary = parquet_path.with_suffix(".tmp.parquet")
            pq.write_table(table, str(temporary), compression="snappy")
            temporary.replace(parquet_path)

        manifest = build_export_manifest(
            [json_path, csv_path, html_path, parquet_path], report_dir)
        with connection() as conn, conn.cursor() as cur:
            cur.execute("""UPDATE daily_report_status SET export_manifest=%s
                           WHERE dt=%s AND run_id=%s AND export_status='published'""",
                        (Json(manifest), report["dt"], report["run_id"]))
        if index % 100 == 0:
            print(f"Upgraded {index}/{len(reports)} report exports", flush=True)


def migrate():
    with connection() as conn, conn.cursor() as cur:
        cur.execute((Path(__file__).resolve().parents[1] / "sql" / "002_integrity.sql").read_text())
        cur.execute((Path(__file__).resolve().parents[1] / "sql" / "003_completion.sql").read_text())
        cur.execute((Path(__file__).resolve().parents[1] / "sql" / "004_plan_tables.sql").read_text(encoding="utf-8-sig"))
        cur.execute((Path(__file__).resolve().parents[1] / "sql" / "005_retractable_metrics.sql").read_text(encoding="utf-8-sig"))
        cur.execute((Path(__file__).resolve().parents[1] / "sql" / "006_archive_offsets.sql").read_text(encoding="utf-8-sig"))
        cur.execute((Path(__file__).resolve().parents[1] / "sql" / "007_hardening.sql").read_text(encoding="utf-8-sig"))
    batches = fetch_all("""SELECT p.batch_id,p.rows_valid,p.archive_manifest,
                            d.manifest_sha256 AS verified_manifest_sha256
                            FROM pipeline_batches p
                            LEFT JOIN dq_archive_verification d USING (batch_id)
                            ORDER BY p.batch_id""")
    for index, batch in enumerate(batches, 1):
        manifest = batch.get("archive_manifest")
        if manifest:
            if batch.get("verified_manifest_sha256") == manifest_fingerprint(manifest):
                continue
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
    _upgrade_report_exports()
    print("Integrity migration complete; legacy archives and report exports upgraded.")


if __name__ == "__main__":
    migrate()
