"""Apply additive schema and baseline legacy archives while writers are stopped."""

from pathlib import Path
from psycopg2.extras import Json, execute_values
import pyarrow.parquet as pq
from streaming.sink import fingerprint

from common.archive import build_manifest
from common.db import connection, fetch_all
from common.settings import DATA_DIR


def migrate():
    with connection() as conn, conn.cursor() as cur:
        cur.execute((Path(__file__).resolve().parents[1] / "sql" / "002_integrity.sql").read_text())
        cur.execute((Path(__file__).resolve().parents[1] / "sql" / "003_completion.sql").read_text())
        cur.execute((Path(__file__).resolve().parents[1] / "sql" / "004_plan_tables.sql").read_text(encoding="utf-8-sig"))
    for batch in fetch_all("SELECT batch_id,rows_valid FROM pipeline_batches WHERE archive_manifest IS NULL ORDER BY batch_id"):
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
    print("Integrity migration complete; historical archives baselined against committed row counts.")


if __name__ == "__main__":
    migrate()
