"""Environment configuration shared by containers and local commands.

Primary source: config/settings.yaml (committed, no magic numbers in code).
Override any value with the corresponding environment variable (upper-cased).
Environment variables always win over YAML values so Docker Compose .env works.
"""

import os
from pathlib import Path

# Locate the YAML relative to this file so it works from any working directory.
_YAML_PATH = Path(__file__).resolve().parents[1] / "config" / "settings.yaml"


def _load_yaml():
    """Return the parsed YAML dict, or {} if PyYAML is not installed."""
    try:
        import yaml
        with open(_YAML_PATH) as fh:
            return yaml.safe_load(fh) or {}
    except Exception:
        return {}


_cfg = _load_yaml()
_sim = _cfg.get("simulation", {})
_kfk = _cfg.get("kafka", {})
_dq = _cfg.get("data_quality", {})
_pip = _cfg.get("pipeline", {})
_srv = _cfg.get("serving", {})
_db = _cfg.get("database", {})
_sto = _cfg.get("storage", {})

DATA_DIR = Path(os.getenv("DATA_DIR", _sto.get("data_dir", "/data")))
DATABASE_URL = os.getenv("DATABASE_URL", _db.get("url", "postgresql://fleet:fleet_dev@postgres:5432/fleet"))
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", _kfk.get("bootstrap_servers", "kafka:9092"))
TOPIC = os.getenv("KAFKA_TOPIC", _kfk.get("topic", "trip-events"))
DEAD_LETTER_TOPIC = os.getenv("KAFKA_DEAD_LETTER_TOPIC", _kfk.get("dead_letter_topic", "trip-events-dead-letter"))
SIM_DAY_SECONDS = int(os.getenv("SIM_DAY_SECONDS", _sim.get("sim_day_seconds", 300)))
EVENT_INTERVAL_SECONDS = float(os.getenv("EVENT_INTERVAL_SECONDS", _sim.get("event_interval_seconds", 2.0)))
VEHICLE_COUNT = int(os.getenv("VEHICLE_COUNT", _sim.get("vehicle_count", 12)))
NO_DATA_SECONDS = int(os.getenv("NO_DATA_SECONDS", _pip.get("no_data_seconds", 120)))
DQ_FAILURE_THRESHOLD = float(os.getenv("DQ_FAILURE_THRESHOLD", _dq.get("dq_failure_threshold", 0.05)))
MAX_CHANGED_DATES_PER_RUN = int(os.getenv("MAX_CHANGED_DATES_PER_RUN", _pip.get("max_changed_dates_per_run", 5)))
IDLE_ALERT_MINUTES = int(os.getenv("IDLE_ALERT_MINUTES", _srv.get("idle_alert_minutes", 5)))

_minio = _cfg.get("minio", {})
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", _minio.get("endpoint", "http://localhost:9000"))
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", _minio.get("access_key", "minioadmin"))
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", _minio.get("secret_key", "minioadmin"))
MINIO_BUCKET = os.getenv("MINIO_BUCKET", _minio.get("bucket", "fleet-raw"))

if SIM_DAY_SECONDS <= 0 or EVENT_INTERVAL_SECONDS <= 0 or VEHICLE_COUNT <= 0:
    raise ValueError("Clock, interval and fleet size must be positive")
if not 0 <= DQ_FAILURE_THRESHOLD <= 1:
    raise ValueError("DQ_FAILURE_THRESHOLD must be between 0 and 1")
if MAX_CHANGED_DATES_PER_RUN <= 0:
    raise ValueError("MAX_CHANGED_DATES_PER_RUN must be positive")


def vehicles():
    return {f"V-{i:03d}" for i in range(1, VEHICLE_COUNT + 1)}
