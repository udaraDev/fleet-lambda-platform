"""Environment configuration shared by containers and local commands."""

import os
from pathlib import Path

DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://fleet:fleet_dev@postgres:5432/fleet")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = "trip-events"
SIM_DAY_SECONDS = int(os.getenv("SIM_DAY_SECONDS", "300"))
EVENT_INTERVAL_SECONDS = float(os.getenv("EVENT_INTERVAL_SECONDS", "2"))
VEHICLE_COUNT = int(os.getenv("VEHICLE_COUNT", "12"))
NO_DATA_SECONDS = int(os.getenv("NO_DATA_SECONDS", "120"))
DQ_FAILURE_THRESHOLD = float(os.getenv("DQ_FAILURE_THRESHOLD", "0.05"))
MAX_CHANGED_DATES_PER_RUN = int(os.getenv("MAX_CHANGED_DATES_PER_RUN", "5"))

if SIM_DAY_SECONDS <= 0 or EVENT_INTERVAL_SECONDS <= 0 or VEHICLE_COUNT <= 0:
    raise ValueError("Clock, interval and fleet size must be positive")
if not 0 <= DQ_FAILURE_THRESHOLD <= 1:
    raise ValueError("DQ_FAILURE_THRESHOLD must be between 0 and 1")
if MAX_CHANGED_DATES_PER_RUN <= 0:
    raise ValueError("MAX_CHANGED_DATES_PER_RUN must be positive")


def vehicles():
    return {f"V-{i:03d}" for i in range(1, VEHICLE_COUNT + 1)}
