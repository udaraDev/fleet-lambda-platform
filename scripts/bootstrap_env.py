"""Create a local .env with random, URL-safe demo credentials."""

import argparse
import secrets
from pathlib import Path


STATIC = {
    "API_PORT": "8001",
    "AIRFLOW_PORT": "8080",
    "MINIO_API_PORT": "9000",
    "MINIO_CONSOLE_PORT": "9001",
    "PROMETHEUS_PORT": "9090",
    "GRAFANA_PORT": "3000",
    "SIM_DAY_SECONDS": "300",
    "EVENT_INTERVAL_SECONDS": "2",
    "VEHICLE_COUNT": "12",
    "NO_DATA_SECONDS": "120",
    "DQ_FAILURE_THRESHOLD": "0.05",
    "MAX_CHANGED_DATES_PER_RUN": "5",
    "MINIO_ROOT_USER": "fleet-root-" + secrets.token_hex(4),
    "MINIO_APP_ACCESS_KEY": "fleet-app-" + secrets.token_hex(6),
}

SECRETS = (
    "POSTGRES_PASSWORD",
    "FLEET_API_PASSWORD",
    "FLEET_STREAM_PASSWORD",
    "FLEET_BATCH_PASSWORD",
    "AIRFLOW_DB_PASSWORD",
    "AIRFLOW_ADMIN_PASSWORD",
    "AIRFLOW_SECRET_KEY",
    "MINIO_ROOT_PASSWORD",
    "MINIO_APP_SECRET_KEY",
    "GRAFANA_PASSWORD",
)


def generate(path, force=False):
    path = Path(path)
    if path.exists() and not force:
        raise FileExistsError(f"{path} already exists; use --force only to rotate every local credential")
    values = dict(STATIC)
    values.update({name: secrets.token_urlsafe(32) for name in SECRETS})
    path.write_text(
        "# Generated local credentials. Do not commit this file.\n" +
        "\n".join(f"{name}={value}" for name, value in values.items()) + "\n",
        encoding="utf-8",
    )
    return path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", default=".env")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    print(generate(args.path, args.force))


if __name__ == "__main__":
    main()
