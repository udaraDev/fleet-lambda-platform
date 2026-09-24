"""Create least-privilege PostgreSQL roles for the local platform."""

import os
import time

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import make_dsn, parse_dsn


ROLES = {
    "fleet_api": "FLEET_API_PASSWORD",
    "fleet_stream": "FLEET_STREAM_PASSWORD",
    "fleet_batch": "FLEET_BATCH_PASSWORD",
    "airflow_user": "AIRFLOW_DB_PASSWORD",
}


def required(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def set_role(cur, role, password):
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (role,))
    if not cur.fetchone():
        cur.execute(sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(role)))
    cur.execute(sql.SQL("ALTER ROLE {} WITH LOGIN PASSWORD %s NOSUPERUSER NOCREATEDB NOCREATEROLE")
                .format(sql.Identifier(role)), (password,))


def connect_with_retry(dsn):
    """Wait through PostgreSQL's one-time init/restart before changing roles."""
    for attempt in range(12):
        try:
            return psycopg2.connect(dsn, connect_timeout=5)
        except psycopg2.OperationalError:
            if attempt == 11:
                raise
            time.sleep(1)


def configure():
    admin_url = required("DATABASE_ADMIN_URL")
    passwords = {role: required(variable) for role, variable in ROLES.items()}
    admin_dsn = parse_dsn(admin_url)
    maintenance = dict(admin_dsn, dbname="postgres")
    with connect_with_retry(make_dsn(**maintenance)) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            for role, password in passwords.items():
                set_role(cur, role, password)
            cur.execute("SELECT 1 FROM pg_database WHERE datname='airflow'")
            if not cur.fetchone():
                cur.execute("CREATE DATABASE airflow OWNER airflow_user")
            else:
                cur.execute("ALTER DATABASE airflow OWNER TO airflow_user")

    with connect_with_retry(admin_url) as conn, conn.cursor() as cur:
        cur.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
        cur.execute("REVOKE ALL ON DATABASE fleet FROM PUBLIC")
        for role in ("fleet_api", "fleet_stream", "fleet_batch"):
            cur.execute(sql.SQL("GRANT CONNECT ON DATABASE fleet TO {}").format(sql.Identifier(role)))
            cur.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(sql.Identifier(role)))
        cur.execute("GRANT TEMP ON DATABASE fleet TO fleet_stream, fleet_batch")
        cur.execute("GRANT SELECT ON ALL TABLES IN SCHEMA public TO fleet_api")
        for role in ("fleet_stream", "fleet_batch"):
            cur.execute(sql.SQL("GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO {}")
                        .format(sql.Identifier(role)))
            cur.execute(sql.SQL("GRANT USAGE,SELECT,UPDATE ON ALL SEQUENCES IN SCHEMA public TO {}")
                        .format(sql.Identifier(role)))
        cur.execute("GRANT CREATE ON DATABASE fleet TO fleet_batch")
        cur.execute("ALTER DEFAULT PRIVILEGES FOR ROLE fleet IN SCHEMA public GRANT SELECT ON TABLES TO fleet_api")
        cur.execute("ALTER DEFAULT PRIVILEGES FOR ROLE fleet IN SCHEMA public GRANT SELECT,INSERT,UPDATE,DELETE ON TABLES TO fleet_stream,fleet_batch")
        cur.execute("ALTER DEFAULT PRIVILEGES FOR ROLE fleet IN SCHEMA public GRANT USAGE,SELECT,UPDATE ON SEQUENCES TO fleet_stream,fleet_batch")

    airflow_dsn = dict(admin_dsn, dbname="airflow")
    with connect_with_retry(make_dsn(**airflow_dsn)) as conn, conn.cursor() as cur:
        # Existing demo volumes contain metadata created before the dedicated role.
        # Give Airflow full access only to its own metadata schema, never fleet data.
        cur.execute("GRANT ALL ON SCHEMA public TO airflow_user")
        cur.execute("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow_user")
        cur.execute("GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow_user")
        cur.execute("ALTER DEFAULT PRIVILEGES FOR ROLE fleet IN SCHEMA public GRANT ALL ON TABLES TO airflow_user")
        cur.execute("ALTER DEFAULT PRIVILEGES FOR ROLE fleet IN SCHEMA public GRANT ALL ON SEQUENCES TO airflow_user")


if __name__ == "__main__":
    configure()
    print("PostgreSQL application roles configured.")
