from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor

from common.settings import DATABASE_URL, SIM_DAY_SECONDS


@contextmanager
def connection():
    conn = psycopg2.connect(DATABASE_URL, connect_timeout=5, options='-c timezone=UTC')
    try:
        with conn:
            yield conn
    finally:
        conn.close()


def clock_start():
    with connection() as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO simulation_clock (id, day_seconds) VALUES (1, %s) ON CONFLICT DO NOTHING",
                    (SIM_DAY_SECONDS,))
        cur.execute("SELECT started_at, day_seconds FROM simulation_clock WHERE id = 1")
        started_at, day_seconds = cur.fetchone()
        if day_seconds != SIM_DAY_SECONDS:
            raise ValueError("SIM_DAY_SECONDS differs from the persisted simulation clock")
        return started_at


def fetch_all(sql, params=()):
    with connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]
