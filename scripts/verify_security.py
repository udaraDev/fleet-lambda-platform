"""Runtime proof that each application database identity has only its intended access."""

import os

import psycopg2

from common.db import connection


def main():
    expected = os.environ["EXPECTED_DB_ROLE"]
    with connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT current_user")
        actual = cur.fetchone()[0]
        if actual != expected:
            raise AssertionError(f"Expected database role {expected}, got {actual}")

        if expected == "fleet_api":
            try:
                cur.execute("CREATE TEMP TABLE role_permission_test(value integer)")
            except psycopg2.errors.InsufficientPrivilege:
                conn.rollback()
            else:
                raise AssertionError("Read-only API role unexpectedly accepted a write")
        else:
            cur.execute("CREATE TEMP TABLE role_permission_test(value integer)")
            conn.rollback()
    print(f"Database role verified: {expected}")


if __name__ == "__main__":
    main()
