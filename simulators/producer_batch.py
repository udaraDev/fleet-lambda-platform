import csv
import time
from datetime import datetime, timedelta, timezone

from common.db import clock_start
from common.domain import EXPENSE_FIELDS, SIM_START, simulated_time
from common.logging_conf import log
from common.settings import DATA_DIR, SIM_DAY_SECONDS, VEHICLE_COUNT
from simulators.fixtures import expense_rows


def main():
    landing = DATA_DIR / "landing" / "expenses"
    landing.mkdir(parents=True, exist_ok=True)
    started = clock_start()
    while True:
        today = simulated_time(started, datetime.now(timezone.utc), SIM_DAY_SECONDS).date()
        day = SIM_START.date()
        while day < today:
            target = landing / f"expenses_{day.isoformat()}.csv"
            if not target.exists():
                temporary = target.with_suffix(".tmp")
                with temporary.open("w", newline="", encoding="utf-8") as handle:
                    writer = csv.DictWriter(handle, fieldnames=EXPENSE_FIELDS)
                    writer.writeheader()
                    writer.writerows(expense_rows(day.isoformat(), VEHICLE_COUNT))
                temporary.replace(target)  # Readers never see a partially written CSV.
                log("batch_producer", "expense_file_published", report_date=day, rows=VEHICLE_COUNT)
            day += timedelta(days=1)
        time.sleep(2)


if __name__ == "__main__":
    main()
