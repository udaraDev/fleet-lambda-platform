import json
from datetime import datetime, timezone


def log(stage, message, **fields):
    print(json.dumps({"timestamp": datetime.now(timezone.utc).isoformat(),
                      "stage": stage, "message": message, **fields}, default=str), flush=True)
