"""Opt-in live fault check: briefly stop the producer, then always restart it."""

import json
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROBE = """import json, urllib.request, urllib.error
try:
    response = urllib.request.urlopen('http://127.0.0.1:8000/health/pipeline', timeout=10)
except urllib.error.HTTPError as error:
    response = error
print(json.dumps({'http_status': response.code, 'body': json.load(response)}))
"""


def compose(*args):
    return subprocess.check_output(["docker", "compose", *args], cwd=ROOT, text=True, timeout=60)


def probe():
    return json.loads(compose("exec", "-T", "api", "python", "-c", PROBE))


def await_status(healthy, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = probe()
        if result["body"].get("healthy") is healthy:
            return result
        time.sleep(5)
    raise AssertionError(f"Pipeline did not become healthy={healthy} within {timeout}s")


def verify():
    initial = probe()
    assert initial["http_status"] == 200 and initial["body"]["healthy"], "Start with a healthy stack"
    threshold = initial["body"]["threshold_real_seconds"]
    try:
        compose("stop", "producer-stream")
        stale = await_status(False, threshold + 180)
        assert stale["http_status"] == 503
        assert stale["body"]["status"] == "no_recent_data"
        assert stale["body"]["last_event_age_seconds"] > threshold
        print(json.dumps({"no_data_check": "passed", **stale}), flush=True)
    finally:
        compose("start", "producer-stream")
    recovered = await_status(True, 180)
    assert recovered["http_status"] == 200
    print(json.dumps({"recovery_check": "passed", **recovered}), flush=True)


if __name__ == "__main__":
    verify()
