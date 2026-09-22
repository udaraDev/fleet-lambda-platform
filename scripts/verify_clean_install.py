"""Isolated empty-volume Compose verification; preserves the normal project.

Uses existing built images, a unique project and alternate localhost ports. This
tests initialization without a prior database/checkpoint, not an uncached download
on a second machine. Test volumes are retained for inspection; containers stop.
"""
import json
import os
import subprocess
import time
import uuid
from pathlib import Path
from scripts.verify_running import verify

ROOT = Path(__file__).resolve().parents[1]


def main():
    name = 'fleet-clean-' + uuid.uuid4().hex[:8]
    env = dict(os.environ, API_PORT='18001', AIRFLOW_PORT='18080', SIM_DAY_SECONDS='120')
    output = ROOT / 'output' / 'evidence'
    output.mkdir(parents=True, exist_ok=True)
    result = {'project': name, 'simulated_day_real_seconds': 120,
              'scope': 'fresh volumes; existing images and same Docker host', 'passed': False}
    def compose(*args):
        return subprocess.check_output(['docker', 'compose', '-p', name, *args], cwd=ROOT,
                                       env=env, text=True, stderr=subprocess.STDOUT, timeout=300)
    try:
        result['startup'] = compose('up', '-d', '--no-build')
        verify('http://127.0.0.1:18001', 480)
        result['dag_imports'] = compose('exec', '-T', 'airflow-scheduler', 'airflow', 'dags', 'list-import-errors')
        if 'No data found' not in result['dag_imports']:
            raise AssertionError(result['dag_imports'])
        result['services'] = compose('ps')
        result['passed'] = True
    except Exception as exc:
        result['error'] = str(exc)
        raise
    finally:
        result['finished_utc'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        try:
            result['shutdown'] = compose('down')  # No -v: retain exact test volumes.
        finally:
            (output / 'clean-install.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
            print(json.dumps({k:v for k,v in result.items() if k not in {'startup','shutdown','services'}}, indent=2))


if __name__ == '__main__':
    main()
