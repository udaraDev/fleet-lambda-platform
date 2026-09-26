"""Isolated empty-volume Compose verification; preserves the normal project.

Uses existing built images, a unique project and alternate localhost ports. This
tests initialization without a prior database/checkpoint, not an uncached download
on a second machine. Test volumes are removed after the evidence is recorded.
"""
import json
import os
import subprocess
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def require_pipeline_verification(output):
    """Return verified JSON output or fail closed on missing/invalid evidence."""
    start = output.find('{')
    if start < 0:
        raise AssertionError("Pipeline verification returned no valid JSON")
    try:
        result, _ = json.JSONDecoder().raw_decode(output[start:])
    except json.JSONDecodeError as exc:
        raise AssertionError("Pipeline verification returned no valid JSON") from exc
    if result.get('result') != 'passed':
        raise AssertionError(f"Pipeline verification did not pass: {result}")
    return result


def main():
    name = 'fleet-clean-' + uuid.uuid4().hex[:8]
    env = dict(
        os.environ,
        API_PORT='18001',
        AIRFLOW_PORT='18080',
        MINIO_API_PORT='19000',
        MINIO_CONSOLE_PORT='19001',
        PROMETHEUS_PORT='19090',
        GRAFANA_PORT='13000',
        SIM_DAY_SECONDS='120',
    )
    output = ROOT / 'output' / 'evidence'
    output.mkdir(parents=True, exist_ok=True)
    result = {'project': name, 'simulated_day_real_seconds': 120,
              'scope': os.getenv(
                  'CLEAN_INSTALL_SCOPE',
                  'fresh volumes; existing images and same Docker host'),
              'passed': False}
    def compose(*args):
        return subprocess.check_output(['docker', 'compose', '-p', name, *args], cwd=ROOT,
                                       env=env, text=True, stderr=subprocess.STDOUT, timeout=900)
    try:
        result['startup'] = compose('up', '-d', '--no-build')
        # Run the HTTP verification from a disposable tools container on the
        # project network. ``compose exec`` can return an empty success result
        # when the target container is restarted under Docker Desktop load.
        result['pipeline_verification'] = compose(
            'run', '--rm', '--no-deps', 'tools', 'python', '-m', 'scripts.verify_running',
            '--base-url', 'http://api:8000', '--wait-seconds', '720',
        )
        require_pipeline_verification(result['pipeline_verification'])
        result['dag_imports'] = compose('exec', '-T', 'airflow-scheduler', 'airflow', 'dags', 'list-import-errors')
        if 'No data found' not in result['dag_imports']:
            raise AssertionError(result['dag_imports'])
        result['services'] = compose('ps')
        result['passed'] = True
    except Exception as exc:
        result['error'] = str(exc)
        if isinstance(exc, subprocess.CalledProcessError):
            result['command_output'] = exc.output
        try:
            result['failure_services'] = compose('ps', '-a')
            result['failure_logs'] = compose('logs', '--tail', '200')
        except Exception as diagnostic_exc:
            result['diagnostic_error'] = str(diagnostic_exc)
        raise
    finally:
        result['finished_utc'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        try:
            result['shutdown'] = compose('down', '-v', '--remove-orphans')
        finally:
            (output / 'clean-install.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
            print(json.dumps({k:v for k,v in result.items() if k not in {'startup','shutdown','services'}}, indent=2))


if __name__ == '__main__':
    main()
