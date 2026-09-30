"""Build the code-only submission archive from an explicit, reviewable file set."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
import subprocess
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo


ROOT = Path(__file__).resolve().parents[1]
DESTINATION = ROOT / "output" / "fleet-lambda-platform-source-submission.zip"
SOURCE_DIRS = {
    ".github", "airflow", "api", "batch", "common", "config", "docker",
    "docs", "scripts", "simulators", "sql", "streaming", "tests",
}
SOURCE_FILES = {
    ".dockerignore", ".env.example", ".gitattributes", ".gitignore",
    "docker-compose.yml", "Dockerfile", "Makefile",
    "README.md", "requirements-api.txt", "requirements-dev.txt",
    "requirements.txt",
}
EVIDENCE = {
    "output/evidence/clean-install.json",
    "output/evidence/dashboard.png",
    "output/evidence/final-verification.json",
    "output/evidence/performance-benchmark.json",
}
EXTRA = {"scripts/package_source_submission.py"}
REQUIRED = {
    "README.md", "docker-compose.yml", ".env.example",
    "docs/CONTRIBUTION_STATEMENT.md", "docs/img/architecture.png",
    "scripts/package_source_submission.py", "simulators/producer_stream.py",
    "simulators/producer_batch.py", "streaming/raw_job.py",
    "streaming/speed_job.py", "batch/spark_reconcile.py",
    "airflow/dags/daily_profitability_dag.py", "api/main.py",
    "tests/test_completion.py", "output/evidence/clean-install.json",
}


def selected_files() -> list[str]:
    tracked = subprocess.check_output(
        ["git", "ls-files", "-z"], cwd=ROOT
    ).decode("utf-8").strip("\0").split("\0")
    names = set(EXTRA)
    for name in tracked:
        name = name.replace("\\", "/")
        if name in SOURCE_FILES or name in EVIDENCE or name.split("/", 1)[0] in SOURCE_DIRS:
            names.add(name)
    missing = sorted(REQUIRED - names)
    if missing:
        raise RuntimeError(f"Required submission files were not selected: {missing}")
    for name in names:
        path = ROOT / name
        if not path.is_file() or not path.resolve().is_relative_to(ROOT.resolve()):
            raise RuntimeError(f"Missing or unsafe source path: {name}")
        parts = path.relative_to(ROOT).parts
        if any(part in {".env", ".git", "__pycache__", "tmp", "data"} for part in parts):
            raise RuntimeError(f"Private or generated file selected: {name}")
    return sorted(names)


def main() -> None:
    names = selected_files()
    DESTINATION.parent.mkdir(parents=True, exist_ok=True)
    temporary = DESTINATION.with_suffix(".zip.part")
    digests: list[str] = []
    try:
        with ZipFile(temporary, "w", compression=ZIP_DEFLATED, compresslevel=9) as archive:
            for name in names:
                contents = (ROOT / name).read_bytes()
                digests.append(f"{hashlib.sha256(contents).hexdigest()}  {name}")
                info = ZipInfo(name, date_time=(2026, 9, 30, 0, 0, 0))
                info.compress_type = ZIP_DEFLATED
                info.external_attr = 0o644 << 16
                archive.writestr(info, contents, compress_type=ZIP_DEFLATED, compresslevel=9)
            info = ZipInfo("MANIFEST.sha256", date_time=(2026, 9, 30, 0, 0, 0))
            info.compress_type = ZIP_DEFLATED
            archive.writestr(info, "\n".join(digests) + "\n")
        with ZipFile(temporary) as archive:
            if archive.testzip() is not None:
                raise RuntimeError("Submission archive failed its CRC check")
            if set(archive.namelist()) != set(names) | {"MANIFEST.sha256"}:
                raise RuntimeError("Submission archive contents changed unexpectedly")
        os.replace(temporary, DESTINATION)
    finally:
        temporary.unlink(missing_ok=True)
    print(f"Created {DESTINATION} ({len(names)} project files + checksum manifest, "
          f"{DESTINATION.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
