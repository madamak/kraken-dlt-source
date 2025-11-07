import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pipelines.run_pipeline as rp


def test_run_pipeline_summary(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt"))
    package_dir = tmp_path / "pkg" / "completed_jobs"
    package_dir.mkdir(parents=True)
    job_file = package_dir / "dummy.test.insert_values.gz"
    job_file.parent.mkdir(parents=True, exist_ok=True)
    job_file.write_bytes(b"")

    import gzip

    with gzip.open(job_file, "wt", encoding="utf-8") as fp:
        fp.write("INSERT INTO {}\n")
        fp.write("VALUES\n")
        fp.write("(1)\n")
        fp.write("(2)\n")

    class StubJobFileInfo:
        def __init__(self, table_name: str):
            self.table_name = table_name

    class StubJob:
        def __init__(self, file_path: Path, table_name: str):
            self.file_path = str(file_path)
            self.job_file_info = StubJobFileInfo(table_name)

    class StubPackage:
        def __init__(self, file_path: Path):
            self.jobs = {"completed_jobs": [StubJob(file_path, "dummy")]}  # type: ignore[arg-type]

    class StubLoadInfo:
        def __init__(self, job_path: Path):
            self.pipeline = {"pipeline_name": "stub"}
            self.dataset_name = "stub_dataset"
            now = datetime.now(UTC)
            self.started_at = now
            self.finished_at = now + timedelta(seconds=1)
            self.load_packages = [StubPackage(job_path)]

        def asdict(self):
            return {"pipeline": self.pipeline}

        def raise_on_failed_jobs(self):
            return None

    class StubPipeline:
        def __init__(self, *args, **kwargs):
            self.job_path = job_file
            self.pipeline_name = "stub"

        def run(self, _source):
            return StubLoadInfo(self.job_path)

    def fake_source(*args, **kwargs):
        return []

    monkeypatch.setattr(rp.dlt, "pipeline", StubPipeline)
    monkeypatch.setattr(rp, "kraken_futures_source", fake_source)

    load_info, summary = rp.run_pipeline(resources=None, start_timestamp=None, dev_mode=True, raise_on_failed=True)
    assert summary["rows"]["dummy"] == 2

    monkeypatch.setattr(rp.dlt, "pipeline", StubPipeline)
    monkeypatch.setattr(rp, "kraken_futures_source", fake_source)
    rp.main(["--json", "--dev-mode"])
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert payload["summary"]["rows"]["dummy"] == 2
