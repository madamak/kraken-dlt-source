from __future__ import annotations

import argparse
import json
import logging
from typing import Any, Dict, Iterable, Sequence

import dlt

from kraken_dlt_source import kraken_futures_source
from kraken_dlt_source.futures.resources import ALL_RESOURCE_NAMES


LOGGER = logging.getLogger(__name__)


def available_resources() -> Iterable[str]:
    return ALL_RESOURCE_NAMES


def run_pipeline(
    resources: Sequence[str] | None,
    start_timestamp: str | None,
    dev_mode: bool,
    raise_on_failed: bool,
) -> tuple[dlt.LoadInfo, Dict[str, Any]]:
    pipeline = dlt.pipeline(
        pipeline_name="kraken_futures",
        destination="duckdb",
        dataset_name="kraken_futures_data",
        dev_mode=dev_mode,
    )

    source = kraken_futures_source(start_timestamp=start_timestamp, resources_to_load=resources)
    load_info = pipeline.run(source)
    summary = _summarize_load(load_info)
    LOGGER.info(
        "Loaded pipeline %s into dataset %s in %.2fs",
        pipeline.pipeline_name,
        load_info.dataset_name,
        summary["duration_seconds"],
    )
    for resource, rows in summary["rows"].items():
        LOGGER.info("%s: %s rows", resource, rows)

    if raise_on_failed:
        load_info.raise_on_failed_jobs()

    return load_info, summary


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Kraken Futures DLT pipeline")
    parser.add_argument(
        "--resources",
        nargs="+",
        choices=ALL_RESOURCE_NAMES,
        help="Subset of resources to load. Defaults to all resources when omitted.",
    )
    parser.add_argument(
        "--since",
        dest="start_timestamp",
        help="Optional ISO 8601 timestamp or millisecond epoch to seed the first incremental load.",
    )
    parser.add_argument(
        "--dev-mode",
        action="store_true",
        help="Enable DLT development mode (clears state between runs).",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Raise an exception if DLT reports failed jobs.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available resource names and exit.",
    )
    parser.add_argument(
        "--json",
        dest="output_json",
        action="store_true",
        help="Emit the load information as JSON for scripting.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.list:
        for name in available_resources():
            print(name)
        return

    load_info, summary = run_pipeline(
        resources=args.resources,
        start_timestamp=args.start_timestamp,
        dev_mode=args.dev_mode,
        raise_on_failed=args.strict,
    )

    if args.output_json:
        payload = load_info.asdict()
        payload["summary"] = summary
        print(json.dumps(payload, default=str, indent=2))


def _summarize_load(load_info: dlt.LoadInfo) -> Dict[str, Any]:
    duration = 0.0
    if load_info.started_at and load_info.finished_at:
        duration = (load_info.finished_at - load_info.started_at).total_seconds()

    row_counts: Dict[str, int] = {}
    for package in load_info.load_packages:
        jobs = package.jobs.get("completed_jobs", []) if package.jobs else []
        for job in jobs:
            table_name = job.job_file_info.table_name if job.job_file_info else None
            resource = table_name
            if not table_name:
                continue
            if table_name.startswith("_dlt_"):
                continue
            stats = pipeline_rows(job.file_path, table_name)
            row_counts[resource] = row_counts.get(resource, 0) + stats

    return {"duration_seconds": duration, "rows": row_counts}


def pipeline_rows(job_file_path: str, table_name: str) -> int:
    try:
        import gzip

        count = 0
        if job_file_path.endswith("insert_values.gz"):
            with gzip.open(job_file_path, "rt", encoding="utf-8") as fp:
                for line in fp:
                    stripped = line.lstrip()
                    if stripped.startswith("("):
                        count += 1
        return count
    except Exception:
        return 0


if __name__ == "__main__":
    main()
