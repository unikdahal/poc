"""Exercise the same read queries at different memory settings."""

import json
import os
import threading
from pathlib import Path
from tempfile import TemporaryDirectory

from harness import tpch
from harness.agent2.common import (EngineStartError, attempt, begin, engine, fingerprint, median,
                                   query_h, save, tpch_reference)

TASK = "memory"
QUERIES = (9, 18, 21)
SCALES = (10, 30)
GIB = 2**30


class SpillSampler:
    def __init__(self, path):
        self.path = path
        self.observed = {}
        self.peak_bytes = 0
        self.peak_files = 0
        self.done = threading.Event()

    def __enter__(self):
        def sample():
            while not self.done.is_set():
                size = count = 0
                for parent, _, files in os.walk(self.path):
                    for file in files:
                        path = Path(parent) / file
                        try:
                            length = path.stat().st_size
                        except OSError:
                            continue
                        name = str(path.relative_to(self.path))
                        self.observed[name] = max(self.observed.get(name, 0), length)
                        size += length
                        count += 1
                self.peak_bytes = max(size, self.peak_bytes)
                self.peak_files = max(count, self.peak_files)
                self.done.wait(.1)

        self.thread = threading.Thread(target=sample, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.done.set()
        self.thread.join()

    def result(self):
        return {"observed_files": len(self.observed), "sampled_peak_files": self.peak_files,
                "sampled_peak_bytes": self.peak_bytes, "examples": list(self.observed.items())[:20]}


def spark_spill(path):
    metrics = []
    for file in path.iterdir():
        if not file.is_file() or file.name.startswith("."):
            continue
        with file.open(errors="replace") as log:
            for line in log:
                event = json.loads(line)
                if event.get("Event") == "SparkListenerTaskEnd" and "Task Metrics" in event:
                    metrics.append(event["Task Metrics"])
    return {"task_events": len(metrics),
            "memory_bytes_spilled": sum(m.get("Memory Bytes Spilled", 0) for m in metrics) if metrics else None,
            "disk_bytes_spilled": sum(m.get("Disk Bytes Spilled", 0) for m in metrics) if metrics else None}


def run_case(result, scale, name, memory, pool, query, reference):
    label = f"{name}-{memory}g-{pool}-sf{scale}-q{query}"
    with TemporaryDirectory(prefix="agent2-spill-") as scratch:
        folder = Path(scratch)
        spill = folder / "spill"
        spill.mkdir(mode=0o777)
        folder.chmod(0o755)
        events = folder / "events"
        events.mkdir()
        run_options = None
        if name == "sail":
            extra = {"SAIL_RUNTIME__MEMORY_POOL__TYPE": pool,
                     "SAIL_RUNTIME__TEMPORARY_FILES__PATHS": '["/spill"]'}
            if pool == "fair":
                extra["SAIL_RUNTIME__MEMORY_POOL__FAIR__MAX_SIZE"] = str(memory * GIB // 2)
            run_options = ["-v", f"{spill}:/spill"]
        else:
            extra = {"spark.memory.offHeap.size": f"{memory * 256}m", "spark.local.dir": str(spill),
                     "spark.eventLog.enabled": "true", "spark.eventLog.dir": events.as_uri(),
                     "spark.eventLog.compress": "false"}
        port = 17000 + scale * 50 + memory * 25 + query if name == "comet" else None
        with engine(result, name, label=label, memory=f"{memory}g", extra=extra,
                    run_options=run_options, port=port) as (case, spark, stats, _):
            case["samples"] = records = []
            for _ in range(3):
                with SpillSampler(spill) as sampler:
                    record, rows = attempt(lambda: query_h(spark, f"tpch_sf{scale}", query, 300), stats)
                record["local_files"] = sampler.result()
                if rows is not None:
                    record["matches_spark"] = None if reference is None else fingerprint(
                        rows, tpch.ordered(query)
                    ) == reference
                records.append(record)
                if record["status"] != "ok":
                    break
            if len(records) == 3 and all(record.get("matches_spark") for record in records):
                case["validated_metrics"] = median(records)
        case = result["engines"][label]
        if name == "comet":
            try:
                case["spark_spill_including_startup"] = spark_spill(events)
            except (OSError, ValueError) as exc:
                case["spark_spill_error"] = str(exc)
        save(TASK, result)


def main():
    result = begin(TASK)
    result["measurement"]["query_timeout_s"] = 300
    for scale in SCALES:
        reference = tpch_reference(result, scale, QUERIES, TASK)
        if str(scale) not in result.get("data_load", {}):
            continue
        for name, pools in (("sail", ("unbounded", "fair")), ("comet", ("driver",))):
            for memory in (2, 4, 8):
                for pool in pools:
                    for query in QUERIES:
                        try:
                            run_case(result, scale, name, memory, pool, query, reference.get(query))
                        except EngineStartError:
                            save(TASK, result)


if __name__ == "__main__":
    main()
