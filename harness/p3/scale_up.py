"""Attempt the largest scale factors on the hosted disk and memory budget."""

import os
import threading
from pathlib import Path
from contextlib import contextmanager
from tempfile import TemporaryDirectory

from harness import common as base, tpch
from harness.p3.common import (VARIANT, begin, bounded, cmd, collect, fingerprint, group, load_tpcds,
                               error, load_tpch, median_if_valid, query_file, save, timed, tpch_rows)

TASK = "scale-up"
SCALE = 50 if VARIANT == "tpch50" else 10
if VARIANT == "tpch50":
    QUERIES = range(1, 23)
elif VARIANT.startswith("tpcds10-"):
    shard = int(VARIANT.rsplit("-", 1)[-1])
    QUERIES = range(33 * (shard - 1) + 1, 33 * shard + 1)
else:
    QUERIES = (7, 9, 12, 18, 20, 27, 36, 49, 66, 77, 83, 98)
PATHS = Path(__file__).with_name("queries")


def free_bytes():
    stat = os.statvfs(base.ROOT)
    return stat.f_bavail * stat.f_frsize


def run_query(spark, q):
    if VARIANT == "tpch50":
        return [tpch_rows(spark, "tpch_sf50", q, 650)]
    bounded(lambda: tpch.use(spark, "p3_tpcds_sf10"), 30, spark.interruptAll)
    return [collect(spark, sql, 650) for sql in query_file(PATHS / f"q{q}.sql")]


def signature(rows, q):
    return [fingerprint(batch, tpch.ordered(q) if VARIANT == "tpch50" else False) for batch in rows]


def watch_disk(stop, observations):
    while not stop.is_set():
        observations.append(free_bytes())
        stop.wait(1)


def spill_stats(path):
    total = count = 0
    for file in path.rglob("*"):
        try:
            if file.is_file():
                total += file.stat().st_size
                count += 1
        except OSError:
            pass
    return total, count


@contextmanager
def memory_cap(result, name, stats):
    if name != "comet":
        yield
        return
    limit = 8 * 2**30
    cfg = result["engines"][name]
    cgroup = Path(f"/sys/fs/cgroup/p3-comet-{os.getpid()}")
    try:
        cmd("sudo", "mkdir", str(cgroup), timeout=8)
        cmd("sudo", "tee", str(cgroup / "memory.max"), input=f"{limit}\n", timeout=8)
        cmd("sudo", "tee", str(cgroup / "memory.swap.max"), input="0\n", timeout=8)
        processes = [stats.proc, *stats.proc.children(recursive=True)]
        for process in processes:
            cmd("sudo", "tee", str(cgroup / "cgroup.procs"), input=f"{process.pid}\n", timeout=8)
        cfg["host_cgroup"] = {"memory_max": int((cgroup / "memory.max").read_text()),
                              "memory_swap_max": int((cgroup / "memory.swap.max").read_text()),
                              "startup_outside_cgroup": True}
    except Exception as exc:
        cfg["host_cgroup"] = {"error": error(exc)}
    try:
        yield
    finally:
        if (cgroup / "memory.events").exists():
            cfg["host_cgroup"]["memory_events"] = dict(line.split() for line in (cgroup / "memory.events").read_text().splitlines())


def main():
    result = begin(TASK)
    result["scale"] = SCALE
    result["disk_before_generation_bytes"] = free_bytes()
    disk = result["load_disk_samples"] = []
    reference = {}

    def prepare(spark, *_):
        done = threading.Event()
        thread = threading.Thread(target=watch_disk, args=(done, disk), daemon=True)
        thread.start()
        try:
            if VARIANT == "tpch50":
                result["data_load"] = load_tpch(spark, SCALE)
            else:
                result["data_load"] = load_tpcds(spark, SCALE)
        finally:
            done.set()
            thread.join(5)
        for q in QUERIES:
            print(f"Spark reference {VARIANT} Q{q}", flush=True)
            record, rows = timed(lambda: run_query(spark, q))
            if rows is not None:
                reference[q] = signature(rows, q)
            result["cases"].setdefault("reference", {})[str(q)] = {
                "status": record["status"], "error": record.get("error"), "result": reference.get(q)}
            save(result)
            if record["status"] == "timeout" and "active=True" in record["error"]:
                break
    group(result, "spark-reference", prepare, "spark")
    result["disk_after_reference_bytes"] = free_bytes()
    if not result.get("data_load"):
        result["reason"] = "scale generation or load did not finish on the hosted runner"
        save(result)
        return
    base.start_token_refresher()
    for name in ("sail", "comet"):
        with TemporaryDirectory(prefix="p3-spill-") as tmp:
            path = Path(tmp)
            path.chmod(0o777)
            def execute(spark, stats, _):
                cases = result["cases"][name] = {}
                for q in QUERIES:
                    print(f"{name} {VARIANT} Q{q}", flush=True)
                    samples = []
                    for _ in range(3):
                        done = threading.Event()
                        disk_samples = []
                        def monitor():
                            while not done.is_set():
                                disk_samples.append((*spill_stats(path), free_bytes()))
                                done.wait(.25)
                        thread = threading.Thread(target=monitor, daemon=True)
                        thread.start()
                        try:
                            record, rows = timed(lambda: run_query(spark, q), stats)
                        finally:
                            done.set()
                            thread.join(5)
                        record["matches_spark"] = rows is not None and signature(rows, q) == reference.get(q)
                        record["peak_sampled_spill_bytes"] = max((r[0] for r in disk_samples), default=0)
                        record["peak_sampled_spill_files"] = max((r[1] for r in disk_samples), default=0)
                        record["minimum_free_disk_bytes"] = min((r[2] for r in disk_samples), default=None)
                        samples.append(record)
                        if record["status"] == "timeout":
                            break
                    cases[str(q)] = {"samples": samples, "warm_median": median_if_valid(samples)}
                    save(result)
                    if samples[-1]["status"] == "timeout" and "active=True" in samples[-1]["error"]:
                        break
            def run(spark, stats, container):
                with memory_cap(result, name, stats):
                    execute(spark, stats, container)
            group(result, name, run, name, memory="8g" if name == "sail" else "6g",
                  extra={"SAIL_RUNTIME__MEMORY_POOL__TYPE": "fair",
                         "SAIL_RUNTIME__MEMORY_POOL__FAIR__MAX_SIZE": str(4 * 2**30),
                         "SAIL_RUNTIME__TEMPORARY_FILES__PATHS": '["/spill"]'},
                  options=("-v", f"{path}:/spill"), spark_conf={"spark.memory.offHeap.size": "2g",
                   "spark.local.dir": str(path)})
    result["disk_after_run_bytes"] = free_bytes()
    result["storage_limit"] = "Sail has an 8 GiB container cap. Comet has 6 GiB heap and 2 GiB off-heap. The raw host cgroup record shows whether its post-startup total cap was applied."
    save(result)


if __name__ == "__main__":
    main()
