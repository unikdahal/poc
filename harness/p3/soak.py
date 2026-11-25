"""Five and a half hours of mixed activity and post-load measurements."""

import json
import math
import os
import statistics
import subprocess
import sys
import threading
import time

import boto3
from botocore.config import Config

from harness import common as base, tpch
from harness.p3.common import VARIANT, begin, bounded, cmd, collect, fingerprint, group, load_tpch, save, timed, tpch_rows

TASK = "soak"
QUERIES = (3, 6, 9, 13, 18, 21)
ACTIVE_SECONDS = 5 * 3600 + 5 * 60
TOTAL_SECONDS = 5 * 3600 + 30 * 60
POINT = "SELECT n_name FROM polaris.tpch_sf1.nation WHERE n_nationkey=7"
UPDATE = ("MERGE INTO polaris.p3_soak.counters t USING (SELECT 0 AS id) s ON t.id=s.id "
          "WHEN MATCHED THEN UPDATE SET v=t.v+1")
TARGET = "polaris.p3_soak.counters"


def percentile(values, q):
    xs = sorted(values)
    return xs[math.ceil(len(xs) * q) - 1] if xs else None


def fit(samples, key):
    """Use first differences in 30-minute blocks to avoid a per-sample independence assumption."""
    buckets = {}
    for sample in samples:
        if sample["phase"] == "active" and isinstance(sample.get(key), (int, float)):
            buckets.setdefault(int(sample["elapsed_s"] // 1800), []).append(sample[key] / 2**20)
    centers = [(index / 2, statistics.median(values)) for index, values in sorted(buckets.items()) if len(values) >= 30]
    slopes = [(b[1] - a[1]) / (b[0] - a[0]) for a, b in zip(centers, centers[1:])]
    if len(slopes) < 3:
        return {"reason": "fewer than four sufficiently sampled 30-minute blocks"}
    import random
    rng = random.Random(1933)
    bootstrap = sorted(statistics.mean(rng.choices(slopes, k=len(slopes))) for _ in range(4000))
    return {"rate_mib_per_h": statistics.mean(slopes), "bootstrap_95_ci": [
        percentile(bootstrap, .025), percentile(bootstrap, .975)], "block_slopes_mib_per_h": slopes,
        "method": "bootstrap adjacent 30-minute block-median differences; descriptive interval"}


def diagnostics(container, stats, s3):
    process = cmd("docker", "exec", container, "sh", "-c",
        "awk '/^VmRSS:/{print $2} /^Threads:/{print $2}' /proc/1/status; ls /proc/1/fd | wc -l; df -k /tmp | tail -1",
        check=False, timeout=10).splitlines()
    record = {"cgroup_bytes": stats.mem_current()}
    if len(process) == 4 and all(line.isdigit() for line in process[:3]):
        record.update({"rss_bytes": int(process[0]) * 1024, "threads": int(process[1]),
                       "fd_count": int(process[2]), "local_disk_used_bytes": int(process[3].split()[2]) * 1024})
    else:
        record["process_error"] = process
    count = 0
    for bucket, prefix in (("warehouse", "poc/p3_soak"), ("shuffle", "")):
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            count += page["KeyCount"]
    record["workload_s3_objects"] = count
    return record


def soak(result, session, stats, container, reference):
    mode = VARIANT
    clock = time.monotonic()
    observations = result["cases"]["samples"] = []
    counts = result["cases"]["counts"] = {}
    failures = result["cases"]["errors"] = []
    windows = result["cases"]["latency_windows"] = []
    window = {}
    lock = threading.Lock()
    finish = threading.Event()
    s3 = boto3.client("s3", endpoint_url="http://localhost:9000", aws_access_key_id=base.S3["key"],
        aws_secret_access_key=base.S3["secret"], region_name=base.S3["region"],
        config=Config(connect_timeout=3, read_timeout=4, retries={"max_attempts": 1}))

    def observe(name, record, valid):
        with lock:
            counts.setdefault(name, {"attempts": 0, "matched_spark": 0, "failed": 0})
            counts[name]["attempts"] += 1
            counts[name]["matched_spark"] += int(valid)
            counts[name]["failed"] += int(not valid)
            if valid:
                window.setdefault(name, []).append(record["elapsed_s"])
            elif len(failures) < 300:
                failures.append({"class": name, **record})

    def sample_loop():
        tick = 0
        while not finish.is_set():
            elapsed = time.monotonic() - clock
            status, value = timed(lambda: diagnostics(container, stats, s3))
            observations.append({"elapsed_s": elapsed, "phase": "active" if elapsed < ACTIVE_SECONDS else "idle",
                                 **(value if value is not None else status)})
            with lock:
                windows.append({"elapsed_s": elapsed, **{
                    name: {"count": len(values), "p50_s": statistics.median(values), "p99_s": percentile(values, .99)}
                    for name, values in window.items()}})
                window.clear()
                save(result)
            print(f"soak {mode}: elapsed={elapsed:.0f}s samples={len(observations)} "
                  f"cgroup_bytes={observations[-1].get('cgroup_bytes')}", flush=True)
            tick += 1
            finish.wait(max(0, tick * 30 - (time.monotonic() - clock)))

    def consume_sessions(process):
        for line in process.stdout:
            try:
                record = json.loads(line)
                observe("session_churn", record, record["status"] == "ok" and record["validated"])
            except (KeyError, json.JSONDecodeError):
                print(line.strip(), flush=True)

    client = os.environ["P3_CHURN_PYTHON"]
    churn_log = (base.OUT / "churn-client.log").open("w")
    process = subprocess.Popen([client, "-m", "harness.p3.soak", "--churn"],
                               stdout=subprocess.PIPE, stderr=churn_log, text=True)
    sampler = threading.Thread(target=sample_loop, daemon=True)
    churner = threading.Thread(target=consume_sessions, args=(process,), daemon=True)
    sampler.start()
    churner.start()
    i = 0
    previous = 0
    try:
        while time.monotonic() - clock < ACTIVE_SECONDS:
            q = QUERIES[i % len(QUERIES)]
            record, rows = timed(lambda: tpch_rows(session, "tpch_sf1", q, 90))
            observe("tpch", record | {"query": q}, rows is not None and
                    fingerprint(rows, tpch.ordered(q)) == reference.get(q))
            for _ in range(4):
                record, rows = timed(lambda: collect(session, POINT, 20))
                observe("point", record, rows is not None and fingerprint(rows) == reference["point"])
            if i % 3 == 0:
                record, _ = timed(lambda: collect(session, UPDATE, 45))
                check, rows = timed(lambda: collect(session, f"SELECT v FROM {TARGET}", 20))
                value = rows[0][0] if rows else None
                valid = record["status"] == "ok" and value == previous + reference["merge_delta"]
                observe("merge", record | {"readback": check, "value_before": previous, "value_after": value}, valid)
                if value is not None:
                    previous = value
            i += 1
            time.sleep(.35)
        try:
            process.wait(timeout=40)
        except subprocess.TimeoutExpired:
            process.terminate()
            process.wait(timeout=10)
        churner.join(10)
        result["churn_process_exit_code"] = process.returncode
        bounded(session.stop, 6)
        while time.monotonic() - clock < TOTAL_SECONDS:
            time.sleep(min(15, max(0, TOTAL_SECONDS - (time.monotonic() - clock))))
    finally:
        if process.poll() is None:
            process.terminate()
        churn_log.close()
        finish.set()
        sampler.join(20)
        result["cases"]["trends"] = {key: fit(observations, key) for key in ("rss_bytes", "cgroup_bytes")}
        result["cases"]["sample_duration_s"] = time.monotonic() - clock
        result["cases"]["mode"] = mode
        result["cases"]["tokio_tasks"] = "unavailable from the container process counters"
        save(result)


def main():
    result = begin(TASK)
    result["versions"]["churn_client_pyspark"] = "4.0.1"
    result["cases"]["target"] = {"active_s": ACTIVE_SECONDS, "total_s": TOTAL_SECONDS, "sessions": 10000}
    reference = {}

    def load(spark, *_):
        result["data_load"] = load_tpch(spark, 1)
        collect(spark, "CREATE NAMESPACE polaris.p3_soak")
        collect(spark, f"CREATE TABLE {TARGET} USING iceberg TBLPROPERTIES ('write.merge.mode'='merge-on-read') AS SELECT 0 AS id, 0 AS v")
        collect(spark, f"CREATE TABLE {TARGET}_reference USING iceberg AS SELECT 0 AS id, 0 AS v")
        collect(spark, UPDATE.replace(TARGET, f"{TARGET}_reference"))
        reference["merge_delta"] = collect(spark, f"SELECT v FROM {TARGET}_reference")[0][0]
        reference["point"] = fingerprint(collect(spark, POINT))
        for q in QUERIES:
            observation, rows = timed(lambda: tpch_rows(spark, "tpch_sf1", q, 180))
            if rows is not None:
                reference[q] = fingerprint(rows, tpch.ordered(q))
        result["reference"] = reference

    group(result, "spark-reference", load, "spark")
    if reference.get("merge_delta") != 1 or "point" not in reference:
        return
    base.start_token_refresher()
    group(result, "mixed-soak", lambda *args: soak(result, *args, reference), "sail",
          mode=VARIANT, memory="11g", extra={"SAIL_CLUSTER__WORKER_INITIAL_COUNT": "1",
          "SAIL_CLUSTER__WORKER_MAX_COUNT": "1", "SAIL_CLUSTER__WORKER_TASK_SLOTS": "32",
          "SAIL_SPARK__SESSION_TIMEOUT_SECS": "120"})


def churn_client():
    from pyspark.sql.connect.session import SparkSession

    clock = time.monotonic()
    while time.monotonic() - clock < ACTIVE_SECONDS:
        started = time.monotonic()

        def exercise():
            session = SparkSession("sc://localhost:50051", userId="synthetic_churn")
            try:
                rows = collect(session, "SELECT 1", 12)
                bounded(session.client.release_session, 6)
                return rows is not None and rows[0][0] == 1
            finally:
                session.client.close()

        rec, valid = timed(exercise)
        print(json.dumps(rec | {"validated": bool(valid)}), flush=True)
        time.sleep(max(0, 1.25 - (time.monotonic() - started)))


if __name__ == "__main__":
    churn_client() if "--churn" in sys.argv else main()
