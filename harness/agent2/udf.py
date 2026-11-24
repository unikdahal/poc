"""Validate Python evaluation paths and measure cross-session interference."""

import argparse
import json
import subprocess
import sys
import threading
import time
from importlib.metadata import PackageNotFoundError, version

from harness import common as base
from harness.agent2.common import attempt, begin, bounded, collect, engine, error, latency, median, save, wait_ready

TASK = "udf"
ROWS = 10_000_000
PARTITIONS = 8
LOAD_ROWS = 500_000
DEADLINE = 180
VARIANTS = ("scalar", "pandas", "arrow", "udtf")


def formula(value):
    return (value * 3 + 7) % 1000003


def cpu_heavy(value):
    for _ in range(512):
        value = (1_664_525 * value + 1_013_904_223) & 0xffffffff
    return value


def workload(spark, variant, rows=ROWS):
    from pyspark.sql import functions as f

    if variant == "udtf":
        @f.udtf(returnType="input_id long, v long")
        class Transform:
            def eval(self, value):
                yield value, (value * 3 + 7) % 1000003

        spark.udtf.register("agent2_transform", Transform)
        return spark.sql(f"SELECT input_id AS id, v FROM range(0, {rows}, 1, {PARTITIONS}) r, "
                         "LATERAL agent2_transform(r.id)")
    if variant == "pandas":
        function = f.pandas_udf(formula, "long")
    else:
        function = f.udf(formula, "long", useArrow=variant == "arrow")
    return spark.range(0, rows, 1, PARTITIONS).select("id", function("id").alias("v"))


def aggregate(frame):
    return frame.selectExpr(
        "count(*) AS rows", "sum(id) AS input_sum", "sum(v) AS output_sum",
        "sum(case when v <=> pmod(id * 3 + 7, 1000003) then 0 else 1 end) AS incorrect",
    ).collect()[0].asDict()


def busy_query(spark, rows):
    from pyspark.sql import functions as f

    function = f.udf(cpu_heavy, "long", useArrow=False)
    return spark.range(0, rows, 1, PARTITIONS).select(function("id").alias("v")).selectExpr("sum(v)").collect()[0][0]


def interference(heavy_session, other, stats):
    small_samples = []
    for _ in range(30):
        record, rows = attempt(lambda: collect(other, "SELECT 1", 15))
        record["correct"] = rows is not None and rows[0][0] == 1
        small_samples.append(record)
    heavy = {}
    small_under_load = []

    def run():
        record, value = attempt(lambda: bounded(lambda: busy_query(heavy_session, LOAD_ROWS),
                                                DEADLINE, heavy_session.interruptAll), stats)
        heavy.update(record | {"result_sum": value})

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    time.sleep(.3)
    while thread.is_alive() and len(small_under_load) < 500:
        record, rows = attempt(lambda: collect(other, "SELECT 1", 15))
        record["correct"] = rows is not None and rows[0][0] == 1
        record["ended_before_heavy"] = thread.is_alive()
        small_under_load.append(record)
    thread.join(DEADLINE + 10)
    return {"heavy": heavy, "idle": small_samples, "overlap": small_under_load}


def run_engine(result, name):
    result.setdefault("client_versions", {})[name] = {"python": sys.version.split()[0]}
    for package in ("pyspark", "pyspark-client", "pyarrow", "pandas", "numpy"):
        try:
            result["client_versions"][name][package] = version(package)
        except PackageNotFoundError:
            pass
    with engine(result, name, output_name=TASK) as (case, spark, stats, port):
        variants = case["variants"] = {}
        baseline = spark.range(0, ROWS, 1, PARTITIONS).selectExpr("id", "pmod(id * 3 + 7, 1000003) AS v")
        if name == "spark":
            result["spark_reference"] = bounded(lambda: aggregate(baseline), DEADLINE, spark.interruptAll)
        other = base.connect(port)
        wait_ready(other)
        case["interference"] = interference(spark, other, stats)
        for series in ("idle", "overlap"):
            samples = case["interference"][series]
            case["interference"][series + "_latency"] = latency(r["wall_s"] for r in samples if r["correct"])
        heavy = case["interference"]["heavy"]
        if name == "spark" and heavy.get("status") == "ok":
            result["spark_heavy_sum"] = heavy["result_sum"]
        heavy["matches_spark"] = result.get("spark_heavy_sum") == heavy.get("result_sum") and heavy.get("status") == "ok"

        save(TASK, result)
        small_ref, expected = attempt(lambda: bounded(
            lambda: aggregate(spark.range(0, 1000, 1, PARTITIONS).selectExpr(
                "id", "pmod(id * 3 + 7, 1000003) AS v")),
            40, spark.interruptAll,
        ))
        if name == "spark" and small_ref["status"] == "ok":
            result["spark_small_udtf_reference"] = expected
        for variant in VARIANTS:
            if variant == "udtf":
                record, value = attempt(lambda: bounded(lambda: aggregate(workload(spark, variant, 1000)),
                                                        40, spark.interruptAll), stats)
                case["udtf_1000"] = record | {"output": value, "matches_spark": value is not None and
                                             value == result.get("spark_small_udtf_reference")}
                save(TASK, result)
                if record["status"] == "timeout":
                    raise TimeoutError(record["error"])
            samples = variants[variant] = []
            for _ in range(3):
                record, output = attempt(lambda: bounded(lambda: aggregate(workload(spark, variant)),
                                                          DEADLINE, spark.interruptAll), stats)
                samples.append(record | {"output": output, "matches_spark": output is not None and
                                         output == result.get("spark_reference")})
                save(TASK, result)
                if record["status"] == "timeout":
                    raise TimeoutError(record["error"])
                if record["status"] == "error":
                    break
            if len(samples) == 3 and all(sample["matches_spark"] for sample in samples):
                metrics = median(samples)
                metrics["input_rows_per_s"] = ROWS / metrics["median_wall_s"]
                case.setdefault("validated_metrics", {})[variant] = metrics



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sail-python")
    parser.add_argument("--sail-only", action="store_true")
    args = parser.parse_args()
    if args.sail_only:
        result = json.loads((base.OUT / "udf.json").read_text())
        result["sail_client_command"] = [sys.executable, "-m", "harness.agent2.udf", "--sail-only"]
        run_engine(result, "sail")
        return
    result = begin(TASK)
    result["measurement"]["query_timeout_s"] = DEADLINE
    result["workload"] = {"input_rows": ROWS, "partitions": PARTITIONS, "heavy_rows": LOAD_ROWS,
                          "heavy_iterations_per_row": 512}
    run_engine(result, "spark")
    if args.sail_python:
        try:
            subprocess.run([args.sail_python, "-m", "harness.agent2.udf", "--sail-only"],
                           check=True, timeout=1200)
        except (subprocess.SubprocessError, OSError) as exc:
            result = json.loads((base.OUT / "udf.json").read_text())
            result["sail_client_error"] = error(exc)
            save(TASK, result)
    else:
        run_engine(result, "sail")


if __name__ == "__main__":
    main()
