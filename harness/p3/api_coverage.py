"""Exercise DataFrame plans from two Python and two JVM Connect clients."""

import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from harness.p3.common import VARIANT, base, begin, bounded, cmd, error, fingerprint, group, save, timed

PY4 = "4.0.1"
TOOLS = Path(__file__).with_name("tools")


def python_cases(s, port):
    import pandas as pd
    from pyspark.sql import Window, functions as f
    from pyspark.sql.types import LongType, StructField, StructType

    df = s.range(8).selectExpr("id", "id % 2 AS g", "id + 1 AS a", "id * 2 AS b")
    nested = s.range(3).selectExpr("id", "array(id,id+1) AS a", "map('a',id,'b',id+1) AS m",
                                   "named_struct('old',id,'b',id+1) AS st")
    strings = s.range(2).selectExpr("id", "concat('key-',id,'-123') AS text",
                                   """concat('{"a":',id,'}') AS js""", "concat(id,',text') AS csv")
    dates = s.range(2).selectExpr("date_add(DATE '2024-02-28', cast(id AS int)) AS d",
                                 "TIMESTAMP_NTZ '2024-03-10 02:10:00' AS t")
    window = Window.partitionBy("g").orderBy("id")
    schema = StructType([StructField("id", LongType())])

    def map_batches(iterator):
        for batch in iterator:
            yield pd.DataFrame({"id": batch["id"] + 1})

    def group_batches(batch):
        return pd.DataFrame({"id": [int(batch["id"].sum())]})

    def checkpoint(local):
        return (df.localCheckpoint(eager=True) if local else df.checkpoint(eager=True)).select("id")

    def cached():
        df.cache()
        try:
            return df.where("id > 4").select("id").collect()
        finally:
            df.unpersist()

    def roundtrip(iceberg):
        if iceberg:
            table = f"polaris.p3_api.writer_probe_{port}"
            s.sql("CREATE NAMESPACE IF NOT EXISTS polaris.p3_api").collect()
            s.sql(f"DROP TABLE IF EXISTS {table}").collect()
            df.writeTo(table).using("iceberg").tableProperty("format-version", "2").create()
            df.where("id=1").writeTo(table).append()
            return s.table(table).select("id").collect()
        path = "/tmp/p3-connect-parquet"
        df.write.mode("overwrite").option("compression", "gzip").partitionBy("g").parquet(path)
        return s.read.option("mergeSchema", "true").parquet(path).select("id", "g").collect()

    return {
        "row_number_rank_lag": lambda: df.select("id", f.row_number().over(window), f.rank().over(window), f.lag("a").over(window)),
        "window_rows": lambda: df.select("id", f.sum("a").over(window.rowsBetween(-1, 1))),
        "window_range": lambda: df.select("id", f.avg("a").over(window.rangeBetween(-2, 2))),
        "pivot_values": lambda: df.groupBy("g").pivot("id", [0, 1, 2, 3]).sum("a"),
        "pivot_discovery": lambda: df.groupBy("g").pivot("id").sum("b"),
        "unpivot": lambda: df.unpivot("id", ["a", "b"], "key", "value"),
        "explode": lambda: nested.select("id", f.explode("a")),
        "posexplode": lambda: nested.select("id", f.posexplode("m")),
        "explode_outer": lambda: df.select("id", f.explode_outer(f.when(f.col("g") == 0, f.array("id")))),
        "transform": lambda: nested.select("id", f.transform("a", lambda x, i: x+i)),
        "filter_exists_forall": lambda: nested.select("id", f.filter("a", lambda x: x > 1),
                                                       f.exists("a", lambda x: x > 1), f.forall("a", lambda x: x >= 0)),
        "aggregate": lambda: nested.select("id", f.aggregate("a", f.lit(0).cast("long"), lambda a, b: a+b)),
        "zip_with": lambda: nested.select("id", f.zip_with("a", f.array(f.lit(2)), lambda x, y: x+y)),
        "map_filter_transform": lambda: nested.select("id", f.map_filter("m", lambda k, v: v > 0),
                                                       f.transform_values("m", lambda k, v: v+1)),
        "map_keys_entries_access": lambda: nested.select("id", f.sort_array(f.map_keys("m")), f.sort_array(f.map_entries("m")), f.element_at("m", f.lit("b"))),
        "struct_fields": lambda: nested.select("id", f.col("st").withField("added", f.lit(3)).dropFields("old")),
        "from_to_json": lambda: strings.select("id", f.to_json(f.from_json("js", "a BIGINT"))),
        "json_tuple_path": lambda: strings.select("id", f.json_tuple("js", "a")),
        "get_json_object": lambda: strings.select("id", f.get_json_object("js", "$.a")),
        "from_to_csv": lambda: strings.select("id", f.to_csv(f.from_csv("csv", "a BIGINT, b STRING"))),
        "regexp_extract_replace": lambda: strings.select("id", f.regexp_extract("text", r"([0-9]+)$", 1), f.regexp_replace("text", r"[0-9]", "x")),
        "regexp_count": lambda: strings.select("id", f.regexp_count("text", f.lit("[0-9]"))),
        "date_arithmetic": lambda: dates.select(f.add_months("d", 1), f.last_day("d"), f.datediff("d", f.lit("2024-01-01"))),
        "timestamp_ntz": lambda: dates.select("t", f.expr("t + INTERVAL '1' HOUR").alias("next")),
        "day_time_interval": lambda: dates.select(f.expr("t - TIMESTAMP_NTZ '2024-03-09 02:10:00'")),
        "year_month_interval": lambda: dates.select(f.expr("d + INTERVAL '1-2' YEAR TO MONTH")),
        "approx_count_distinct": lambda: df.agg(f.approx_count_distinct("id")),
        "approx_percentile": lambda: df.agg(f.percentile_approx("a", [0.1, 0.5, 0.9])),
        "percentile": lambda: df.agg(f.percentile("a", f.array(f.lit(0.25), f.lit(0.5)))),
        "cube": lambda: df.cube("g", "a").agg(f.sum("b"), f.grouping_id()),
        "rollup": lambda: df.rollup("g", "a").agg(f.sum("b"), f.grouping_id()),
        "grouping_sets_sql": lambda: df.createOrReplaceTempView("p3_grouping_input") or
            s.sql("SELECT g, a, SUM(b), grouping_id() FROM p3_grouping_input GROUP BY GROUPING SETS ((g),(a),())"),
        "mapInPandas": lambda: df.select("id").mapInPandas(map_batches, schema),
        "applyInPandas": lambda: df.groupBy("g").applyInPandas(group_batches, schema),
        "writer_parquet_options": lambda: roundtrip(False),
        "writer_iceberg": lambda: roundtrip(True),
        "checkpoint": lambda: checkpoint(False),
        "local_checkpoint": lambda: checkpoint(True),
        "cache": cached,
    }


def python_probe(port, path):
    session = base.connect(port)
    output = {}
    for name, operation in python_cases(session, port).items():
        def evaluate():
            value = operation()
            rows = value if isinstance(value, list) else value.collect()
            return fingerprint(rows), [str(row) for row in rows[:8]]

        rec, response = timed(lambda: bounded(evaluate, 40, session.interruptAll))
        if response is not None:
            rec["fingerprint"], rec["sample"] = response
        output[name] = rec
        path.write_text(json.dumps(output))


def execute(result, side, kind, invoke):
    def run(_, stats, container):
        path = base.OUT / f"{side}-client.json"
        output = invoke(path)
        if not path.exists():
            raise RuntimeError(output[-1800:])
        return json.loads(path.read_text())

    extra = {"SAIL_EXECUTION__CHECKPOINT__PATH": "s3://warehouse/p3-checkpoint"} if kind == "sail" else None
    return group(result, side, run, kind, extra=extra)


def main():
    if len(sys.argv) == 4 and sys.argv[1] == "--probe":
        python_probe(int(sys.argv[2]), Path(sys.argv[3]))
        return
    result = begin("api-coverage")
    is_jvm = VARIANT.startswith("jvm")
    newer = VARIANT.endswith("4")
    client_version = PY4 if newer else "3.5.10"
    result["versions"]["client_spark"] = client_version
    result["versions"]["client_language"] = "Java" if is_jvm else "Python"
    with tempfile.TemporaryDirectory(prefix="p3-api-") as directory:
        scratch = Path(directory)
        try:
            if is_jvm:
                scala = "2.13" if newer else "2.12"
                cp_path = scratch / "classpath"
                for attempt in range(3):
                    try:
                        cmd("mvn", "-B", "-U", "-q", "-f", str(TOOLS / "connect-pom.xml"),
                            f"-Dspark.version={client_version}", f"-Dscala.binary.version={scala}",
                            "org.apache.maven.plugins:maven-dependency-plugin:3.8.1:build-classpath",
                            f"-Dmdep.outputFile={cp_path}", timeout=900)
                        break
                    except RuntimeError as exc:
                        result.setdefault("maven_setup_retries", []).append(error(exc))
                        if attempt == 2 or "429" not in str(exc):
                            raise
                        time.sleep(20 * (attempt + 1))
                cp = cp_path.read_text().strip()
                cmd("javac", "-cp", cp, "-d", str(scratch), str(TOOLS / "ConnectProbe.java"), timeout=120)

                def invoke(port, path):
                    raw = cmd("java", "--add-opens=java.base/java.nio=ALL-UNNAMED", "-cp", f"{scratch}:{cp}", "ConnectProbe",
                              f"sc://127.0.0.1:{port}", timeout=420, check=False)
                    (base.OUT / f"jvm-{port}.log").write_text(raw)
                    rows = {}
                    for line in raw.splitlines():
                        if line.startswith("P3\t"):
                            _, name, status, value = line.split("\t", 3)
                            rows[name] = {"status": status, "fingerprint" if status == "ok" else "error": value}
                    if rows:
                        path.write_text(json.dumps(rows))
                    return raw
            else:
                executable = sys.executable
                if newer:
                    venv = scratch / "client"
                    cmd(sys.executable, "-m", "venv", "--system-site-packages", str(venv))
                    executable = str(venv / "bin/python")
                    cmd(executable, "-m", "pip", "install", f"pyspark-client=={client_version}",
                        "pyarrow==17.0.0", "pandas==2.2.3", "py4j==0.10.9.9", timeout=900)
                    (base.OUT / "client-pip-freeze.txt").write_text(cmd(executable, "-m", "pip", "freeze"))

                def invoke(port, path):
                    child = subprocess.run([executable, "-m", "harness.p3.api_coverage", "--probe", str(port), str(path)],
                                           text=True, capture_output=True, timeout=2000)
                    (base.OUT / f"client-{port}.log").write_text(child.stderr)
                    return child.stderr or child.stdout

            def reference(path):
                # Keep a 3.5 client oracle for Python on the reference engine.
                if newer and not is_jvm:
                    child = cmd(sys.executable, "-m", "harness.p3.api_coverage", "--probe", "15002",
                                str(path), check=False, timeout=2000)
                    result["reference_client"] = "3.5.10"
                    return child
                result["reference_client"] = client_version
                return invoke(15002, path)

            spark = execute(result, "reference", "spark", reference)
            sail = execute(result, VARIANT, "sail", lambda path: invoke(50051, path))
            for case in sorted(set(spark or {}) | set(sail or {})):
                ref, target = (spark or {}).get(case, {}), (sail or {}).get(case, {})
                if ref.get("status") != "ok":
                    verdict = "reference error"
                elif target.get("status") != "ok":
                    verdict = "unsupported"
                else:
                    verdict = "match" if ref.get("fingerprint") == target.get("fingerprint") else "differs"
                result["cases"][case] = {"verdict": verdict, "spark": ref, "sail": target}
        except Exception as exc:
            result["error"] = error(exc)
        finally:
            save(result)


if __name__ == "__main__":
    main()
