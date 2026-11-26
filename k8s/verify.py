"""Smoke test for a Sail POC deployment. Run from any machine that can reach port 50051.

  pip install "pyspark-client==4.2.1"          # or pyspark[connect]==3.5.x
  python verify.py sc://localhost:50051 <read_namespace> <table> sail_poc

Prints one line per check and exits non-zero if a required check fails.
"""
import json
import sys
import time

from pyspark.sql import SparkSession

url, read_ns, table, write_ns = sys.argv[1:5]
spark = SparkSession.builder.remote(url).getOrCreate()
results, failed = [], False


def check(name, fn, expect_error=False):
    global failed
    t0 = time.perf_counter()
    try:
        value = fn()
        ok = not expect_error
        detail = value
    except Exception as e:  # noqa: BLE001
        ok = expect_error
        detail = str(e).strip().splitlines()[0][:300]
    secs = round(time.perf_counter() - t0, 3)
    results.append({"check": name, "ok": ok, "seconds": secs, "detail": str(detail)[:300]})
    print(f"{'PASS' if ok else 'FAIL'}  {name:<42} {secs:>8.2f}s  {str(detail)[:120]}", flush=True)
    failed |= not ok


check("1. server answers SELECT 1", lambda: spark.sql("SELECT 1 AS x").collect()[0].x)
check("2. Polaris catalog lists namespaces", lambda: [r[0] for r in spark.sql("SHOW NAMESPACES IN polaris").collect()])
check(f"3. read polaris.{read_ns}.{table}", lambda: spark.sql(f"SELECT count(*) FROM polaris.{read_ns}.{table}").collect()[0][0])
check("4. worker pods run a shuffle query", lambda: spark.table(f"polaris.{read_ns}.{table}").repartition(8).count())
t = f"polaris.{write_ns}.sail_smoke"
check(f"5. create + write {t}", lambda: (spark.sql(f"DROP TABLE IF EXISTS {t}").collect(),
      spark.sql(f"CREATE TABLE {t} (id BIGINT, v STRING) USING iceberg TBLPROPERTIES ('format-version'='2')").collect(),
      spark.sql(f"INSERT INTO {t} VALUES (1,'a'),(2,'b'),(3,'c')").collect(),
      spark.sql(f"DELETE FROM {t} WHERE id = 2").collect(),
      sorted(r.id for r in spark.sql(f"SELECT id FROM {t}").collect()))[-1])
check(f"6. write to {read_ns} is refused", lambda: spark.sql(
    f"CREATE TABLE polaris.{read_ns}.sail_should_fail (id INT) USING iceberg").collect(), expect_error=True)
check(f"7. drop {t}", lambda: spark.sql(f"DROP TABLE {t}").collect())
json.dump(results, open("verify-results.json", "w"), indent=2)
sys.exit(1 if failed else 0)
