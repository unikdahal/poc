"""Generate TPC-H data with tpchgen-cli and load it into Iceberg tables via the Spark reference engine."""

import argparse
import subprocess
import time
from pathlib import Path

from harness.common import connect, log, write_json

TABLES = ["nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem"]


def generate(sf: float, root: Path) -> Path:
    d = root / f"sf{sf:g}"
    if not (d / "lineitem.parquet").exists() and not (d / "lineitem").exists():
        d.mkdir(parents=True, exist_ok=True)
        log(f"generating TPC-H sf={sf:g} into {d}")
        subprocess.run(["tpchgen-cli", "parquet", "-s", f"{sf:g}", f"--output-dir={d}"],
                       check=True)
    return d


def load(spark, sf: float, d: Path, namespace: str) -> dict:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS polaris.{namespace}")
    timings = {}
    for t in TABLES:
        src = d / f"{t}.parquet"
        if not src.exists():
            src = d / t
        t0 = time.time()
        spark.sql(f"DROP TABLE IF EXISTS polaris.{namespace}.{t}")
        spark.sql(
            f"CREATE TABLE polaris.{namespace}.{t} USING iceberg "
            f"TBLPROPERTIES ('format-version'='2') AS SELECT * FROM parquet.`file://{src}`"
        )
        n = spark.sql(f"SELECT count(*) FROM polaris.{namespace}.{t}").collect()[0][0]
        timings[t] = {"rows": n, "seconds": round(time.time() - t0, 2)}
        log(f"loaded {namespace}.{t}: {n} rows")
    return timings


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=float, nargs="+", default=[1])
    ap.add_argument("--port", type=int, default=15002)
    ap.add_argument("--root", default="/tmp/tpch")
    a = ap.parse_args()
    spark = connect(a.port)
    res = {}
    for sf in a.sf:
        d = generate(sf, Path(a.root))
        res[f"sf{sf:g}"] = load(spark, sf, d, f"tpch_sf{sf:g}".replace(".", "_"))
    write_json("data-load", res)


if __name__ == "__main__":
    main()
