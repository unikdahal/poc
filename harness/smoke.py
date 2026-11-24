"""Smoke test: Spark reference engine + Polaris + MinIO + TPC-H load + 22 queries."""

import json
import sys

from harness import tpch
from harness.common import OUT, connect, log, start_spark_connect, stop_proc, warm, write_json
from harness.data import generate, load


def main():
    proc, boot = start_spark_connect(comet="--comet" in sys.argv)
    log(f"spark connect up in {boot:.1f}s")
    try:
        spark = connect(15002)
        warm(spark)
        d = generate(1, __import__("pathlib").Path("/tmp/tpch"))
        loadres = load(spark, 1, d, "tpch_sf1")
        res = {"boot_seconds": boot, "load": loadres, "queries": {}}
        for q in range(1, 23):
            try:
                rows, cols, secs = tpch.run_query(spark, q, "tpch_sf1")
                res["queries"][q] = {"seconds": round(secs, 3), "rows": len(rows)}
                (OUT / "ref").mkdir(exist_ok=True)
                (OUT / "ref" / f"tpch_sf1_q{q}.json").write_text(json.dumps(
                    {"columns": cols, "rows": tpch.canon_rows(rows)}, default=str))
            except Exception as e:  # noqa: BLE001
                res["queries"][q] = {"error": str(e)[:500]}
            log(f"q{q}: {res['queries'][q]}")
        write_json("smoke", res)
    finally:
        stop_proc(proc)


if __name__ == "__main__":
    main()
