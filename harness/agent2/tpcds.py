"""Run the 99 SF1 queries against independently started engines."""

import argparse
import re
import tempfile
from importlib.metadata import version
from pathlib import Path

from harness import common as base
from harness.agent2.common import attempt, begin, bounded, collect, engine, fingerprint, median, save

QUERIES = base.ROOT / "harness" / "queries" / "tpcds"
NAMESPACE = "polaris.tpcds_sf1"


def generate_and_load(spark, path):
    import duckdb

    generator = duckdb.connect()
    generator.execute("SET threads=4")
    generator.execute("LOAD tpcds")
    generator.execute("CALL dsdgen(sf=1)")
    tables = [row[0] for row in generator.execute("SHOW TABLES").fetchall()]
    if len(tables) != 24:
        raise RuntimeError(f"expected 24 generated tables, got {len(tables)}")
    for table in tables:
        generator.execute(f"COPY {table} TO '{path / table}.parquet' (FORMAT parquet)")
    generator.close()
    collect(spark, f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")
    rows = {}
    for table in tables:
        collect(spark, f"DROP TABLE IF EXISTS {NAMESPACE}.{table}")
        collect(spark, f"CREATE TABLE {NAMESPACE}.{table} USING iceberg "
                       f"TBLPROPERTIES ('format-version'='2') AS "
                       f"SELECT * FROM parquet.`file://{path / table}.parquet`")
        rows[table] = collect(spark, f"SELECT count(*) FROM {NAMESPACE}.{table}")[0][0]
    return rows


def statements(query):
    text = (QUERIES / f"q{query}.sql").read_text()
    return [sql.strip() for sql in text.split(";") if sql.strip()]


def run_query(spark, query, timeout):
    def run():
        return [spark.sql(sql).collect() for sql in statements(query)]

    return bounded(run, timeout, spark.interruptAll)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--timeout", type=int, default=150)
    args = parser.parse_args()
    result = begin("tpcds")
    result["versions"]["duckdb"] = version("duckdb")
    result["query_source_sha"] = result["versions"]["sail_sha"]
    result["queries"] = {
        str(query): {"verdict": "unverified", "runs": {}}
        for query in range(1, 100)
    }
    for name in ("spark", "comet", "sail"):
        with engine(result, name, output_name="tpcds") as (case, spark, stats, _):
            if name == "spark":
                with tempfile.TemporaryDirectory(prefix="agent2-dsdgen-") as path:
                    result["table_rows"] = generate_and_load(spark, Path(path))
            if not result.get("table_rows"):
                raise RuntimeError("reference data load was incomplete")
            collect(spark, f"USE {NAMESPACE}")
            for index, query in result["queries"].items():
                sql = statements(int(index))
                samples = []
                query["runs"][name] = samples
                for rep in range(args.reps):
                    record, row_sets = attempt(lambda: run_query(spark, int(index), args.timeout), stats)
                    if row_sets is not None:
                        # Only compare row order for explicitly ordered top-level result sets.
                        record["result"] = [
                            fingerprint(rows, bool(re.search(r"(?im)^order by\b", text)))
                            for rows, text in zip(row_sets, sql)
                        ]
                    samples.append(record)
                    if record["status"] == "timeout":
                        break
                base.log(f"{name} q{index}: {[r['status'] for r in samples]}")
                save("tpcds", result)
                if record["status"] == "timeout" and "client still active: True" in record["error"]:
                    # A fresh engine prevents timed-out work from contaminating later timings.
                    case["stopped_after_query"] = index
                    break
    for query in result["queries"].values():
        runs = query["runs"]
        reference = runs.get("spark", [{}])[0].get("result")
        sail = runs.get("sail", [{}])[0].get("result")
        query["verdict"] = ("Spark error" if "spark" in runs and reference is None else
                            "Sail error" if "sail" in runs and sail is None else
                            "unverified" if reference is None or sail is None else
                            "match" if sail == reference else "values differ")
        query["validated_metrics"] = {}
        for name, samples in runs.items():
            if reference is not None and len(samples) == args.reps and all(r.get("result") == reference for r in samples):
                query["validated_metrics"][name] = median(samples)
            elif name == "comet" and reference is not None:
                query["comet_verdict"] = "values differ" if samples[0].get("result") else "Comet error"
    save("tpcds", result)


if __name__ == "__main__":
    main()
