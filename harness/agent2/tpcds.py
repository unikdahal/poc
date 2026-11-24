"""Run the 99 SF1 queries against independently started engines."""

import argparse
import re
import tempfile
from importlib.metadata import version
from pathlib import Path

from harness import common as base, tpch
from harness.agent2.common import attempt, begin, bounded, collect, engine, fingerprint, median, save

QUERIES = base.ROOT / "harness" / "queries" / "tpcds"
NAMESPACE = "polaris.tpcds_sf1"


def generate_and_load(spark, path):
    import duckdb
    import pyarrow.parquet as parquet
    from duckdb_extensions import import_extension

    import_extension("tpcds")

    generator = duckdb.connect()
    generator.execute("SET threads=4")
    generator.execute("LOAD tpcds")
    generator.execute("CALL dsdgen(sf=1)")
    tables = [row[0] for row in generator.execute("SHOW TABLES").fetchall()]
    if len(tables) != 24:
        raise RuntimeError(f"expected 24 generated tables, got {len(tables)}")
    for table in tables:
        reader = generator.execute(f"SELECT * FROM {table}").fetch_record_batch(rows_per_batch=128_000)
        with parquet.ParquetWriter(path / f"{table}.parquet", reader.schema,
                                   compression="snappy", use_dictionary=False, data_page_version="1.0") as writer:
            for batch in reader:
                writer.write_batch(batch)
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
    if args.reps < 2 or args.timeout <= 0:
        parser.error("at least two repetitions and a positive timeout are required")
    result = begin("tpcds")
    result["measurement"].update({"warm_samples": args.reps - 1, "query_timeout_s": args.timeout})
    result["versions"].update({package: version(package) for package in
                               ("duckdb", "duckdb-extensions", "duckdb-extension-tpcds", "pyarrow")})
    result["query_source_sha"] = result["versions"]["sail_sha"]
    result["parquet_export"] = {"compression": "snappy", "dictionary": False, "data_page_version": "1.0",
                                "batch_rows": 128_000}
    result["queries"] = {
        str(query): {"verdict": "unverified", "runs": {}}
        for query in range(1, 100)
    }
    for name in ("spark", "comet", "sail"):
        remaining = iter(result["queries"].items())
        while True:
            resumed = False
            cycle = sum(label == name or label.startswith(f"{name}-restart") for label in result.get("engines", {}))
            label = f"{name}-restart-{cycle}" if cycle else name
            with engine(result, name, label=label, output_name="tpcds") as (case, spark, stats, _):
                if name == "spark" and not result.get("table_rows"):
                    with tempfile.TemporaryDirectory(prefix="agent2-dsdgen-") as path:
                        result["table_rows"] = generate_and_load(spark, Path(path))
                if not result.get("table_rows"):
                    raise RuntimeError("reference data load was incomplete")
                bounded(lambda: tpch.use(spark, NAMESPACE.split(".")[1]), 60, spark.interruptAll)
                for index, query in remaining:
                    sql = statements(int(index))
                    samples = []
                    query["runs"][name] = samples
                    for _ in range(args.reps):
                        record, row_sets = attempt(lambda: run_query(spark, int(index), args.timeout), stats)
                        if row_sets is not None:
                            record["result"] = [
                                fingerprint(rows, bool(re.search(r"(?im)^order by\b", text)))
                                for rows, text in zip(row_sets, sql)
                            ]
                        samples.append(record)
                        if record["status"] == "timeout":
                            resumed = True
                            case["timed_out_query"] = index
                            break
                    compare(query, args.reps)
                    base.log(f"{name} q{index}: {[r['status'] for r in samples]}")
                    save("tpcds", result)
                    if resumed:
                        break
            if not resumed:
                break
    save("tpcds", result)


def compare(query, repetitions):
    runs = query["runs"]
    reference = runs.get("spark", [{}])[0].get("result")
    sail = runs.get("sail", [{}])[0].get("result")
    query["verdict"] = (
        "Spark error" if any(sample["status"] != "ok" for sample in runs.get("spark", [])) else
        "Sail error" if any(sample["status"] != "ok" for sample in runs.get("sail", [])) else
        "unverified" if reference is None or sail is None else
        "values differ" if any(sample["result"] != reference for sample in runs["sail"]) else "match"
    )
    query["validated_metrics"] = {}
    for name, samples in runs.items():
        if reference is not None and len(samples) == repetitions and all(r.get("result") == reference for r in samples):
            query["validated_metrics"][name] = median(samples)
        elif name == "comet" and reference is not None:
            query["comet_verdict"] = "Comet error" if any(r["status"] != "ok" for r in samples) else "values differ"

if __name__ == "__main__":
    main()
