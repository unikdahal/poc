"""Compare Iceberg CTAS and MERGE writes, then verify the full table contents."""

from harness import common as base, data
from harness.agent2.common import attempt, begin, bounded, collect, engine, median, save

TASK = "ingest"
SOURCE = "polaris.tpch_sf10"
DEST = "polaris.agent2_ingest"
ENGINES = ("spark", "comet", "sail", "sail-cluster")
SCENARIOS = (("ctas_lineitem", "lineitem", 2, None), ("ctas_orders", "orders", 2, None)) + tuple(
    (f"merge_v{version}_{mode}", "orders", version, mode)
    for version in (2, 3) for mode in ("copy-on-write", "merge-on-read")
)
COLUMNS = ("o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate",
           "o_orderpriority", "o_clerk", "o_shippriority", "o_comment")
MERGE_INPUT = f"{DEST}.merge_input"
DEADLINE = 1200


def prepare(spark, result):
    collect(spark, f"CREATE NAMESPACE IF NOT EXISTS {DEST}")
    collect(spark, f"DROP TABLE IF EXISTS {MERGE_INPUT} PURGE")
    columns = ", ".join(f"o.{col}" for col in COLUMNS[1:])
    collect(spark, f"""
        CREATE TABLE {MERGE_INPUT} USING iceberg AS
        WITH ranked AS (
          SELECT o_orderkey, pmod(row_number() OVER (ORDER BY o_orderkey) - 1, 20) AS bucket
          FROM {SOURCE}.orders
        )
        SELECT CASE WHEN r.bucket = 3 THEN o.o_orderkey + 1000000000 ELSE o.o_orderkey END AS o_orderkey,
               {columns},
               CASE WHEN r.bucket = 2 THEN 'delete' WHEN r.bucket = 3 THEN 'insert' ELSE 'update' END AS op
        FROM {SOURCE}.orders o JOIN ranked r ON o.o_orderkey = r.o_orderkey
        WHERE r.bucket < 4
        """, DEADLINE)
    result["merge_source_counts"] = {
        row.op: row.n for row in collect(spark, f"SELECT op, count(*) AS n FROM {MERGE_INPUT} GROUP BY op")
    }
    result["orders_row_count"] = collect(spark, f"SELECT count(*) FROM {SOURCE}.orders")[0][0]


def table_for(name, scenario, rep):
    return f"{DEST}.{name.replace('-', '_')}_{scenario.replace('-', '_')}_{rep}"


def ctas(target, source, version, mode):
    properties = f"'format-version'='{version}'"
    if mode:
        properties += f", 'write.merge.mode'='{mode}'"
    return f"CREATE TABLE {target} USING iceberg TBLPROPERTIES ({properties}) AS SELECT * FROM {SOURCE}.{source}"


def merge(target):
    return f"""
        MERGE INTO {target} t USING {MERGE_INPUT} s ON t.o_orderkey = s.o_orderkey
        WHEN MATCHED AND s.op = 'delete' THEN DELETE
        WHEN MATCHED AND s.op = 'update' THEN UPDATE SET o_totalprice = t.o_totalprice + 1.00
        WHEN NOT MATCHED AND s.op = 'insert' THEN INSERT ({", ".join(COLUMNS)})
        VALUES ({", ".join("s." + col for col in COLUMNS)})
        """


def files(spark, table, snapshot=None):
    relation = f"{table}.files" + (f" VERSION AS OF {snapshot}" if snapshot is not None else "")
    rows = collect(spark, f"SELECT content, file_path, file_size_in_bytes, record_count FROM {relation}", DEADLINE)
    return {row.file_path: (row.content, row.file_size_in_bytes, row.record_count) for row in rows}


def summarize_files(entries):
    counts = {}
    for content, size, rows in entries:
        row = counts.setdefault(str(content), {"files": 0, "bytes": 0, "records": 0})
        row["files"] += 1
        row["bytes"] += size
        row["records"] += rows
    return counts


def validate(spark, table, reference_sql):
    columns = ", ".join(COLUMNS) if ".merge_v" in table or "_merge_v" in table else "*"
    target = f"SELECT {columns} FROM {table}"
    sql = f"""
        SELECT
          (SELECT count(*) FROM ({target} EXCEPT ALL {reference_sql})) AS extra,
          (SELECT count(*) FROM ({reference_sql} EXCEPT ALL {target})) AS missing
        """
    return collect(spark, sql, DEADLINE)[0].asDict()


def main():
    result = begin(TASK)
    result["measurement"]["query_timeout_s"] = DEADLINE
    result["cases"] = {}
    with engine(result, "spark", label="load", output_name=TASK) as (_, spark, _, _):
        from pathlib import Path
        generated = data.generate(10, Path("/tmp/tpch"))
        result["source_load"] = bounded(lambda: data.load(spark, 10, generated, "tpch_sf10"),
                                        2400, spark.interruptAll)
        prepare(spark, result)
    if not result.get("merge_source_counts"):
        return
    for name in ENGINES:
        with engine(result, name, output_name=TASK) as (_, spark, stats, _):
            for scenario, source, version, mode in SCENARIOS:
                runs = result["cases"].setdefault(scenario, {}).setdefault(name, [])
                for rep in range(3):
                    target = table_for(name, scenario, rep)
                    snapshot = None
                    if mode:
                        def setup_table():
                            collect(spark, ctas(target, source, version, mode), DEADLINE)
                            # Read the parent snapshot later through the reference engine.

                        prep, snapshot = attempt(setup_table)
                        if prep["status"] != "ok":
                            runs.append(prep | {"phase": "setup", "table": target})
                            break
                    sql = merge(target) if mode else ctas(target, source, version, mode)
                    record, _ = attempt(lambda: collect(spark, sql, DEADLINE), stats)
                    runs.append(record | {"table": target, "base_snapshot": snapshot})
                    save(TASK, result)
                    if record["status"] == "timeout":
                        raise TimeoutError(record["error"])

    with engine(result, "spark", label="verify", output_name=TASK) as (_, spark, _, _):
        for scenario, source, version, mode in SCENARIOS:
            treatments = result["cases"].get(scenario, {})
            ref = table_for("spark", scenario, 0)
            reference = f"SELECT * FROM {SOURCE}.{source}"
            if mode:
                reference = f"SELECT {', '.join(COLUMNS)} FROM {ref}"
            spark_runs = treatments.get("spark", [])
            if not spark_runs or spark_runs[0]["status"] != "ok":
                continue
            for name, runs in treatments.items():
                for record in runs:
                    if record["status"] != "ok":
                        continue
                    table = record["table"]
                    def inspect():
                        current = files(spark, table)
                        before = {}
                        if mode:
                            snapshot = record["base_snapshot"]
                            if snapshot is None:
                                snapshots = collect(spark, f"SELECT parent_id FROM {table}.snapshots "
                                                           "WHERE parent_id IS NOT NULL", DEADLINE)
                                snapshot = snapshots[0][0] if snapshots else None
                            if snapshot is None:
                                raise RuntimeError("MERGE parent snapshot unavailable")
                            before = files(spark, table, snapshot)
                        return {"current": summarize_files(current.values()),
                                "added": summarize_files(current[p] for p in current.keys() - before.keys()),
                                "removed": summarize_files(before[p] for p in before.keys() - current.keys())}

                    analysis, measured = attempt(inspect)
                    record["files"] = measured if measured is not None else analysis
                    verdict, differences = attempt(lambda: validate(spark, table, reference))
                    record["comparison"] = differences if differences is not None else verdict
                    save(TASK, result)
                if len(runs) == 3 and all(
                    r.get("comparison") == {"extra": 0, "missing": 0} for r in runs
                ):
                    result.setdefault("validated_metrics", {}).setdefault(scenario, {})[name] = median(runs)
    save(TASK, result)


if __name__ == "__main__":
    main()
