"""Compare Iceberg CTAS and MERGE writes, then verify the full table contents."""

from harness.agent2.common import EngineStartError, attempt, begin, collect, engine, load_h, median, save

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
EXPECTED_ORDERS = f"{DEST}.orders_expected"
DEADLINE = 360
VALIDATION_DEADLINE = 600
REPS = 3


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
    retained = [
        "CASE WHEN s.op = 'update' THEN CAST(o.o_totalprice + 1.00 AS DECIMAL(15,2)) "
        "ELSE o.o_totalprice END AS o_totalprice" if col == "o_totalprice" else f"o.{col}"
        for col in COLUMNS
    ]
    collect(spark, f"DROP TABLE IF EXISTS {EXPECTED_ORDERS} PURGE")
    collect(spark, f"""
        CREATE TABLE {EXPECTED_ORDERS} USING iceberg AS
        SELECT {", ".join(retained)}
        FROM {SOURCE}.orders o LEFT JOIN {MERGE_INPUT} s ON o.o_orderkey = s.o_orderkey
        WHERE s.op IS NULL OR s.op = 'update'
        UNION ALL SELECT {", ".join(COLUMNS)} FROM {MERGE_INPUT} WHERE op = 'insert'
        """, DEADLINE)


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


def key_columns(source):
    return ("l_orderkey", "l_linenumber") if source == "lineitem" else ("o_orderkey",)


def reference_schema(spark, table, source):
    keys = key_columns(source)
    grouped = ", ".join(keys)
    null_key = " OR ".join(f"{key} IS NULL" for key in keys)
    duplicate = collect(spark, f"""
        SELECT {grouped} FROM {table}
        GROUP BY {grouped} HAVING count(*) <> 1 OR {null_key} LIMIT 1
        """, VALIDATION_DEADLINE)
    if duplicate:
        raise RuntimeError(f"{table} has a duplicate or null benchmark key: {duplicate[0]}")
    schema = tuple(spark.table(table).columns)
    rows = collect(spark, f"SELECT count(*) FROM {table}", VALIDATION_DEADLINE)[0][0]
    return keys, schema, rows


def validate(spark, table, reference, keys, columns, expected_count):
    count = collect(spark, f"SELECT count(*) FROM {table}", VALIDATION_DEADLINE)[0][0]
    condition = " AND ".join(f"t.{key} = r.{key}" for key in keys)
    same_values = " AND ".join(f"t.{col} <=> r.{col}" for col in columns)
    key = keys[0]
    result = collect(spark, f"""
        SELECT count_if(t.{key} IS NOT NULL AND (r.{key} IS NULL OR NOT ({same_values}))) AS extra,
               count_if(r.{key} IS NOT NULL AND (t.{key} IS NULL OR NOT ({same_values}))) AS missing
        FROM {table} t FULL OUTER JOIN {reference} r ON {condition}
        """, VALIDATION_DEADLINE)[0]
    # With a unique non-null reference key, matching every row and total count also excludes duplicates.
    return {"extra": max(result.extra, count - expected_count, 0),
            "missing": max(result.missing, expected_count - count, 0)}


def write_case(result, label, name, scenario, source, version, mode, port):
    runs = result["cases"].setdefault(scenario, {}).setdefault(name, [])
    with engine(result, name, label=label, output_name=TASK, port=port) as (_, spark, stats, _):
        for rep in range(REPS):
            target = table_for(name, scenario, rep)
            if mode:
                prep, _ = attempt(lambda: collect(spark, ctas(target, source, version, mode), DEADLINE))
                if prep["status"] != "ok":
                    runs.append(prep | {"phase": "setup", "table": target})
                    break
            sql = merge(target) if mode else ctas(target, source, version, mode)
            record, _ = attempt(lambda: collect(spark, sql, DEADLINE), stats)
            runs.append(record | {"table": target})
            save(TASK, result)
            if record["status"] != "ok":
                break


def inspect_files(spark, table, mode):
    current = files(spark, table)
    before = {}
    if mode:
        snapshots = collect(spark, f"SELECT parent_id FROM {table}.snapshots WHERE parent_id IS NOT NULL", DEADLINE)
        if not snapshots:
            raise RuntimeError("MERGE parent snapshot unavailable")
        before = files(spark, table, snapshots[0][0])
    return {"current": summarize_files(current.values()),
            "added": summarize_files(current[p] for p in current.keys() - before.keys()),
            "removed": summarize_files(before[p] for p in before.keys() - current.keys())}


def verify_scenario(result, scenario, mode, reference, schema, port):
    with engine(result, "spark", label=f"verify-{scenario}", port=port, output_name=TASK,
                extra={"spark.sql.shuffle.partitions": "96"}) as (_, spark, _, _):
        for name, runs in result["cases"][scenario].items():
            completed = [record for record in runs if record["status"] == "ok"]
            for record in completed:
                metadata, measured = attempt(lambda: inspect_files(spark, record["table"], mode))
                record["files"] = measured if measured is not None else metadata
            if not completed:
                continue
            final = completed[-1]
            verdict, differences = attempt(lambda: validate(spark, final["table"], reference, *schema))
            final["comparison"] = differences if differences is not None else verdict
            if differences == {"extra": 0, "missing": 0} and len(completed) == REPS:
                result.setdefault("validated_metrics", {}).setdefault(scenario, {})[name] = median(runs)
            save(TASK, result)
            if verdict["status"] == "timeout":
                raise TimeoutError(verdict["error"])


def main():
    result = begin(TASK)
    result["measurement"].update({"query_timeout_s": DEADLINE,
                                   "validation_timeout_s": VALIDATION_DEADLINE,
                                   "validation_scope": "exact final successful output per operation and engine"})
    result["cases"] = {}
    schemas = {}
    with engine(result, "spark", label="load", output_name=TASK,
                extra={"spark.sql.shuffle.partitions": "96"}) as (_, spark, _, _):
        result["source_load"] = load_h(spark, 10)
        prepare(spark, result)
        for name, source in ((f"{SOURCE}.lineitem", "lineitem"), (f"{SOURCE}.orders", "orders"),
                             (EXPECTED_ORDERS, "orders")):
            schemas[name] = reference_schema(spark, name, source)
    if len(schemas) != 3 or not result.get("merge_source_counts"):
        return
    result["reference_schemas"] = schemas
    for index, (scenario, source, version, mode) in enumerate(SCENARIOS):
        for engine_index, name in enumerate(ENGINES):
            label = f"{name}-{scenario}"
            try:
                write_case(result, label, name, scenario, source, version, mode, 18000 + index * 10 + engine_index)
            except EngineStartError:
                continue
        reference = EXPECTED_ORDERS if mode else f"{SOURCE}.{source}"
        try:
            verify_scenario(result, scenario, mode, reference, schemas[reference], 18000 + index * 10 + 9)
        except EngineStartError:
            continue
    save(TASK, result)


if __name__ == "__main__":
    main()
