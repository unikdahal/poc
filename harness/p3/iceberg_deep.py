"""Exercise Iceberg reader/writer pairs, evolution, maintenance, and concurrent commits."""

from concurrent.futures import ThreadPoolExecutor
from importlib.metadata import version
import threading
import time

import boto3

from harness import common as base
from harness.p3.common import VARIANT, begin, bounded, collect, fingerprint, group, median_if_valid, save, timed

TASK = "iceberg-deep"
NS = "polaris.p3_iceberg"
PREFIX = "poc/p3_iceberg"
N = 12000


def s3_client():
    return boto3.client("s3", endpoint_url="http://localhost:9000", aws_access_key_id=base.S3["key"],
        aws_secret_access_key=base.S3["secret"], region_name=base.S3["region"])


def statements(result, key, spark, sqls):
    records = result["cases"].setdefault(key, [])
    for sql in sqls:
        rec, rows = timed(lambda: collect(spark, sql, 100))
        records.append(rec | {"sql": sql, "output": [row.asDict(recursive=True) for row in rows] if rows is not None else None})
        save(result)
        if rec["status"] == "timeout":
            break
    return records


def pycatalog():
    from pyiceberg.catalog import load_catalog
    return load_catalog("p3", type="rest", uri=f"{base.POLARIS}/api/catalog", warehouse="poc",
        token=base.polaris_token(), **{"s3.endpoint": "http://localhost:9000", "s3.access-key-id": base.S3["key"],
        "s3.secret-access-key": base.S3["secret"], "s3.region": base.S3["region"], "s3.force-virtual-addressing": "false"})


def interop(result):
    ref = {}
    targets = [(writer, v, f"{NS}.{writer}_v{v}") for writer in ("spark", "sail", "pyiceberg") for v in (2, 3)]
    row_sql = "SELECT CAST(id AS BIGINT) AS id, concat('row-', id) AS payload FROM range(12)"
    digest_sql = lambda table: f"SELECT id, payload FROM {table} ORDER BY id"

    def spark_setup(spark, *_):
        collect(spark, f"CREATE NAMESPACE {NS}")
        ref["rows"] = fingerprint(collect(spark, row_sql), True)
        for writer, v, table in targets:
            if writer == "spark":
                statements(result, f"write-{writer}-{v}", spark, [
                    f"CREATE TABLE {table} USING iceberg TBLPROPERTIES ('format-version'='{v}') AS {row_sql}"])
    group(result, "spark-writer", spark_setup, "spark")

    def writes(sail, *_):
        for writer, v, table in targets:
            if writer == "sail":
                statements(result, f"write-{writer}-{v}", sail, [
                    f"CREATE TABLE {table} USING iceberg TBLPROPERTIES ('format-version'='{v}') AS {row_sql}"])
    group(result, "sail-writer", writes, "sail")
    try:
        import pyarrow as pa
        from pyiceberg.schema import Schema
        from pyiceberg.types import LongType, NestedField, StringType
        result["versions"]["pyiceberg"] = version("pyiceberg")
        pc = pycatalog()
        for writer, v, table in targets:
            if writer == "pyiceberg":
                def write():
                    t = pc.create_table(table.removeprefix("polaris."), Schema(
                        NestedField(1, "id", LongType(), required=False), NestedField(2, "payload", StringType(), required=False)),
                        properties={"format-version": str(v)})
                    t.append(pa.table({"id": list(range(12)), "payload": [f"row-{i}" for i in range(12)]}))
                record, _ = timed(write)
                result["cases"][f"write-{writer}-{v}"] = record
    except Exception as exc:
        from harness.p3.common import error
        result["cases"]["pyiceberg-startup"] = {"error": error(exc)}
        pc = None
    save(result)

    def check(reader, spark, *_):
        for writer, v, table in targets:
            record, rows = timed(lambda: collect(spark, digest_sql(table), 60))
            record["matches_spark"] = rows is not None and fingerprint(rows, True) == ref.get("rows")
            record["rows"] = [list(row) for row in rows] if rows is not None else None
            result["cases"][f"{writer}-v{v}-to-{reader}"] = record

    for reader in ("spark", "sail"):
        group(result, f"{reader}-reader", lambda *args: check(reader, *args), reader, port=15018 if reader == "spark" else 50052)
    if pc:
        for writer, v, table in targets:
            record, payload = timed(lambda: pc.load_table(table.removeprefix("polaris.")).scan().to_arrow().to_pylist())
            rows = sorted((r["id"], r["payload"]) for r in payload) if payload is not None else None
            result["cases"][f"{writer}-v{v}-to-pyiceberg"] = record | {
                "rows": rows, "matches_spark": rows is not None and fingerprint(rows, True) == ref.get("rows")}
    result["limits"] = {"trino_or_flink": "additional JVM engine excluded from this matrix under the hosted disk and memory limits",
                         "equality_delete_writer": "no supported PyIceberg equality-delete writer API was used"}
    save(result)


def evolution(result):
    table = f"{NS}.nested"
    transforms = f"{NS}.transforms"
    queries = {
        "nested": f"SELECT id, s.label, s.w, s.added, m, a FROM {table} ORDER BY id",
        "pruning": f"SELECT id,v FROM {transforms} WHERE id=42 AND v='abcdef' ORDER BY id",
    }
    reference = {}

    def prepare(spark, *_):
        statements(result, "nested-changes", spark, [
            f"CREATE NAMESPACE {NS}",
            f"""CREATE TABLE {table} USING iceberg AS SELECT id,
              named_struct('name','name','w',CAST(1 AS INT),'obsolete',1) AS s,
              map('key', named_struct('decimal_value', CAST(1.25 AS DECIMAL(6,2)))) AS m,
              array(named_struct('amount', CAST(0.5 AS FLOAT), 'other', 2)) AS a FROM range(4)""",
            f"ALTER TABLE {table} RENAME COLUMN s.name TO label",
            f"ALTER TABLE {table} ADD COLUMN s.added STRING",
            f"ALTER TABLE {table} DROP COLUMN s.obsolete",
            f"ALTER TABLE {table} ALTER COLUMN s.w FIRST",
            f"ALTER TABLE {table} ALTER COLUMN s.w TYPE BIGINT",
            f"ALTER TABLE {table} ALTER COLUMN a.element.amount TYPE DOUBLE",
            f"ALTER TABLE {table} ALTER COLUMN m.value.decimal_value TYPE DECIMAL(12,2)",
        ])
        statements(result, "partition-evolution", spark, [
            f"CREATE TABLE {transforms} (id BIGINT, v STRING, ts TIMESTAMP, region INT) USING iceberg "
            f"PARTITIONED BY (bucket(8,id), truncate(3,v), hours(ts), region)",
            f"INSERT INTO {transforms} SELECT id,'abcdef',TIMESTAMP '2024-01-01 12:00:00',CAST(id%2 AS INT) FROM range(80)",
            f"ALTER TABLE {transforms} DROP PARTITION FIELD truncate(3,v)",
            f"ALTER TABLE {transforms} ADD PARTITION FIELD truncate(4,v)",
            f"INSERT INTO {transforms} SELECT 42,'abcdef',TIMESTAMP '2024-01-02 12:00:00',0",
            f"SELECT spec_id, count(*) AS files FROM {transforms}.files GROUP BY spec_id ORDER BY spec_id",
        ])
        for key, statement in queries.items():
            rec, rows = timed(lambda: collect(spark, statement))
            if rows is not None:
                reference[key] = fingerprint(rows, True)
            result["cases"][f"spark-{key}"] = rec | {"result": reference.get(key)}
        statements(result, "reference-plan", spark, [f"EXPLAIN FORMATTED {queries['pruning']}"])

    group(result, "spark-evolution", prepare, "spark")
    def read(sail, *_):
        for key, statement in queries.items():
            rec, rows = timed(lambda: collect(sail, statement))
            result["cases"][f"sail-{key}"] = rec | {"matches_spark": rows is not None and
                fingerprint(rows, True) == reference.get(key), "rows": [list(row) for row in rows] if rows is not None else None}
        statements(result, "sail-plan", sail, [f"EXPLAIN ANALYZE {queries['pruning']}"])
    group(result, "sail-evolution", read, "sail")


def merge_sql(table, v):
    return (f"MERGE INTO {table} t USING (SELECT id FROM range(0,100)) s ON t.id=s.id "
            f"WHEN MATCHED THEN UPDATE SET v=t.v+{v}")


def maintenance(result):
    table = f"{NS}.merge_history"
    baseline = {}

    def prepare(spark, *_):
        statements(result, "create-maintenance-table", spark, [
            f"CREATE NAMESPACE {NS}",
            f"CREATE TABLE {table} USING iceberg TBLPROPERTIES ('format-version'='2',"
            "'write.merge.mode'='merge-on-read','write.distribution-mode'='hash','write.target-file-size-bytes'='1048576') "
            f"AS SELECT id, CAST(0 AS BIGINT) AS v FROM range({N})",
            f"ALTER TABLE {table} WRITE ORDERED BY id",
        ])
        for key in (1, 5, 20, 50):
            baseline[key] = fingerprint(collect(spark, f"SELECT sum(v) FROM (SELECT IF(id<100,{key},0) AS v FROM range({N}))"))
    group(result, "spark-maintenance-reference", prepare, "spark")

    def measures(spark, stats, name, expected):
        records = []
        for _ in range(3):
            record, rows = timed(lambda: collect(spark, f"SELECT sum(v) FROM {table}", 60), stats)
            record["matches_spark"] = rows is not None and fingerprint(rows) == expected
            records.append(record)
        result["cases"][name] = {"samples": records, "warm_median": median_if_valid(records)}

    done = 0
    for trial, checkpoint in enumerate((1, 5, 20, 50)):
        def writes(spark, stats, *_):
            for _ in range(done, checkpoint):
                rec, _ = timed(lambda: collect(spark, merge_sql(table, 1), 60))
                result["cases"].setdefault("merge_attempts", []).append(rec)
                if rec["status"] != "ok":
                    raise RuntimeError(rec)
            measures(spark, stats, f"sail-after-{checkpoint}", baseline.get(checkpoint))
            return True
        success = group(result, f"sail-merge-{checkpoint}", writes, "sail", port=50220 + trial,
                        extra={"SAIL_EXECUTION__DEFAULT_PARALLELISM": "4"})
        if not success:
            break
        done = checkpoint
        def inspect(sp, st, *_):
            measures(sp, st, f"spark-after-{checkpoint}", baseline.get(checkpoint))
            statements(result, f"files-after-{checkpoint}", sp, [
                f"SELECT content,spec_id,sort_order_id,count(*) files,min(file_size_in_bytes),max(file_size_in_bytes) FROM {table}.files GROUP BY 1,2,3",
                f"SELECT count(*) FROM {table}.manifests"])
        group(result, f"spark-files-{checkpoint}", inspect, "spark", port=15220 + trial)
    if done == 50:
        def compact(spark, stats, *_):
            statements(result, "compaction", spark, [
                "CALL polaris.system.rewrite_data_files(table => 'p3_iceberg.merge_history', options => map('rewrite-all','true'))",
                "CALL polaris.system.rewrite_position_delete_files(table => 'p3_iceberg.merge_history')",
                f"SELECT content,count(*) FROM {table}.files GROUP BY content"])
            measures(spark, stats, "spark-after-compaction", baseline[50])
            small = f"{NS}.metadata_growth"
            collect(spark, f"CREATE TABLE {small} (id BIGINT) USING iceberg")
            record = result["cases"]["metadata_growth"] = {"completed_commits": 0}
            for n in range(1, 501):
                collect(spark, f"INSERT INTO {small} VALUES ({n})", 45)
                record["completed_commits"] = n
                if n in (1, 50, 100, 250, 500):
                    keys = s3_client().list_objects_v2(Bucket="warehouse", Prefix=f"{PREFIX}/metadata_growth/metadata/")
                    metadata = [item for item in keys.get("Contents", []) if item["Key"].endswith("metadata.json")]
                    newest = max(metadata, key=lambda item: item["Key"]) if metadata else None
                    manifests = collect(spark, f"SELECT count(*) FROM {small}.manifests", 45)[0][0]
                    record[str(n)] = {"metadata_json_bytes": newest["Size"] if newest else None,
                                      "manifest_count": manifests}
                    save(result)
        group(result, "spark-compaction", compact, "spark", port=15250)
        group(result, "sail-after-compaction", lambda spark, stats, *_: measures(spark, stats,
              "sail-after-compaction", baseline[50]), "sail", port=50250)


def conflict(result):
    table = f"{NS}.conflict"
    for isolation in ("serializable", "snapshot"):
        for peer in ("spark", "sail"):
            for trial in range(3):
                suffix = f"{isolation}_{peer}_{trial}"
                target = f"{table}_{suffix}"
                result["cases"][suffix] = case = {}

                def prepare(spark, *_):
                    collect(spark, f"CREATE NAMESPACE IF NOT EXISTS {NS}")
                    collect(spark, f"CREATE TABLE {target} USING iceberg TBLPROPERTIES "
                        f"('write.merge.mode'='merge-on-read','write.merge.isolation-level'='{isolation}') "
                        f"AS SELECT id, CAST(0 AS BIGINT) AS v FROM range({N})")
                    return True
                if not group(result, f"prepare-{suffix}", prepare, "spark"):
                    continue

                def first(sail, *rest):
                    def second(other, *_):
                        barrier = threading.Barrier(2)
                        def execute(session):
                            barrier.wait(10)
                            started = time.monotonic()
                            rec, _ = timed(lambda: collect(session, merge_sql(target, 1), 90))
                            return rec | {"started": started}
                        with ThreadPoolExecutor(max_workers=2) as pool:
                            future = pool.submit(execute, sail)
                            case["peer"] = execute(other)
                            case["sail"] = future.result()
                    if peer == "spark":
                        group(result, f"peer-{suffix}", second, "spark", port=15191)
                    else:
                        second(base.connect(50051))
                group(result, f"writer-{suffix}", first, "sail")
                def verify(spark, *_):
                    rows = collect(spark, f"SELECT v,count(*) AS n FROM {target} WHERE id<100 GROUP BY v ORDER BY v")
                    case["distribution"] = [row.asDict() for row in rows]
                    successful = sum(case.get(label, {}).get("status") == "ok" for label in ("sail", "peer"))
                    case["matches_committed_increments"] = len(rows) == 1 and rows[0].v == successful and rows[0].n == 100
                    failures = [label for label in ("sail", "peer") if case.get(label, {}).get("status") != "ok"]
                    if failures and case["matches_committed_increments"]:
                        case["retry"] = statements(result, f"retry-{suffix}", spark, [merge_sql(target, 1),
                            f"SELECT v,count(*) AS n FROM {target} WHERE id<100 GROUP BY v"])
                group(result, f"verify-{suffix}", verify, "spark", port=15211 + trial)
                save(result)


def main():
    result = begin(TASK)
    base.start_token_refresher()
    {"interop": interop, "evolution": evolution, "maintenance": maintenance, "conflict": conflict}[VARIANT](result)
    save(result)


if __name__ == "__main__":
    main()
