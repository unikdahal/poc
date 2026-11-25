"""Exercise Iceberg reader/writer pairs, evolution, maintenance, and concurrent commits."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from importlib.metadata import version
from io import BytesIO
import json
import threading
import time

import boto3

from harness import common as base
from harness.p3.common import VARIANT, begin, bounded, cmd, collect, error, fingerprint, group, median_if_valid, save, timed

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
        token=base.polaris_token(), **{"header.X-Iceberg-Access-Delegation": "", "s3.endpoint": "http://localhost:9000", "s3.access-key-id": base.S3["key"],
        "s3.secret-access-key": base.S3["secret"], "s3.region": base.S3["region"], "s3.force-virtual-addressing": "false"})


FLINK_VERSION = "1.20.4"
FLINK_IMAGE = "flink:1.20.4-scala_2.12-java17@sha256:912157e4e22718a5fa712cd3d3ce90fe0be10679b40fcb56aeaae42aad19f254"
ICEBERG_VERSION = "1.12.0"
HADOOP_CLIENT_VERSION = "3.3.4"
FLINK_CATALOG = """CREATE CATALOG p3cat WITH ('type'='iceberg','catalog-type'='rest',
'uri'='http://localhost:8181/api/catalog','warehouse'='poc','credential'='root:s3cr3t',
'scope'='PRINCIPAL_ROLE:ALL','io-impl'='org.apache.iceberg.aws.s3.S3FileIO',
's3.endpoint'='http://localhost:9000','s3.path-style-access'='true','s3.access-key-id'='admin',
's3.secret-access-key'='password','client.region'='us-east-1');"""


@contextmanager
def flink_service(result):
    from harness.p3.common import sail_state

    result["versions"]["flink"] = {"version": FLINK_VERSION, "image": FLINK_IMAGE,
                                     "iceberg_runtime": ICEBERG_VERSION, "hadoop_client": HADOOP_CLIENT_VERSION, "scala": "2.12"}
    with tempfile.TemporaryDirectory(prefix="p3-flink-") as directory:
        work = Path(directory)
        mounts = []
        dependencies = [("org/apache/iceberg", name, ICEBERG_VERSION)
                        for name in ("iceberg-flink-runtime-1.20", "iceberg-aws-bundle")]
        dependencies += [("org/apache/hadoop", name, HADOOP_CLIENT_VERSION)
                         for name in ("hadoop-client-api", "hadoop-client-runtime")]
        for organization, name, release in dependencies:
            artifact = f"{name}-{release}.jar"
            path = work / artifact
            cmd("curl", "-fsSL", "--retry", "3", "--retry-all-errors",
                f"https://repo.maven.apache.org/maven2/{organization}/{name}/{release}/{artifact}",
                "-o", str(path), timeout=180)
            mounts.extend(("-v", f"{path}:/opt/flink/lib/{artifact}:ro"))
        container = "p3-flink"
        try:
            cmd("docker", "run", "-d", "--name", container, "--network", "host", "--memory", "5g", "--cpus", "3",
                "-e", "AWS_REGION=us-east-1", *mounts, FLINK_IMAGE,
                "bash", "-c", "bin/start-cluster.sh && tail -f /dev/null", timeout=300)
            base.wait_port(8081, timeout=90)
            yield container
        finally:
            launcher = cmd("docker", "logs", "--tail", "800", container, check=False)
            logs = cmd("docker", "exec", container, "bash", "-c", "tail -n 250 /opt/flink/log/*.log", check=False)
            (base.OUT / "flink-service.log").write_text(launcher + "\n" + logs)
            cmd("docker", "stop", "-t", "6", container, check=False, timeout=15)
            result["flink_container_state"] = sail_state(container)
            cmd("docker", "rm", "-f", container, check=False)


def flink_sql(container, label, *statements_):
    script = "\n".join([
        "SET 'table.dml-sync'='true';",
        "SET 'sql-client.execution.result-mode'='tableau';",
        "SET 'parallelism.default'='1';", FLINK_CATALOG, *statements_, "QUIT;",
    ])
    with (base.OUT / f"flink-{label}.log").open("w") as log:
        end = subprocess.run(["docker", "exec", "-i", container, "bash", "-c",
                              "cat > /tmp/p3.sql && bin/sql-client.sh embedded -f /tmp/p3.sql"],
                             input=script, text=True, stdout=log, stderr=subprocess.STDOUT, timeout=240)
    raw = (base.OUT / f"flink-{label}.log").read_text(errors="replace")
    plain = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", raw)
    if end.returncode or "[ERROR]" in plain or "Could not execute SQL" in plain:
        raise RuntimeError(plain[-1900:])
    return plain


def interop(result):
    ref = {}
    ordinary = [(writer, v, "base", f"{NS}.{writer}_v{v}") for writer in ("spark", "sail", "pyiceberg", "flink") for v in (2, 3)]
    deleted = [(writer, v, "delete", f"{NS}.{writer}_delete_v{v}") for writer in ("spark", "sail") for v in (2, 3)]
    equality = [("flink", v, "equality", f"{NS}.flink_equality_v{v}") for v in (2, 3)]
    targets = ordinary + deleted + equality
    row_sql = "SELECT CAST(id AS BIGINT) AS id, concat('row-', id) AS payload FROM range(12)"
    digest_sql = lambda table: f"SELECT id, payload FROM {table} ORDER BY id"

    def writes(label, session, *_):
        for writer, v, kind, table in targets:
            if writer != label:
                continue
            options = f"'format-version'='{v}', 'write.delete.mode'='merge-on-read'"
            sql = [f"CREATE TABLE {table} USING iceberg TBLPROPERTIES ({options}) AS {row_sql}"]
            if kind == "delete":
                sql.append(f"DELETE FROM {table} WHERE id IN (1, 7)")
            statements(result, f"write-{table.rsplit('.', 1)[-1]}", session, sql)

    def spark_setup(spark, *_):
        collect(spark, f"CREATE NAMESPACE {NS}")
        ref["base"] = fingerprint(collect(spark, f"{row_sql} ORDER BY id"), True)
        ref["delete"] = fingerprint(collect(spark, f"{row_sql} WHERE id NOT IN (1,7) ORDER BY id"), True)
        ref["equality"] = fingerprint(collect(spark, "SELECT id, IF(id=4,'changed',concat('row-',id)) FROM range(12) ORDER BY id"), True)
        writes("spark", spark)

    group(result, "spark-writer", spark_setup, "spark")
    group(result, "sail-writer", lambda *args: writes("sail", *args), "sail")
    pc = None
    try:
        import pyarrow as pa
        from pyiceberg.schema import Schema
        from pyiceberg.types import LongType, NestedField, StringType
        result["versions"]["pyiceberg"] = version("pyiceberg")
        pc = pycatalog()
        for writer, v, kind, table in ordinary:
            if writer == "pyiceberg":
                def write():
                    t = pc.create_table(table.removeprefix("polaris."), Schema(
                        NestedField(1, "id", LongType(), required=False), NestedField(2, "payload", StringType(), required=False)),
                        properties={"format-version": str(v)})
                    t.append(pa.table({"id": list(range(12)), "payload": [f"row-{i}" for i in range(12)]}))
                record, _ = timed(write)
                result["cases"][f"write-{table.rsplit('.', 1)[-1]}"] = record
    except Exception as exc:
        result["cases"]["pyiceberg-startup"] = {"error": error(exc)}
    save(result)

    def observe_rows(key, record, rows, fixture):
        result["cases"][key] = record | {"fixture": fixture, "rows": rows, "expected": ref.get(fixture),
                                        "matches_spark": rows is not None and fingerprint(rows, True) == ref.get(fixture)}
        save(result)

    try:
        with flink_service(result) as handle:
            values = ",".join(f"({i},'row-{i}')" for i in range(12))
            for writer, v, fixture, table in ordinary + equality:
                if writer != "flink":
                    continue
                name = table.replace("polaris.", "p3cat.")
                key = table.rsplit(".", 1)[-1]
                fields = "id BIGINT, payload STRING" if fixture == "base" else "id BIGINT NOT NULL, payload STRING, PRIMARY KEY(id) NOT ENFORCED"
                opts = f"'format-version'='{v}'"
                if fixture == "equality":
                    opts += ",'write.upsert.enabled'='true','write.distribution-mode'='hash'"
                commands = ["SET 'execution.runtime-mode'='STREAMING';", "SET 'execution.checkpointing.interval'='2 s';",
                            f"CREATE TABLE {name} ({fields}) WITH ({opts});",
                            f"INSERT INTO {name} VALUES {values};"]
                if fixture == "equality":
                    commands.append(f"INSERT INTO {name} VALUES (4,'changed');")
                record, _ = timed(lambda: flink_sql(handle, f"write-{key}", *commands))
                result["cases"][f"write-{key}"] = record
                save(result)
            for _, _, fixture, table in targets:
                name = table.replace("polaris.", "p3cat.")
                key = table.rsplit(".", 1)[-1]
                record, plain = timed(lambda: flink_sql(handle, f"read-{key}",
                    "SET 'execution.runtime-mode'='BATCH';", f"SELECT id,payload FROM {name} ORDER BY id;"))
                rows = [[int(i), payload] for i, payload in re.findall(r"(?m)^\|\s*(\d+)\s*\|\s*([^| ]+)\s*\|", plain or "")]
                observe_rows(f"{key}-to-flink", record, rows if plain else None, fixture)
    except Exception as exc:
        result["cases"]["flink-startup"] = {"error": error(exc)}

    def check(reader, spark, *_):
        for writer, v, fixture, table in targets:
            key = table.rsplit(".", 1)[-1]
            record, rows = timed(lambda: collect(spark, digest_sql(table), 60))
            observe_rows(f"{key}-to-{reader}", record, [list(row) for row in rows] if rows is not None else None, fixture)
            if reader == "spark" and fixture != "base":
                statements(result, f"files-{key}", spark, [f"SELECT * FROM {table}.files"])

    for reader in ("spark", "sail"):
        group(result, f"{reader}-reader", lambda *args: check(reader, *args), reader, port=15018 if reader == "spark" else 50052)
    if pc:
        for writer, v, fixture, table in targets:
            record, payload = timed(lambda: pc.load_table(table.removeprefix("polaris.")).scan().to_arrow().to_pylist())
            rows = sorted((r["id"], r["payload"]) for r in payload) if payload is not None else None
            observe_rows(f"{table.rsplit('.', 1)[-1]}-to-pyiceberg", record, rows, fixture)
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


def nested_collections(result):
    table = f"{NS}.nested_collections"
    check_sql = f"SELECT id, m, a FROM {table} ORDER BY id"
    expected = {}

    def prepare(spark, *_):
        statements(result, "prepare-nested", spark, [
            f"CREATE NAMESPACE {NS}",
            f"""CREATE TABLE {table} USING iceberg AS SELECT id,
              map('k', named_struct('keep',CAST(id AS INT),'prior','old','remove',true)) AS m,
              array(named_struct('keep',CAST(id AS FLOAT),'prior','old','remove',true)) AS a
              FROM range(4)"""])
        for nested in ("m.value", "a.element"):
            statements(result, f"evolve-{nested}", spark, [
                f"ALTER TABLE {table} RENAME COLUMN {nested}.prior TO label",
                f"ALTER TABLE {table} ADD COLUMN {nested}.added STRING",
                f"ALTER TABLE {table} DROP COLUMN {nested}.remove",
                f"ALTER TABLE {table} ALTER COLUMN {nested}.keep AFTER label",
                f"ALTER TABLE {table} ALTER COLUMN {nested}.keep TYPE "
                + ("BIGINT" if nested.startswith("m.") else "DOUBLE"),
            ])
        rows = collect(spark, check_sql)
        expected["signature"] = fingerprint(rows, True)
        result["cases"]["spark-read-old-files"] = {"rows": [row.asDict(recursive=True) for row in rows],
                                                   "signature": expected["signature"], "sql": check_sql}
    group(result, "spark-nested-collections", prepare, "spark")

    def verify(sail, *_):
        observation, rows = timed(lambda: collect(sail, check_sql))
        result["cases"]["sail-read-old-files"] = observation | {
            "rows": [row.asDict(recursive=True) for row in rows] if rows is not None else None,
            "matches_spark": rows is not None and fingerprint(rows, True) == expected.get("signature")}
    group(result, "sail-nested-collections", verify, "sail")


def merge_sql(table, v):
    return (f"MERGE INTO {table} t USING (SELECT id FROM range(0,100)) s ON t.id=s.id "
            f"WHEN MATCHED THEN UPDATE SET v=t.v+{v}")


def maintenance(result):
    table = f"{NS}.merge_history"
    baseline = {}
    file_detail = VARIANT == "maintenance-files"

    def delete_links(spark, label):
        statements(result, label, spark, [
            f"""SELECT data.file_path, count(DISTINCT deletes.delete_file_path) AS delete_files
            FROM {table}.data_files data LEFT JOIN {table}.position_deletes deletes
            ON data.file_path = deletes.file_path
            GROUP BY data.file_path ORDER BY data.file_path""",
        ])

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
            if file_detail:
                delete_links(sp, f"data-delete-links-{checkpoint}")
        group(result, f"spark-files-{checkpoint}", inspect, "spark", port=15220 + trial)
    if done == 50:
        def compact(spark, stats, *_):
            statements(result, "compaction", spark, [
                "CALL polaris.system.rewrite_data_files(table => 'p3_iceberg.merge_history', options => map('rewrite-all','true'))",
                "CALL polaris.system.rewrite_position_delete_files(table => 'p3_iceberg.merge_history')",
                f"SELECT content,count(*) FROM {table}.files GROUP BY content"])
            measures(spark, stats, "spark-after-compaction", baseline[50])
            if file_detail:
                delete_links(spark, "data-delete-links-compacted")
                return
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


def layout(result):
    table = f"{NS}.file_layout"
    logical = f"SELECT count(*), sum(id), sum(g), sum(length(payload)) FROM {table}"
    source = "SELECT 99999-id AS id, CAST(pmod(id,8) AS INT) AS g, sha2(CAST(id AS STRING),512) AS payload FROM range(100000)"
    refs = {}

    def prepare(spark, *_):
        collect(spark, f"CREATE NAMESPACE {NS}")
        collect(spark, f"CREATE TABLE {table} (id BIGINT, g INT, payload STRING) USING iceberg PARTITIONED BY (g) "
                       "TBLPROPERTIES ('format-version'='2','write.distribution-mode'='hash',"
                       "'write.target-file-size-bytes'='1048576')")
        collect(spark, f"ALTER TABLE {table} WRITE ORDERED BY id")
        refs["signature"] = fingerprint(collect(spark, logical.replace(table, f"({source})")))
    group(result, "spark-layout-create", prepare, "spark")
    if "signature" not in refs:
        return

    def write(sail, *_):
        statements(result, "layout-write", sail, [f"INSERT INTO {table} {source}"])
    group(result, "sail-layout-write", write, "sail")

    def verify(spark, *_):
        import pyarrow.parquet as pq
        observed = fingerprint(collect(spark, logical))
        result["cases"]["layout-readback"] = {"matches_spark": observed == refs["signature"], "expected": refs["signature"], "actual": observed}
        files = collect(spark, f"SELECT file_path,sort_order_id,partition.g,record_count,file_size_in_bytes FROM {table}.files")
        checks = result["cases"]["physical_layout"] = []
        store = s3_client()
        for path, order_id, g, rows, size in files:
            bucket, key = path.removeprefix("s3://").split("/", 1)
            with store.get_object(Bucket=bucket, Key=key)["Body"] as stream:
                parquet = pq.read_table(BytesIO(stream.read()), columns=["id", "g"]).to_pydict()
            ids, partitions = parquet["id"], set(parquet["g"])
            checks.append({"file": path, "size_bytes": size, "records": rows, "sort_order_id": order_id,
                           "partition": g, "content_matches_partition": partitions == {g},
                           "ids_ascending": all(a <= b for a, b in zip(ids, ids[1:]))})
        save(result)
    group(result, "spark-layout-read", verify, "spark")


def conflict_trial(result, isolation, peer, trial):
    table = f"{NS}.conflict"
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
        return

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



def conflict(result):
    modes = ("snapshot",) if VARIANT == "conflict-snapshot" else ("serializable", "snapshot")
    if VARIANT != "conflict-snapshot":
        for isolation in modes:
            for peer in ("spark", "sail"):
                for trial in range(3):
                    conflict_trial(result, isolation, peer, trial)
        return

    for peer in ("spark", "sail"):
        for trial in range(3):
            print(f"snapshot contention with {peer}: trial {trial + 1}", flush=True)
            subprocess.run([sys.executable, "-u", "-m", "harness.p3.iceberg_deep", "--snapshot-cell", peer, str(trial)],
                           timeout=720, check=False)
    updated = json.loads((base.OUT / "result.json").read_text())
    result.update(updated)


def main():
    result = begin(TASK)
    base.start_token_refresher()
    {"interop": interop, "evolution": evolution, "maintenance": maintenance, "conflict": conflict, "conflict-snapshot": conflict, "layout": layout, "maintenance-files": maintenance,
     "nested-collections": nested_collections}[VARIANT](result)
    save(result)


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--snapshot-cell":
        recorded = json.loads((base.OUT / "result.json").read_text())
        conflict_trial(recorded, "snapshot", sys.argv[2], int(sys.argv[3]))
    else:
        main()
