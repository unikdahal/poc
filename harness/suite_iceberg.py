"""Differential Iceberg tests: Sail vs Spark 3.5 + Iceberg, through the same Polaris catalog.

Every write test runs the same SQL on two identical copies of a table, one written by Spark
(reference) and one by Sail, then reads both copies with both engines and compares.
"""

import threading
import time
import traceback

from harness.common import canon, connect, log, warm, write_json

NS = "polaris.dml"
COLS = "id, region, amount, qty, updated, flag, note"

SRC_SQL = f"""
CREATE TABLE {NS}.src0 USING iceberg AS
SELECT id,
       concat('R', cast(id % 5 AS string)) AS region,
       cast((id * 37 % 100000) / 100.0 AS decimal(18,2)) AS amount,
       cast(id % 50 AS int) AS qty,
       date_add(date'2024-01-01', cast(id % 365 AS int)) AS updated,
       (id % 2 = 0) AS flag,
       CASE WHEN id % 11 = 0 THEN NULL ELSE concat('n', cast(id AS string)) END AS note
FROM range(1, 10001)
"""

MSRC_SQL = f"""
CREATE TABLE {NS}.msrc USING iceberg AS
SELECT id,
       concat('R', cast(id % 5 AS string)) AS region,
       cast((id * 13 % 50000) / 100.0 AS decimal(18,2)) AS amount,
       cast(id % 40 AS int) AS qty,
       date_add(date'2025-01-01', cast(id % 200 AS int)) AS updated,
       (id % 3 = 0) AS flag,
       CASE WHEN id % 10 = 0 THEN 'D' ELSE 'U' END AS op
FROM range(8001, 12001)
"""

OPS = [
    ("delete", "DELETE FROM {t} WHERE qty % 7 = 0"),
    ("update", "UPDATE {t} SET amount = amount * 1.10, note = 'upd' WHERE region = 'R2' AND id % 3 = 0"),
    ("merge", f"""MERGE INTO {{t}} AS t USING {NS}.msrc AS s ON t.id = s.id
      WHEN MATCHED AND s.op = 'D' THEN DELETE
      WHEN MATCHED THEN UPDATE SET amount = s.amount, qty = t.qty + 1, note = 'merged'
      WHEN NOT MATCHED THEN INSERT ({COLS}) VALUES (s.id, s.region, s.amount, s.qty, s.updated, s.flag, 'ins')
      WHEN NOT MATCHED BY SOURCE AND t.region = 'R4' AND t.id < 100 THEN DELETE"""),
    ("insert", f"INSERT INTO {{t}} SELECT id + 100000, region, amount, qty, updated, flag, 'app' FROM {NS}.src0 WHERE id <= 500"),
]

VARIANTS = {
    "v2_cow_part": (2, "copy-on-write", True),
    "v2_cow_unpart": (2, "copy-on-write", False),
    "v2_mor_part": (2, "merge-on-read", True),
    "v2_mor_unpart": (2, "merge-on-read", False),
    "v3_cow_part": (3, "copy-on-write", True),
    "v3_mor_part": (3, "merge-on-read", True),
    "v3_mor_unpart": (3, "merge-on-read", False),
}

RESULTS: list[dict] = []


def rec(name, status, **kw):
    r = {"test": name, "status": status, **kw}
    RESULTS.append(r)
    log(f"{status.upper():5s} {name} {kw if status != 'pass' else ''}")
    return r


def q(spark, sql):
    t0 = time.perf_counter()
    rows = spark.sql(sql).collect()
    return rows, time.perf_counter() - t0


def try_sql(spark, sql):
    try:
        rows, secs = q(spark, sql)
        return True, rows, secs
    except Exception as e:  # noqa: BLE001
        return False, str(e).split("\n")[0][:600], None


def snapshot(spark, table):
    rows, _ = q(spark, f"SELECT {COLS} FROM {table} ORDER BY id")
    return [[canon(v) for v in r] for r in rows]


def files_summary(spark, table):
    try:
        rows, _ = q(spark, f"SELECT content, file_format, count(*) AS files, sum(record_count) AS records "
                           f"FROM {table}.files GROUP BY content, file_format ORDER BY content, file_format")
        return [list(map(canon, r)) for r in rows]
    except Exception as e:  # noqa: BLE001
        return f"error: {str(e)[:200]}"


def diff(a, b, limit=3):
    if a == b:
        return None
    sa, sb = {tuple(map(str, r)) for r in a}, {tuple(map(str, r)) for r in b}
    return {"rows_a": len(a), "rows_b": len(b), "only_a": sorted(sa - sb)[:limit], "only_b": sorted(sb - sa)[:limit]}


def create_copy(spark, name, fmt, mode, part):
    props = {"format-version": str(fmt)}
    if mode == "merge-on-read":
        props.update({"write.delete.mode": mode, "write.update.mode": mode, "write.merge.mode": mode})
    p = ", ".join(f"'{k}'='{v}'" for k, v in props.items())
    spark.sql(f"DROP TABLE IF EXISTS {NS}.{name}")
    spark.sql(f"""CREATE TABLE {NS}.{name} (id bigint, region string, amount decimal(18,2), qty int,
                 updated date, flag boolean, note string) USING iceberg
                 {"PARTITIONED BY (region)" if part else ""} TBLPROPERTIES ({p})""")
    spark.sql(f"INSERT INTO {NS}.{name} SELECT {COLS} FROM {NS}.src0")


def dml_matrix(ref, sail):
    for vname, (fmt, mode, part) in VARIANTS.items():
        a, b = f"{NS}.{vname}_spark", f"{NS}.{vname}_sail"
        creator = "spark"
        try:
            create_copy(ref, f"{vname}_spark", fmt, mode, part)
            create_copy(ref, f"{vname}_sail", fmt, mode, part)
        except Exception as e:  # noqa: BLE001
            # Spark 3.5 may not create v3 tables; fall back to Sail creating both copies
            rec(f"dml/{vname}/setup-by-spark", "info", error=str(e)[:300])
            try:
                create_copy(sail, f"{vname}_spark", fmt, mode, part)
                create_copy(sail, f"{vname}_sail", fmt, mode, part)
                creator = "sail"
            except Exception as e2:  # noqa: BLE001
                rec(f"dml/{vname}/setup", "error", error=str(e2)[:400])
                continue
        steps = []
        for opname, tmpl in OPS:
            ok_a, ra, ta = try_sql(ref, tmpl.format(t=a))
            ok_b, rb, tb = try_sql(sail, tmpl.format(t=b))
            step = {"op": opname, "spark_ok": ok_a, "sail_ok": ok_b,
                    "spark_s": ta and round(ta, 3), "sail_s": tb and round(tb, 3)}
            if not ok_a:
                step["spark_error"] = ra
            if not ok_b:
                step["sail_error"] = rb
            if ok_a and ok_b:
                d = diff(snapshot(ref, a), snapshot(ref, b))
                step["equal_after_op"] = d is None
                if d:
                    step["diff"] = d
            steps.append(step)
            if not (ok_a and ok_b):
                break
        # cross-engine reads of both final tables
        cross = {}
        try:
            sa_ref, sb_ref = snapshot(ref, a), snapshot(ref, b)
            sa_sail, sb_sail = snapshot(sail, a), snapshot(sail, b)
            cross = {
                "spark_reads_sail_written_equal_to_spark_written": diff(sa_ref, sb_ref) is None,
                "sail_reads_spark_written_equal_to_spark_reads": diff(sa_sail, sa_ref) is None,
                "sail_reads_sail_written_equal_to_spark_reads": diff(sb_sail, sb_ref) is None,
                "rows": len(sa_ref),
            }
            if not cross["sail_reads_spark_written_equal_to_spark_reads"]:
                cross["diff_sail_reads_spark_written"] = diff(sa_sail, sa_ref)
        except Exception as e:  # noqa: BLE001
            cross = {"error": str(e)[:400]}
        files = {"spark_written": files_summary(ref, a), "sail_written": files_summary(ref, b)}
        all_ok = all(s.get("spark_ok") and s.get("sail_ok") and s.get("equal_after_op") for s in steps) \
            and len(steps) == len(OPS) and cross and all(v for k, v in cross.items() if isinstance(v, bool))
        rec(f"dml/{vname}", "pass" if all_ok else "fail", created_by=creator, steps=steps, cross=cross, files=files)


def basic_rw(ref, sail):
    # Sail CTAS (partitioned by transform) -> Spark reads
    t = f"{NS}.sail_ctas"
    ok, r, _ = try_sql(sail, f"DROP TABLE IF EXISTS {t}")
    ok, r, secs = try_sql(sail, f"CREATE TABLE {t} USING iceberg PARTITIONED BY (days(updated)) AS SELECT * FROM {NS}.src0")
    if not ok:
        ok2, r2, _ = try_sql(sail, f"CREATE TABLE {t} USING iceberg PARTITIONED BY (region) AS SELECT * FROM {NS}.src0")
        rec("rw/sail-ctas-days-transform", "fail", error=r, fallback_identity_ok=ok2, fallback_error=None if ok2 else r2)
    else:
        rec("rw/sail-ctas-days-transform", "pass", seconds=round(secs, 3))
    try:
        d = diff(snapshot(ref, t), snapshot(ref, f"{NS}.src0"))
        spec, _ = q(ref, f"SELECT count(DISTINCT partition) FROM {t}.files")
        rec("rw/spark-reads-sail-ctas", "pass" if d is None else "fail", diff=d, partitions=spec[0][0])
    except Exception as e:  # noqa: BLE001
        rec("rw/spark-reads-sail-ctas", "error", error=str(e)[:400])

    # INSERT OVERWRITE (dynamic) by Sail on a partitioned table
    t2 = f"{NS}.sail_overwrite"
    ref.sql(f"DROP TABLE IF EXISTS {t2}")
    ref.sql(f"CREATE TABLE {t2} USING iceberg PARTITIONED BY (region) AS SELECT * FROM {NS}.src0")
    try_sql(sail, "SET spark.sql.sources.partitionOverwriteMode=dynamic")
    ok, r, _ = try_sql(sail, f"INSERT OVERWRITE {t2} SELECT id, region, amount * 2, qty, updated, flag, note FROM {NS}.src0 WHERE region = 'R1'")
    if ok:
        rows, _ = q(ref, f"SELECT region, count(*), sum(amount) FROM {t2} GROUP BY region ORDER BY region")
        exp, _ = q(ref, f"SELECT region, count(*), sum(CASE WHEN region='R1' THEN amount*2 ELSE amount END) FROM {NS}.src0 GROUP BY region ORDER BY region")
        same = [list(map(canon, x)) for x in rows] == [list(map(canon, x)) for x in exp]
        rec("rw/sail-insert-overwrite-dynamic", "pass" if same else "fail",
            got=[list(map(canon, x)) for x in rows], expected=[list(map(canon, x)) for x in exp])
    else:
        rec("rw/sail-insert-overwrite-dynamic", "fail", error=r)
    try_sql(sail, "SET spark.sql.sources.partitionOverwriteMode=static")


def types_roundtrip(ref, sail):
    sel = """SELECT cast(id AS int) AS i, id AS l, cast(id / 7.0 AS double) AS d, cast(id / 3.0 AS float) AS f,
        cast(id * 1234567.123456789 AS decimal(38,10)) AS dec38,
        timestamp'2024-03-10 01:30:00' + make_interval(0, 0, 0, 0, cast(id AS int), 0, 0) AS ts,
        cast(timestamp'2024-03-10 01:30:00' AS timestamp_ntz) AS tsntz,
        date'2000-02-29' + cast(id AS int) AS dt,
        named_struct('a', id, 'b', concat('s', cast(id AS string))) AS st,
        array(id, id + 1, NULL) AS arr, map('k', id) AS mp, cast(concat('x', cast(id AS string)) AS binary) AS bin
        FROM range(1, 201)"""
    for writer, reader, wname in ((sail, ref, "sail"), (ref, sail, "spark")):
        t = f"{NS}.types_by_{wname}"
        try_sql(writer, f"DROP TABLE IF EXISTS {t}")
        ok, r, _ = try_sql(writer, f"CREATE TABLE {t} USING iceberg AS {sel}")
        if not ok:
            rec(f"types/{wname}-writes", "fail", error=r)
            continue
        try:
            a = [[canon(v) for v in x] for x in writer.sql(f"SELECT * FROM {t} ORDER BY l").collect()]
            b = [[canon(v) for v in x] for x in reader.sql(f"SELECT * FROM {t} ORDER BY l").collect()]
            sch_w = writer.table(t).schema.simpleString()
            sch_r = reader.table(t).schema.simpleString()
            d = diff(a, b)
            rec(f"types/{wname}-writes-other-reads", "pass" if d is None and sch_w == sch_r else "fail",
                diff=d, schema_writer=sch_w, schema_reader=sch_r)
        except Exception as e:  # noqa: BLE001
            rec(f"types/{wname}-writes-other-reads", "error", error=str(e)[:400])


def schema_partition_evolution(ref, sail):
    t = f"{NS}.evolve"
    ref.sql(f"DROP TABLE IF EXISTS {t}")
    ref.sql(f"CREATE TABLE {t} USING iceberg PARTITIONED BY (region) AS SELECT * FROM {NS}.src0 WHERE id <= 1000")
    ref.sql(f"ALTER TABLE {t} ADD COLUMN extra string")
    ref.sql(f"ALTER TABLE {t} RENAME COLUMN note TO memo")
    ref.sql(f"ALTER TABLE {t} ADD PARTITION FIELD bucket(4, id)")
    ref.sql(f"INSERT INTO {t} SELECT id + 1000, region, amount, qty, updated, flag, note, 'e' FROM {NS}.src0 WHERE id <= 1000")
    ok, r, _ = try_sql(sail, f"SELECT count(*), count(extra), count(memo), sum(amount) FROM {t}")
    exp = ref.sql(f"SELECT count(*), count(extra), count(memo), sum(amount) FROM {t}").collect()
    rec("evolution/sail-reads-after-spark-schema+partition-evolution",
        "pass" if ok and [canon(v) for v in r[0]] == [canon(v) for v in exp[0]] else "fail",
        got=[canon(v) for v in r[0]] if ok else r, expected=[canon(v) for v in exp[0]])
    ok, r, _ = try_sql(sail, f"ALTER TABLE {t} ADD COLUMNS (added_by_sail int)")
    ok2, r2, _ = try_sql(sail, f"INSERT INTO {t} SELECT id + 5000, region, amount, qty, updated, flag, note, 's', 7 FROM {NS}.src0 WHERE id <= 10")
    if ok and ok2:
        got = ref.sql(f"SELECT count(*), sum(added_by_sail) FROM {t}").collect()[0]
        rec("evolution/sail-adds-column-and-inserts-spark-reads", "pass" if canon(got[1]) == 70 else "fail",
            got=[canon(v) for v in got])
    else:
        rec("evolution/sail-adds-column-and-inserts-spark-reads", "fail", alter_error=None if ok else r,
            insert_error=None if ok2 else r2)


def time_travel_and_metadata(ref, sail):
    t = f"{NS}.tt"
    ref.sql(f"DROP TABLE IF EXISTS {t}")
    ref.sql(f"CREATE TABLE {t} USING iceberg AS SELECT * FROM {NS}.src0 WHERE id <= 100")
    snaps1 = ref.sql(f"SELECT snapshot_id, committed_at FROM {t}.snapshots ORDER BY committed_at").collect()
    time.sleep(1.5)
    ref.sql(f"INSERT INTO {t} SELECT * FROM {NS}.src0 WHERE id > 100 AND id <= 300")
    sid = snaps1[0][0]
    ok, r, _ = try_sql(sail, f"SELECT count(*) FROM {t} VERSION AS OF {sid}")
    rec("timetravel/version-as-of-snapshot-id", "pass" if ok and r[0][0] == 100 else "fail",
        got=r[0][0] if ok else r)
    ts = snaps1[0][1].strftime("%Y-%m-%d %H:%M:%S.%f")
    ok, r, _ = try_sql(sail, f"SELECT count(*) FROM {t} TIMESTAMP AS OF '{ts}'")
    rec("timetravel/timestamp-as-of", "pass" if ok and r[0][0] == 100 else "fail", got=r[0][0] if ok else r, ts=ts)
    ref.sql(f"ALTER TABLE {t} CREATE TAG v1 AS OF VERSION {sid}")
    ok, r, _ = try_sql(sail, f"SELECT count(*) FROM {t} VERSION AS OF 'v1'")
    rec("timetravel/tag", "pass" if ok and r[0][0] == 100 else "fail", got=r[0][0] if ok else r)
    for mt in ["snapshots", "history", "files", "manifests", "refs", "metadata_log_entries",
               "entries", "partitions", "all_data_files", "all_manifests", "data_files", "delete_files"]:
        ok, r, _ = try_sql(sail, f"SELECT count(*) FROM {t}.{mt}")
        okr, rr, _ = try_sql(ref, f"SELECT count(*) FROM {t}.{mt}")
        status = "pass" if ok and okr and r[0][0] == rr[0][0] else ("fail" if ok else "unsupported")
        rec(f"metadata/{mt}", status, sail=r[0][0] if ok else r, spark=rr[0][0] if okr else rr)


def maintenance(ref, sail):
    t = f"{NS}.maint"
    ref.sql(f"DROP TABLE IF EXISTS {t}")
    ref.sql(f"CREATE TABLE {t} USING iceberg AS SELECT * FROM {NS}.src0 WHERE id <= 10")
    for i in range(10):
        ref.sql(f"INSERT INTO {t} SELECT * FROM {NS}.src0 WHERE id > {10 + i * 100} AND id <= {110 + i * 100}")
    before = ref.sql(f"SELECT count(*), sum(amount) FROM {t}").collect()[0]
    nfiles = ref.sql(f"SELECT count(*) FROM {t}.files").collect()[0][0]
    ok, r, secs = try_sql(sail, "CALL system.rewrite_data_files(table => 'dml.maint', options => map('rewrite-all', 'true'))")
    after = ref.sql(f"SELECT count(*), sum(amount) FROM {t}").collect()[0]
    nfiles2 = ref.sql(f"SELECT count(*) FROM {t}.files").collect()[0][0]
    rec("maintenance/sail-rewrite_data_files", "pass" if ok and list(before) == list(after) and nfiles2 < nfiles else "fail",
        result=[list(map(canon, x)) for x in r] if ok else r, files_before=nfiles, files_after=nfiles2,
        data_equal=list(map(canon, before)) == list(map(canon, after)))
    nsnap = ref.sql(f"SELECT count(*) FROM {t}.snapshots").collect()[0][0]
    ok, r, _ = try_sql(sail, "CALL system.expire_snapshots(table => 'dml.maint', retain_last => 1, older_than => TIMESTAMP '2100-01-01 00:00:00')")
    nsnap2 = ref.sql(f"SELECT count(*) FROM {t}.snapshots").collect()[0][0]
    rec("maintenance/sail-expire_snapshots", "pass" if ok and nsnap2 < nsnap else "fail",
        result=[list(map(canon, x)) for x in r] if ok else r, snapshots_before=nsnap, snapshots_after=nsnap2)
    for proc in ["remove_orphan_files(table => 'dml.maint')", "rewrite_manifests(table => 'dml.maint')",
                 "rewrite_position_delete_files(table => 'dml.maint')"]:
        ok, r, _ = try_sql(sail, f"CALL system.{proc}")
        rec(f"maintenance/sail-{proc.split('(')[0]}", "pass" if ok else "unsupported", detail=None if ok else r)


def concurrent_commits(ref, sail_port):
    t = f"{NS}.concurrent"
    ref.sql(f"DROP TABLE IF EXISTS {t}")
    ref.sql(f"CREATE TABLE {t} USING iceberg AS SELECT * FROM {NS}.src0 WHERE id <= 100")
    errors = {"sail_a": [], "sail_b": [], "spark": []}
    n = 10

    def worker(name, session, base):
        for i in range(n):
            lo = base + i * 100
            try:
                session.sql(f"INSERT INTO {t} SELECT id + {lo}, region, amount, qty, updated, flag, note "
                            f"FROM {NS}.src0 WHERE id <= 100").collect()
            except Exception as e:  # noqa: BLE001
                errors[name].append(str(e).split("\n")[0][:300])

    sessions = {"sail_a": connect(sail_port), "sail_b": connect(sail_port), "spark": ref}
    ths = [threading.Thread(target=worker, args=(k, s, 100000 * (j + 1))) for j, (k, s) in enumerate(sessions.items())]
    t0 = time.time()
    [x.start() for x in ths]
    [x.join() for x in ths]
    total = ref.sql(f"SELECT count(*) FROM {t}").collect()[0][0]
    ok_commits = sum(n - len(v) for v in errors.values())
    expected = 100 + ok_commits * 100
    rec("concurrency/3-writers-x10-appends", "pass" if total == expected and ok_commits == 3 * n else "fail",
        rows=total, expected_rows_from_successful_commits=expected, successful_commits=ok_commits,
        errors={k: v[:3] for k, v in errors.items()}, error_counts={k: len(v) for k, v in errors.items()},
        seconds=round(time.time() - t0, 1))


def main():
    ref, sail = connect(15002), connect(50051)
    warm(ref)
    warm(sail)
    ref.sql("CREATE NAMESPACE IF NOT EXISTS polaris.dml")
    for t in ["src0", "msrc"]:
        ref.sql(f"DROP TABLE IF EXISTS {NS}.{t}")
    ref.sql(SRC_SQL)
    ref.sql(MSRC_SQL)
    for fn in (basic_rw, types_roundtrip, schema_partition_evolution, time_travel_and_metadata, maintenance, dml_matrix):
        try:
            fn(ref, sail)
        except Exception as e:  # noqa: BLE001
            rec(f"{fn.__name__}/crashed", "error", error=str(e)[:500], tb=traceback.format_exc()[-1500:])
    try:
        concurrent_commits(ref, 50051)
    except Exception as e:  # noqa: BLE001
        rec("concurrency/crashed", "error", error=str(e)[:500])
    write_json("iceberg", RESULTS)


if __name__ == "__main__":
    main()
