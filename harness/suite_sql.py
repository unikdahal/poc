"""Spark SQL compatibility: Sail vs Spark 3.5 on a compatibility corpus and the 22 TPC-H queries."""

import json

from harness import tpch
from harness.common import OUT, canon, connect, decimal_loose, log, same_rows, warm, write_json
from harness.compat_queries import ANSI, Q


def run(spark, sql):
    try:
        df = spark.sql(sql)
        rows = df.collect()
        return {"ok": True, "schema": df.schema.simpleString(), "rows": [[canon(v) for v in r] for r in rows]}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "error": str(e).split("\n")[0][:400]}


def compare(a, b, ordered=True):
    if not a["ok"] and not b["ok"]:
        return "both_error"
    if not b["ok"]:
        return "sail_error"
    if not a["ok"]:
        return "spark_error"
    if same_rows(a["rows"], b["rows"], ordered):
        return "match" if a["schema"] == b["schema"] else "match_values_type_differs"
    if decimal_loose(a["rows"], b["rows"], ordered):
        return "value_repr_differs"
    return "mismatch"


def main():
    ref = connect(15002, {"spark.sql.session.timeZone": "UTC"})
    sail = connect(50051, {"spark.sql.session.timeZone": "UTC"})
    warm(ref)
    warm(sail)
    defaults = {}
    for k in ["spark.sql.ansi.enabled", "spark.sql.session.timeZone", "spark.sql.legacy.timeParserPolicy",
              "spark.sql.storeAssignmentPolicy"]:
        for name, s in (("spark", ref), ("sail", sail)):
            try:
                defaults.setdefault(k, {})[name] = s.conf.get(k)
            except Exception as e:  # noqa: BLE001
                defaults.setdefault(k, {})[name] = f"error: {str(e)[:120]}"
    for s in (ref, sail):
        s.conf.set("spark.sql.ansi.enabled", "false")

    results = []
    for qid, cat, sql in Q:
        a, b = run(ref, sql), run(sail, sql)
        verdict = compare(a, b)
        results.append({"id": qid, "category": cat, "verdict": verdict, "sql": sql, "spark": a, "sail": b})
        log(f"{verdict:28s} {qid}")

    ansi = []
    for s in (ref, sail):
        s.conf.set("spark.sql.ansi.enabled", "true")
    for qid, sql in ANSI:
        a, b = run(ref, sql), run(sail, sql)
        ansi.append({"id": qid, "verdict": compare(a, b), "sql": sql, "spark": a, "sail": b})
    for s in (ref, sail):
        s.conf.set("spark.sql.ansi.enabled", "false")

    stmts = []
    for sql in ["USE polaris.tpch_sf1", "USE NAMESPACE polaris.tpch_sf1", "USE polaris", "SHOW TABLES IN polaris.tpch_sf1",
                "SHOW NAMESPACES IN polaris", "DESCRIBE TABLE polaris.tpch_sf1.nation", "DESCRIBE TABLE EXTENDED polaris.tpch_sf1.nation",
                "SHOW CREATE TABLE polaris.tpch_sf1.nation", "SHOW TBLPROPERTIES polaris.tpch_sf1.nation",
                "SHOW COLUMNS IN polaris.tpch_sf1.nation", "SELECT current_catalog(), current_database()",
                "CREATE OR REPLACE TEMPORARY VIEW tv AS SELECT 1 AS a", "CACHE TABLE tv", "UNCACHE TABLE tv",
                "ANALYZE TABLE polaris.tpch_sf1.nation COMPUTE STATISTICS", "EXPLAIN SELECT * FROM polaris.tpch_sf1.nation"]:
        a, b = run(ref, sql), run(sail, sql)
        stmts.append({"sql": sql, "spark_ok": a["ok"], "sail_ok": b["ok"], "sail_error": b.get("error"),
                      "spark_error": a.get("error")})
    tp = []
    for q in range(1, 23):
        try:
            ra, ca, ta = tpch.run_query(ref, q, "tpch_sf1")
            a = {"ok": True, "rows": tpch.canon_rows(ra), "schema": str(ca)}
        except Exception as e:  # noqa: BLE001
            a, ta = {"ok": False, "error": str(e)[:300]}, None
        try:
            rb, cb, tb = tpch.run_query(sail, q, "tpch_sf1")
            b = {"ok": True, "rows": tpch.canon_rows(rb), "schema": str(cb)}
        except Exception as e:  # noqa: BLE001
            b, tb = {"ok": False, "error": str(e)[:300]}, None
        v = compare(a, b, tpch.ordered(q))
        tp.append({"q": q, "verdict": v, "spark_rows": len(a.get("rows", [])), "sail_rows": len(b.get("rows", [])),
                   "sail_error": b.get("error"), "first_diff": None if v.startswith("match") else
                   next(([x, y] for x, y in zip(a.get("rows", []), b.get("rows", [])) if x != y), None)})
        log(f"tpch q{q}: {v}")

    summary = {}
    for r in results:
        summary[r["verdict"]] = summary.get(r["verdict"], 0) + 1
    write_json("sql-compat", {"defaults": defaults, "summary": summary, "corpus": results, "ansi": ansi, "statements": stmts, "tpch_sf1": tp})


if __name__ == "__main__":
    main()
