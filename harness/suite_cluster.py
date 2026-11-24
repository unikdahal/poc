"""Sail distributed execution in local-cluster mode: shuffle backends, correctness, cost, cleanup,
head-of-line blocking, and fixed per-query overhead.

local-cluster runs the driver and workers in one process and talks over the real RPC and
Arrow Flight paths, so the job graph, task regions and shuffle code are the same as on Kubernetes.
"""

import json
import statistics
import subprocess
import threading
import time

from harness import tpch
from harness.common import (OUT, CgroupStats, PeakSampler, canon, connect, log, same_rows, sh, start_sail,
                            stop_container, warm, write_json)

NS = "tpch_sf1"
CLUSTER = {
    "SAIL_CLUSTER__WORKER_INITIAL_COUNT": "2",
    "SAIL_CLUSTER__WORKER_MAX_COUNT": "2",
    "SAIL_CLUSTER__WORKER_TASK_SLOTS": "16",
    "SAIL_EXECUTION__DEFAULT_PARALLELISM": "8",
}
BACKENDS = {
    "local": ("local", {}),
    "cluster-flight": ("local-cluster", {**CLUSTER, "SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE": "flight"}),
    "cluster-storage": ("local-cluster", {**CLUSTER, "SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE": "storage",
                                          "SAIL_CLUSTER__SHUFFLE_BACKEND__STORAGE__PATH": "s3://shuffle/sail"}),
    "cluster-celeborn": ("local-cluster", {**CLUSTER, "SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE": "celeborn",
                                           "SAIL_CLUSTER__SHUFFLE_BACKEND__CELEBORN__MASTER_ENDPOINTS": '["celeborn-master:12097"]'}),
}


def shuffle_objects() -> int:
    out = sh("docker run --rm --network poc -e AWS_ACCESS_KEY_ID=admin -e AWS_SECRET_ACCESS_KEY=password "
             "-e AWS_REGION=us-east-1 -e AWS_ENDPOINT_URL=http://minio:9000 amazon/aws-cli:2.39.6 "
             "s3 ls --recursive s3://shuffle/ ", check=False)
    return len([l for l in out.splitlines() if l.strip() and not l.startswith("An error")])


def ref_results():
    res = {}
    for q in range(1, 23):
        p = OUT / "ref" / f"tpch_sf1_q{q}.json"
        if p.exists():
            res[q] = json.loads(p.read_text())["rows"]
    return res


def run_backend(name, mode, env, ref, sf_ns):
    stop_container("sail")
    try:
        boot = start_sail("sail", "spark", 50051, mode, env, cpus="4", memory="12g")
    except Exception as e:  # noqa: BLE001
        return {"error": f"start failed: {e}"}
    s = connect(50051)
    try:
        first = warm(s)
    except Exception as e:  # noqa: BLE001
        return {"error": f"not ready: {e}", "logs": sh("docker logs --tail 50 sail 2>&1", check=False)[-3000:]}
    st = CgroupStats("sail")
    out = {"boot_s": round(boot, 2), "first_query_s": round(first, 2), "queries": {}}
    try:
        s.sql(f"USE polaris.{sf_ns}")
        out["explain_q5"] = "\n".join(r[0] for r in s.sql(
            "EXPLAIN " + tpch.statements(5)[0]).collect())[:6000]
    except Exception as e:  # noqa: BLE001
        out["explain_q5"] = f"error: {e}"
    for rep in range(2):
        for q in range(1, 23):
            c0 = st.cpu_seconds()
            objs = None
            try:
                with PeakSampler(st) as ps:
                    rows, cols, secs = tpch.run_query(s, q, sf_ns)
                rec = {"s": round(secs, 3), "cpu_s": round(st.cpu_seconds() - c0, 3), "peak_mb": round(ps.peak / 2**20)}
                if rep == 0 and ref and q in ref:
                    rec["matches_spark"] = same_rows(tpch.canon_rows(rows), ref[q], tpch.ordered(q))
            except Exception as e:  # noqa: BLE001
                rec = {"error": str(e).split("\n")[0][:400]}
            if name == "cluster-storage" and rep == 0:
                objs = shuffle_objects()
                rec["shuffle_objects_after"] = objs
            out["queries"].setdefault(q, []).append(rec)
            log(f"{name} rep{rep} q{q}: {rec}")
    out["total_s_rep2"] = round(sum(v[-1].get("s", 0) for v in out["queries"].values()), 2)
    out["total_cpu_s_rep2"] = round(sum(v[-1].get("cpu_s", 0) for v in out["queries"].values()), 2)
    out["errors"] = sum(1 for v in out["queries"].values() if "error" in v[0])
    out["mismatches"] = [q for q, v in out["queries"].items() if v[0].get("matches_spark") is False]
    s.stop()
    if name == "cluster-storage":
        time.sleep(3)
        out["shuffle_objects_after_session_stop"] = shuffle_objects()
    out["sail_log_tail"] = sh("docker logs --tail 30 sail 2>&1", check=False)[-2500:]
    return out


def latency(fn, n=100):
    xs = []
    for _ in range(n):
        t0 = time.perf_counter()
        fn()
        xs.append((time.perf_counter() - t0) * 1000)
    xs.sort()
    return {"n": n, "p50_ms": round(xs[n // 2], 2), "p95_ms": round(xs[int(n * .95) - 1], 2),
            "p99_ms": round(xs[int(n * .99) - 1], 2), "max_ms": round(xs[-1], 2)}


def overhead_and_hol(mode, env, label):
    """Fixed overhead per query, new-session cost, and head-of-line blocking under a heavy query."""
    import adbc_driver_flightsql.dbapi as fsql

    res = {}
    stop_container("sail")
    stop_container("sail-flight")
    start_sail("sail", "spark", 50051, mode, env, cpus="4", memory="12g")
    start_sail("sail-flight", "flight", 32010, mode, env, cpus="4", memory="12g")
    s = connect(50051)
    warm(s)
    point = f"SELECT * FROM polaris.{NS}.nation WHERE n_nationkey = 7"
    res["spark_connect_select1"] = latency(lambda: s.sql("SELECT 1").collect())
    res["spark_connect_iceberg_point"] = latency(lambda: s.sql(point).collect(), 50)
    # new-session cost: session creation + first query
    xs = []
    for _ in range(5):
        t0 = time.perf_counter()
        s2 = connect(50051)
        s2.sql("SELECT 1").collect()
        xs.append(round(time.perf_counter() - t0, 3))
        s2.stop()
    res["spark_connect_new_session_first_query_s"] = xs
    cur = None
    for _ in range(60):
        try:
            cur = fsql.connect("grpc://localhost:32010").cursor()
            cur.execute("SELECT 1")
            cur.fetchall()
            break
        except Exception:  # noqa: BLE001
            time.sleep(1)

    def fq(sql, c=None):
        c = c or cur
        c.execute(sql)
        return c.fetchall()

    res["flight_select1"] = latency(lambda: fq("SELECT 1"))
    res["flight_iceberg_point"] = latency(lambda: fq(point), 50)

    # head-of-line blocking: a heavy query and a stream of small ones on the same server
    heavy = f"SELECT count(*) FROM polaris.{NS}.lineitem a JOIN polaris.{NS}.orders o ON a.l_orderkey = o.o_orderkey JOIN polaris.{NS}.lineitem b ON b.l_orderkey = o.o_orderkey"

    def under_load(small_fn, heavy_fn):
        done = {}

        def h():
            t0 = time.perf_counter()
            try:
                heavy_fn()
                done["heavy_s"] = round(time.perf_counter() - t0, 2)
            except Exception as e:  # noqa: BLE001
                done["heavy_error"] = str(e)[:200]
        t = threading.Thread(target=h)
        t.start()
        time.sleep(1.0)
        xs = []
        while t.is_alive() and len(xs) < 400:
            t0 = time.perf_counter()
            small_fn()
            xs.append((time.perf_counter() - t0) * 1000)
        t.join()
        xs.sort()
        n = len(xs)
        done.update({"small_n": n, "small_p50_ms": round(xs[n // 2], 2) if n else None,
                     "small_p99_ms": round(xs[max(0, int(n * .99) - 1)], 2) if n else None,
                     "small_max_ms": round(xs[-1], 2) if n else None})
        return done

    other = connect(50051)
    other.sql("SELECT 1").collect()
    res["hol_spark_connect_separate_sessions"] = under_load(
        lambda: other.sql(point).collect(), lambda: s.sql(heavy).collect())
    c_heavy = fsql.connect("grpc://localhost:32010").cursor()
    c_small = fsql.connect("grpc://localhost:32010").cursor()
    res["hol_flight_sql_two_connections"] = under_load(lambda: fq(point, c_small), lambda: fq(heavy, c_heavy))
    res["sail_log_tail"] = sh("docker logs --tail 20 sail-flight 2>&1", check=False)[-2000:]
    stop_container("sail-flight")
    stop_container("sail")
    log(f"{label}: {json.dumps({k: v for k, v in res.items() if 'log' not in k})[:1500]}")
    return res


def spark_overhead():
    from harness.common import connect as c

    s = c(15002)
    warm(s)
    point = f"SELECT * FROM polaris.{NS}.nation WHERE n_nationkey = 7"
    r = {"spark_connect_select1": latency(lambda: s.sql("SELECT 1").collect()),
         "spark_connect_iceberg_point": latency(lambda: s.sql(point).collect(), 50)}
    xs = []
    for _ in range(5):
        t0 = time.perf_counter()
        s2 = c(15002)
        s2.sql("SELECT 1").collect()
        xs.append(round(time.perf_counter() - t0, 3))
    r["spark_connect_new_session_first_query_s"] = xs
    return r


def main():
    import sys

    which = sys.argv[1] if len(sys.argv) > 1 else "backends"
    if which == "spark-overhead":
        write_json("overhead-spark", spark_overhead())
        return
    if which == "overhead":
        res = {"local": overhead_and_hol("local", {}, "local"),
               "local-cluster": overhead_and_hol("local-cluster", {**CLUSTER}, "local-cluster")}
        write_json("overhead-sail", res)
        return
    ref = ref_results()
    res = {}
    for name, (mode, env) in BACKENDS.items():
        log(f"=== backend {name}")
        res[name] = run_backend(name, mode, env, ref, NS)
        write_json("cluster-backends", res)
    stop_container("sail")


if __name__ == "__main__":
    main()
