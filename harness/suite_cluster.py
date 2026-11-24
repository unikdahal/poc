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
# ~1.5M x ~600 matches per supplier key: tens of seconds on 4 vCPU
HEAVY = (f"SELECT count(*), sum(a.l_extendedprice * b.l_discount) FROM polaris.{NS}.lineitem a "
         f"JOIN polaris.{NS}.lineitem b ON a.l_suppkey = b.l_suppkey WHERE a.l_orderkey < 1000000")
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


def run_with_timeout(spark, q, ns, timeout):
    """Run a query; on timeout interrupt it and report a hang instead of blocking the suite."""
    box = {}

    def target():
        try:
            box["res"] = tpch.run_query(spark, q, ns)
        except Exception as e:  # noqa: BLE001
            box["err"] = e
    t = threading.Thread(target=target, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        try:
            spark.interruptAll()
        except Exception:  # noqa: BLE001
            pass
        t.join(30)
        raise TimeoutError(f"query did not finish within {timeout}s (interrupt {'worked' if not t.is_alive() else 'did not stop it'})")
    if "err" in box:
        raise box["err"]
    return box["res"]


class ObjectSampler:
    """Counts objects under s3://shuffle every 0.5 s while a query runs (peak = intermediate data)."""

    def __init__(self):
        import boto3
        self.s3 = boto3.client("s3", endpoint_url="http://localhost:9000", aws_access_key_id="admin",
                               aws_secret_access_key="password", region_name="us-east-1")
        self.peak_objects = 0
        self.peak_bytes = 0
        self._stop = threading.Event()

    def count(self):
        n = b = 0
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket="shuffle"):
            for o in page.get("Contents", []):
                n += 1
                b += o["Size"]
        return n, b

    def __enter__(self):
        def run():
            while not self._stop.is_set():
                try:
                    n, b = self.count()
                    self.peak_objects, self.peak_bytes = max(self.peak_objects, n), max(self.peak_bytes, b)
                except Exception:  # noqa: BLE001
                    pass
                time.sleep(0.5)
        self.t = threading.Thread(target=run, daemon=True)
        self.t.start()
        return self

    def __exit__(self, *a):
        self._stop.set()
        self.t.join()


def run_backend(name, mode, env, ref, sf_ns, reps=3, timeout=180):
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
        tpch.use(s, sf_ns)
        out["explain_q5"] = "\n".join(r[0] for r in s.sql(
            "EXPLAIN " + tpch.statements(5)[0]).collect())[:6000]
    except Exception as e:  # noqa: BLE001
        out["explain_q5"] = f"error: {e}"
    hangs = []
    for rep in range(reps):
        for q in range(1, 23):
            c0 = st.cpu_seconds()
            sampler = ObjectSampler() if name == "cluster-storage" else None
            try:
                with PeakSampler(st) as ps:
                    if sampler:
                        with sampler:
                            rows, cols, secs = run_with_timeout(s, q, sf_ns, timeout)
                    else:
                        rows, cols, secs = run_with_timeout(s, q, sf_ns, timeout)
                rec = {"s": round(secs, 3), "cpu_s": round(st.cpu_seconds() - c0, 3), "peak_mb": round(ps.peak / 2**20)}
                if rep == 0 and ref and q in ref:
                    rec["matches_spark"] = same_rows(tpch.canon_rows(rows), ref[q], tpch.ordered(q))
            except Exception as e:  # noqa: BLE001
                rec = {"error": str(e).split("\n")[0][:400]}
                if isinstance(e, TimeoutError):
                    hangs.append({"rep": rep, "q": q, "log_tail": sh("docker logs --tail 60 sail 2>&1", check=False)[-5000:]})
                    # a hung job can hold the session: start a fresh one
                    try:
                        s = connect(50051)
                        warm(s, tries=10)
                    except Exception:  # noqa: BLE001
                        stop_container("sail")
                        start_sail("sail", "spark", 50051, mode, env, cpus="4", memory="12g")
                        s = connect(50051)
                        warm(s)
                        st = CgroupStats("sail")
            if sampler:
                rec["shuffle_peak_objects"] = sampler.peak_objects
                rec["shuffle_peak_mb"] = round(sampler.peak_bytes / 2**20, 1)
                rec["shuffle_objects_after"] = sampler.count()[0]
            out["queries"].setdefault(q, []).append(rec)
            log(f"{name} rep{rep} q{q}: {rec}")
    out["hangs"] = hangs
    def med(v, key):
        xs = sorted(x[key] for x in v[1:] if key in x)
        return xs[len(xs) // 2] if xs else 0
    out["total_s_median_warm"] = round(sum(med(v, "s") for v in out["queries"].values()), 2)
    out["total_cpu_s_median_warm"] = round(sum(med(v, "cpu_s") for v in out["queries"].values()), 2)
    out["errors"] = sum(1 for v in out["queries"].values() if "error" in v[0])
    out["mismatches"] = [q for q, v in out["queries"].items() if v[0].get("matches_spark") is False]
    try:
        s.stop()
    except Exception:  # noqa: BLE001
        pass
    if name == "cluster-storage":
        time.sleep(3)
        out["shuffle_objects_after_session_stop"] = ObjectSampler().count()[0]
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
    heavy = HEAVY

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
