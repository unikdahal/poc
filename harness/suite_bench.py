"""TPC-H-derived benchmark over Iceberg + Polaris + S3: Spark 3.5, Spark 3.5 + Comet, Sail.

All engines run on the same runner, one at a time, with 4 cores. Reported per query:
wall time (planning + execution + collect at the client), engine CPU-seconds, peak engine memory.
The first repetition is a warm-up and is reported separately.
"""

import argparse
import json
import math
import time

from harness import tpch
from harness.common import (OUT, CgroupStats, PeakSampler, PidStats, connect, jvm_pid, log, same_rows, sh,
                            start_sail, start_spark_connect, stop_container, stop_proc, warm, write_json)

CLUSTER = {
    "SAIL_CLUSTER__WORKER_INITIAL_COUNT": "2",
    "SAIL_CLUSTER__WORKER_MAX_COUNT": "2",
    "SAIL_CLUSTER__WORKER_TASK_SLOTS": "16",
    "SAIL_EXECUTION__DEFAULT_PARALLELISM": "8",
}


def bench_engine(name, spark, stats, ns, reps, ref, *, query_runner=tpch.run_query,
                 validate_all=False, on_sample=None):
    res = {"queries": {}}
    for rep in range(reps):
        for q in range(1, 23):
            c0 = stats.cpu_seconds()
            try:
                with PeakSampler(stats) as ps:
                    rows, cols, secs = query_runner(spark, q, ns)
                cpu = stats.cpu_seconds() - c0
                r = {"s": round(secs, 3), "cpu_s": round(cpu, 2) if math.isfinite(cpu) else None,
                     "peak_mb": round(ps.peak / 2**20) if ps.peak > 0 else None}
                if rep == 0 or validate_all:
                    cr = tpch.canon_rows(rows)
                    if name == "spark" and rep == 0:
                        ref[q] = cr
                    elif q in ref:
                        r["matches_spark"] = same_rows(cr, ref[q], tpch.ordered(q))
            except Exception as e:  # noqa: BLE001
                r = {"error": str(e).split("\n")[0][:300]}
                if isinstance(e, TimeoutError):
                    r["timeout"] = True
            res["queries"].setdefault(q, []).append(r)
            if on_sample:
                on_sample(res)
                if r.get("timeout") and "client still active: True" in r["error"]:
                    return res
            log(f"{name} {ns} rep{rep} q{q}: {r}")
    return res


def throughput(port, ns, streams, stats):
    """`streams` concurrent sessions each run all 22 queries in a different order."""
    import random
    import threading

    lat, errs = [], []
    sessions = [connect(port) for _ in range(streams)]
    for x in sessions:
        warm(x)

    def stream(i, sp):
        order = list(range(1, 23))
        random.Random(i).shuffle(order)
        for q in order:
            t0 = time.perf_counter()
            try:
                tpch.run_query(sp, q, ns)
                lat.append(time.perf_counter() - t0)
            except Exception as e:  # noqa: BLE001
                errs.append(f"q{q}: {str(e)[:150]}")
    c0 = stats.cpu_seconds()
    t0 = time.perf_counter()
    with PeakSampler(stats) as ps:
        ths = [threading.Thread(target=stream, args=(i, sp)) for i, sp in enumerate(sessions)]
        [t.start() for t in ths]
        [t.join() for t in ths]
    wall = time.perf_counter() - t0
    lat.sort()
    n = len(lat)
    return {"streams": streams, "wall_s": round(wall, 2), "queries": n, "queries_per_hour": round(n / wall * 3600),
            "cpu_s": round(stats.cpu_seconds() - c0, 1), "peak_mb": round(ps.peak / 2**20),
            "latency_p50_s": round(lat[n // 2], 2) if n else None, "latency_p95_s": round(lat[int(n * .95) - 1], 2) if n else None,
            "errors": errs[:5], "error_count": len(errs)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", default="1")
    ap.add_argument("--engines", default="spark,comet,sail,sail-cluster")
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--throughput", type=int, default=0, help="also run N concurrent query streams per engine")
    a = ap.parse_args()
    ns = f"tpch_sf{a.sf}"
    out, ref = {}, {}
    for eng in a.engines.split(","):
        log(f"===== {eng}")
        if eng in ("spark", "comet"):
            proc, boot = start_spark_connect(port=15010, comet=(eng == "comet"), memory="10g")
            try:
                s = connect(15010)
                warm(s)
                stats = PidStats(jvm_pid(proc))
                if eng == "comet":
                    try:
                        tpch.use(s, ns)
                        out["comet_explain_q1"] = "\n".join(r[0] for r in s.sql("EXPLAIN FORMATTED " + tpch.statements(1)[0]).collect())[:8000]
                    except Exception as e:  # noqa: BLE001
                        out["comet_explain_q1"] = str(e)[:500]
                out[eng] = bench_engine(eng, s, stats, ns, a.reps, ref)
                out[eng]["boot_s"] = round(boot, 1)
                if a.throughput:
                    out[eng]["throughput"] = throughput(15010, ns, a.throughput, stats)
                    log(f"{eng} throughput: {out[eng]['throughput']}")
            finally:
                stop_proc(proc)
                sh("pkill -f SparkConnectServer", check=False)
                time.sleep(5)
        else:
            mode, env = ("local", {}) if eng == "sail" else ("local-cluster", CLUSTER)
            boot = start_sail("sail-bench", "spark", 50061, mode, env, cpus="4", memory="12g")
            try:
                s = connect(50061)
                warm(s)
                out[eng] = bench_engine(eng, s, CgroupStats("sail-bench"), ns, a.reps, ref)
                out[eng]["boot_s"] = round(boot, 1)
                if a.throughput:
                    out[eng]["throughput"] = throughput(50061, ns, a.throughput, CgroupStats("sail-bench"))
                    log(f"{eng} throughput: {out[eng]['throughput']}")
            finally:
                stop_container("sail-bench")
        write_json(f"bench-sf{a.sf}", out)
    # summary: medians of reps 2..n
    summ = {}
    for eng, r in out.items():
        if not isinstance(r, dict) or "queries" not in r:
            continue
        tot_s = tot_cpu = 0.0
        ok = True
        for q, reps in r["queries"].items():
            meas = [x for x in reps[1:] if "s" in x] or [x for x in reps if "s" in x]
            if not meas:
                ok = False
                continue
            ss = sorted(x["s"] for x in meas)
            cs = sorted(x["cpu_s"] for x in meas if x["cpu_s"] is not None)
            tot_s += ss[len(ss) // 2]
            tot_cpu += cs[len(cs) // 2] if cs else 0
        summ[eng] = {"total_wall_s": round(tot_s, 2), "total_cpu_s": round(tot_cpu, 2), "all_queries_ran": ok,
                     "errors": [q for q, v in r["queries"].items() if "error" in v[0]],
                     "mismatches": [q for q, v in r["queries"].items() if v[0].get("matches_spark") is False]}
    out["summary"] = summ
    write_json(f"bench-sf{a.sf}", out)
    log(json.dumps(summ, indent=2))


if __name__ == "__main__":
    main()
