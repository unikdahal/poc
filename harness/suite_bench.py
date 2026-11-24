"""TPC-H-derived benchmark over Iceberg + Polaris + S3: Spark 3.5, Spark 3.5 + Comet, Sail.

All engines run on the same runner, one at a time, with 4 cores. Reported per query:
wall time (planning + execution + collect at the client), engine CPU-seconds, peak engine memory.
The first repetition is a warm-up and is reported separately.
"""

import argparse
import json
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


def bench_engine(name, spark, stats, ns, reps, ref):
    res = {"queries": {}}
    for rep in range(reps):
        for q in range(1, 23):
            c0 = stats.cpu_seconds()
            try:
                with PeakSampler(stats) as ps:
                    rows, cols, secs = tpch.run_query(spark, q, ns)
                r = {"s": round(secs, 3), "cpu_s": round(stats.cpu_seconds() - c0, 2), "peak_mb": round(ps.peak / 2**20)}
                if rep == 0:
                    cr = tpch.canon_rows(rows)
                    if name == "spark":
                        ref[q] = cr
                    elif q in ref:
                        r["matches_spark"] = same_rows(cr, ref[q], tpch.ordered(q))
            except Exception as e:  # noqa: BLE001
                r = {"error": str(e).split("\n")[0][:300]}
            res["queries"].setdefault(q, []).append(r)
            log(f"{name} {ns} rep{rep} q{q}: {r}")
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", default="1")
    ap.add_argument("--engines", default="spark,comet,sail,sail-cluster")
    ap.add_argument("--reps", type=int, default=3)
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
                        s.sql(f"USE polaris.{ns}")
                        out["comet_explain_q1"] = "\n".join(r[0] for r in s.sql("EXPLAIN FORMATTED " + tpch.statements(1)[0]).collect())[:8000]
                    except Exception as e:  # noqa: BLE001
                        out["comet_explain_q1"] = str(e)[:500]
                out[eng] = bench_engine(eng, s, stats, ns, a.reps, ref)
                out[eng]["boot_s"] = round(boot, 1)
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
            cs = sorted(x["cpu_s"] for x in meas)
            tot_s += ss[len(ss) // 2]
            tot_cpu += cs[len(cs) // 2]
        summ[eng] = {"total_wall_s": round(tot_s, 2), "total_cpu_s": round(tot_cpu, 2), "all_queries_ran": ok,
                     "errors": [q for q, v in r["queries"].items() if "error" in v[0]],
                     "mismatches": [q for q, v in r["queries"].items() if v[0].get("matches_spark") is False]}
    out["summary"] = summ
    write_json(f"bench-sf{a.sf}", out)
    log(json.dumps(summ, indent=2))


if __name__ == "__main__":
    main()
