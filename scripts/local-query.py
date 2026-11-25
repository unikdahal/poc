"""Run TPC-H queries on the locally running engines and compare their results.

  python scripts/local-query.py --query 1
  python scripts/local-query.py --query all --sf 1 --engines comet,sail
  python scripts/local-query.py --sql "SELECT count(*) FROM polaris.tpch_sf1.lineitem"
"""

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from harness import tpch  # noqa: E402
from harness.common import connect, same_rows  # noqa: E402

PORTS = {"comet": 15002, "spark": 15003, "sail": 50051}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engines", default="comet,sail")
    ap.add_argument("--sf", default="1")
    ap.add_argument("--query", default="1", help="1-22 or 'all'")
    ap.add_argument("--sql")
    a = ap.parse_args()
    engines = [e for e in a.engines.split(",") if e in PORTS]
    sessions = {e: connect(PORTS[e]) for e in engines}
    ns = f"tpch_sf{a.sf}".replace(".", "_")

    if a.sql:
        for e, s in sessions.items():
            t0 = time.perf_counter()
            rows = s.sql(a.sql).collect()
            print(f"{e:6s} {time.perf_counter() - t0:8.3f}s  {rows[:5]}")
        return

    queries = range(1, 23) if a.query == "all" else [int(a.query)]
    print(f"{'q':>3} " + " ".join(f"{e:>10s}" for e in engines) + "  results")
    for q in queries:
        times, results = [], {}
        for e, s in sessions.items():
            try:
                rows, _, secs = tpch.run_query(s, q, ns)
                times.append(f"{secs:9.2f}s")
                results[e] = tpch.canon_rows(rows)
            except Exception as ex:  # noqa: BLE001
                times.append(f"{'error':>10s}")
                results[e] = f"ERROR {str(ex).splitlines()[0][:120]}"
        ref = results[engines[0]]
        verdicts = []
        for e in engines[1:]:
            r = results[e]
            if isinstance(r, str) or isinstance(ref, str):
                verdicts.append(f"{e}: {r if isinstance(r, str) else 'ref error'}")
            else:
                verdicts.append(f"{e}: {'same' if same_rows(ref, r, tpch.ordered(q)) else 'DIFFERENT'}")
        print(f"{q:>3} " + " ".join(times) + "  " + "; ".join(verdicts))


if __name__ == "__main__":
    main()
