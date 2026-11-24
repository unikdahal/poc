"""Run the 22 queries on the Spark reference server and store results for later comparison."""

import json
import sys

from harness import tpch
from harness.common import OUT, connect, log, warm, write_json


def main():
    ns = sys.argv[1] if len(sys.argv) > 1 else "tpch_sf1"
    s = connect(15002)
    warm(s)
    (OUT / "ref").mkdir(exist_ok=True)
    times = {}
    for q in range(1, 23):
        rows, cols, secs = tpch.run_query(s, q, ns)
        (OUT / "ref" / f"{ns}_q{q}.json").write_text(json.dumps({"columns": cols, "rows": tpch.canon_rows(rows)}, default=str))
        times[q] = round(secs, 3)
        log(f"ref {ns} q{q}: {secs:.2f}s rows={len(rows)}")
    write_json(f"ref-{ns}", times)


if __name__ == "__main__":
    main()
