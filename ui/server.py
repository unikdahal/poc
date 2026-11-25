"""Query workbench: run one SQL text on several engines and compare timings, resources and results.

  python ui/server.py            # http://localhost:8088
  python ui/server.py --port 9000 --host 0.0.0.0

Engines are found on the ports scripts/local-up.sh uses. Nothing here is engine-specific except
how a statement is sent (Spark Connect or Flight SQL) and where CPU/memory are read from
(the Spark JVM process tree, or the Sail container's cgroup).
"""

import argparse
import collections
import json
import re
import socket
import statistics
import sys
import threading
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from harness import tpch  # noqa: E402
from harness.common import (CATALOG, POLARIS, REALM, CgroupStats, PeakSampler, PidStats, canon,  # noqa: E402
                            polaris_token)

STATIC = Path(__file__).resolve().parent / "index.html"
DISPLAY_ROWS = 500
COMPARE_ROWS = 200_000
SAMPLE_DIFF = 25

ENGINES = {  # the first selected engine is the reference for result comparison
    "spark": {"label": "Spark 3.5", "kind": "connect", "port": 15003, "pidfile": "/tmp/poc-pids/spark-15003"},
    "comet": {"label": "Spark 3.5 + Comet", "kind": "connect", "port": 15002, "pidfile": "/tmp/poc-pids/spark-15002"},
    "sail": {"label": "Sail (Spark Connect)", "kind": "connect", "port": 50051, "container": "sail"},
    "sail-flight": {"label": "Sail (Flight SQL)", "kind": "flight", "port": 32010, "container": "sail-flight"},
}


def port_open(port: int) -> bool:
    with socket.socket() as s:
        s.settimeout(0.3)
        return s.connect_ex(("localhost", port)) == 0


def split_statements(sql: str) -> list[str]:
    """Split on semicolons outside quotes and comments."""
    out, cur, q, i = [], [], None, 0
    while i < len(sql):
        c = sql[i]
        if q:
            cur.append(c)
            if c == q:
                q = None
        elif c in "'\"`":
            q = c
            cur.append(c)
        elif sql.startswith("--", i):
            j = sql.find("\n", i)
            i = len(sql) if j < 0 else j
            continue
        elif c == ";":
            out.append("".join(cur))
            cur = []
        else:
            cur.append(c)
        i += 1
    out.append("".join(cur))
    return [s.strip() for s in out if s.strip()]


QUERY_START = re.compile(r"^\s*(\(|select|with|values|table|from|show|describe|desc|explain)\b", re.I)


def result_index(stmts: list[str]) -> int:
    """The statement whose rows are shown and compared: the last query-like one (q15 ends with DROP VIEW)."""
    for i in range(len(stmts) - 1, -1, -1):
        if QUERY_START.match(stmts[i]):
            return i
    return len(stmts) - 1


class Engine:
    def __init__(self, eid: str, spec: dict):
        self.id, self.spec = eid, spec
        self.lock = threading.Lock()
        self.session = None
        self.namespace = None
        self.use_failed = False

    def up(self) -> bool:
        return port_open(self.spec["port"])

    def stats(self):
        try:
            if "container" in self.spec:
                return CgroupStats(self.spec["container"])
            return PidStats(int(Path(self.spec["pidfile"]).read_text()))
        except Exception:  # noqa: BLE001
            return None

    def _connect(self):
        if self.spec["kind"] == "flight":
            import adbc_driver_flightsql.dbapi as fsql
            return fsql.connect(f"grpc://localhost:{self.spec['port']}")
        from pyspark.sql import SparkSession
        b = SparkSession.builder.remote(f"sc://localhost:{self.spec['port']}")
        b = b.config("spark.sql.session.timeZone", "UTC")
        try:
            return b.create()
        except Exception:  # noqa: BLE001
            return b.getOrCreate()

    def reset(self):
        self.session, self.namespace = None, None

    def use(self, ns: str | None):
        if not ns or ns == self.namespace:
            return
        if self.spec["kind"] == "flight":
            # The Flight server has one shared session; USE may or may not be accepted.
            self.use_failed = True
            for stmt in (f"USE {ns}", f"USE polaris.{ns}"):
                try:
                    self._flight_exec(stmt)
                    self.use_failed = False
                    break
                except Exception:  # noqa: BLE001
                    pass
        else:
            tpch.use(self.session, ns)
        self.namespace = ns

    def _flight_exec(self, stmt):
        cur = self.session.cursor()
        try:
            cur.execute(stmt)
            tbl = cur.fetch_arrow_table()
            return tbl
        finally:
            cur.close()

    def execute(self, sql: str, ns: str | None):
        """Run all statements; return (columns, types, rows) of the last one."""
        if self.session is None:
            self.session = self._connect()
        self.use(ns)
        stmts = split_statements(sql)
        cols, types, rows = [], [], []
        target = result_index(stmts)
        for i, stmt in enumerate(stmts):
            last = i == target
            if self.spec["kind"] == "flight":
                tbl = self._flight_exec(stmt)
                if last:
                    cols = tbl.schema.names
                    types = [str(f.type) for f in tbl.schema]
                    rows = [tuple(r.values()) for r in tbl.to_pylist()]
            else:
                df = self.session.sql(stmt)
                if last:
                    rows = df.collect()
                    cols = df.columns
                    types = [f.dataType.simpleString() for f in df.schema.fields]
                else:
                    df.collect()
        return cols, types, [tuple(r) for r in rows]

    def explain(self, sql: str, ns: str | None) -> str:
        stmts = split_statements(sql)
        stmt = stmts[result_index(stmts)]
        try:
            _, _, rows = self.execute(f"EXPLAIN FORMATTED {stmt}", ns)
        except Exception:  # noqa: BLE001
            _, _, rows = self.execute(f"EXPLAIN {stmt}", ns)
        return "\n".join(str(r[0]) for r in rows)


POOL = {eid: Engine(eid, spec) for eid, spec in ENGINES.items()}
HISTORY: collections.deque = collections.deque(maxlen=100)


class QueryTimeout(Exception):
    pass


def bounded(engine: Engine, fn, timeout: float):
    """Run fn() with a deadline. On timeout, interrupt the engine's running operations."""
    box = {}

    def work():
        try:
            box["v"] = fn()
        except BaseException as e:  # noqa: BLE001
            box["e"] = e
    t = threading.Thread(target=work, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        try:
            if engine.spec["kind"] == "connect" and engine.session is not None:
                engine.session.interruptAll()
        except Exception:  # noqa: BLE001
            pass
        engine.reset()
        raise QueryTimeout(f"timed out after {timeout:g} s; running operations were interrupted")
    if "e" in box:
        raise box["e"]
    return box["v"]


def run_on(engine: Engine, sql: str, ns: str | None, reps: int, warmup: bool, explain: bool,
           timeout: float = 600) -> dict:
    res = {"engine": engine.id, "label": engine.spec["label"], "runs": []}
    if not engine.up():
        engine.reset()
        return res | {"ok": False, "error": f"not running on port {engine.spec['port']}",
                      "error_head": f"not running on port {engine.spec['port']}"}
    with engine.lock:
        try:
            stats = engine.stats()
            if warmup:
                t0 = time.perf_counter()
                bounded(engine, lambda: engine.execute(sql, ns), timeout)
                res["warmup_s"] = round(time.perf_counter() - t0, 4)
            result = None
            for _ in range(max(1, reps)):
                cpu0 = stats.cpu_seconds() if stats else float("nan")
                peak = PeakSampler(stats) if stats else None
                t0 = time.perf_counter()
                if peak:
                    with peak:
                        result = bounded(engine, lambda: engine.execute(sql, ns), timeout)
                else:
                    result = bounded(engine, lambda: engine.execute(sql, ns), timeout)
                wall = time.perf_counter() - t0
                cpu = (stats.cpu_seconds() - cpu0) if stats else float("nan")
                res["runs"].append({"wall_s": round(wall, 4),
                                    "cpu_s": None if cpu != cpu else round(cpu, 3),
                                    "peak_mb": round(peak.peak / 2**20, 1) if peak and peak.peak > 0 else None})
            cols, types, rows = result
            walls = [r["wall_s"] for r in res["runs"]]
            cpus = [r["cpu_s"] for r in res["runs"] if r["cpu_s"] is not None]
            peaks = [r["peak_mb"] for r in res["runs"] if r["peak_mb"] is not None]
            res.update(ok=True, columns=cols, types=types, row_count=len(rows),
                       wall_median_s=round(statistics.median(walls), 4), wall_min_s=min(walls),
                       cpu_median_s=round(statistics.median(cpus), 3) if cpus else None,
                       peak_mb=max(peaks) if peaks else None,
                       rows=[[canon(v) for v in r] for r in rows[:DISPLAY_ROWS]],
                       _canon=[[canon(v) for v in r] for r in rows[:COMPARE_ROWS]],
                       truncated_compare=len(rows) > COMPARE_ROWS)
            if explain:
                try:
                    res["plan"] = bounded(engine, lambda: engine.explain(sql, ns), 120)
                except Exception as e:  # noqa: BLE001
                    res["plan"] = f"EXPLAIN failed: {e}"
        except Exception as e:  # noqa: BLE001
            msg = str(e)
            if engine.use_failed and "not found" in msg.lower():
                msg = ("Sail's Flight SQL server can't switch namespace per query; its default database is set at "
                       "start (SAIL_CATALOG__DEFAULT_DATABASE, tpch_sf<SF> from local-up.sh). Use fully qualified "
                       "names such as polaris.tpch_sf1.orders.\n\n" + msg)
            res.update(ok=False, error=msg[:4000], error_head=msg.strip().splitlines()[0][:300] if msg.strip() else type(e).__name__)
            if "UNAVAILABLE" in msg or "Connection refused" in msg or "closed" in msg.lower():
                engine.reset()
    return res


def _num(x):
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str) and re.fullmatch(r"-?\d+(\.\d+)?([eE][-+]?\d+)?", x):
        return float(x)
    return None


def compare(ref: dict, other: dict, ordered: bool, rel_tol: float) -> dict:
    a, b = ref["_canon"], other["_canon"]
    out = {"engine": other["engine"], "reference": ref["engine"], "ordered": ordered,
           "row_count": [len(a), len(b)],
           "same_column_names": [c.lower() for c in ref["columns"]] == [c.lower() for c in other["columns"]],
           "type_diffs": [{"column": c, "reference": t1, "other": t2}
                          for c, t1, t2 in zip(ref["columns"], ref["types"], other["types"]) if t1 != t2]}
    if len(ref["columns"]) != len(other["columns"]):
        out["verdict"] = "different columns"
        return out
    key = lambda r: json.dumps(r, default=str)  # noqa: E731
    if ordered:
        exact = a == b
    else:
        exact = collections.Counter(map(key, a)) == collections.Counter(map(key, b))
    if exact:
        out["verdict"] = "identical"
        return out
    # numeric tolerance: line rows up (sorted unless ORDER BY) and compare cell by cell
    if len(a) == len(b):
        sa, sb = (a, b) if ordered else (sorted(a, key=key), sorted(b, key=key))
        max_abs, max_rel, bad, cells = 0.0, 0.0, [], 0
        for i, (ra, rb) in enumerate(zip(sa, sb)):
            for j, (x, y) in enumerate(zip(ra, rb)):
                if x == y:
                    continue
                nx, ny = _num(x), _num(y)
                if nx is None or ny is None:
                    bad.append({"row": i, "column": ref["columns"][j], "reference": x, "other": y})
                    continue
                d = abs(nx - ny)
                r = d / max(abs(nx), abs(ny), 1e-300)
                cells += 1
                max_abs, max_rel = max(max_abs, d), max(max_rel, r)
                if r > rel_tol:
                    bad.append({"row": i, "column": ref["columns"][j], "reference": x, "other": y,
                                "abs_diff": d, "rel_diff": r})
        out.update(numeric_cells_differing=cells, max_abs_diff=max_abs, max_rel_diff=max_rel)
        if not bad:
            out["verdict"] = f"equal within tolerance ({rel_tol:g})"
            return out
        out["cell_diffs"] = bad[:SAMPLE_DIFF]
        out["cell_diff_count"] = len(bad)
    if out.get("cell_diffs"):
        out["verdict"] = "different"
        return out
    ca, cb = collections.Counter(map(key, a)), collections.Counter(map(key, b))
    out["only_in_reference"] = [json.loads(k) for k in list((ca - cb).elements())[:SAMPLE_DIFF]]
    out["only_in_other"] = [json.loads(k) for k in list((cb - ca).elements())[:SAMPLE_DIFF]]
    out["only_counts"] = [sum((ca - cb).values()), sum((cb - ca).values())]
    out["verdict"] = "different"
    return out


def run(body: dict) -> dict:
    sql = body["sql"]
    wanted = set(body.get("engines", []))
    engines = [e for eid, e in POOL.items() if eid in wanted]
    ns = body.get("namespace") or None
    reps = int(body.get("reps", 1))
    warmup = bool(body.get("warmup", False))
    explain = bool(body.get("explain", False))
    parallel = body.get("mode") == "parallel"
    rel_tol = float(body.get("rel_tol", 1e-9))
    timeout = float(body.get("timeout_s", 600))
    t0 = time.time()
    if parallel:
        results = [None] * len(engines)

        def work(i, e):
            results[i] = run_on(e, sql, ns, reps, warmup, explain, timeout)
        ts = [threading.Thread(target=work, args=(i, e)) for i, e in enumerate(engines)]
        [t.start() for t in ts]
        [t.join() for t in ts]
    else:
        results = [run_on(e, sql, ns, reps, warmup, explain, timeout) for e in engines]
    stmts = split_statements(sql)
    ordered = bool(re.search(r"\border\s+by\b", stmts[result_index(stmts)], re.I)) if stmts else False
    ok = [r for r in results if r.get("ok")]
    comparisons = [compare(ok[0], r, ordered, rel_tol) for r in ok[1:]] if len(ok) > 1 else []
    if ok:
        best = min(r["wall_median_s"] for r in ok)
        for r in ok:
            r["vs_fastest"] = round(r["wall_median_s"] / best, 2) if best > 0 else None
    for r in results:
        r.pop("_canon", None)
    out = {"sql": sql, "namespace": ns, "reps": reps, "warmup": warmup, "mode": "parallel" if parallel else "sequential",
           "started": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t0)), "results": results,
           "comparisons": comparisons}
    HISTORY.appendleft({k: out[k] for k in ("sql", "namespace", "started", "reps")} | {
        "summary": [{"engine": r["engine"], "ok": r.get("ok"), "wall_median_s": r.get("wall_median_s"),
                     "rows": r.get("row_count")} for r in results],
        "verdicts": [c["verdict"] for c in comparisons]})
    return out


def catalog_tree() -> dict:
    import requests
    tok = polaris_token()
    h = {"Authorization": f"Bearer {tok}", "Polaris-Realm": REALM}
    base = f"{POLARIS}/api/catalog/v1/{CATALOG}"
    nss = requests.get(f"{base}/namespaces", headers=h, timeout=10).json().get("namespaces", [])
    tree = {}
    for ns in nss:
        name = ".".join(ns)
        tabs = requests.get(f"{base}/namespaces/{name}/tables", headers=h, timeout=10).json().get("identifiers", [])
        tree[name] = sorted(t["name"] for t in tabs)
    return tree


def table_columns(ns: str, table: str) -> list:
    import requests
    tok = polaris_token()
    h = {"Authorization": f"Bearer {tok}", "Polaris-Realm": REALM}
    meta = requests.get(f"{POLARIS}/api/catalog/v1/{CATALOG}/namespaces/{ns}/tables/{table}",
                        headers=h, timeout=10).json()["metadata"]
    sid = meta.get("current-schema-id", 0)
    schema = next(s for s in meta["schemas"] if s.get("schema-id", 0) == sid)
    return [{"name": f["name"], "type": f["type"] if isinstance(f["type"], str) else f["type"].get("type")}
            for f in schema["fields"]]


def presets() -> list:
    out = []
    for q in range(1, 23):
        p = tpch.QDIR / f"q{q}.sql"
        if p.exists():
            out.append({"name": f"TPC-H q{q}", "sql": p.read_text().strip()})
    out += [
        {"name": "Row counts", "sql": "SELECT 'lineitem' t, count(*) n FROM lineitem UNION ALL SELECT 'orders', count(*) FROM orders"},
        {"name": "Point lookup", "sql": "SELECT * FROM orders WHERE o_orderkey = 7"},
        {"name": "Decimal avg (known diff)", "sql": "SELECT l_returnflag, avg(l_extendedprice) FROM lineitem GROUP BY 1 ORDER BY 1"},
    ]
    return out


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code: int, body, ctype="application/json"):
        data = body if isinstance(body, bytes) else json.dumps(body, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        try:
            path = self.path.split("?")[0]
            if path in ("/", "/index.html"):
                return self._send(200, STATIC.read_bytes(), "text/html; charset=utf-8")
            if path == "/api/engines":
                return self._send(200, [{"id": e.id, "label": e.spec["label"], "port": e.spec["port"],
                                         "kind": e.spec["kind"], "up": e.up()} for e in POOL.values()])
            if path == "/api/catalog":
                return self._send(200, catalog_tree())
            if path.startswith("/api/columns/"):
                _, _, _, ns, table = path.split("/")
                return self._send(200, table_columns(ns, table))
            if path == "/api/presets":
                return self._send(200, presets())
            if path == "/api/history":
                return self._send(200, list(HISTORY))
            self._send(404, {"error": "not found"})
        except Exception as e:  # noqa: BLE001
            self._send(500, {"error": str(e), "trace": traceback.format_exc()[-2000:]})

    def do_POST(self):
        try:
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
            if self.path == "/api/run":
                return self._send(200, run(body))
            if self.path == "/api/reset":
                for e in POOL.values():
                    e.reset()
                return self._send(200, {"ok": True})
            self._send(404, {"error": "not found"})
        except Exception as e:  # noqa: BLE001
            self._send(500, {"error": str(e), "trace": traceback.format_exc()[-2000:]})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8088)
    a = ap.parse_args()
    srv = ThreadingHTTPServer((a.host, a.port), Handler)
    print(f"query workbench on http://{a.host}:{a.port}", flush=True)
    srv.serve_forever()


if __name__ == "__main__":
    main()
