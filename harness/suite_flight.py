"""Arrow Flight SQL / ADBC probe against Sail's `sail flight server`.

Exercises what a production ADBC client typically uses, records what works, and measures
latency, isolation, ticket lifecycle and streaming of large results.
"""

import statistics
import threading
import time

import adbc_driver_flightsql.dbapi as fsql
import pyarrow.flight as flight

from harness.common import CgroupStats, PeakSampler, canon, log, write_json

URI = "grpc://localhost:32010"
R: dict = {}


def probe(name, fn):
    t0 = time.perf_counter()
    try:
        out = fn()
        R[name] = {"ok": True, "result": out, "seconds": round(time.perf_counter() - t0, 4)}
    except Exception as e:  # noqa: BLE001
        R[name] = {"ok": False, "error": f"{type(e).__name__}: {str(e).splitlines()[0][:400]}"}
    log(f"{'OK ' if R[name]['ok'] else 'ERR'} {name}: {str(R[name].get('result', R[name].get('error')))[:160]}")


def lat(fn, n):
    xs = []
    for _ in range(n):
        t0 = time.perf_counter()
        fn()
        xs.append((time.perf_counter() - t0) * 1000)
    xs.sort()
    return {"n": n, "p50_ms": round(xs[n // 2], 2), "p95_ms": round(xs[int(n * 0.95) - 1], 2),
            "p99_ms": round(xs[int(n * 0.99) - 1], 2), "mean_ms": round(statistics.mean(xs), 2)}


def any_cmd_statement_query(sql: str) -> bytes:
    """Hand-encode google.protobuf.Any(CommandStatementQuery{query})."""
    def ld(field, payload: bytes) -> bytes:
        n, out = len(payload), bytearray()
        while True:
            b = n & 0x7F
            n >>= 7
            out.append(b | (0x80 if n else 0))
            if not n:
                break
        return bytes([(field << 3) | 2]) + bytes(out) + payload
    inner = ld(1, sql.encode())
    return ld(1, b"type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery") + ld(2, inner)


def main():
    conn = fsql.connect(URI)
    cur = conn.cursor()

    def q(sql):
        c = conn.cursor()  # fresh cursor: a failed probe must not leave state behind
        try:
            c.execute(sql)
            return c.fetchall()
        finally:
            c.close()

    def qlat(sql):
        cur.execute(sql)
        return cur.fetchall()

    probe("query/select-1", lambda: q("SELECT 1 AS x"))
    probe("query/iceberg-count", lambda: q("SELECT count(*) FROM polaris.tpch_sf1.lineitem"))
    probe("query/tpch-q6-style", lambda: [list(map(canon, r)) for r in q(
        "SELECT sum(l_extendedprice * l_discount) FROM polaris.tpch_sf1.lineitem WHERE l_shipdate >= DATE '1994-01-01' "
        "AND l_shipdate < DATE '1995-01-01' AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24")])
    probe("ddl/create-namespace-via-execute", lambda: q("CREATE NAMESPACE IF NOT EXISTS polaris.flight"))
    probe("ddl/create-table-via-execute", lambda: q("CREATE TABLE IF NOT EXISTS polaris.flight.t (id bigint, s string) USING iceberg"))
    probe("dml/insert-via-execute", lambda: q("INSERT INTO polaris.flight.t VALUES (1, 'a'), (2, 'b')"))
    probe("dml/read-back", lambda: q("SELECT count(*) FROM polaris.flight.t"))

    def execute_update():
        st = conn.cursor().adbc_statement
        st.set_sql_query("INSERT INTO polaris.flight.t VALUES (3, 'c')")
        return st.execute_update()
    probe("adbc/execute_update (CommandStatementUpdate)", execute_update)
    probe("adbc/parameterized-query (prepared statement)", lambda: (lambda c: (c.execute("SELECT ? AS p", parameters=(41,)), c.fetchall())[1])(conn.cursor()))
    probe("adbc/prepare", lambda: (conn.cursor().adbc_prepare("SELECT 1"), "prepared")[1])
    probe("adbc/get_info", lambda: {str(k): str(v)[:60] for k, v in list(conn.adbc_get_info().items())[:12]})
    probe("adbc/get_table_types", lambda: conn.adbc_get_table_types())
    probe("adbc/get_objects(depth=tables)", lambda: conn.adbc_get_objects(depth="tables").read_all().num_rows)
    probe("adbc/get_table_schema", lambda: str(conn.adbc_get_table_schema("lineitem", db_schema_filter="tpch_sf1")))

    # session isolation between two connections
    conn2 = fsql.connect(URI)
    c2 = conn2.cursor()

    def isolation():
        q("CREATE OR REPLACE TEMPORARY VIEW only_on_conn1 AS SELECT 42 AS v")
        c2.execute("SELECT v FROM only_on_conn1")
        return {"conn2_sees_conn1_temp_view": c2.fetchall()}
    probe("isolation/temp-view-visible-across-connections", isolation)

    def set_leak():
        q("SET datafusion.execution.batch_size = 1234")
        c2.execute("SET datafusion.execution.batch_size")
        return c2.fetchall()
    probe("isolation/set-visible-across-connections", set_leak)

    # concurrency cross-talk: each thread checks it gets its own literal back
    def concurrent():
        bad, errs = [], []

        def w(i):
            cc = fsql.connect(URI).cursor()
            for j in range(20):
                try:
                    cc.execute(f"SELECT {i * 1000 + j} AS v")
                    v = cc.fetchall()[0][0]
                    if v != i * 1000 + j:
                        bad.append((i, j, v))
                except Exception as e:  # noqa: BLE001
                    errs.append(str(e)[:120])
        ths = [threading.Thread(target=w, args=(i,)) for i in range(8)]
        [t.start() for t in ths]
        [t.join() for t in ths]
        return {"wrong_results": len(bad), "errors": len(errs), "sample_error": errs[:1]}
    probe("concurrency/8-connections-x20", concurrent)

    # latency
    R["latency/select-1"] = lat(lambda: qlat("SELECT 1"), 200)
    R["latency/iceberg-point-lookup"] = lat(lambda: qlat("SELECT * FROM polaris.tpch_sf1.nation WHERE n_nationkey = 7"), 50)
    log(f"latency: {R['latency/select-1']} {R['latency/iceberg-point-lookup']}")

    # cancellation of a long query
    def cancel():
        cc = fsql.connect(URI).cursor()
        err = {}

        def run():
            try:
                cc.execute("SELECT count(*) FROM polaris.tpch_sf1.lineitem a JOIN polaris.tpch_sf1.lineitem b ON a.l_quantity = b.l_quantity")
                cc.fetchall()
                err["finished"] = True
            except Exception as e:  # noqa: BLE001
                err["error"] = str(e)[:200]
        t = threading.Thread(target=run)
        t.start()
        time.sleep(3)
        try:
            cc.adbc_cancel()
            err["cancel_call"] = "ok"
        except Exception as e:  # noqa: BLE001
            err["cancel_call"] = str(e)[:200]
        t.join(timeout=60)
        err["still_running_after_60s"] = t.is_alive()
        return err
    probe("adbc/cancel-long-query", cancel)

    # Flight-level ticket lifecycle
    client = flight.FlightClient(URI)

    def tickets():
        info = client.get_flight_info(flight.FlightDescriptor.for_command(any_cmd_statement_query("SELECT 7 AS v")))
        tk = info.endpoints[0].ticket
        first = client.do_get(tk).read_all().to_pylist()
        try:
            client.do_get(tk).read_all()
            second = "succeeded"
        except Exception as e:  # noqa: BLE001
            second = str(e).splitlines()[0][:160]
        return {"first_do_get": first, "second_do_get": second, "endpoints": len(info.endpoints)}
    probe("flight/ticket-single-use", tickets)

    stats = CgroupStats("sail-flight")

    def abandoned():
        m0 = stats.mem_current()
        for _ in range(300):
            client.get_flight_info(flight.FlightDescriptor.for_command(
                any_cmd_statement_query("SELECT * FROM polaris.tpch_sf1.orders LIMIT 20000")))
        time.sleep(5)
        m1 = stats.mem_current()
        return {"mem_before_mb": round(m0 / 2**20, 1), "mem_after_300_unfetched_mb": round(m1 / 2**20, 1)}
    probe("flight/abandoned-tickets-memory", abandoned)

    def big_result():
        with PeakSampler(stats) as ps:
            t0 = time.perf_counter()
            c = conn.cursor()
            c.execute("SELECT * FROM polaris.tpch_sf1.lineitem")
            tbl = c.fetch_arrow_table()
            secs = time.perf_counter() - t0
        return {"rows": tbl.num_rows, "seconds": round(secs, 2), "rows_per_s": int(tbl.num_rows / secs),
                "arrow_mb": round(tbl.nbytes / 2**20, 1), "sail_peak_mem_mb": round(ps.peak / 2**20, 1)}
    probe("stream/lineitem-sf1-full-scan", big_result)
    probe("isolation/set-after-sessions", lambda: q("SELECT 1"))
    write_json("flight-sql", R)


if __name__ == "__main__":
    main()
