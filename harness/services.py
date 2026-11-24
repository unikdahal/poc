"""Start/stop long-lived engine servers across workflow steps.

  python -m harness.services spark [--comet] [--port 15002]
  python -m harness.services sail --name sail --server spark --port 50051 [--mode local-cluster] [--env K=V ...]
  python -m harness.services stop-spark
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from harness.common import OUT, connect, log, start_sail, start_spark_connect, warm, write_token

PIDS = Path("/tmp/poc-pids")
PIDS.mkdir(exist_ok=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("what", choices=["spark", "sail", "stop-spark", "token"])
    ap.add_argument("--comet", action="store_true")
    ap.add_argument("--port", type=int)
    ap.add_argument("--name", default="sail")
    ap.add_argument("--server", default="spark", choices=["spark", "flight"])
    ap.add_argument("--mode", default="local")
    ap.add_argument("--env", nargs="*", default=[])
    ap.add_argument("--memory", default="6g")
    a = ap.parse_args()
    if a.what == "token":
        write_token()
        return
    if a.what == "spark":
        port = a.port or 15002
        # detach: the server must outlive this step
        code = ("from harness.common import start_spark_connect;import time,sys;"
                f"p,_=start_spark_connect(port={port}, comet={a.comet}, memory='{a.memory}');"
                "open('/tmp/poc-pids/spark-%d'%" + str(port) + ",'w').write(str(p.pid));p.wait()")
        t0 = time.time()
        subprocess.Popen([sys.executable, "-c", code], start_new_session=True,
                         stdout=open(OUT / f"spark-launcher-{port}.log", "w"), stderr=subprocess.STDOUT,
                         env=dict(os.environ))
        from harness.common import wait_port
        wait_port(port, timeout=900)
        s = connect(port)
        warm(s)
        log(f"spark connect ready on {port} in {time.time() - t0:.1f}s (comet={a.comet})")
        return
    if a.what == "stop-spark":
        for f in PIDS.glob("spark-*"):
            try:
                os.killpg(os.getpgid(int(f.read_text())), signal.SIGTERM)
            except Exception:  # noqa: BLE001
                pass
            f.unlink()
        subprocess.run("pkill -f SparkConnectServer", shell=True)
        time.sleep(3)
        return
    env = dict(kv.split("=", 1) for kv in a.env)
    port = a.port or (50051 if a.server == "spark" else 32010)
    t = start_sail(a.name, a.server, port, a.mode, env)
    if a.server == "spark":
        s = connect(port)
        w = warm(s)
    else:
        import adbc_driver_flightsql.dbapi as f
        t0 = time.time()
        for _ in range(120):
            try:
                c = f.connect(f"grpc://localhost:{port}").cursor()
                c.execute("SELECT 1")
                c.fetchall()
                break
            except Exception:  # noqa: BLE001
                time.sleep(1)
        w = time.time() - t0
    log(f"sail {a.name} ({a.server}, {a.mode}) port open {t:.1f}s, first query ok after +{w:.1f}s")


if __name__ == "__main__":
    main()
