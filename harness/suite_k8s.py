"""Sail in kubernetes-cluster mode on kind: worker lifecycle, cold start, per-session workers,
idle reaping, and failure injection (worker pod kill, server pod kill) per shuffle backend.

Prerequisites (done by the workflow): kind cluster `poc`, the Sail image loaded into it, and the
RustFS/Polaris containers attached to the `kind` docker network.
"""

import json
import subprocess
import threading
import time

from harness import tpch
from harness.common import CATALOG, S3, TOKEN_FILE, connect, log, sh, write_json, write_token

NSK = "sail"
R: dict = {}


def kc(args: str, check=True) -> str:
    return sh(f"kubectl -n {NSK} {args}", check=check)


def ip_on_kind(container: str) -> str:
    return sh(f"docker inspect -f '{{{{(index .NetworkSettings.Networks \"kind\").IPAddress}}}}' {container}").strip()


def refresh_token_secret():
    write_token()
    sh(f"kubectl -n {NSK} create secret generic polaris-token --from-file=polaris={TOKEN_FILE} "
       f"--dry-run=client -o yaml | kubectl apply -f -")


def manifest(image: str, backend: str, polaris_ip: str, s3_ip: str) -> str:
    env = {
        "RUST_LOG": "info",
        "SAIL_MODE": "kubernetes-cluster",
        "SAIL_CLUSTER__DRIVER_LISTEN_HOST": "0.0.0.0",
        "SAIL_KUBERNETES__IMAGE": image,
        "SAIL_KUBERNETES__IMAGE_PULL_POLICY": "Never",
        "SAIL_KUBERNETES__NAMESPACE": NSK,
        "SAIL_KUBERNETES__WORKER_SERVICE_ACCOUNT_NAME": "sail-user",
        "SAIL_CLUSTER__WORKER_INITIAL_COUNT": "2",
        "SAIL_CLUSTER__WORKER_MAX_COUNT": "4",
        "SAIL_CLUSTER__WORKER_TASK_SLOTS": "8",
        "SAIL_CLUSTER__WORKER_MAX_IDLE_TIME_SECS": "60",
        "SAIL_EXECUTION__DEFAULT_PARALLELISM": "8",
        "SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE": backend,
        "SAIL_CATALOG__LIST": (f'[{{type="iceberg-rest", name="polaris", uri="http://{polaris_ip}:8181/api/catalog", '
                               f'warehouse="{CATALOG}", bearer_access_token_file="/tokens/polaris"}}]'),
        "SAIL_CATALOG__DEFAULT_CATALOG": "polaris",
        "AWS_ACCESS_KEY_ID": S3["key"], "AWS_SECRET_ACCESS_KEY": S3["secret"], "AWS_REGION": S3["region"],
        "AWS_ENDPOINT": f"http://{s3_ip}:9000", "AWS_ALLOW_HTTP": "true",
    }
    if backend == "storage":
        env["SAIL_CLUSTER__SHUFFLE_BACKEND__STORAGE__PATH"] = "s3://shuffle/k8s"
    worker_env = [{"name": k, "value": v} for k, v in env.items()
                  if k.startswith(("AWS_", "SAIL_CATALOG", "SAIL_CLUSTER__SHUFFLE", "SAIL_EXECUTION"))]
    template = {"spec": {"containers": [{
        "name": "worker", "env": worker_env,
        "resources": {"requests": {"cpu": "250m", "memory": "256Mi"}, "limits": {"memory": "3Gi"}},
        "volumeMounts": [{"name": "tokens", "mountPath": "/tokens"}]}],
        "volumes": [{"name": "tokens", "secret": {"secretName": "polaris-token"}}]}}
    env["SAIL_KUBERNETES__WORKER_POD_TEMPLATE"] = json.dumps(template)
    env_yaml = "\n".join(f"            - name: {k}\n              value: {json.dumps(v)}" for k, v in env.items())
    return f"""apiVersion: apps/v1
kind: Deployment
metadata:
  name: sail-spark-server
  namespace: {NSK}
spec:
  replicas: 1
  selector: {{matchLabels: {{app: sail-server}}}}
  template:
    metadata: {{labels: {{app: sail-server}}}}
    spec:
      serviceAccountName: sail-user
      containers:
        - name: server
          image: {image}
          imagePullPolicy: Never
          command: ["sail"]
          args: ["spark", "server", "--ip", "0.0.0.0", "--port", "50051"]
          ports: [{{containerPort: 50051}}]
          volumeMounts: [{{name: tokens, mountPath: /tokens}}]
          env:
{env_yaml}
            - name: SAIL_CLUSTER__DRIVER_EXTERNAL_HOST
              valueFrom: {{fieldRef: {{fieldPath: status.podIP}}}}
            - name: SAIL_KUBERNETES__DRIVER_POD_NAME
              valueFrom: {{fieldRef: {{fieldPath: metadata.name}}}}
      volumes: [{{name: tokens, secret: {{secretName: polaris-token}}}}]
---
apiVersion: v1
kind: Service
metadata: {{name: sail-spark-server, namespace: {NSK}}}
spec:
  selector: {{app: sail-server}}
  ports: [{{port: 50051, targetPort: 50051}}]
"""


RBAC = f"""apiVersion: v1
kind: Namespace
metadata: {{name: {NSK}}}
---
apiVersion: v1
kind: ServiceAccount
metadata: {{name: sail-user, namespace: {NSK}}}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: {{name: sail-server, namespace: {NSK}}}
rules: [{{apiGroups: [""], resources: ["pods"], verbs: ["*"]}}]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: {{name: sail-server, namespace: {NSK}}}
subjects: [{{kind: ServiceAccount, name: sail-user, namespace: {NSK}}}]
roleRef: {{kind: Role, name: sail-server, apiGroup: rbac.authorization.k8s.io}}
"""


class PodWatcher:
    """Polls worker pods every second; records the timeline."""

    def __init__(self):
        self.timeline = []
        self._stop = threading.Event()

    def pods(self):
        out = kc("get pods -l app.kubernetes.io/component=worker -o json", check=False)
        try:
            items = json.loads(out)["items"]
        except Exception:  # noqa: BLE001
            return []
        return [(p["metadata"]["name"], p["status"].get("phase"),
                 p["metadata"]["labels"].get("sail.lakesail.com/worker-manager")) for p in items
                if not p["metadata"].get("deletionTimestamp")]

    def __enter__(self):
        self.t0 = time.time()

        def run():
            while not self._stop.is_set():
                ps = self.pods()
                self.timeline.append((round(time.time() - self.t0, 1), len(ps),
                                      len({m for _, _, m in ps}), sum(1 for _, ph, _ in ps if ph == "Running"),
                                      sorted({ph for _, ph, _ in ps})))
                time.sleep(1)
        self.th = threading.Thread(target=run, daemon=True)
        self.th.start()
        return self

    def __exit__(self, *a):
        self._stop.set()
        self.th.join()

    def peak(self):
        return max((x[1] for x in self.timeline), default=0), max((x[2] for x in self.timeline), default=0)


port_forward = None


def start_port_forward():
    global port_forward
    if port_forward:
        port_forward.kill()
    kc("rollout status deploy/sail-spark-server --timeout=300s")
    port_forward = subprocess.Popen(["kubectl", "-n", NSK, "port-forward", "svc/sail-spark-server", "50051:50051"],
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(3)


def wait_no_workers(timeout=240):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if not PodWatcher().pods():
            return round(time.time() - t0, 1)
        time.sleep(2)
    return None


def heavy_sql():
    return ("SELECT count(*), sum(a.l_extendedprice * b.l_discount) FROM polaris.tpch_sf1.lineitem a "
            "JOIN polaris.tpch_sf1.lineitem b ON a.l_suppkey = b.l_suppkey WHERE a.l_orderkey < 3000000")


def run_backend(image, backend, pip, sip):
    res = {}
    kc("delete deploy sail-spark-server --ignore-not-found --wait=true", check=False)
    wait_no_workers(120)
    open("/tmp/sail-k8s.yaml", "w").write(manifest(image, backend, pip, sip))
    sh("kubectl apply -f /tmp/sail-k8s.yaml")
    start_port_forward()

    # cold session: session create + first query; workers launched on demand
    with PodWatcher() as w:
        t0 = time.perf_counter()
        s = connect(50051)
        s.sql("SELECT 1").collect()
        res["session_first_select1_s"] = round(time.perf_counter() - t0, 2)
        t1 = time.perf_counter()
        rows, _, secs = tpch.run_query(s, 1, "tpch_sf1")
        res["first_distributed_query_q1_s"] = round(time.perf_counter() - t1, 2)
        t2 = time.perf_counter()
        tpch.run_query(s, 1, "tpch_sf1")
        res["warm_q1_s"] = round(time.perf_counter() - t2, 2)
        t3 = time.perf_counter()
        s.sql("SELECT * FROM polaris.tpch_sf1.nation WHERE n_nationkey = 7").collect()
        res["warm_point_lookup_s"] = round(time.perf_counter() - t3, 3)
    res["cold_session_pods_timeline"] = w.timeline[:60]
    res["pods_peak_single_session"] = w.peak()

    # all 22 queries on the cluster for correctness and timing
    qs = {}
    for q in range(1, 23):
        try:
            rows, _, secs = tpch.run_query(s, q, "tpch_sf1")
            ref = json.loads(open(f"out/ref/tpch_sf1_q{q}.json").read())["rows"]
            from harness.common import same_rows
            qs[q] = {"s": round(secs, 2), "matches_spark": same_rows(tpch.canon_rows(rows), ref, tpch.ordered(q))}
        except Exception as e:  # noqa: BLE001
            qs[q] = {"error": str(e).split("\n")[0][:300]}
    res["tpch_sf1"] = qs

    # three concurrent sessions -> worker pods per session
    with PodWatcher() as w:
        sess = [connect(50051) for _ in range(3)]
        ths = [threading.Thread(target=lambda x=x: tpch.run_query(x, 9, "tpch_sf1")) for x in sess]
        t0 = time.time()
        [t.start() for t in ths]
        [t.join() for t in ths]
        res["three_sessions_q9_wall_s"] = round(time.time() - t0, 1)
    res["three_sessions_pods_peak_and_managers"] = w.peak()

    # failure injection: kill one worker pod during a long query
    def kill_worker_during(sql, delay):
        out = {}
        before = {p for p, _, _ in PodWatcher().pods()}
        sess2 = connect(50051)
        sess2.sql("SELECT count(*) FROM polaris.tpch_sf1.orders").collect()
        own = [(p, m) for p, ph, m in PodWatcher().pods() if p not in before]
        own_mgr = own[0][1] if own else None

        def run():
            t0 = time.time()
            try:
                out["result"] = sess2.sql(sql).collect()[0][0]
                out["outcome"] = "succeeded"
            except Exception as e:  # noqa: BLE001
                out["outcome"] = "failed"
                out["error"] = str(e).split("\n")[0][:300]
            out["seconds"] = round(time.time() - t0, 1)
        t = threading.Thread(target=run)
        t.start()
        time.sleep(delay)
        pods = [p for p, ph, m in PodWatcher().pods() if ph == "Running" and m == own_mgr]
        out["query_still_running_at_kill"] = t.is_alive()
        out["own_session_workers_at_kill"] = len(pods)
        if pods:
            kc(f"delete pod {pods[0]} --grace-period=0 --force", check=False)
            out["killed"] = pods[0]
        t.join(timeout=900)
        out["hung"] = t.is_alive()
        return out

    baseline = None
    try:
        baseline = connect(50051)
        t0 = time.time()
        expected = baseline.sql(heavy_sql()).collect()[0][0]
        res["heavy_query_baseline_s"] = round(time.time() - t0, 1)
        k = kill_worker_during(heavy_sql(), max(3.0, res["heavy_query_baseline_s"] * 0.4))
        k["correct_result"] = k.get("result") == expected if "result" in k else None
        res["kill_worker_mid_query"] = k
    except Exception as e:  # noqa: BLE001
        res["kill_worker_mid_query"] = {"error": str(e)[:300]}

    # idle reaping: close every session, then watch pods for 4 minutes
    for x in [y for y in [s] + sess + [baseline] if y is not None]:
        try:
            x.stop()
        except Exception:  # noqa: BLE001
            pass
    with PodWatcher() as w:
        time.sleep(240)
    res["idle_timeline_after_sessions_closed"] = w.timeline[::10]
    res["seconds_until_no_worker_pods_after_idle"] = next((t for t, n, *_ in w.timeline if n == 0), None)
    # one session left open but idle
    with PodWatcher() as w:
        s_idle = connect(50051)
        s_idle.sql("SELECT count(*) FROM polaris.tpch_sf1.orders").collect()
        time.sleep(180)
    res["idle_open_session_timeline"] = w.timeline[::10]
    t0 = time.perf_counter()
    s_idle.sql("SELECT count(*) FROM polaris.tpch_sf1.orders").collect()
    res["query_after_workers_reaped_s"] = round(time.perf_counter() - t0, 2)
    allp = json.loads(kc("get pods -o json", check=False) or "{}").get("items", [])
    res["pod_objects_in_namespace_by_phase"] = {ph: sum(1 for p in allp if p["status"].get("phase") == ph)
                                                for ph in {p["status"].get("phase") for p in allp}}
    try:
        s_idle.stop()
    except Exception:  # noqa: BLE001
        pass

    # server pod killed during a query: what happens to the client and the workers
    try:
        s3 = connect(50051)
        s3.sql("SELECT 1").collect()
        out = {}

        def run():
            try:
                s3.sql(heavy_sql()).collect()
                out["outcome"] = "succeeded"
            except Exception as e:  # noqa: BLE001
                out["outcome"] = "failed"
                out["error"] = str(e).split("\n")[0][:300]
        t = threading.Thread(target=run)
        t.start()
        time.sleep(max(3.0, res.get("heavy_query_baseline_s", 10) * 0.4))
        out["query_still_running_at_kill"] = t.is_alive()
        out["workers_before_kill"] = len(PodWatcher().pods())
        server = kc("get pods -l app=sail-server -o jsonpath={.items[0].metadata.name}").strip()
        kc(f"delete pod {server} --grace-period=0 --force", check=False)
        t.join(timeout=300)
        out["client_hung"] = t.is_alive()
        out["seconds_until_old_workers_gone"] = wait_no_workers(180)
        start_port_forward()
        try:
            t0 = time.time()
            s4 = connect(50051)
            s4.sql("SELECT count(*) FROM polaris.tpch_sf1.nation").collect()
            out["new_session_after_restart_ok_s"] = round(time.time() - t0, 1)
        except Exception as e:  # noqa: BLE001
            out["new_session_after_restart_error"] = str(e)[:200]
        try:
            s3.sql("SELECT 1").collect()
            out["old_session_usable_after_restart"] = True
        except Exception as e:  # noqa: BLE001
            out["old_session_usable_after_restart"] = False
            out["old_session_error"] = str(e).split("\n")[0][:200]
        res["kill_server_mid_query"] = out
    except Exception as e:  # noqa: BLE001
        res["kill_server_mid_query"] = {"error": str(e)[:300]}
    res["server_log_tail"] = kc("logs deploy/sail-spark-server --tail=40", check=False)[-3000:]
    return res


def main():
    import sys

    image = sys.argv[1]
    backends = sys.argv[2].split(",") if len(sys.argv) > 2 else ["flight", "storage"]
    sh(f"cat <<'EOF' | kubectl apply -f -\n{RBAC}\nEOF")
    refresh_token_secret()
    pip, sip = ip_on_kind("poc-polaris-1"), ip_on_kind("poc-minio-1")
    R["ips"] = {"polaris": pip, "s3": sip}
    stop = threading.Event()

    def refresher():
        while not stop.wait(600):
            try:
                refresh_token_secret()
            except Exception as e:  # noqa: BLE001
                log(f"secret refresh failed {e}")
    threading.Thread(target=refresher, daemon=True).start()
    for b in backends:
        log(f"===== k8s backend {b}")
        try:
            R[b] = run_backend(image, b, pip, sip)
        except Exception as e:  # noqa: BLE001
            import traceback
            R[b] = {"crashed": str(e)[:500], "tb": traceback.format_exc()[-2000:],
                    "server_log": kc("logs deploy/sail-spark-server --tail=60", check=False)[-4000:],
                    "pods": kc("get pods -o wide", check=False)}
        write_json("k8s", R)
    stop.set()


if __name__ == "__main__":
    main()
