"""Run isolated kind treatments and correlate client results with pod and stage events."""

from __future__ import annotations

import datetime as dt
import json
import os
import statistics
import subprocess
import threading
import time
from pathlib import Path

import yaml

from harness import common as base, suite_k8s as existing, tpch
from harness.p3.common import (VARIANT, begin, bounded, cmd, collect, fingerprint, group, load_tpch,
                               error, median_if_valid, sail_state, save, timed, tpch_rows)

KIND = "kindest/node:v1.34.0@sha256:7416a61b42b1662ca6ca89f02028ac133a309a2a30ba309614e8ec94d976dc5a"
TASK = "k8s-scale"
CLUSTER = "p3"
QUERIES = tuple(range(1, 23))
FAULTS = ("map-kill", "reduce-kill", "drain", "netem", "pod-oom", "driver-restart")
NAME = "sail-spark-server"
PORT = 50051


def kubectl(*args, check=True, timeout=60, input=None):
    return cmd("kubectl", "-n", "sail", *args, check=check, timeout=timeout, input=input)


def get_pods():
    return json.loads(kubectl("get", "pods", "-o", "json"))["items"]


def pod_record(pod):
    spec, meta, state = pod["spec"], pod["metadata"], pod["status"]
    return {"name": meta["name"], "uid": meta["uid"], "node": spec.get("nodeName"),
            "created": meta["creationTimestamp"], "started": state.get("startTime"), "phase": state.get("phase"), "ip": state.get("podIP"),
            "labels": meta.get("labels", {}), "containers": state.get("containerStatuses", [])}


def workers(pods):
    return [pod for pod in pods if pod["labels"].get("app.kubernetes.io/component") == "worker"]


def start_cluster(result):
    binary = Path(os.environ["RUNNER_TEMP"]) / "p3-kind"
    cmd("curl", "-fsSL", "https://github.com/kubernetes-sigs/kind/releases/download/v0.30.0/kind-linux-amd64",
        "-o", str(binary), timeout=60)
    import hashlib

    if hashlib.sha256(binary.read_bytes()).hexdigest() != "517ab7fc89ddeed5fa65abf71530d90648d9638ef0c4cde22c2c11f8097b8889":
        raise RuntimeError("kind binary checksum mismatch")
    binary.chmod(0o755)
    kind = str(binary)
    config = {"kind": "Cluster", "apiVersion": "kind.x-k8s.io/v1alpha4",
              "nodes": [{"role": "control-plane"}, *[{"role": "worker"} for _ in range(3)]]}
    path = base.OUT / "kind.yaml"
    path.write_text(yaml.safe_dump(config))
    cmd(kind, "create", "cluster", "--name", CLUSTER, "--image", KIND, "--config", str(path), "--wait", "180s", timeout=400)
    result["kind"] = {"version": "0.30.0", "image": KIND, "nodes": kubectl("get", "nodes", "-o", "wide")}
    cmd("docker", "network", "connect", "kind", "poc-polaris-1")
    cmd("docker", "network", "connect", "kind", "poc-minio-1")
    local_image = "p3-sail:790c622e5c4d"
    cmd("docker", "tag", base.SAIL_IMAGE, local_image)
    cmd(kind, "load", "docker-image", local_image, "--name", CLUSTER, timeout=480)
    kubectl("apply", "-f", "-", input=existing.RBAC)
    existing.refresh_token_secret()
    return kind, local_image


def deploy(image, backend, count, memory="2Gi", cpu="1"):
    kubectl("delete", "deployment", NAME, "--ignore-not-found", "--wait=true", timeout=120)
    kubectl("delete", "pods", "-l", "app.kubernetes.io/component=worker", "--ignore-not-found", "--wait=true", timeout=120)
    existing.refresh_token_secret()
    manifest = existing.manifest(image, backend, existing.ip_on_kind("poc-polaris-1"), existing.ip_on_kind("poc-minio-1"))
    deployment, service = list(yaml.safe_load_all(manifest))
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    container["resources"] = {"requests": {"cpu": "250m", "memory": "256Mi"},
                              "limits": {"cpu": "1", "memory": "3Gi"}}
    for variable in container["env"]:
        name = variable["name"]
        if name in ("SAIL_CLUSTER__WORKER_INITIAL_COUNT", "SAIL_CLUSTER__WORKER_MAX_COUNT"):
            variable["value"] = str(count)
        if name == "SAIL_KUBERNETES__WORKER_POD_TEMPLATE":
            template = json.loads(variable["value"])
            template["metadata"] = {"labels": {"p3-worker": "true"}}
            template["spec"]["topologySpreadConstraints"] = [{"maxSkew": 1, "topologyKey": "kubernetes.io/hostname",
                "whenUnsatisfiable": "ScheduleAnyway", "labelSelector": {"matchLabels": {"p3-worker": "true"}}}]
            template["spec"]["containers"][0]["resources"] = {
                "requests": {"cpu": "150m", "memory": "32Mi"}, "limits": {"cpu": cpu, "memory": memory}}
            variable["value"] = json.dumps(template)
    kubectl("apply", "-f", "-", input=yaml.safe_dump_all([deployment, service]))
    kubectl("rollout", "status", "deployment/" + NAME, "--timeout=120s", timeout=130)
    forward = subprocess.Popen(["kubectl", "-n", "sail", "port-forward", "svc/" + NAME, f"{PORT}:{PORT}"],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    base.wait_port(PORT, timeout=25)
    return forward


class Watcher:
    def __init__(self):
        self.events = []
        self.stop = threading.Event()

    def __enter__(self):
        self.start = time.monotonic()

        def run():
            prev = None
            while not self.stop.is_set():
                rec, pods = timed(lambda: [pod_record(pod) for pod in get_pods()])
                if pods is not None:
                    frame = {pod["uid"]: pod for pod in pods}
                    if frame != prev:
                        self.events.append({"elapsed_s": time.monotonic() - self.start, "pods": list(frame.values())})
                        prev = frame
                else:
                    self.events.append(rec)
                self.stop.wait(.5)
        self.thread = threading.Thread(target=run, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join(25)


def internal_id(client):
    client_id = client.client._session_id
    rows = collect(client, f"SELECT session_id FROM system.session.sessions WHERE client_session_id='{client_id}'", 12)
    return rows[0][0] if rows else None


def stage_rows(client, internal, started):
    rows = collect(client, f"""
        SELECT CAST(job_id AS BIGINT) AS job, CAST(stage AS BIGINT) AS stage, mode, status,
               inputs, placement, distribution, created_at, stopped_at
        FROM system.execution.stages
        WHERE session_id='{internal}' AND created_at >= TIMESTAMP '{started.isoformat(sep=" ")}'
        ORDER BY job, stage
        """, 12)
    return [row.asDict(recursive=True) for row in rows]


def running_stage(client, internal, started, label):
    nodes = stage_rows(client, internal, started)
    rows = collect(client, f"""
        SELECT CAST(job_id AS BIGINT), CAST(stage AS BIGINT), count(*)
        FROM system.execution.tasks
        WHERE session_id='{internal}' AND status='RUNNING'
          AND created_at >= TIMESTAMP '{started.isoformat(sep=" ")}'
        GROUP BY job_id, stage
        """, 12)
    underway = {(row[0], row[1]): row[2] for row in rows}
    for node in nodes:
        key = (node["job"], node["stage"])
        want = (not node["inputs"] and node["distribution"].lower() != "single") if label == "map-kill" else             any("blocking" in edge["mode"].lower() for edge in node["inputs"])
        if key in underway and want and "worker" in node["placement"].lower():
            return node | {"running_tasks": underway[key], "worker_assignment_exposed": False}
    return None

def shuffle_bytes():
    import boto3
    from botocore.config import Config
    s3 = boto3.client("s3", endpoint_url="http://localhost:9000", aws_access_key_id=base.S3["key"],
                      aws_secret_access_key=base.S3["secret"], region_name=base.S3["region"],
                      config=Config(connect_timeout=2, read_timeout=2, retries={"max_attempts": 0}))
    return sum(o["Size"] for page in s3.get_paginator("list_objects_v2").paginate(Bucket="shuffle", Prefix="k8s")
               for o in page.get("Contents", []))


def benchmark(result, backend, count, reference):
    spark = base.connect(PORT)
    tpch.use(spark, "tpch_sf10")
    internal = internal_id(spark)
    entries = result["cases"]["queries"] = {}
    for q in QUERIES:
        samples = []
        for rep in range(3):
            started_at = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
            first = {}

            def execute():
                rows = None
                for sql in tpch.statements(q):
                    df = spark.sql(sql)
                    if sql.lstrip().lower().startswith(("select", "with")):
                        rows = []
                        for row in df.toLocalIterator():
                            if not rows:
                                first["elapsed_s"] = time.perf_counter() - clock[0]
                            rows.append(row)
                    else:
                        df.collect()
                return rows

            clock = [time.perf_counter()]
            rec, rows = timed(lambda: bounded(execute, 180, spark.interruptAll))
            rec["matches_spark"] = rows is not None and fingerprint(rows, tpch.ordered(q)) == reference.get(q)
            if rec["matches_spark"]:
                rec["client_first_row_s"] = first.get("elapsed_s")
            rec["started_at"] = started_at.isoformat()
            if rep == 2:
                stage_status, stage = timed(lambda: stage_rows(spark, internal, started_at))
                rec["stage_records"] = stage if stage is not None else stage_status
                if backend == "storage":
                    observation, size = timed(shuffle_bytes)
                    rec["retained_shuffle_bytes"] = size if size is not None else observation
            samples.append(rec)
            if rec["status"] == "timeout":
                break
        median = median_if_valid(samples)
        if median and all(record.get("client_first_row_s") is not None for record in samples[1:]):
            median["client_first_row_s"] = statistics.median(record["client_first_row_s"] for record in samples[1:])
        entries[str(q)] = {"samples": samples, "warm_median": median}
        save(result)


def apply_netem(pods):
    applied = []
    for pod in pods:
        peers = [other["ip"] for other in pods if other["node"] != pod["node"] and other["ip"]]
        if not peers:
            continue
        prefix = ("docker", "exec", pod["node"], "tc")
        commands = [("qdisc", "replace", "dev", "eth0", "root", "handle", "1:", "prio"),
                    ("qdisc", "replace", "dev", "eth0", "parent", "1:3", "handle", "30:", "netem", "delay", "50ms", "loss", "1%")]
        commands.extend(("filter", "add", "dev", "eth0", "protocol", "ip", "parent", "1:", "prio", str(idx),
                         "u32", "match", "ip", "dst", f"{ip}/32", "flowid", "1:3")
                        for idx, ip in enumerate(peers, 1))
        for command in commands:
            cmd(*prefix, *command)
        applied.append({"node": pod["node"], "destination_pods": peers})
    return applied


def fault(result, label, ref):
    observations = result["cases"].setdefault(label, [])
    session = base.connect(PORT)
    tpch.use(session, "tpch_sf10")
    internal = internal_id(session)
    outcome, stage_observations = {}, []
    query = 9
    start = time.monotonic()
    started_at = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)

    def execute():
        record, rows = timed(lambda: tpch_rows(session, "tpch_sf10", query, 150))
        outcome.update(record | {"matches_spark": rows is not None and fingerprint(rows, tpch.ordered(query)) == ref})

    thread = threading.Thread(target=execute, daemon=True)
    thread.start()
    applied = None
    if label == "pod-oom":
        applied = {"status": "configured", "worker_memory": "40Mi"}
    elif label == "driver-restart":
        time.sleep(2)
        pods = [pod_record(pod) for pod in get_pods() if pod["metadata"]["labels"].get("app") == "sail-server"]
        active = thread.is_alive()
        if pods and active:
            previous = pods[0]
            applied, _ = timed(lambda: kubectl("delete", "pod", previous["name"], "--grace-period=0", "--force", "--wait=false"))
            applied.update({"client_active_at_injection": active, "server_pod": previous["name"], "server_uid": previous["uid"]})
    else:
        while thread.is_alive() and time.monotonic() - start < 40:
            running = [pod for pod in workers([pod_record(pod) for pod in get_pods()]) if pod["phase"] == "Running"]
            if not running:
                time.sleep(.2)
                continue
            if label in ("map-kill", "reduce-kill"):
                rec, stage = timed(lambda: running_stage(session, internal, started_at, label))
                if rec["status"] != "ok":
                    stage_observations.append(rec)
                if not stage or not thread.is_alive():
                    time.sleep(.1)
                    continue
            pod = running[0]
            if label == "drain":
                applied, _ = timed(lambda: kubectl("drain", pod["node"], "--ignore-daemonsets", "--delete-emptydir-data",
                                                   "--force", "--timeout=50s", timeout=55))
                kubectl("uncordon", pod["node"], check=False)
            elif label == "netem":
                applied, paths = timed(lambda: apply_netem(running))
                applied["paths"] = paths
            else:
                applied, _ = timed(lambda: kubectl("delete", "pod", pod["name"], "--grace-period=0", "--force", "--wait=false"))
                applied["observed_stage"] = stage
            applied.update({"pod": pod["name"], "node": pod["node"], "elapsed_from_query_s": time.monotonic() - start})
            break
    thread.join(155)
    if label == "netem":
        for node in ("p3-worker", "p3-worker2", "p3-worker3"):
            cmd("docker", "exec", node, "tc", "qdisc", "del", "dev", "eth0", "root", check=False)
    if applied and label == "driver-restart":
        ready, output = timed(lambda: kubectl("rollout", "status", "deployment/" + NAME, "--timeout=90s", timeout=100))
        applied["replacement_readiness"] = ready | {"output": output}
        applied["current_driver"] = [pod_record(pod) for pod in get_pods() if pod["metadata"]["labels"].get("app") == "sail-server"]
    observations.append({"query": query, "injection": applied, "query_result": outcome,
                         "client_active": thread.is_alive(), "stage_probe_errors": stage_observations})
    save(result)

def burst(result, reference):
    seed = base.connect(PORT)
    seed_status, seed_rows = timed(lambda: tpch_rows(seed, "tpch_sf10", 6, 60))
    seed_status["matches_spark"] = seed_rows is not None and fingerprint(seed_rows, tpch.ordered(6)) == reference.get(6)
    started = time.monotonic()
    clean = False
    starting_pods = [pod_record(pod) for pod in get_pods()]
    while time.monotonic() - started < 120:
        if not any(pod["phase"] in ("Running", "Pending") for pod in workers([pod_record(pod) for pod in get_pods()])):
            clean = True
            break
        time.sleep(2)
    active, clients = [], []
    together = threading.Barrier(20, timeout=12)

    def arrive():
        together.wait()
        def exercise():
            session = base.connect(PORT)
            clients.append(session)
            return tpch_rows(session, "tpch_sf10", 6, 140)
        record, rows = timed(exercise)
        record["matches_spark"] = rows is not None and fingerprint(rows, tpch.ordered(6)) == reference.get(6)
        active.append(record)

    ts = [threading.Thread(target=arrive, daemon=True) for _ in range(20)]
    for thread in ts:
        thread.start()
    for thread in ts:
        thread.join(150)
    latency = sorted(r["elapsed_s"] for r in active if r["matches_spark"])
    result["cases"]["burst"] = {"seed": seed_status, "pods_after_seed": starting_pods,
        "reaped_before_arrival": clean, "idle_wait_s": time.monotonic() - started, "attempts": active,
        "p50_s": statistics.median(latency) if latency else None,
        "p95_s": latency[max(0, -(-len(latency) * 95 // 100) - 1)] if latency else None}
    for client in clients:
        bounded(client.stop, 5)
    time.sleep(70)
    result["cases"]["burst"]["remaining"] = [pod_record(pod) for pod in get_pods()]


def main():
    result = begin(TASK)
    reference = {}

    def load(spark, *_):
        result["data_load"] = load_tpch(spark, 10)
        for q in QUERIES:
            rec, rows = timed(lambda: tpch_rows(spark, "tpch_sf10", q))
            if rows is not None:
                reference[q] = fingerprint(rows, tpch.ordered(q))
            result["cases"].setdefault("spark_reference", {})[q] = rec["status"]
    group(result, "spark-reference", load, "spark")
    if not reference:
        return
    kind, image = start_cluster(result)
    processes = []
    try:
        failure = VARIANT in FAULTS or VARIANT == "burst"
        backend, count = ("flight", 1 if VARIANT == "burst" else 2) if failure else VARIANT.split("-")
        result["deployment"] = {"shuffle": backend, "workers": int(count), "limits": {"server": "1 CPU, 3 GiB", "worker": "1 CPU, 2 GiB"}}
        with Watcher() as watcher:
            if VARIANT == "burst":
                processes.append(deploy(image, backend, int(count), memory="512Mi", cpu="250m"))
                burst(result, reference)
            elif failure:
                for trial in range(3):
                    print(f"{VARIANT} trial {trial + 1}", flush=True)
                    if processes:
                        processes.pop().terminate()
                    processes.append(deploy(image, backend, int(count), memory="40Mi" if VARIANT == "pod-oom" else "2Gi"))
                    fault(result, VARIANT, reference.get(9))
            else:
                processes.append(deploy(image, backend, int(count)))
                benchmark(result, backend, int(count), reference)
        result["pod_timeline"] = watcher.events
    finally:
        for proc in processes:
            proc.terminate()
        result["last_pods"] = [pod_record(pod) for pod in get_pods()]
        result["kind_container_states"] = {name: sail_state(name) for name in
            ("p3-control-plane", "p3-worker", "p3-worker2", "p3-worker3")}
        for pod in result["last_pods"]:
            log = kubectl("logs", pod["name"], "--tail=230", check=False, timeout=15)
            (base.OUT / (pod["name"] + ".log")).write_text(log)
        (base.OUT / "kubernetes.log").write_text(kubectl("get", "events", "--sort-by=.lastTimestamp", check=False))
        save(result)
        cmd(kind, "delete", "cluster", "--name", CLUSTER, check=False, timeout=120)


if __name__ == "__main__":
    main()
