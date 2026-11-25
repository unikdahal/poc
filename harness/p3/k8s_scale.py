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
                               median_if_valid, sail_state, save, timed, tpch_rows)

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
            "created": meta["creationTimestamp"], "started": state.get("startTime"), "phase": state.get("phase"),
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


def deploy(image, backend, count, memory="2Gi"):
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
                "requests": {"cpu": "200m", "memory": "32Mi"}, "limits": {"cpu": "1", "memory": memory}}
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


def stage_rows(control, client):
    client_id = client.client._session_id
    rows = collect(control, f"SELECT session_id FROM system.session.sessions WHERE client_session_id='{client_id}'", 12)
    if not rows:
        return []
    internal = rows[0][0]
    return [row.asDict() for row in collect(control, f"""
        SELECT CAST(job_id AS BIGINT) AS job, CAST(stage AS BIGINT) AS stage, mode, status,
               created_at, stopped_at
        FROM system.execution.stages WHERE session_id='{internal}' ORDER BY job, stage
        """, 12)]


def shuffle_bytes():
    import boto3
    from botocore.config import Config
    s3 = boto3.client("s3", endpoint_url="http://localhost:9000", aws_access_key_id=base.S3["key"],
                      aws_secret_access_key=base.S3["secret"], region_name=base.S3["region"],
                      config=Config(connect_timeout=2, read_timeout=2, retries={"max_attempts": 0}))
    return sum(o["Size"] for page in s3.get_paginator("list_objects_v2").paginate(Bucket="shuffle", Prefix="k8s")
               for o in page.get("Contents", []))


def benchmark(result, backend, count, reference):
    spark, control = base.connect(PORT), base.connect(PORT)
    tpch.use(spark, "tpch_sf10")
    entries = result["cases"]["queries"] = {}
    for q in QUERIES:
        samples = []
        for rep in range(3):
            started_at = dt.datetime.now()
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
                return rows

            clock = [time.perf_counter()]
            rec, rows = timed(lambda: bounded(execute, 180, spark.interruptAll))
            rec["matches_spark"] = rows is not None and fingerprint(rows, tpch.ordered(q)) == reference.get(q)
            if rec["matches_spark"]:
                rec["client_first_row_s"] = first.get("elapsed_s")
            rec["started_at"] = started_at.isoformat()
            if rep == 2:
                stage_status, stage = timed(lambda: stage_rows(control, spark))
                rec["stage_records"] = stage if stage is not None else stage_status
                if backend == "storage":
                    observation, size = timed(shuffle_bytes)
                    rec["retained_shuffle_bytes"] = size if size is not None else observation
            samples.append(rec)
            if rec["status"] == "timeout":
                break
        entries[str(q)] = {"samples": samples, "warm_median": median_if_valid(samples)}
        save(result)


def fault(result, label, ref):
    observations = result["cases"].setdefault(label, [])
    session, control = base.connect(PORT), base.connect(PORT)
    outcome = {}
    query = 9
    start = time.monotonic()

    def execute():
        record, rows = timed(lambda: tpch_rows(session, "tpch_sf10", query, 150))
        outcome.update(record | {"matches_spark": rows is not None and fingerprint(rows, tpch.ordered(query)) == ref})

    thread = threading.Thread(target=execute, daemon=True)
    thread.start()
    applied = None
    if label == "pod-oom":
        applied = "worker pod memory limit set to 40Mi before query"
    elif label == "driver-restart":
        time.sleep(3)
        applied, _ = timed(lambda: kubectl("rollout", "restart", "deployment/" + NAME))
    else:
        while thread.is_alive() and time.monotonic() - start < 40:
            running = workers([pod_record(pod) for pod in get_pods()])
            if not running:
                time.sleep(.5)
                continue
            if label in ("map-kill", "reduce-kill"):
                stage_rec, stage = timed(lambda: stage_rows(control, session))
                if not stage:
                    time.sleep(.3)
                    continue
                phases = [row for row in stage if row["status"].upper() in ("RUNNING", "ACTIVE")]
                target_stage = 0 if label == "map-kill" else max(row["stage"] for row in stage)
                if all(row["stage"] != target_stage for row in phases):
                    continue
            pod = running[0]
            if label == "drain":
                applied, _ = timed(lambda: kubectl("drain", pod["node"], "--ignore-daemonsets", "--delete-emptydir-data",
                                                   "--force", "--timeout=50s", timeout=55))
                kubectl("uncordon", pod["node"], check=False)
            elif label == "netem":
                applied, _ = timed(lambda: cmd("docker", "exec", pod["node"], "tc", "qdisc", "replace", "dev", "eth0",
                                              "root", "netem", "delay", "50ms", "loss", "1%"))
            else:
                applied, _ = timed(lambda: kubectl("delete", "pod", pod["name"], "--grace-period=0", "--force", "--wait=false"))
                applied["observed_stage"] = target_stage
            break
    thread.join(155)
    if label == "netem":
        for node in ("p3-worker", "p3-worker2", "p3-worker3"):
            cmd("docker", "exec", node, "tc", "qdisc", "del", "dev", "eth0", "root", check=False)
    observations.append({"query": query, "injection": applied, "query_result": outcome, "client_active": thread.is_alive()})
    save(result)


def burst(result, reference):
    started = time.monotonic()
    clean = False
    while time.monotonic() - started < 120:
        if not workers([pod_record(pod) for pod in get_pods()]):
            clean = True
            break
        time.sleep(2)
    active = []

    def arrive():
        session = base.connect(PORT)
        record, rows = timed(lambda: tpch_rows(session, "tpch_sf10", 6, 140))
        record["matches_spark"] = rows is not None and fingerprint(rows, tpch.ordered(6)) == reference.get(6)
        active.append(record)
        bounded(session.stop, 5)

    ts = [threading.Thread(target=arrive, daemon=True) for _ in range(20)]
    for thread in ts:
        thread.start()
    for thread in ts:
        thread.join(150)
    latency = sorted(r["elapsed_s"] for r in active if r["matches_spark"])
    result["cases"]["burst"] = {"reaped_before_arrival": clean, "attempts": active,
        "p50_s": statistics.median(latency) if latency else None,
        "p95_s": latency[max(0, -(-len(latency) * 95 // 100) - 1)] if latency else None}
    time.sleep(65)
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
        failure = VARIANT == "failure"
        backend, count = ("flight", 2) if failure else VARIANT.split("-")
        result["deployment"] = {"shuffle": backend, "workers": int(count), "limits": {"server": "1 CPU, 3 GiB", "worker": "1 CPU, 2 GiB"}}
        with Watcher() as watcher:
            if failure:
                for treatment in FAULTS:
                    for _ in range(3):
                        if processes:
                            processes.pop().terminate()
                        processes.append(deploy(image, backend, int(count), memory="40Mi" if treatment == "pod-oom" else "2Gi"))
                        fault(result, treatment, reference.get(9))
                processes.pop().terminate()
                processes.append(deploy(image, backend, int(count)))
                burst(result, reference)
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
        (base.OUT / "kubernetes.log").write_text(kubectl("get", "events", "--sort-by=.lastTimestamp", check=False))
        save(result)
        cmd(kind, "delete", "cluster", "--name", CLUSTER, check=False, timeout=120)


if __name__ == "__main__":
    main()
