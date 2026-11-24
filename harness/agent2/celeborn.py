"""Compare shuffle settings and inspect worker loss on the same SF1 data."""

import datetime as dt
import re
import threading
import time
from contextlib import contextmanager, nullcontext

import requests

from harness import common as base, tpch
from harness.agent2.common import (SAIL_CONTAINER, attempt, begin, collect, command, engine, error,
                                   fingerprint, median, query_h, save, tpch_reference)

TASK = "celeborn"
QUERIES = tuple(range(1, 23))
STAGE_QUERIES = (5, 9, 18, 21)
IMAGE_TAGS = ("0.6.3", "0.7.0")
MASTER = "celeborn-master"
PREFIX = "SAIL_CLUSTER__SHUFFLE_BACKEND__CELEBORN__"
DEFAULT_SPLIT = 2**30


def wait_for_log(name, message):
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        output = base.container_logs(name, 120)
        if message in output:
            return
        running = command("docker", "inspect", "-f", "{{.State.Running}}", name, check=False)
        if running != "true":
            raise RuntimeError(f"{name} stopped before startup: {output[-1300:]}")
        time.sleep(1)
    raise TimeoutError(f"{name} did not emit {message!r}")


@contextmanager
def backend(result, label, tag, count):
    case = result.setdefault("backends", {}).setdefault(label, {})
    launched = []
    port = 12096
    workers = []
    try:
        image = f"apache/celeborn:{tag}"
        command("docker", "pull", image, timeout=300)
        case["image"] = image
        case["digest"] = command("docker", "image", "inspect", "-f", "{{json .RepoDigests}}", image)
        defaults = (base.ROOT / "compose" / "celeborn-defaults.conf").read_text()
        case["config"] = defaults + "\nceleborn.metrics.conf /opt/celeborn/conf/metrics.properties\n"
        case["metrics_config"] = (
            "*.sink.prometheusServlet.class=org.apache.celeborn.common.metrics.sink.PrometheusServlet\n"
        )
        path = base.OUT / f"{label}-celeborn.conf"
        metrics_path = base.OUT / f"{label}-metrics.properties"
        path.write_text(case["config"])
        metrics_path.write_text(case["metrics_config"])

        def start(name, env, ports, program):
            args = ["docker", "run", "-d", "--name", name, "--hostname", name, "--network", "poc",
                    "-v", f"{path.resolve()}:/opt/celeborn/conf/celeborn-defaults.conf:ro",
                    "-v", f"{metrics_path.resolve()}:/opt/celeborn/conf/metrics.properties:ro",
                    "-e", f"CELEBORN_LOCAL_HOSTNAME={name}", "-e", "CELEBORN_NO_DAEMONIZE=1", *ports]
            for variable in env:
                args.extend(("-e", variable))
            command(*args, image, *program)
            launched.append(name)

        start(MASTER, ["CELEBORN_MASTER_MEMORY=512m"], [],
              ["start-master.sh", "--host", MASTER, "--port", "12097"])
        for i in range(1, count + 1):
            name = f"celeborn-worker-{i}"
            start(name, ["CELEBORN_WORKER_MEMORY=1g", "CELEBORN_WORKER_OFFHEAP_MEMORY=1g"],
                  ["-p", f"127.0.0.1::{port}"],
                  ["start-worker.sh", "--host", name, "--port", "12000", f"celeborn://{MASTER}:12097"])
            workers.append(name)
        for worker in workers:
            wait_for_log(worker, "Register worker successfully")
        yield workers
    except Exception as exc:
        case["error"] = error(exc)
        save(TASK, result)
        raise
    finally:
        for name in reversed(launched):
            logfile = base.OUT / f"{label}-{name}.log"
            logfile.write_text(base.container_logs(name, 2000))
            base.stop_container(name)


class CombinedStats:
    def __init__(self, stats):
        self.stats = stats

    def cpu_seconds(self):
        return sum(stat.cpu_seconds() for stat in self.stats)

    def mem_current(self):
        samples = [stat.mem_current() for stat in self.stats]
        return sum(samples) if all(n >= 0 for n in samples) else -1


def metrics(worker):
    address = command("docker", "port", worker, "12096/tcp")
    response = requests.get(f"http://{address}/metrics/prometheus", timeout=5)
    response.raise_for_status()
    return response.text


def save_metrics(label, phase, workers):
    for worker in workers:
        try:
            value = metrics(worker)
            (base.OUT / f"{label}-{worker}-{phase}.prom").write_text(value)
        except (requests.RequestException, RuntimeError) as exc:
            (base.OUT / f"{label}-{worker}-{phase}.log").write_text(error(exc))


def stage_snapshot(controller, session_id, since):
    stages = collect(controller, f"""
        SELECT CAST(job_id AS BIGINT) AS job_id, CAST(stage AS BIGINT) AS stage,
               CAST(partitions AS BIGINT) AS partitions, mode, distribution, status, created_at, stopped_at
        FROM system.execution.stages
        WHERE session_id = '{session_id}' AND created_at >= TIMESTAMP '{since}'
        ORDER BY job_id, stage
        """, 20)
    tasks = collect(controller, f"""
        SELECT CAST(job_id AS BIGINT) AS job_id, CAST(stage AS BIGINT) AS stage, created_at, stopped_at
        FROM system.execution.tasks
        WHERE session_id = '{session_id}' AND created_at >= TIMESTAMP '{since}'
        """, 20)
    by_stage = {}
    for row in tasks:
        by_stage.setdefault((row.job_id, row.stage), []).append(row)
    records = []
    for row in stages:
        task_rows = by_stage.get((row.job_id, row.stage), [])
        first_task = min((task.created_at for task in task_rows), default=None)
        last_task = max((task.stopped_at for task in task_rows if task.stopped_at is not None), default=None)
        record = row.asDict() | {"first_task_created_at": first_task, "last_task_stopped_at": last_task,
                                 "task_attempts": len(task_rows)}
        for key, start, end in (
            ("created_to_stopped_s", row.created_at, row.stopped_at),
            ("first_task_to_last_stop_s", first_task, last_task),
        ):
            record[key] = (end - start).total_seconds() if start is not None and end is not None else None
        records.append(record)
    return records


def session_id(spark, controller):
    client_id = getattr(spark, "session_id", None) or getattr(spark.client, "_session_id", None)
    if not client_id or not re.fullmatch("[0-9a-fA-F-]+", client_id):
        raise RuntimeError("unable to identify the query client session")
    rows = collect(controller,
                   f"SELECT session_id FROM system.session.sessions WHERE client_session_id = '{client_id}'", 20)
    if len(rows) != 1 or not re.fullmatch("[0-9a-fA-F-]+", rows[0].session_id):
        raise RuntimeError(f"expected one server session for the query client, got {len(rows)}")
    return rows[0].session_id


def fault(result, spark, worker, reference):
    observation = result["worker_kill"] = {"worker": worker, "query": 9}
    start = time.monotonic()

    def run():
        record, rows = attempt(lambda: query_h(spark, "tpch_sf1", 9, 180))
        observation["query_result"] = record
        if rows is not None and reference is not None:
            observation["matches_spark"] = fingerprint(rows, tpch.ordered(9)) == reference

    job = threading.Thread(target=run, daemon=True)
    job.start()
    time.sleep(.5)
    observation["query_active_at_kill"] = job.is_alive()
    if observation["query_active_at_kill"]:
        observation["kill_at_wall_s"] = time.monotonic() - start
        command("docker", "kill", worker)
        observation["worker_state"] = command("docker", "inspect", "-f", "{{json .State}}", worker)
    job.join(190)
    observation["client_still_active"] = job.is_alive()
    if not job.is_alive():
        observation["subsequent_query"], rows = attempt(lambda: collect(spark, "SELECT 1", 20))
        observation["subsequent_query"]["correct"] = rows is not None and rows[0][0] == 1


def run_case(result, label, mode, tag, count, codec, split, reference):
    result.setdefault("treatments", {})[label] = {
        "image_tag": tag, "workers": count, "compression": codec, "partition_split_bytes": split,
    }
    env = {"RUST_LOG": "info"}
    if tag:
        env.update({"SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE": "celeborn",
                    PREFIX + "MASTER_ENDPOINTS": '["celeborn-master:12097"]', PREFIX + "COMPRESSION": codec,
                    PREFIX + "PARTITION_SPLIT_THRESHOLD": str(split)})
    elif mode == "local-cluster":
        env["SAIL_CLUSTER__SHUFFLE_BACKEND__TYPE"] = "flight"
    with (backend(result, label, tag, count) if tag else nullcontext([])) as workers:
        with engine(result, "sail", label=label, mode=mode, extra=env, output_name=TASK) as (case, spark, stats, port):
            cluster = CombinedStats([stats, *[base.CgroupStats(name) for name in [MASTER, *workers]]]) if workers else stats
            case["resource_scope"] = [SAIL_CONTAINER, *([MASTER, *workers] if workers else [])]
            controller = base.connect(port) if mode == "local-cluster" else None
            session = None
            if controller:
                record, session = attempt(lambda: session_id(spark, controller))
                case["stage_session"] = record | {"server_session_id": session}
            save_metrics(label, "before", workers)
            samples = case["queries"] = {}
            for query in QUERIES:
                records = samples[str(query)] = []
                for rep in range(3):
                    before = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
                    record, rows = attempt(lambda: query_h(spark, "tpch_sf1", query, 180), cluster)
                    if rows is not None and query in reference:
                        record["matches_spark"] = fingerprint(rows, tpch.ordered(query)) == reference[query]
                    records.append(record)
                    if rep == 2 and session and query in STAGE_QUERIES:
                        stats_record, stage_rows = attempt(lambda: stage_snapshot(controller, session, before))
                        record["stages"] = stage_rows if stage_rows is not None else stats_record
                    save(TASK, result)
                    if record["status"] == "timeout":
                        raise TimeoutError(record["error"])
                if len(records) == 3 and all(r.get("matches_spark") for r in records):
                    case.setdefault("validated_metrics", {})[str(query)] = median(records)
            save_metrics(label, "after", workers)
            if label == "celeborn-0.6.3-two-workers":
                fault(result, spark, workers[0], reference.get(9))


def main():
    result = begin(TASK)
    result["measurement"]["query_timeout_s"] = 180
    reference = tpch_reference(result, 1, QUERIES, TASK)
    if not result.get("data_load"):
        return
    treatments = [
        ("local", "local", None, 0, None, None),
        ("cluster-flight", "local-cluster", None, 0, None, None),
        ("celeborn-0.6.3-default", "local-cluster", IMAGE_TAGS[0], 1, "lz4", DEFAULT_SPLIT),
        ("celeborn-0.6.3-uncompressed", "local-cluster", IMAGE_TAGS[0], 1, "none", DEFAULT_SPLIT),
        ("celeborn-0.6.3-small-split", "local-cluster", IMAGE_TAGS[0], 1, "lz4", 16 * 2**20),
        ("celeborn-0.6.3-two-workers", "local-cluster", IMAGE_TAGS[0], 2, "lz4", DEFAULT_SPLIT),
        ("celeborn-0.7.0-default", "local-cluster", IMAGE_TAGS[1], 1, "lz4", DEFAULT_SPLIT),
    ]
    for label, *options in treatments:
        try:
            run_case(result, label, *options, reference)
        except Exception as exc:
            result.setdefault("case_errors", {})[label] = error(exc)
        save(TASK, result)


if __name__ == "__main__":
    main()
