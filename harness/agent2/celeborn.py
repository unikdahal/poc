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
        path = base.ROOT / "compose" / "celeborn-defaults.conf"
        case["config"] = path.read_text()

        def start(name, env, ports, program):
            args = ["docker", "run", "-d", "--name", name, "--hostname", name, "--network", "poc",
                    "-v", f"{path}:/opt/celeborn/conf/celeborn-defaults.conf:ro",
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
        if not workers:
            raise
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
    sql = f"""
        SELECT s.job_id, s.stage, s.partitions, s.mode, s.distribution, s.status, s.created_at, s.stopped_at,
               min(t.created_at) AS first_task_created_at, max(t.stopped_at) AS last_task_stopped_at,
               count(t.partition) AS task_attempts
        FROM system.execution.stages s LEFT JOIN system.execution.tasks t
          ON s.session_id = t.session_id AND s.job_id = t.job_id AND s.stage = t.stage
        WHERE s.session_id = '{session_id}' AND s.created_at >= TIMESTAMP '{since}'
        GROUP BY s.job_id, s.stage, s.partitions, s.mode, s.distribution, s.status, s.created_at, s.stopped_at
        ORDER BY s.job_id, s.stage
        """
    records = []
    for row in collect(controller, sql, 20):
        record = row.asDict()
        for key, start, end in (
            ("created_to_stopped_s", row.created_at, row.stopped_at),
            ("first_task_to_last_stop_s", row.first_task_created_at, row.last_task_stopped_at),
        ):
            record[key] = (end - start).total_seconds() if start is not None and end is not None else None
        records.append(record)
    return records


def session_id(spark):
    session = getattr(spark, "session_id", None) or getattr(spark.client, "_session_id", None)
    if not session or not re.fullmatch("[0-9a-fA-F-]+", session):
        raise RuntimeError("unable to identify the query session")
    return session


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
                    if rep == 2 and controller and query in STAGE_QUERIES:
                        stats_record, stage_rows = attempt(lambda: stage_snapshot(controller, session_id(spark), before))
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
