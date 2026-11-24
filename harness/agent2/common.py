"""Measurement, engine lifecycle and result collection for the additional suites."""

from __future__ import annotations

import hashlib
import json
import math
import os
import platform
import re
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path

from harness import common as base
from harness import tpch
from harness.suite_bench import CLUSTER

SAIL_TAG = "790c622e5c4d"
SAIL_SHA = "790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f"
SAIL_CONTAINER = "agent2-sail"
SAIL_PORT = 50051
SPARK_PORT = 15002
TIMEOUT = 240
REPS = 3


def command(*args, check=True, timeout=60):
    run = subprocess.run(args, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                         check=False, timeout=timeout)
    if check and run.returncode:
        raise RuntimeError(f"{args[0]} exited {run.returncode}: {run.stdout.strip()[-1500:]}")
    return run.stdout.strip()


def error(exc):
    text = str(exc)
    for pattern in (r"(?i)(Bearer\s+)[\w.~-]+", r"\beyJ[\w.-]{30,}"):
        text = re.sub(pattern, "[redacted]", text)
    return f"{type(exc).__name__}: {text.strip()[:900]}"


def bounded(fn, seconds, interrupt=None):
    """Bound both the operation and its cancellation RPC."""
    result = {}

    def call():
        try:
            result["value"] = fn()
        except Exception as exc:
            result["exception"] = exc

    thread = threading.Thread(target=call, daemon=True)
    thread.start()
    thread.join(seconds)
    if thread.is_alive():
        if interrupt:
            threading.Thread(target=interrupt, daemon=True).start()
            thread.join(5)
        raise TimeoutError(f"deadline of {seconds}s exceeded (client still active: {thread.is_alive()})")
    if "exception" in result:
        raise result["exception"]
    return result["value"]


def collect(session, sql, seconds=TIMEOUT):
    return bounded(lambda: session.sql(sql).collect(), seconds, session.interruptAll)


def attempt(fn, stats=None):
    started = time.perf_counter()
    initial_cpu = stats.cpu_seconds() if stats else None
    sample = None
    try:
        if stats:
            with base.PeakSampler(stats) as sample:
                result = fn()
        else:
            result = fn()
        record = {"status": "ok"}
    except Exception as exc:
        result = None
        record = {"status": "timeout" if isinstance(exc, TimeoutError) else "error", "error": error(exc)}
    record["wall_s"] = round(time.perf_counter() - started, 6)
    if stats:
        cpu = stats.cpu_seconds() - initial_cpu
        record["cpu_s"] = round(cpu, 6) if math.isfinite(cpu) and cpu >= 0 else None
        record["peak_bytes"] = sample.peak if sample and sample.peak > 0 else None
    return record, result


def median(records, warm=True):
    samples = records[1:] if warm else records
    if not samples or any(r["status"] != "ok" for r in samples):
        return None
    return {
        f"median_{key}": round(statistics.median(r[key] for r in samples), 6)
        if all(r.get(key) is not None for r in samples) else None
        for key in ("wall_s", "cpu_s", "peak_bytes")
    }


def latency(values):
    values = sorted(values)
    if not values:
        return {"p50_s": None, "p95_s": None}
    return {"p50_s": statistics.median(values), "p95_s": values[math.ceil(.95 * len(values)) - 1]}


def canonical(value):
    if isinstance(value, Decimal):
        return str(value.normalize()) if value else "0"
    if isinstance(value, float):
        return base.canon(value)
    return base.canon(value)


def fingerprint(rows, ordered=False):
    encoded = [json.dumps([canonical(value) for value in row], default=str, separators=(",", ":")) for row in rows]
    if not ordered:
        encoded.sort()
    return {"rows": len(encoded), "sha256": hashlib.sha256("\n".join(encoded).encode()).hexdigest()}


def query_h(session, ns, query, timeout=TIMEOUT):
    return bounded(lambda: tpch.run_query(session, query, ns)[0], timeout, session.interruptAll)


def stop_spark(process):
    if not process:
        return
    base.stop_proc(process)
    process.wait(timeout=15)


def wait_ready(session):
    bounded(lambda: base.warm(session, tries=15), 60, session.interruptAll)


@contextmanager
def engine(result, name, *, label=None, memory=None, extra=None, mode=None, server="spark",
           run_options=None, output_name=None):
    """Run one engine and retain its effective configuration and logs on exit."""
    label = label or name
    case = result.setdefault("engines", {}).setdefault(label, {})
    process = None
    port = SPARK_PORT
    use_sail = name.startswith("sail")
    record = None
    try:
        if use_sail:
            mode = mode or ("local-cluster" if name == "sail-cluster" else "local")
            env = (CLUSTER if mode == "local-cluster" else {}) | (extra or {})
            memory = memory or "12g"
            port = SAIL_PORT if server == "spark" else 32010
            case["config"] = {"mode": mode, "environment": env, "cpus": 4, "memory": memory,
                              "server": server, "docker_options": run_options or []}
            if run_options:
                # For bind-mounted spill directories.
                args = ["docker", "run", "-d", "--name", SAIL_CONTAINER, "--network", "poc",
                        "-p", f"{port}:{port}", "--cpus", "4", "--memory", memory, *run_options,
                        "-v", f"{base.TOKEN_DIR}:/tokens:ro"]
                for key, value in base.sail_env(mode, env).items():
                    args.extend(("-e", f"{key}={value}"))
                base.stop_container(SAIL_CONTAINER)
                started = time.monotonic()
                command(*args, base.SAIL_IMAGE, server, "server", "--ip", "0.0.0.0", "--port", str(port))
                base.wait_port(port, timeout=75)
                case["boot_s"] = time.monotonic() - started
            else:
                case["boot_s"] = base.start_sail(SAIL_CONTAINER, server, port, mode, env, "4", memory)
            stats = base.CgroupStats(SAIL_CONTAINER)
        else:
            memory = memory or ("8g" if name == "comet" else "9g")
            case["config"] = {"comet": name == "comet", "cores": 4, "driver_memory": memory,
                              "comet_off_heap_default": "4g" if name == "comet" else None,
                              "overrides": extra or {}, "table_cache": False}
            process, case["boot_s"] = base.start_spark_connect(port, name == "comet", memory, extra_conf=extra)
            stats = base.PidStats(base.jvm_pid(process))
        session = base.connect(port) if server == "spark" else None
        if session:
            wait_ready(session)
        yield case, session, stats, port
    except Exception as exc:
        case["error"] = error(exc)
        base.log(f"{label}: {case['error']}")
    finally:
        if use_sail:
            record = base.container_logs(SAIL_CONTAINER, 2000)
            state = command("docker", "inspect", "--format", "{{json .State}}", SAIL_CONTAINER, check=False)
            if state.startswith("{"):
                case["container_state"] = json.loads(state)
            base.stop_container(SAIL_CONTAINER)
        else:
            stop_spark(process)
            file = base.OUT / f"spark-connect-{port}{'-comet' if name == 'comet' else ''}.log"
            if file.exists():
                record = file.read_text(errors="replace")
        if record and "No such container" not in record:
            case["log_excerpt"] = scrub_log(record[-1800:])
            (base.OUT / f"{label}.log").write_text(scrub_log(record))
        if output_name:
            save(output_name, result)


def scrub_log(record):
    return re.sub(r"\beyJ[\w.-]{30,}", "[redacted]", record)


def metadata(task):
    image_sha = command("docker", "run", "--rm", "--entrypoint", "cat", base.SAIL_IMAGE, "/SAIL_SHA", check=False)
    if image_sha != SAIL_SHA:
        raise RuntimeError(f"pinned image revision could not be verified: {image_sha[-500:]}")
    return {
        "task": task,
        "command": [sys.executable, "-m", f"harness.agent2.{task.replace('-', '_')}", *sys.argv[1:]],
        "revision": command("git", "rev-parse", "HEAD"),
        "run_id": os.getenv("GITHUB_RUN_ID"),
        "versions": base.resolve_versions() | {
            "python": platform.python_version(), "java": command("java", "-version", check=False).splitlines()[0],
            "sail_sha": image_sha,
            "sail_image": base.SAIL_IMAGE,
            "sail_image_digest": command("docker", "image", "inspect", "--format", "{{json .RepoDigests}}", base.SAIL_IMAGE),
            "infra_images": command("docker", "compose", "-f", "compose/docker-compose.yml", "ps", "--format", "json"),
        },
        "runner": {"cpus": os.cpu_count(), "platform": platform.platform(),
                   "memory_bytes": os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")},
        "measurement": {"cpu": "container cgroup v2 or Spark process tree", "memory": "sampled at 100 ms",
                        "warm_samples": REPS - 1, "query_timeout_s": TIMEOUT},
    }


def save(task, data):
    target = base.OUT / f"{task}.json"
    with tempfile.NamedTemporaryFile(mode="w", dir=base.OUT, delete=False) as file:
        json.dump(data, file, indent=2, allow_nan=False, default=str)
        file.write("\n")
    Path(file.name).replace(target)
    base.log(f"wrote {target}")


def begin(task):
    result = metadata(task)
    base.write_token()
    if task != "catalog":
        base.start_token_refresher()
    save(task, result)
    return result


def load_h(spark, scale):
    with tempfile.TemporaryDirectory(prefix=f"agent2-tpch-sf{scale}-") as path:
        directory = base.ROOT / path
        from harness import data

        generated = data.generate(scale, directory)
        return bounded(lambda: data.load(spark, scale, generated, f"tpch_sf{scale}"),
                       3600, spark.interruptAll)


def tpch_reference(result, scale, queries, task, load=True):
    reference = {}
    with engine(result, "spark", label=f"spark-reference-sf{scale}", output_name=task) as (case, spark, _, _):
        if load:
            result.setdefault("data_load", {})[str(scale)] = load_h(spark, scale)
        case["queries"] = {}
        for query in queries:
            record, rows = attempt(lambda: query_h(spark, f"tpch_sf{scale}", query))
            case["queries"][str(query)] = record
            if rows is not None:
                reference[query] = fingerprint(rows, tpch.ordered(query))
    return reference
