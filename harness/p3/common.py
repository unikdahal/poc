"""Engine lifecycle, bounded execution, typed observations, and correctness-gated timings."""

from __future__ import annotations

import datetime
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
from contextlib import contextmanager, nullcontext
from decimal import Decimal
from pathlib import Path

from harness import common as base, data, tpch

PIN = "790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f"
UPSTREAM = "https://github.com/lakehq/sail"
VARIANT = os.getenv("P3_VARIANT", "default")
SQL_TIMEOUT = 240
REPS = 3
PORT = {"spark": 15002, "comet": 15012, "sail": 50051, "flight": 32010}


def cmd(*args, check=True, timeout=60, input=None):
    done = subprocess.run(args, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          timeout=timeout, input=input)
    if check and done.returncode:
        raise RuntimeError(f"{args[0]} exit {done.returncode}: {scrub(done.stdout[-1800:])}")
    return done.stdout.strip()


def scrub(text):
    text = re.sub(r"\beyJ[\w.-]{30,}", "[token]", str(text))
    return re.sub(r"(?i)(Bearer\s+)[A-Za-z0-9._~-]+", r"\1[token]", text)


def error(exc):
    return f"{type(exc).__name__}: {scrub(exc)[:2200]}"


def save(result, name="result"):
    with tempfile.NamedTemporaryFile(mode="w", dir=base.OUT, delete=False) as f:
        json.dump(result, f, indent=2, default=str, allow_nan=False)
        f.write("\n")
    Path(f.name).replace(base.OUT / f"{name}.json")


def begin(task):
    result = {"task": task, "variant": VARIANT, "revision": cmd("git", "rev-parse", "HEAD"),
              "run_id": os.getenv("GITHUB_RUN_ID"), "command": [sys.executable, "-m", f"harness.p3.{task.replace('-', '_')}", *sys.argv[1:]],
              "versions": base.resolve_versions() | {"python": platform.python_version(),
              "sail_sha": os.getenv("P3_SAIL_SHA", PIN), "sail_image": base.SAIL_IMAGE,
              "java": cmd("java", "-version", check=False).split("\n")[0]},
              "runner": {"cpu_count": os.cpu_count(), "total_memory_bytes": os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES"),
                         "disk": cmd("df", "-B1", str(base.ROOT)).splitlines()[-1]},
              "engines": {}, "cases": {}}
    for filename, key in (("image-digests.txt", "images"), ("pip-freeze.txt", "python_dependencies")):
        path = base.OUT / filename
        if path.exists():
            result["versions"][key] = path.read_text().splitlines()
    save(result)
    return result


def bounded(fn, timeout, interrupt=None):
    state = {}

    def run():
        try:
            state["value"] = fn()
        except Exception as exc:
            state["error"] = exc

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        if interrupt:
            def cancel():
                try:
                    interrupt()
                except Exception:
                    pass
            threading.Thread(target=cancel, daemon=True).start()
            thread.join(3)
        raise TimeoutError(f"{timeout}s deadline, client active={thread.is_alive()}")
    if "error" in state:
        raise state["error"]
    return state["value"]


def collect(session, sql, timeout=SQL_TIMEOUT):
    return bounded(lambda: session.sql(sql).collect(), timeout, session.interruptAll)


def timed(fn, stats=None):
    cpu0 = stats.cpu_seconds() if stats else None
    with (base.PeakSampler(stats) if stats else nullcontext()) as sampler:
        started = time.perf_counter()
        try:
            value, rec = fn(), {"status": "ok"}
        except Exception as exc:
            value = None
            rec = {"status": "timeout" if isinstance(exc, TimeoutError) else "error", "error": error(exc)}
        rec["elapsed_s"] = time.perf_counter() - started
    if stats:
        cpu = stats.cpu_seconds() - cpu0
        rec["cpu_s"] = cpu if math.isfinite(cpu) and cpu >= 0 else None
        rec["peak_bytes"] = sampler.peak if sampler.peak > 0 else None
    return rec, value


def typed(v):
    if isinstance(v, Decimal):
        return {"decimal": str(v)}
    if isinstance(v, float):
        return {"float": repr(v)}
    if isinstance(v, (datetime.date, datetime.datetime)):
        return {"datetime": v.isoformat()}
    if isinstance(v, bytes):
        return {"binary": v.hex()}
    if isinstance(v, (list, tuple)):
        return [typed(x) for x in v]
    if isinstance(v, dict):
        return {str(k): typed(x) for k, x in sorted(v.items(), key=lambda pair: str(pair[0]))}
    return v


def numeric(v):
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return Decimal(str(v))
    if isinstance(v, dict) and len(v) == 1 and ("decimal" in v or "float" in v):
        value = v.get("decimal", v.get("float"))
        return Decimal(value)
    return None


def normalized(v):
    n = numeric(v)
    if n is not None:
        if n.is_finite():
            f = format(n, "f")
            return {"number": f.rstrip("0").rstrip(".") if "." in f and n else f if n else "0"}
        return {"number": str(n)}
    if isinstance(v, dict):
        return {k: normalized(value) for k, value in sorted(v.items())}
    if isinstance(v, list):
        return [normalized(x) for x in v]
    return v


def row_key(row):
    return json.dumps(normalized(row), sort_keys=True, separators=(",", ":"))


def fingerprint(rows, ordered=False):
    elements = [row_key([typed(v) for v in row]) for row in rows]
    if not ordered:
        elements.sort()
    return {"rows": len(elements), "sha256": hashlib.sha256("\n".join(elements).encode()).hexdigest()}


def median_if_valid(samples):
    warm = samples[1:]
    if len(warm) < 2 or any(s.get("status") != "ok" or s.get("matches_spark") is not True for s in samples):
        return None
    fields = ("elapsed_s", "cpu_s", "peak_bytes")
    return {field: statistics.median([r[field] for r in warm])
            for field in fields if all(r.get(field) is not None for r in warm)}


def sail_state(name):
    raw = cmd("docker", "inspect", "--format", "{{json .State}}", name, check=False)
    return json.loads(raw) if raw.startswith("{") else {"inspect_error": scrub(raw)}


@contextmanager
def engine(result, name, *, label=None, memory=None, cores=4, mode="local", extra=None, options=(),
           port=None, image=None, ready=True, spark_conf=None):
    label = label or name
    cfg = result["engines"].setdefault(label, {})
    sail = name in ("sail", "flight")
    port = port or PORT[name]
    proc, started = None, False
    container = f"p3-{label}"
    try:
        if sail:
            image = image or base.SAIL_IMAGE
            inner = PORT[name]
            memory = memory or "12g"
            env = base.sail_env(mode, extra)
            recorded_env = {key: value for key, value in env.items() if key not in ("AWS_SECRET_ACCESS_KEY", "AWS_ACCESS_KEY_ID")}
            cfg["settings"] = {"mode": mode, "cores": cores, "memory": memory, "port": port,
                               "image": image, "docker_options": list(options), "env": recorded_env}
            args = ["docker", "run", "-d", "--name", container, "--network", "poc",
                    "-p", f"127.0.0.1:{port}:{inner}", "--cpus", str(cores), "--memory", memory,
                    "-v", f"{base.TOKEN_DIR}:/tokens:ro", *options]
            for key, value in env.items():
                args += ["-e", f"{key}={value}"]
            launch = time.perf_counter()
            cmd(*args, image, "spark" if name == "sail" else "flight",
                "server", "--ip", "0.0.0.0", "--port", str(inner), timeout=90)
            started = True
            base.wait_port(port, timeout=45)
            cfg["listen_s"] = time.perf_counter() - launch
            stats = base.CgroupStats(container)
        else:
            memory = memory or ("8g" if name == "comet" else "9g")
            spark_conf = {"spark.jars.ivySettings": str(Path(__file__).with_name("tools") / "ivysettings.xml")} | (spark_conf or {})
            cfg["settings"] = {"comet": name == "comet", "cores": cores, "driver_memory": memory,
                               "catalog_cache": False, "overrides": spark_conf or {}, "port": port}
            proc, cfg["listen_s"] = base.start_spark_connect(port=port, comet=name == "comet",
                                                             memory=memory, cores=cores, extra_conf=spark_conf)
            stats = base.PidStats(base.jvm_pid(proc))
        session = None if name == "flight" else base.connect(port)
        if session and ready:
            cfg["readiness_select1"], rows = timed(lambda: collect(session, "SELECT 1", 90))
            if rows is None or rows[0][0] != 1:
                raise RuntimeError(cfg["readiness_select1"])
        yield session, stats, container
    except Exception as exc:
        cfg["error"] = error(exc)
        raise
    finally:
        if sail and started:
            cfg["before_shutdown"] = sail_state(container)
            if cfg["before_shutdown"].get("Running"):
                cmd("docker", "stop", "-t", "8", container, check=False, timeout=20)
            cfg["exit_state"] = sail_state(container)
            (base.OUT / f"{label}.log").write_text(scrub(cmd("docker", "logs", "--tail", "1800", container, check=False)))
            cmd("docker", "rm", "-f", container, check=False)
        if proc:
            base.stop_proc(proc)
        save(result)


def group(result, name, operation, engine_name, **options):
    """Run an independent treatment and retain evidence even when startup fails."""
    try:
        with engine(result, engine_name, label=name, **options) as handle:
            return operation(*handle)
    except Exception as exc:
        result["engines"].setdefault(name, {})["error"] = error(exc)
        save(result)
        return None


def tpch_rows(session, namespace, q, deadline=SQL_TIMEOUT):
    return bounded(lambda: tpch.run_query(session, q, namespace)[0], deadline, session.interruptAll)


def load_tpch(session, sf):
    with tempfile.TemporaryDirectory(prefix=f"p3-tpch-{sf}-") as directory:
        path = data.generate(sf, Path(directory))
        return bounded(lambda: data.load(session, sf, path, f"tpch_sf{sf}"), 2500, session.interruptAll)


def load_tpcds(session, sf):
    import duckdb
    import pyarrow.parquet as pq
    from duckdb_extensions import import_extension

    import_extension("tpcds")
    namespace = f"polaris.p3_tpcds_sf{sf}"
    collect(session, f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    counts = {}
    with tempfile.TemporaryDirectory(prefix="p3-tpcds-") as directory:
        con = duckdb.connect(str(Path(directory) / "generated.db"))
        con.execute("SET threads=4")
        con.execute("SET memory_limit='3GB'")
        con.execute("LOAD tpcds")
        con.execute(f"CALL dsdgen(sf={int(sf)})")
        for table, in con.sql("SHOW TABLES").fetchall():
            path = Path(directory) / f"{table}.parquet"
            reader = con.execute(f"SELECT * FROM {table}").fetch_record_batch(rows_per_batch=128_000)
            with pq.ParquetWriter(path, reader.schema, compression="snappy", use_dictionary=False) as writer:
                for batch in reader:
                    writer.write_batch(batch)
            collect(session, f"CREATE TABLE {namespace}.{table} USING iceberg AS SELECT * FROM parquet.`file://{path}`", 1200)
            counts[table] = collect(session, f"SELECT count(*) FROM {namespace}.{table}")[0][0]
            path.unlink()
        con.close()
    return counts


def query_file(path):
    return [sql.strip() for sql in path.read_text().split(";") if sql.strip()]
