"""Shared helpers: catalog bootstrap, engine processes, sessions, measurement, results."""

from __future__ import annotations

import datetime as dt
import decimal
import json
import math
import os
import socket
import subprocess
import threading
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
OUT = Path(os.environ.get("POC_OUT", ROOT / "out"))
OUT.mkdir(parents=True, exist_ok=True)

POLARIS = "http://localhost:8181"
REALM = "POLARIS"
CATALOG = "poc"  # Polaris catalog (warehouse) name
SQL_CATALOG = "polaris"  # catalog name used in SQL by every engine
TOKEN_DIR = Path(os.environ.get("POC_TOKEN_DIR", "/tmp/poc-tokens"))
TOKEN_FILE = TOKEN_DIR / "polaris"
SAIL_IMAGE = os.environ.get("SAIL_IMAGE", "ghcr.io/unikdahal/poc/sail:latest")
S3 = {"key": "admin", "secret": "password", "region": "us-east-1"}


# --------------------------------------------------------------------------- results

def write_json(name: str, obj) -> Path:
    p = OUT / f"{name}.json"
    p.write_text(json.dumps(obj, indent=2, default=str))
    print(f"[result] wrote {p}")
    return p


def log(msg: str) -> None:
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def sh(cmd: str, check: bool = True, capture: bool = True) -> str:
    r = subprocess.run(cmd, shell=True, text=True, capture_output=capture)
    if check and r.returncode != 0:
        raise RuntimeError(f"command failed ({r.returncode}): {cmd}\n{r.stdout}\n{r.stderr}")
    return (r.stdout or "") + (r.stderr or "") if capture else ""


def wait_port(port: int, host: str = "127.0.0.1", timeout: float = 180) -> float:
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=1):
                return time.time() - start
        except OSError:
            time.sleep(0.25)
    raise TimeoutError(f"port {port} not open after {timeout}s")


# --------------------------------------------------------------------------- Polaris

def polaris_token() -> str:
    r = requests.post(
        f"{POLARIS}/api/catalog/v1/oauth/tokens",
        auth=("root", "s3cr3t"),
        headers={"Polaris-Realm": REALM},
        data={"grant_type": "client_credentials", "scope": "PRINCIPAL_ROLE:ALL"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def write_token() -> str:
    TOKEN_DIR.mkdir(parents=True, exist_ok=True)
    tok = polaris_token()
    tmp = TOKEN_FILE.with_suffix(".tmp")
    tmp.write_text(tok)
    tmp.replace(TOKEN_FILE)
    os.chmod(TOKEN_FILE, 0o644)
    return tok


def start_token_refresher(interval: float = 600) -> threading.Thread:
    def loop():
        while True:
            time.sleep(interval)
            try:
                write_token()
                log("refreshed Polaris token file")
            except Exception as e:  # noqa: BLE001
                log(f"token refresh failed: {e}")

    t = threading.Thread(target=loop, daemon=True)
    t.start()
    return t


def polaris_bootstrap() -> dict:
    """Create the `poc` catalog on MinIO and grant content management. Idempotent."""
    deadline = time.time() + 180
    while True:
        try:
            tok = polaris_token()
            break
        except Exception:  # noqa: BLE001
            if time.time() > deadline:
                raise
            time.sleep(2)
    h = {"Authorization": f"Bearer {tok}", "Polaris-Realm": REALM, "Content-Type": "application/json"}
    existing = requests.get(f"{POLARIS}/api/management/v1/catalogs", headers=h, timeout=30).json()
    names = [c["name"] for c in existing.get("catalogs", [])]
    info = {"existing": names}
    if CATALOG not in names:
        storage = {
            "storageType": "S3",
            "allowedLocations": ["s3://warehouse/"],
            "endpoint": "http://localhost:9000",
            "endpointInternal": "http://minio:9000",
            "pathStyleAccess": True,
            "stsUnavailable": True,
            "region": S3["region"],
        }
        body = {"catalog": {"name": CATALOG, "type": "INTERNAL", "readOnly": False,
                            "properties": {"default-base-location": "s3://warehouse/poc"},
                            "storageConfigInfo": storage}}
        r = requests.post(f"{POLARIS}/api/management/v1/catalogs", headers=h, json=body, timeout=30)
        if r.status_code >= 400 and "stsUnavailable" in r.text:
            storage.pop("stsUnavailable")
            r = requests.post(f"{POLARIS}/api/management/v1/catalogs", headers=h, json=body, timeout=30)
        info["create_status"] = r.status_code
        info["create_body"] = r.text[:2000]
        r.raise_for_status()
    g = requests.put(
        f"{POLARIS}/api/management/v1/catalogs/{CATALOG}/catalog-roles/catalog_admin/grants",
        headers=h, json={"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}, timeout=30)
    info["grant_status"] = g.status_code
    cfg = requests.get(f"{POLARIS}/api/catalog/v1/config", params={"warehouse": CATALOG},
                       headers=h, timeout=30)
    info["config"] = cfg.json() if cfg.ok else cfg.text
    try:
        info["polaris_version"] = requests.get(f"{POLARIS}/api/management/v1/catalogs",
                                               headers=h, timeout=10).headers.get("server")
    except Exception:  # noqa: BLE001
        pass
    write_token()
    return info


class RequestCounter:
    """Counts Polaris REST requests by scraping its access log between two marks."""

    def __init__(self, container: str = "poc-polaris-1"):
        self.container = container
        self.since = None

    def mark(self):
        self.since = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    def lines(self) -> list[str]:
        out = sh(f"docker logs --since {self.since} {self.container} 2>&1", check=False)
        return [l for l in out.splitlines() if "/api/catalog/" in l]


# --------------------------------------------------------------------------- Sail

def sail_env(mode: str = "local", extra: dict | None = None) -> dict:
    env = {
        "SAIL_MODE": mode,
        "SAIL_CATALOG__LIST": (
            f'[{{type="iceberg-rest", name="{SQL_CATALOG}", uri="http://polaris:8181/api/catalog", '
            f'warehouse="{CATALOG}", bearer_access_token_file="/tokens/polaris"}}]'
        ),
        "SAIL_CATALOG__DEFAULT_CATALOG": SQL_CATALOG,
        "AWS_ACCESS_KEY_ID": S3["key"],
        "AWS_SECRET_ACCESS_KEY": S3["secret"],
        "AWS_REGION": S3["region"],
        "AWS_ENDPOINT": "http://minio:9000",
        "AWS_ALLOW_HTTP": "true",
        "RUST_LOG": os.environ.get("SAIL_RUST_LOG", "warn"),
    }
    env.update(extra or {})
    return env


def start_sail(name: str = "sail", server: str = "spark", port: int = 50051, mode: str = "local",
               extra_env: dict | None = None, cpus: str | None = None, memory: str | None = None) -> float:
    """Start a Sail server container on the `poc` network. Returns seconds until the port opened."""
    stop_container(name)
    env = sail_env(mode, extra_env)
    envs = " ".join(f"-e {k}={json.dumps(v)}" for k, v in env.items())
    limits = (f"--cpus {cpus} " if cpus else "") + (f"--memory {memory} " if memory else "")
    inner = 50051 if server == "spark" else 32010
    t0 = time.time()
    sh(f"docker run -d --name {name} --network poc -p {port}:{inner} {limits}"
       f"-v {TOKEN_DIR}:/tokens:ro {envs} {SAIL_IMAGE} {server} server --ip 0.0.0.0 --port {inner}")
    wait_port(port)
    # The port opens before the server is ready to plan; a trivial round trip is done by callers.
    return time.time() - t0


def stop_container(name: str) -> None:
    sh(f"docker rm -f {name}", check=False)


def container_logs(name: str, tail: int = 200) -> str:
    return sh(f"docker logs --tail {tail} {name} 2>&1", check=False)


class CgroupStats:
    """CPU-seconds and memory of a container, read from its cgroup v2 files on the host."""

    def __init__(self, container: str):
        cid = sh(f"docker inspect -f '{{{{.Id}}}}' {container}").strip()
        cands = [Path(f"/sys/fs/cgroup/system.slice/docker-{cid}.scope"),
                 Path(f"/sys/fs/cgroup/docker/{cid}")]
        self.path = next((p for p in cands if p.exists()), None)
        self.container = container

    def _read(self, f: str) -> str:
        if self.path:
            try:
                return (self.path / f).read_text()
            except OSError:
                pass
        return sh(f"docker exec {self.container} cat /sys/fs/cgroup/{f}", check=False)

    def cpu_seconds(self) -> float:
        for line in self._read("cpu.stat").splitlines():
            if line.startswith("usage_usec"):
                return int(line.split()[1]) / 1e6
        return float("nan")

    def mem_current(self) -> int:
        try:
            return int(self._read("memory.current").strip())
        except ValueError:
            return -1


class PidStats:
    """CPU-seconds and RSS of a process tree (used for the Spark JVM)."""

    def __init__(self, pid: int):
        import psutil

        self.proc = psutil.Process(pid)

    def _tree(self):
        procs = [self.proc]
        try:
            procs += self.proc.children(recursive=True)
        except Exception:  # noqa: BLE001
            pass
        return procs

    def cpu_seconds(self) -> float:
        tot = 0.0
        for p in self._tree():
            try:
                t = p.cpu_times()
                tot += t.user + t.system
            except Exception:  # noqa: BLE001
                pass
        return tot

    def mem_current(self) -> int:
        tot = 0
        for p in self._tree():
            try:
                tot += p.memory_info().rss
            except Exception:  # noqa: BLE001
                pass
        return tot


class PeakSampler:
    """Samples mem_current() every 100 ms while a block runs."""

    def __init__(self, stats):
        self.stats = stats
        self.peak = 0
        self._stop = threading.Event()

    def __enter__(self):
        def run():
            while not self._stop.is_set():
                self.peak = max(self.peak, self.stats.mem_current())
                self._stop.wait(0.1)  # returns at once on exit, so it adds no wait to the timed block

        self._t = threading.Thread(target=run, daemon=True)
        self._t.start()
        return self

    def __exit__(self, *a):
        self._stop.set()
        self._t.join()
        self.peak = max(self.peak, self.stats.mem_current())


# --------------------------------------------------------------------------- Spark reference

def resolve_versions() -> dict:
    v = json.loads((ROOT / "versions.json").read_text()) if (ROOT / "versions.json").exists() else {}
    return v


def start_spark_connect(port: int = 15002, comet: bool = False, memory: str = "6g",
                        cores: int = 4, extra_conf: dict | None = None) -> tuple[subprocess.Popen, float]:
    """Start a Spark Connect server (Spark 3.5 from the pyspark package) with Iceberg [+ Comet]."""
    v = resolve_versions()
    spark_ver = v["spark"]
    iceberg = v["iceberg"]
    pkgs = [
        f"org.apache.spark:spark-connect_2.12:{spark_ver}",
        f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{iceberg}",
        f"org.apache.iceberg:iceberg-aws-bundle:{iceberg}",
    ]
    conf = {
        "spark.connect.grpc.binding.port": str(port),
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        f"spark.sql.catalog.{SQL_CATALOG}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{SQL_CATALOG}.type": "rest",
        f"spark.sql.catalog.{SQL_CATALOG}.uri": f"{POLARIS}/api/catalog",
        f"spark.sql.catalog.{SQL_CATALOG}.warehouse": CATALOG,
        f"spark.sql.catalog.{SQL_CATALOG}.credential": "root:s3cr3t",
        f"spark.sql.catalog.{SQL_CATALOG}.scope": "PRINCIPAL_ROLE:ALL",
        f"spark.sql.catalog.{SQL_CATALOG}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"spark.sql.catalog.{SQL_CATALOG}.s3.endpoint": "http://localhost:9000",
        f"spark.sql.catalog.{SQL_CATALOG}.s3.path-style-access": "true",
        f"spark.sql.catalog.{SQL_CATALOG}.s3.access-key-id": S3["key"],
        f"spark.sql.catalog.{SQL_CATALOG}.s3.secret-access-key": S3["secret"],
        f"spark.sql.catalog.{SQL_CATALOG}.client.region": S3["region"],
        # Iceberg's SparkCatalog caches loaded tables (30 s by default), which hides commits made by
        # another engine. Disabled so every read sees the catalog's current snapshot.
        f"spark.sql.catalog.{SQL_CATALOG}.cache-enabled": "false",
        "spark.sql.defaultCatalog": SQL_CATALOG,
        "spark.sql.session.timeZone": "UTC",
        "spark.driver.memory": memory,
        "spark.sql.shuffle.partitions": str(cores * 2),
        "spark.ui.enabled": "false",
    }
    if comet:
        pkgs.append(f"org.apache.datafusion:comet-spark-spark3.5_2.12:{v['comet']}")
        conf.update({
            "spark.plugins": "org.apache.spark.CometPlugin",
            "spark.shuffle.manager": "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager",
            "spark.comet.exec.shuffle.enabled": "true",
            "spark.comet.explainFallback.enabled": "true",
            "spark.memory.offHeap.enabled": "true",
            "spark.memory.offHeap.size": "4g",
            "spark.driver.extraClassPath": "",
        })
        conf.pop("spark.driver.extraClassPath")
    conf.update(extra_conf or {})
    import pyspark

    spark_home = Path(pyspark.__file__).parent
    args = [str(spark_home / "bin" / "spark-submit"), "--master", f"local[{cores}]",
            "--class", "org.apache.spark.sql.connect.service.SparkConnectServer",
            "--packages", ",".join(pkgs)]
    for k, val in conf.items():
        args += ["--conf", f"{k}={val}"]
    args.append("spark-internal")
    logf = open(OUT / f"spark-connect-{port}{'-comet' if comet else ''}.log", "w")
    env = dict(os.environ, SPARK_HOME=str(spark_home), AWS_REGION=S3["region"])
    t0 = time.time()
    proc = subprocess.Popen(args, stdout=logf, stderr=subprocess.STDOUT, env=env)
    wait_port(port, timeout=600)
    return proc, time.time() - t0


def jvm_pid(proc: subprocess.Popen) -> int:
    import psutil

    p = psutil.Process(proc.pid)
    for c in [p] + p.children(recursive=True):
        if "java" in (c.name() or ""):
            return c.pid
    return proc.pid


def stop_proc(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()


# --------------------------------------------------------------------------- Spark Connect clients

def connect(port: int, conf: dict | None = None):
    from pyspark.sql import SparkSession

    b = SparkSession.builder.remote(f"sc://localhost:{port}")
    for k, val in (conf or {}).items():
        b = b.config(k, val)
    try:
        s = b.create()
    except Exception:  # noqa: BLE001
        s = b.getOrCreate()
    return s


def warm(spark, tries: int = 60) -> float:
    t0 = time.time()
    last = None
    for _ in range(tries):
        try:
            spark.sql("SELECT 1").collect()
            return time.time() - t0
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(1)
    raise RuntimeError(f"engine not ready: {last}")


# --------------------------------------------------------------------------- comparison

def canon(v):
    """Canonical, engine-independent representation of one value."""
    if v is None:
        return None
    if isinstance(v, float):
        if math.isnan(v):
            return "NaN"
        if math.isinf(v):
            return "Inf" if v > 0 else "-Inf"
        return float(f"{v:.9g}")
    if isinstance(v, decimal.Decimal):
        return str(v)
    if isinstance(v, (dt.datetime, dt.date)):
        return v.isoformat()
    if isinstance(v, bytes):
        return v.hex()
    if isinstance(v, (list, tuple)):
        return [canon(x) for x in v]
    if isinstance(v, dict):
        return {str(k): canon(x) for k, x in sorted(v.items(), key=lambda kv: str(kv[0]))}
    if hasattr(v, "asDict"):
        return canon(v.asDict(recursive=True))
    return v


def rows_canon(rows) -> list:
    return [[canon(x) for x in r] for r in rows]


def same_rows(a: list, b: list, ordered: bool) -> bool:
    if ordered:
        return a == b
    key = lambda r: json.dumps(r, sort_keys=True, default=str)  # noqa: E731
    return sorted(a, key=key) == sorted(b, key=key)


def decimal_loose(a: list, b: list, ordered: bool) -> bool:
    """Equal after converting decimals to floats (detects scale-only differences)."""
    def conv(x):
        if isinstance(x, list):
            return [conv(y) for y in x]
        if isinstance(x, str):
            try:
                if any(c.isdigit() for c in x) and x.replace(".", "", 1).replace("-", "", 1).isdigit():
                    return float(f"{float(x):.9g}")
            except ValueError:
                pass
        return x
    return same_rows(conv(a), conv(b), ordered)
