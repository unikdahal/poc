"""Measure catalog listing behavior and verify token rotation against Polaris."""

import datetime as dt
import os
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import requests

from harness import common as base
from harness.agent2.common import attempt, begin, collect, command, engine, fingerprint, latency, save, wait_ready

TASK = "catalog"
NAMESPACES = 20
TABLES = 100
ACCESS = re.compile(r"agent2-access (\w+) (\S+) (\d{3})")
PROXY_PORT = 18181


def namespace(index):
    return f"polaris.agent2_catalog_{index:02}"


def queries(index):
    ns = namespace(index)
    return {"show": f"SHOW TABLES IN {ns}", "describe": f"DESCRIBE TABLE {ns}.t000",
            "select": f"SELECT id FROM {ns}.t000"}


def comparable(kind, rows):
    if kind == "show":
        return fingerprint([(row[1],) for row in rows])
    if kind == "describe":
        return fingerprint([(str(row[0]).lower(), str(row[1]).lower())
                            for row in rows if row[0] == "id"])
    return fingerprint(rows)


def setup(spark, result):
    counts = result["table_counts"] = {}
    for ns in range(NAMESPACES):
        name = namespace(ns)
        collect(spark, f"CREATE NAMESPACE IF NOT EXISTS {name}")
        for number in range(TABLES):
            collect(spark, f"CREATE TABLE IF NOT EXISTS {name}.t{number:03} (id bigint) USING iceberg")
        collect(spark, f"INSERT OVERWRITE {name}.t000 SELECT CAST({ns} AS BIGINT)")
        counts[name] = len(collect(spark, queries(ns)["show"]))
        save(TASK, result)
    if counts != {namespace(ns): TABLES for ns in range(NAMESPACES)}:
        raise RuntimeError(f"unexpected table counts: {counts}")


def stamp():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def access_logs(since, until=None):
    args = ["docker", "logs", "--since", since]
    if until:
        args += ["--until", until]
    output = command(*args, "poc-polaris-1")
    return [{"method": method, "path": path, "status": int(status)}
            for method, path, status in ACCESS.findall(output) if path.startswith("/api/catalog/")]


def measure(spark, kind, sql, stats, count_requests=False):
    since = stamp()
    record, rows = attempt(lambda: collect(spark, sql, 60), stats)
    if rows is not None:
        record["output"] = comparable(kind, rows)
    if count_requests:
        time.sleep(.15)
        record["requests"] = access_logs(since, stamp())
        record["request_count"] = len(record["requests"])
    return record


def options(cache, uri="http://polaris:8181", token_file="polaris"):
    return {"SAIL_CATALOG__LIST": (
        f'[{{type="iceberg-rest", name="polaris", uri="{uri}/api/catalog", '
        f'warehouse="poc", bearer_access_token_file="/tokens/{token_file}", '
        f'database_cache_type="none", view_cache_type="none", table_cache_type="{cache}", '
        'table_cache_size=100, table_cache_ttl_secs=0}]'
    )}


def sample(result, name, cache, reference):
    label = name if name == "spark" else f"sail-{cache}"
    extra = options(cache) if name == "sail" else None
    with engine(result, name, label=label, extra=extra, output_name=TASK) as (case, spark, stats, port):
        positions = case["operations"] = {}
        phases = ("first", "warm_same_session", "warm_new_session")
        other = base.connect(port)
        wait_ready(other)
        for index in range(NAMESPACES):
            for kind, sql in queries(index).items():
                for phase in phases:
                    key = (index, kind)
                    record = measure(other if phase == "warm_new_session" else spark, kind, sql, stats)
                    if name == "spark" and phase == "first":
                        reference[key] = record.get("output")
                    record["matches_spark"] = record.get("output") is not None and record.get("output") == reference.get(key)
                    positions.setdefault(kind, {}).setdefault(phase, []).append(record | {"namespace": index})
        case["latencies"] = {
            kind: {phase: latency(r["wall_s"] for r in rows if r["matches_spark"])
                   for phase, rows in phases_data.items()}
            for kind, phases_data in positions.items()
        }
        matrix = case["request_matrix"] = []
        for index in range(10):
            kind = ("show", "describe", "select")[index % 3]
            sql = queries(index)[kind]
            record = measure(spark, kind, sql, stats, count_requests=True)
            record["matches_spark"] = record.get("output") is not None and record.get("output") == reference.get((index, kind))
            matrix.append(record | {"sql": sql, "kind": kind})
        case["access_logging_observed"] = any(record["request_count"] for record in matrix)
        if not case["access_logging_observed"]:
            for record in matrix:
                record["request_count"] = None


def issue_token():
    response = requests.post(f"{base.POLARIS}/api/catalog/v1/oauth/tokens", auth=("root", "s3cr3t"),
                             headers={"Polaris-Realm": base.REALM},
                             data={"grant_type": "client_credentials", "scope": "PRINCIPAL_ROLE:ALL"},
                             timeout=15)
    response.raise_for_status()
    return response.json()


def replace_token(path, value):
    temp = path.with_suffix(".tmp")
    temp.write_text(value)
    temp.chmod(0o644)
    temp.replace(path)


def token_probe(result):
    """Pass a real 401 through, rotating the mounted file before the response is delivered."""
    observation = result["token_probe"] = {"requests": []}
    token = issue_token()
    old = token["access_token"]
    ttl = token.get("expires_in")
    observation["expires_in_s"] = ttl
    if not isinstance(ttl, (int, float)) or not 0 < ttl <= 180:
        observation["status"] = "expiry not tested within job time budget"
        return
    token_path = base.TOKEN_DIR / "expiry"
    replace_token(token_path, old)
    rotated = {"token": None}
    rotation_lock = threading.Lock()

    class Proxy(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def do_GET(self):
            authorization = self.headers.get("Authorization", "")
            response = requests.get(base.POLARIS + self.path, timeout=15,
                                    headers={"Authorization": authorization, "Polaris-Realm": base.REALM},
                                    allow_redirects=False)
            observation["requests"].append({
                "path": self.path.split("?")[0], "status": response.status_code,
                "old_token": authorization == f"Bearer {old}",
                "rotated_token": rotated["token"] is not None and authorization == f'Bearer {rotated["token"]}',
            })
            if response.status_code == 401:
                with rotation_lock:
                    if rotated["token"] is None:
                        replacement = issue_token()["access_token"]
                        replace_token(token_path, replacement)
                        rotated["token"] = replacement
            self.send_response(response.status_code)
            self.send_header("Content-Type", response.headers.get("Content-Type", "application/json"))
            self.send_header("Content-Length", str(len(response.content)))
            self.end_headers()
            self.wfile.write(response.content)

    with ThreadingHTTPServer(("0.0.0.0", PROXY_PORT), Proxy) as proxy:
        thread = threading.Thread(target=proxy.serve_forever, daemon=True)
        thread.start()
        try:
            with engine(result, "sail", label="sail-token", extra=options("none", f"http://host.docker.internal:{PROXY_PORT}", "expiry"),
                        run_options=["--add-host=host.docker.internal:host-gateway"], output_name=TASK) as (_, spark, _, _):
                before = measure(spark, "show", queries(0)["show"], None)
                observation["before_expiry"] = before
                if before["status"] != "ok":
                    return
                until = time.monotonic() + ttl + 60
                route = f"{base.POLARIS}/api/catalog/v1/poc/namespaces/agent2_catalog_00/tables"
                while time.monotonic() < until:
                    status = requests.get(route, timeout=10,
                                          headers={"Authorization": f"Bearer {old}", "Polaris-Realm": base.REALM}).status_code
                    if status == 401:
                        break
                    time.sleep(3)
                observation["direct_status_after_wait"] = status
                start = len(observation["requests"])
                after = measure(spark, "show", queries(0)["show"], None)
                observation["after_expiry"] = after
                trace = observation["requests"][start:]
                expired_paths = {entry["path"] for entry in trace if entry["old_token"] and entry["status"] == 401}
                retried_paths = {entry["path"] for entry in trace if entry["rotated_token"] and entry["status"] == 200}
                observation["retried_paths"] = sorted(expired_paths & retried_paths)
                observation["retry_verified"] = (
                    status == 401 and after.get("output") is not None and after["output"] == before.get("output")
                    and bool(expired_paths) and expired_paths <= retried_paths
                )
        finally:
            proxy.shutdown()
            thread.join()
            token_path.unlink(missing_ok=True)


def main():
    result = begin(TASK)
    result["polaris_config"] = {"access_log": os.getenv("POLARIS_ACCESS_LOG"),
                                "token_lifetime": os.getenv("POLARIS_TOKEN_LIFETIME")}
    base.start_token_refresher(30)
    with engine(result, "spark", label="load", output_name=TASK) as (_, spark, _, _):
        setup(spark, result)
    if sum(result.get("table_counts", {}).values()) != NAMESPACES * TABLES:
        return
    reference = {}
    sample(result, "spark", "none", reference)
    sample(result, "sail", "none", reference)
    sample(result, "sail", "global", reference)
    try:
        token_probe(result)
    except Exception as exc:
        from harness.agent2.common import error

        result.setdefault("token_probe", {})["error"] = error(exc)
    save(TASK, result)


if __name__ == "__main__":
    main()
