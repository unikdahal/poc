"""Inject timestamped network failures and verify the resulting table state in Spark."""

import base64
import hashlib
import http.client
import json
import os
import statistics
import tarfile
import tempfile
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from subprocess import Popen
from urllib.parse import unquote, urlparse

import boto3
import requests

from harness.p3.common import VARIANT, base, begin, cmd, collect, error, fingerprint, group, save, scrub, timed

TOX_VERSION = "2.12.0"
TOX_CHECKSUM = "65e042d3fc2290c099527bc506446a6fd863a09f113b8944b636ece70221c2b3"
ADMIN = "http://127.0.0.1:8474"
PROXIES = {"s3": (19000, 19001, 9000), "catalog": (18181, 18182, 8181)}
CATALOG = "p3fault"
NS = f"{CATALOG}.test"
BUCKET = "p3-fault"
HOST_GATEWAY = "--add-host=host.docker.internal:host-gateway"
READ = "SELECT count(*), sum(ascii(substr(payload,1,1))) FROM " + NS + ".scan"
CHANGES = "SELECT count(*), sum(id), sum(version) FROM "
HOP_HEADERS = {"connection", "proxy-connection", "keep-alive", "transfer-encoding", "trailer", "upgrade"}


def credentials():
    return {"aws_access_key_id": base.S3["key"], "aws_secret_access_key": base.S3["secret"],
            "region_name": base.S3["region"], "endpoint_url": "http://localhost:9000"}


def upstream(method, path, **kwargs):
    response = requests.request(method, ADMIN + path, timeout=8, **kwargs)
    response.raise_for_status()
    return response


def toxic(proxy, name, kind, **attributes):
    return upstream("POST", f"/proxies/{proxy}/toxics", json={
        "name": name, "type": kind, "stream": "downstream", "toxicity": 1.0, "attributes": attributes,
    })


class Relay(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, relay_port, destination_port):
        super().__init__(("0.0.0.0", relay_port), Forward)
        self.destination_port = destination_port
        self.history = []
        self.drop_every = None
        self.requests_since_armed = 0
        self.gate = None
        self.on_unauthorized = None
        self.token_pacing_remaining = 0
        self.token_pacing_lock = threading.Lock()
        self.lock = threading.Lock()

    def mark(self):
        with self.lock:
            return len(self.history)

    def since(self, mark):
        with self.lock:
            return self.history[mark:].copy()


class Forward(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def forward(self):
        relay = self.server
        path = unquote(urlparse(self.path).path)
        entry = {"method": self.command, "path": path, "at": time.time()}
        with relay.lock:
            relay.history.append(entry)
            relay.requests_since_armed += 1
            if relay.drop_every and relay.requests_since_armed % relay.drop_every == 0:
                entry["status"] = 503
                entry["injected"] = True
                self.send_error(503, "synthetic S3 Service Unavailable")
                self.close_connection = True
                return
            gate = relay.gate
            if gate and gate["matches"](self.command, path):
                relay.gate = None
                entry["gate_hit"] = True
                gate["entered"].set()
            else:
                gate = None
        if gate:
            gate["release"].wait(25)
            if gate["suppress"]:
                self.close_connection = True
                return

        if self.command == "GET" and "/tables/token_piece_" in path and relay.token_pacing_remaining > 0:
            with relay.token_pacing_lock:
                if relay.token_pacing_remaining > 0:
                    relay.token_pacing_remaining -= 1
                    entry["pacing_delay_s"] = 5.1
                    time.sleep(5.1)
        connection = http.client.HTTPConnection("127.0.0.1", relay.destination_port, timeout=90)
        try:
            headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP_HEADERS}
            if self.headers.get("Transfer-Encoding") == "chunked":
                raise ValueError("HTTP relay does not decode transfer-encoded request bodies")
            size = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(size) if size else None
            connection.request(self.command, self.path, body=body, headers=headers)
            response = connection.getresponse()
            entry["status"] = response.status
            if response.status == 401:
                with relay.lock:
                    rotate = relay.on_unauthorized
                if rotate:
                    rotate()
                    entry["rotated_token_before_401_response"] = True
            self.send_response_only(response.status, response.reason)
            for key, value in response.getheaders():
                if key.lower() not in HOP_HEADERS:
                    self.send_header(key, value)
            self.send_header("Connection", "close")
            self.end_headers()
            self.close_connection = True
            if self.command != "HEAD":
                while chunk := response.read(128 * 1024):
                    self.wfile.write(chunk)
        except Exception as exc:
            entry["relay_error"] = error(exc)
            self.close_connection = True
        finally:
            connection.close()
            entry["ended"] = time.time()

    do_HEAD = do_GET = do_POST = do_PUT = do_DELETE = do_PATCH = forward


@contextmanager
def proxies(result):
    with tempfile.TemporaryDirectory(prefix="p3-tox-") as directory:
        scratch = Path(directory)
        archive = scratch / "toxiproxy.tar.gz"
        url = f"https://github.com/Shopify/toxiproxy/releases/download/v{TOX_VERSION}/toxiproxy_{TOX_VERSION}_linux_amd64.tar.gz"
        cmd("curl", "-fsSL", "--retry", "3", url, "-o", str(archive), timeout=150)
        if hashlib.sha256(archive.read_bytes()).hexdigest() != TOX_CHECKSUM:
            raise RuntimeError("Toxiproxy release checksum differs")
        with tarfile.open(archive) as tar:
            member = next(m for m in tar.getmembers() if Path(m.name).name == "toxiproxy-server")
            executable = scratch / "toxiproxy-server"
            executable.write_bytes(tar.extractfile(member).read())
        executable.chmod(0o755)
        with (base.OUT / "toxiproxy.log").open("w") as log, Popen([str(executable), "-host", "127.0.0.1"], stdout=log, stderr=log) as proc:
            relays = {}
            try:
                for _ in range(100):
                    try:
                        upstream("GET", "/version")
                        break
                    except requests.RequestException:
                        time.sleep(.1)
                for service, (port, relay_port, destination) in PROXIES.items():
                    relay = Relay(relay_port, destination)
                    threading.Thread(target=relay.serve_forever, daemon=True).start()
                    relays[service] = relay
                    upstream("POST", "/proxies", json={"name": service, "listen": f"0.0.0.0:{port}",
                                                        "upstream": f"127.0.0.1:{relay_port}", "enabled": True})
                result["proxy_routes"] = {name: {"toxiproxy": ports[0], "http_relay": ports[1], "service": ports[2]}
                                          for name, ports in PROXIES.items()}
                yield relays
            finally:
                for relay in relays.values():
                    relay.shutdown()
                    relay.server_close()
                proc.terminate()
                proc.wait(timeout=10)


def issue_token():
    response = requests.post(f"{base.POLARIS}/api/catalog/v1/oauth/tokens", auth=("root", "s3cr3t"),
                             headers={"Polaris-Realm": base.REALM},
                             data={"grant_type": "client_credentials", "scope": "PRINCIPAL_ROLE:ALL"}, timeout=15)
    response.raise_for_status()
    value = response.json()
    token = value["access_token"]
    body = token.split(".")[1]
    expiration = json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))["exp"]
    return token, expiration, value.get("expires_in")


def write_token():
    token, expires_at, ttl = issue_token()
    path = base.TOKEN_DIR / f".p3fault-{threading.get_ident()}"
    path.write_text(token)
    path.chmod(0o644)
    os.replace(path, base.TOKEN_DIR / "p3fault")
    return token, expires_at, ttl


def auth(token):
    return {"Authorization": f"Bearer {token}", "Polaris-Realm": base.REALM}


def conf():
    prefix = f"spark.sql.catalog.{CATALOG}"
    values = {"type": "rest", "uri": f"{base.POLARIS}/api/catalog", "warehouse": CATALOG,
              "credential": "root:s3cr3t", "scope": "PRINCIPAL_ROLE:ALL",
              "io-impl": "org.apache.iceberg.aws.s3.S3FileIO", "s3.endpoint": "http://localhost:9000",
              "s3.path-style-access": "true", "s3.access-key-id": base.S3["key"],
              "s3.secret-access-key": base.S3["secret"], "client.region": base.S3["region"], "cache-enabled": "false"}
    return {prefix: "org.apache.iceberg.spark.SparkCatalog"} | {f"{prefix}.{k}": v for k, v in values.items()}


def sail_env():
    return {"AWS_ENDPOINT": f"http://host.docker.internal:{PROXIES['s3'][0]}",
            "SAIL_CATALOG__DEFAULT_CATALOG": CATALOG, "RUST_LOG": "info", "SAIL_CATALOG__LIST":
            f'[{{type="iceberg-rest",name="{CATALOG}",uri="http://host.docker.internal:{PROXIES["catalog"][0]}/api/catalog",'
            f'warehouse="{CATALOG}",bearer_access_token_file="/tokens/p3fault"}}]'}


def provision():
    boto3.client("s3", **credentials()).create_bucket(Bucket=BUCKET)
    payload = {"catalog": {"name": CATALOG, "type": "INTERNAL", "readOnly": False,
        "properties": {"default-base-location": f"s3://{BUCKET}/"},
        "storageConfigInfo": {"storageType": "S3", "allowedLocations": [f"s3://{BUCKET}/"], "region": "us-east-1",
          "endpoint": "http://localhost:9000", "endpointInternal": f"http://host.docker.internal:{PROXIES['s3'][0]}",
          "pathStyleAccess": True, "stsUnavailable": True}}}
    token, *_ = write_token()
    response = requests.post(f"{base.POLARIS}/api/management/v1/catalogs", json=payload, headers=auth(token), timeout=20)
    if not response.ok:
        raise RuntimeError(f"catalog provision {response.status_code}: {scrub(response.text[:600])}")


def setup(session, *_):
    collect(session, f"CREATE NAMESPACE {NS}")
    if VARIANT == "token":
        for i in range(130):
            collect(session, f"CREATE TABLE {NS}.token_piece_{i} USING iceberg AS SELECT {i + 1} AS id", 45)
        return {"token": fingerprint(collect(session, token_sql(), 180))}
    collect(session, f"CREATE TABLE {NS}.scan USING iceberg AS SELECT /*+ REPARTITION(112) */ id, sha2(CAST(id AS STRING), 512) AS payload "
                     "FROM range(1800000)", 450)
    reference = {"scan": fingerprint(collect(session, READ, 240))}
    for fault in ("commit_reset", "catalog_down"):
        for rep in range(3):
            name = f"{NS}.{fault}_{rep}"
            collect(session, f"CREATE TABLE {name} USING iceberg TBLPROPERTIES ('format-version'='2') "
                             "AS SELECT id, 0 AS version FROM range(10000)")
            reference[name] = {"before": fingerprint(collect(session, CHANGES + name)),
                               "after_one": fingerprint(collect(session, f"SELECT count(*), sum(id), sum(version) "
                                        f"FROM (SELECT * FROM {name} UNION ALL SELECT id + 10000, 1 FROM {name})"))}
    collect(session, f"CREATE TABLE {NS}.token_query USING iceberg AS SELECT CAST(1 AS INT) AS id")
    reference["token"] = fingerprint(collect(session, f"SELECT id FROM {NS}.token_query"))
    return reference


def stats(traffic):
    return {service: {"requests": len(requests), "injected_503": sum(r.get("injected", False) for r in requests),
                      "samples": requests if len(requests) <= 12 else requests[:10] + [r for r in requests if r.get("injected") or r.get("gate_hit")]}
            for service, requests in traffic.items()}


def probe_read(result, label, relays, expected, session, engine_stats, _):
    runs = result["cases"][label] = []
    delay = int(label.split("-")[-1]) if label.startswith("latency-") else None
    if delay:
        toxic("s3", label, "latency", latency=delay, jitter=0)
    if label == "http-503":
        relays["s3"].drop_every = 100
        relays["s3"].requests_since_armed = 0
    try:
        for rep in range(3):
            marks = {name: relay.mark() for name, relay in relays.items()}
            fault = None
            if label == "scan-reset":
                fault = arm(relays["s3"], "s3", lambda method, path: method == "GET" and "/data/" in path, "reset")
            rec, rows = timed(lambda: collect(session, READ, 240), engine_stats)
            traffic = {name: relay.since(marks[name]) for name, relay in relays.items()}
            rec["matches_spark"] = rows is not None and fingerprint(rows) == expected
            rec["traffic"] = stats(traffic)
            if fault:
                fault["thread"].join(28)
                rec["fault"] = fault["outcome"]
            runs.append(rec)
            save(result)
    finally:
        relays["s3"].drop_every = None
        if delay:
            upstream("DELETE", f"/proxies/s3/toxics/{label}")
    exercised = all(r["traffic"]["s3"]["requests"] for r in runs)
    if label == "http-503":
        exercised &= any(r["traffic"]["s3"]["injected_503"] for r in runs)
    if label == "scan-reset":
        exercised &= all(r["fault"].get("injected") for r in runs)
    result.setdefault("fault_observed", {})[label] = exercised
    if exercised and all(r["matches_spark"] for r in runs):
        result.setdefault("median_valid_s", {})[label] = statistics.median(r["elapsed_s"] for r in runs[1:])


def arm(relay, proxy, predicate, kind):
    outcome = {}
    gate = {"matches": predicate, "entered": threading.Event(), "release": threading.Event(), "suppress": False}
    relay.gate = gate

    def acting():
        if not gate["entered"].wait(35):
            outcome["not_observed"] = "No fault trigger matched a proxied request"
            relay.gate = None
            return
        pause = kind == "down"
        try:
            gate["suppress"] = True
            if pause:
                cmd("docker", "pause", "poc-polaris-1", timeout=10)
                upstream("POST", f"/proxies/{proxy}", json={"enabled": False})
            else:
                toxic(proxy, "reset", "reset_peer", timeout=0)
            outcome.update({"injected": True, "at": time.time(), "kind": kind})
            time.sleep(8 if pause else 1.5)
        except Exception as exc:
            outcome["error"] = error(exc)
        finally:
            gate["release"].set()
            if pause:
                cmd("docker", "unpause", "poc-polaris-1", timeout=10, check=False)
                upstream("POST", f"/proxies/{proxy}", json={"enabled": True})
            else:
                upstream("DELETE", f"/proxies/{proxy}/toxics/reset")

    thread = threading.Thread(target=acting, daemon=True)
    thread.start()
    return {"thread": thread, "outcome": outcome}


def probe_commits(result, label, relays, session, stats_, _):
    for rep in range(3):
        name = f"{NS}.{label}_{rep}"
        marker = relays["catalog"].mark()
        fault = arm(relays["catalog"], "catalog", lambda method, path: method == "POST" and path.endswith(f"/tables/{label}_{rep}"),
                    "reset" if label == "commit_reset" else "down")
        sql = f"INSERT INTO {name} SELECT id + 10000, 1 FROM {name}"
        rec, _ = timed(lambda: collect(session, sql, 180), stats_)
        fault["thread"].join(40)
        result["cases"][name] = {"sql": sql, "client": rec, "fault": fault["outcome"],
                                 "catalog": relays["catalog"].since(marker)}
        save(result)


def verify(result, reference, spark, *_):
    store = boto3.client("s3", **credentials())
    for name, expected in reference.items():
        if name in ("scan", "token") or name not in result["cases"]:
            continue
        record = result["cases"][name]
        rec, rows = timed(lambda: collect(spark, CHANGES + name))
        matched = next((k for k, fp in expected.items() if rows is not None and fingerprint(rows) == fp), "unexpected")
        record["validation"] = rec | {"rows": [list(r) for r in rows] if rows is not None else None,
                                     "integrity_verdict": matched}
        try:
            snapshots = collect(spark, f"SELECT snapshot_id, parent_id, operation FROM {name}.snapshots ORDER BY committed_at")
            ids = {r[0] for r in snapshots}
            data_files = {r[0].split(f"s3://{BUCKET}/", 1)[-1] for r in collect(spark, f"SELECT DISTINCT file_path FROM {name}.all_files")}
            location = collect(spark, f"DESCRIBE TABLE EXTENDED {name}")
            key = next(r[1].split(f"s3://{BUCKET}/", 1)[-1] for r in location if r[0] == "Location")
            paths = {row["Key"] for page in store.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=key + "/data/")
                     for row in page.get("Contents", [])}
            record["validation"].update({"snapshots": [list(r) for r in snapshots], "snapshot_chain_valid": all(
                r[1] is None or r[1] in ids for r in snapshots), "data_objects": sorted(paths),
                "referenced_files": sorted(data_files), "orphan_data_candidates": sorted(paths - data_files),
                "missing_data_files": sorted(data_files - paths)})
        except Exception as exc:
            record["validation"]["metadata_error"] = error(exc)
        save(result)
    rec, rows = timed(lambda: collect(spark, READ, 270))
    result["post_fault_scan"] = rec | {"matches_spark": rows is not None and fingerprint(rows) == reference["scan"]}


def token_sql():
    branches = [f"SELECT id AS v FROM {NS}.token_piece_{i}" for i in range(130)]
    return "SELECT count(*), sum(v) FROM (" + " UNION ALL ".join(branches) + ") p"


def expiration(result, relays, expected, session, stats_, container):
    token, exp, ttl = write_token()
    point = f"{base.POLARIS}/api/catalog/v1/{CATALOG}/namespaces/test/tables/token_piece_0"
    relay = relays["catalog"]
    statement = token_sql()
    evidence = result["cases"]["token_expiry"] = {"issued_expires_at": exp, "reported_ttl_s": ttl,
        "pacing_max_requests": 120, "pacing_s_per_request": 5.1, "query_sql": statement,
        "direct_rest_before": requests.get(point, headers=auth(token), timeout=10).status_code, "rotations": []}
    mark = relay.mark()
    begin_at = time.time()

    def rotate():
        _, next_exp, _ = write_token()
        evidence["rotations"].append({"at": time.time(), "expires_at": next_exp})

    relay.on_unauthorized = rotate
    relay.token_pacing_remaining = 120
    status = {}

    def query():
        rec, rows = timed(lambda: collect(session, statement, 700), stats_)
        status.update(rec | {"matches_spark": rows is not None and fingerprint(rows) == expected})

    running = threading.Thread(target=query, daemon=True)
    running.start()
    while time.time() < exp + 2:
        time.sleep(min(1, max(0, exp + 2 - time.time())))
    evidence["query_active_past_expiration"] = running.is_alive()
    evidence["direct_rest_expired"] = requests.get(point, headers=auth(token), timeout=10).status_code
    save(result)
    while running.is_alive() and time.time() - begin_at < 714:
        running.join(30)
        print(f"token query active={running.is_alive()} elapsed={time.time() - begin_at:.0f}s", flush=True)
    evidence["query"] = status
    evidence["client_active"] = running.is_alive()
    evidence["catalog_requests"] = relay.since(mark)
    evidence["paced_requests"] = sum(bool(r.get("pacing_delay_s")) for r in evidence["catalog_requests"])
    evidence["request_statuses"] = {str(code): sum(r.get("status") == code for r in evidence["catalog_requests"])
                                    for code in (200, 401, 403)}
    relay.on_unauthorized = None
    relay.token_pacing_remaining = 0
    save(result)

def main():
    result = begin("faults-storage")
    result["versions"]["toxiproxy"] = {"version": TOX_VERSION, "sha256": TOX_CHECKSUM}
    try:
        with proxies(result) as relays:
            provision()
            reference = group(result, "spark-reference", setup, "spark", spark_conf=conf())
            if not reference:
                return
            result["reference"] = reference
            options = {"extra": sail_env(), "options": (HOST_GATEWAY,)}
            if VARIANT == "token":
                group(result, "token-expiry", lambda *args: expiration(result, relays, reference["token"], *args), "sail", **options)
                return
            read_cases = ("baseline", "latency-20", "latency-100", "http-503", "scan-reset")
            if VARIANT == "storage":
                for case in read_cases:
                    write_token()
                    group(result, case, lambda *args: probe_read(result, case, relays, reference["scan"], *args), "sail", **options)
                commit = ("commit_reset",)
            else:
                commit = ("catalog_down",)
            for case in commit:
                write_token()
                group(result, case, lambda *args: probe_commits(result, case, relays, *args), "sail", **options)
            group(result, "spark-integrity", lambda *args: verify(result, reference, *args), "spark", spark_conf=conf())
    except Exception as exc:
        result["error"] = error(exc)
    finally:
        save(result)


if __name__ == "__main__":
    main()
