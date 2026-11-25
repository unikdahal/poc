"""Distinguish catalog authorization, session state, and shared resource limits."""

import json
import math
import statistics
import subprocess
import sys
import threading
import time

import boto3
import pyarrow.flight as flight
import requests

from harness import common as base
from harness.p3.common import VARIANT, begin, collect, error, fingerprint, group, save, scrub, timed
from harness.suite_flight import any_cmd_statement_query

TENANTS = ("tenant_a", "tenant_b")
HEAVY = ("SELECT sum(a.id+b.id) FROM range(40000) a CROSS JOIN range(40000) b")
REFERENCE = "SELECT CAST(40000 AS BIGINT)*40000*39999"
MEMORY_HEAVY = """SELECT sum(v) FROM (
  SELECT pmod(id,1000)*25000 + id DIV 1000 AS k, max(pmod(id,31)) AS v FROM range(25000000)
  GROUP BY pmod(id,1000)*25000 + id DIV 1000
)"""
MEMORY_REFERENCE = "SELECT sum(pmod(id,31)) FROM range(25000000)"
TOKEN_URL = f"{base.POLARIS}/api/catalog/v1/oauth/tokens"
SOURCE = "https://github.com/lakehq/sail/blob/790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f/"


def token(auth):
    r = requests.post(TOKEN_URL, auth=auth, headers={"Polaris-Realm": base.REALM},
                      data={"grant_type": "client_credentials", "scope": "PRINCIPAL_ROLE:ALL"}, timeout=20)
    r.raise_for_status()
    return r.json()["access_token"]


def create_roles(result):
    auth = {"Authorization": f"Bearer {base.polaris_token()}", "Polaris-Realm": base.REALM}
    secrets = {}
    for name in TENANTS:
        operations = result["cases"].setdefault(f"provision-{name}", [])

        def manage(method, path, payload):
            r = requests.request(method, f"{base.POLARIS}/api/management/v1/{path}", headers=auth,
                                 json=payload, timeout=20)
            operations.append({"method": method, "path": path, "status": r.status_code,
                               "error": scrub(r.text[:300]) if not r.ok else None})
            save(result)
            r.raise_for_status()
            return r.json() if r.content else {}

        storage = {"storageType": "S3", "allowedLocations": [f"s3://warehouse/{name}"],
                   "endpoint": "http://localhost:9000", "endpointInternal": "http://minio:9000",
                   "pathStyleAccess": True, "stsUnavailable": False, "region": "us-east-1"}
        s3 = boto3.client("s3", endpoint_url="http://localhost:9000", aws_access_key_id=base.S3["key"],
                          aws_secret_access_key=base.S3["secret"], region_name=base.S3["region"])
        bucket = name.replace("_", "-")
        s3.create_bucket(Bucket=bucket)
        storage["allowedLocations"] = [f"s3://{bucket}/"]
        manage("POST", "catalogs", {"catalog": {"name": name, "type": "INTERNAL", "readOnly": False,
            "properties": {"default-base-location": f"s3://{bucket}/"}, "storageConfigInfo": storage}})
        creds = manage("POST", "principals", {"principal": {"name": name},
                                               "credentialRotationRequired": False})["credentials"]
        manage("POST", "principal-roles", {"principalRole": {"name": name}})
        manage("PUT", f"principals/{name}/principal-roles", {"principalRole": {"name": name}})
        manage("PUT", f"principal-roles/{name}/catalog-roles/{name}", {"catalogRole": {"name": "catalog_admin"}})
        manage("PUT", f"catalogs/{name}/catalog-roles/catalog_admin/grants",
               {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"})
        secrets[name] = token((creds["clientId"], creds["clientSecret"]))
        (base.TOKEN_DIR / name).write_text(secrets[name])
        (base.TOKEN_DIR / name).chmod(0o644)
    return secrets


def spark_catalogs():
    conf = {}
    for name in TENANTS:
        key = f"spark.sql.catalog.{name}"
        properties = {"type": "rest", "uri": f"{base.POLARIS}/api/catalog", "warehouse": name,
                      "credential": "root:s3cr3t", "scope": "PRINCIPAL_ROLE:ALL",
                      "io-impl": "org.apache.iceberg.aws.s3.S3FileIO", "s3.endpoint": "http://localhost:9000",
                      "s3.path-style-access": "true", "s3.access-key-id": base.S3["key"],
                      "s3.secret-access-key": base.S3["secret"], "client.region": base.S3["region"],
                      "cache-enabled": "false"}
        conf[key] = "org.apache.iceberg.spark.SparkCatalog"
        conf.update({f"{key}.{k}": v for k, v in properties.items()})
    return conf


def catalog_env(per_alias=False, vending=False):
    configs = []
    for name in TENANTS:
        identity = name if per_alias else TENANTS[0]
        delegation = ',access_delegation="vended-credentials"' if vending else ""
        configs.append(f'{{type="iceberg-rest",name="{name}",uri="http://polaris:8181/api/catalog",'
                       f'warehouse="{name}",bearer_access_token_file="/tokens/{identity}"{delegation}}}')
    return {"SAIL_CATALOG__LIST": "[" + ",".join(configs) + "]",
            "SAIL_CATALOG__DEFAULT_CATALOG": TENANTS[0]}


def setup_tables(spark, *_):
    for index, name in enumerate(TENANTS, start=1):
        collect(spark, f"CREATE NAMESPACE {name}.isolated")
        for suffix in ("anchor", "connect_a", "connect_b", "flight_a", "flight_b", "aliases_a", "aliases_b"):
            collect(spark, f"CREATE TABLE {name}.isolated.{suffix} USING iceberg AS SELECT {index} AS id")
    return {"cpu": fingerprint(collect(spark, REFERENCE)), "memory": fingerprint(collect(spark, MEMORY_REFERENCE))}


def rest_checks(result, secrets):
    headers = {"Polaris-Realm": base.REALM}
    admin = boto3.client("s3", endpoint_url="http://localhost:9000", aws_access_key_id=base.S3["key"],
                         aws_secret_access_key=base.S3["secret"], region_name=base.S3["region"])
    paths = {}
    credentials = {}
    for actor in TENANTS:
        for table in TENANTS:
            endpoint = f"{base.POLARIS}/api/catalog/v1/{table}/namespaces/isolated/tables/anchor"
            for scope in ("ordinary", "vended"):
                extra = {"X-Iceberg-Access-Delegation": "vended-credentials"} if scope == "vended" else {}
                response = requests.get(endpoint, headers=headers | extra | {"Authorization": f"Bearer {secrets[actor]}"}, timeout=20)
                value = response.json()
                config = value.get("config", {})
                if response.ok and actor == table and scope == "vended":
                    credentials[actor] = config
                    locations = {"anchor": value["metadata"]["location"]}
                    other = requests.get(endpoint.rsplit("/", 1)[0] + "/connect_a",
                        headers=headers | {"Authorization": f"Bearer {secrets[actor]}"}, timeout=20)
                    other.raise_for_status()
                    locations["other_table"] = other.json()["metadata"]["location"]
                    for key, location in locations.items():
                        bucket, prefix = location.removeprefix("s3://").split("/", 1)
                        data = admin.list_objects_v2(Bucket=bucket, Prefix=prefix.rstrip("/") + "/data/").get("Contents", [])
                        if data:
                            paths[(actor, key)] = (bucket, data[0]["Key"])
                result["cases"][f"rest-{actor}-{table}-{scope}"] = {
                    "http": response.status_code, "vended_storage_config_keys": sorted(config),
                    "storage_credentials_count": len(value.get("storage-credentials", [])),
                    "error": scrub(value.get("error", {}).get("message", "")) if not response.ok else None,
                }
    operation, _ = timed(lambda: boto3.client("sts", endpoint_url="http://localhost:9000",
        aws_access_key_id=base.S3["key"], aws_secret_access_key=base.S3["secret"], region_name=base.S3["region"]).assume_role(
            RoleArn="arn:aws:iam::000000000000:role/tenant-a", RoleSessionName="tenant-a-check", DurationSeconds=900))
    result["cases"]["sts-assume-role"] = operation
    for actor, config in credentials.items():
        try:
            storage = boto3.client("s3", endpoint_url="http://localhost:9000", region_name=base.S3["region"],
                aws_access_key_id=config["s3.access-key-id"], aws_secret_access_key=config["s3.secret-access-key"],
                aws_session_token=config["s3.session-token"])
        except Exception as exc:
            result["cases"][f"sts-{actor}-credential-client"] = {"status": "error", "error": error(exc)}
            continue
        for (table, role), (bucket, key) in paths.items():
            rec = {"bucket": bucket, "object": key}
            try:
                response = storage.get_object(Bucket=bucket, Key=key, Range="bytes=0-3")
                with response["Body"] as body:
                    rec.update({"http": response["ResponseMetadata"]["HTTPStatusCode"], "prefix_hex": body.read().hex()})
            except Exception as exc:
                rec.update({"error": error(exc), "http": getattr(exc, "response", {}).get("ResponseMetadata", {}).get("HTTPStatusCode")})
            result["cases"][f"sts-{actor}-{table}-{role}"] = rec
    save(result)


def op(result, label, query, execute):
    record, value = timed(lambda: execute(query))
    if value is not None:
        record["rows"] = [list(row) for row in value]
    result["cases"][label] = record | {"sql": query}
    save(result)
    return record


def cross_actions(result, label, actor, execute):
    for target in TENANTS:
        table = f"{target}.isolated.{label}_{actor[-1]}"
        for verb, sql in (("read", f"SELECT id FROM {table}"), ("write", f"INSERT INTO {table} VALUES (99)"),
                          ("drop", f"DROP TABLE {table}")):
            op(result, f"{label}-{actor}-{target}-{verb}", sql, execute)


def named_session(port, actor, bearer):
    from pyspark.sql.connect.client import ChannelBuilder
    from pyspark.sql.connect.session import SparkSession

    class HeaderChannel(ChannelBuilder):
        def metadata(self):
            return list(super().metadata()) + [("authorization", f"Bearer {bearer}")]

    return SparkSession(HeaderChannel(f"sc://localhost:{port}"), userId=actor)


def session_probes(result, a, b):
    for name, spark in (("a", a), ("b", b)):
        execute = lambda sql: collect(spark, sql, 30)
        if name == "a":
            for sql in ("CREATE TEMPORARY VIEW tenant_marker AS SELECT 71 AS v",
                        "SET spark.sql.session.timeZone=Pacific/Honolulu"):
                op(result, f"session-{name}-{sql.split()[0]}", sql, execute)
            a.catalog.setCurrentCatalog("tenant_a")
            a.catalog.setCurrentDatabase("isolated")
            record, _ = timed(lambda: a.udf.register("tenant_increment", lambda x: x + 1, "int"))
            result["cases"]["python-udf-register-a"] = record
        zone, value = timed(lambda: spark.conf.get("spark.sql.session.timeZone"))
        result["cases"][f"session-{name}-config-zone"] = zone | {"value": value}
        for key, sql in (("view", "SELECT * FROM tenant_marker"),
                         ("zone", "SELECT current_timezone()"),
                         ("context", "SELECT current_catalog(), current_database()"),
                         ("udf", "SELECT tenant_increment(1)")):
            op(result, f"session-{name}-{key}", sql, execute)


def fairness(result, label, a, b, expected, heavy=HEAVY, stats=None):
    def sample():
        record, rows = timed(lambda: collect(b, "SELECT 1", 15))
        record["matches_spark"] = rows is not None and rows[0][0] == 1
        return record

    # Keep each trial independent so the server cleanup after a timed-out workload is observable.
    idle = [sample() for _ in range(40)]
    load = {}

    def background():
        record, rows = timed(lambda: collect(a, heavy, 90), stats)
        load.update(record | {"matches_spark": rows is not None and fingerprint(rows) == expected})

    th = threading.Thread(target=background, daemon=True)
    th.start()
    time.sleep(.2)
    overlap = []
    while th.is_alive() and len(overlap) < 150:
        overlap.append(sample())
    th.join(100)

    def p99(rows):
        observations = sorted(r["elapsed_s"] for r in rows if r["matches_spark"])
        return observations[math.ceil(.99 * len(observations)) - 1] if observations else None

    result["cases"][label] = {"idle": idle, "under_load": overlap, "heavy": load, "sql": heavy,
                              "p99_idle_s": p99(idle), "p99_under_load_s": p99(overlap) if load.get("matches_spark") else None}


def main():
    result = begin("tenancy")
    secrets = create_roles(result)
    expected = group(result, "spark-reference", setup_tables, "spark", spark_conf=spark_catalogs())
    if expected is None:
        return
    rest_checks(result, secrets)

    def connect_test(label, port):
        def run(*_):
            a, b = [named_session(port, name, secrets[name]) for name in TENANTS]
            for actor, session in zip(TENANTS, (a, b)):
                key = f"spark.sql.catalog.{actor}.bearer_access_token_file"
                rec, _ = timed(lambda: session.conf.set(key, f"/tokens/{actor}"))
                result["cases"][f"{label}-{actor}-session-option"] = rec
                cross_actions(result, label, actor, lambda sql: collect(session, sql, 40))
            if label == "connect":
                session_probes(result, a, b)
        return run

    group(result, "connect-global", connect_test("connect", 50051), "sail", extra=catalog_env())
    group(result, "connect-aliases", connect_test("aliases", 50052), "sail", port=50052, extra=catalog_env(True))

    def dynamic_catalog(*_):
        for actor in TENANTS:
            session = named_session(50053, actor, secrets[actor])
            options = {"": "iceberg-rest", ".uri": "http://polaris:8181/api/catalog", ".warehouse": actor,
                       ".bearer_access_token": secrets[actor]}
            for suffix, value in options.items():
                rec, _ = timed(lambda: session.conf.set(f"spark.sql.catalog.per_session{suffix}", value))
                result["cases"][f"dynamic-{actor}-set{suffix}"] = rec
            op(result, f"dynamic-{actor}-own-catalog", "SELECT id FROM per_session.isolated.anchor",
               lambda sql: collect(session, sql, 30))

    group(result, "connect-dynamic", dynamic_catalog, "sail", port=50053, extra=catalog_env())

    for ambient in ("valid", "invalid"):
        def vended_read(session, *_):
            for actor in TENANTS:
                op(result, f"sail-vending-{ambient}-{actor}", f"SELECT id FROM {actor}.isolated.anchor",
                   lambda sql: collect(session, sql, 35))
        settings = catalog_env(True, vending=True)
        if ambient == "invalid":
            settings |= {"AWS_ACCESS_KEY_ID": "invalid-synthetic", "AWS_SECRET_ACCESS_KEY": "invalid-synthetic"}
        group(result, f"vending-{ambient}", vended_read, "sail", extra=settings)

    def flight_test(*_):
        def executor(client, opts):
            def execute(sql):
                info = client.get_flight_info(flight.FlightDescriptor.for_command(any_cmd_statement_query(sql)), opts)
                return [list(row.values()) for ep in info.endpoints for row in client.do_get(ep.ticket, opts).read_all().to_pylist()]
            return execute

        for attempt in range(10):
            with flight.FlightClient("grpc://127.0.0.1:32010") as client:
                ready = op(result, f"flight-ready-{attempt}", "SELECT 1", executor(client, flight.FlightCallOptions(timeout=5)))
            if ready["status"] == "ok":
                break
            time.sleep(.4)
        for actor in TENANTS:
            with flight.FlightClient("grpc://127.0.0.1:32010") as client:
                opts = flight.FlightCallOptions(timeout=35, headers=[(b"authorization", f"Bearer {secrets[actor]}".encode())])
                execute = executor(client, opts)
                cross_actions(result, "flight", actor, execute)
                if actor == "tenant_a":
                    op(result, "flight-create-temp-view", "CREATE TEMPORARY VIEW flight_tenant_marker AS SELECT 71 AS v", execute)
                    op(result, "flight-set-zone", "SET timezone = 'Pacific/Honolulu'", execute)
                for label, sql in (("view", "SELECT * FROM flight_tenant_marker"), ("zone", "SELECT current_timezone()"),
                                   ("context", "SELECT current_catalog(), current_database()")):
                    op(result, f"flight-{actor}-{label}", sql, execute)

    group(result, "flight-global", flight_test, "flight", extra=catalog_env() | {"RUST_LOG": "info"})
    for mode, mem, profile in (("local", "8g", "cpu"), ("local-cluster", "8g", "cpu"), ("local", "2g", "cpu")):
        trial_labels = []
        for rep in range(3):
            port = 50500 + rep
            label = f"fairness-{profile}-{mode}-{mem}-{rep}"
            def run(_, stats, __):
                sessions = [named_session(port, name, secrets[name]) for name in TENANTS]
                fairness(result, label, *sessions, expected[profile], MEMORY_HEAVY if profile == "memory" else HEAVY, stats)
            group(result, label, run, "sail", port=port, memory=mem, mode=mode, extra=catalog_env())
            trial_labels.append(label)
        samples = [result["cases"].get(k, {}) for k in trial_labels[1:]]
        if all(s.get("p99_under_load_s") is not None for s in samples):
            result.setdefault("fairness_medians", {})[f"{profile}-{mode}-{mem}"] = {
                name: statistics.median(s[name] for s in samples) for name in ("p99_idle_s", "p99_under_load_s")
            }
    result["source_candidates"] = {
        "flight_session": SOURCE + "crates/sail-flight/src/service.rs#L58-L69",
        "spark_session": SOURCE + "crates/sail-session/src/session_manager/actor/handler.rs#L45",
        "server_catalogs": SOURCE + "crates/sail-session/src/catalog.rs#L28-L98",
        "vending": SOURCE + "crates/sail-catalog-iceberg/src/provider.rs#L106-L119",
    }
    save(result)


def memory_treatment(result, rep):
    label = f"{VARIANT}-rep-{rep}"

    def run(session, stats, _):
        peer = named_session(50051, "tenant_b", "synthetic-tenant-b")
        fairness(result, label, session, peer, result["memory_reference"], MEMORY_HEAVY, stats)

    group(result, label, run, "sail", memory=VARIANT.rsplit("-", 1)[-1])


def memory_sweep():
    result = begin("tenancy")
    expected = group(result, "spark-reference", lambda session, *_: fingerprint(collect(session, MEMORY_REFERENCE)), "spark")
    if expected is None:
        return
    result["memory_reference"] = expected
    save(result)
    for rep in range(3):
        print(f"{VARIANT} trial {rep+1}", flush=True)
        subprocess.run([sys.executable, "-u", "-m", "harness.p3.tenancy", "--memory-trial", str(rep)],
                       timeout=240, check=False)
    result = json.loads((base.OUT / "result.json").read_text())
    keys = [f"{VARIANT}-rep-{i}" for i in range(3)]
    records = [result["cases"].get(k, {}) for k in keys]
    if len(records) == 3 and all(r.get("p99_under_load_s") is not None for r in records):
        result["fairness_medians"] = {VARIANT: {
            name: statistics.median(r[name] for r in records[1:])
            for name in ("p99_idle_s", "p99_under_load_s")}}
    save(result)


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "--memory-trial":
        memory_treatment(json.loads((base.OUT / "result.json").read_text()), int(sys.argv[2]))
    elif VARIANT.startswith("memory-"):
        memory_sweep()
    else:
        main()
