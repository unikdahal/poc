"""Distinguish catalog authorization, session state, and shared resource limits."""

import math
import statistics
import threading
import time

import boto3
import pyarrow.flight as flight
import requests

from harness import common as base
from harness.p3.common import begin, collect, fingerprint, group, save, scrub, timed
from harness.suite_flight import any_cmd_statement_query

TENANTS = ("tenant_a", "tenant_b")
HEAVY = ("SELECT sum(a.id+b.id) FROM range(40000) a CROSS JOIN range(40000) b")
REFERENCE = "SELECT CAST(40000 AS BIGINT)*40000*39999"
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

        storage = {"storageType": "S3", "allowedLocations": ["s3://warehouse/"],
                   "endpoint": "http://localhost:9000", "endpointInternal": "http://minio:9000",
                   "pathStyleAccess": True, "stsUnavailable": False, "region": "us-east-1"}
        manage("POST", "catalogs", {"catalog": {"name": name, "type": "INTERNAL", "readOnly": False,
            "properties": {"default-base-location": f"s3://warehouse/{name}"}, "storageConfigInfo": storage}})
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


def catalog_env(per_alias=False):
    configs = []
    for name in TENANTS:
        identity = name if per_alias else TENANTS[0]
        configs.append(f'{{type="iceberg-rest",name="{name}",uri="http://polaris:8181/api/catalog",'
                       f'warehouse="{name}",bearer_access_token_file="/tokens/{identity}"}}')
    return {"SAIL_CATALOG__LIST": "[" + ",".join(configs) + "]",
            "SAIL_CATALOG__DEFAULT_CATALOG": TENANTS[0]}


def setup_tables(spark, *_):
    for index, name in enumerate(TENANTS, start=1):
        collect(spark, f"CREATE NAMESPACE {name}.isolated")
        for suffix in ("anchor", "connect_a", "connect_b", "flight_a", "flight_b", "aliases_a", "aliases_b"):
            collect(spark, f"CREATE TABLE {name}.isolated.{suffix} USING iceberg AS SELECT {index} AS id")
    return fingerprint(collect(spark, REFERENCE))


def rest_checks(result, secrets):
    headers = {"Polaris-Realm": base.REALM}
    for actor in TENANTS:
        for table in TENANTS:
            endpoint = f"{base.POLARIS}/api/catalog/v1/{table}/namespaces/isolated/tables/anchor"
            for scope in ("ordinary", "vended"):
                extra = {"X-Iceberg-Access-Delegation": "vended-credentials"} if scope == "vended" else {}
                response = requests.get(endpoint, headers=headers | extra | {"Authorization": f"Bearer {secrets[actor]}"}, timeout=20)
                value = response.json()
                config = value.get("config", {})
                result["cases"][f"rest-{actor}-{table}-{scope}"] = {
                    "http": response.status_code, "vended_storage_config_keys": sorted(config),
                    "storage_credentials_count": len(value.get("storage-credentials", [])),
                    "error": scrub(value.get("error", {}).get("message", "")) if not response.ok else None,
                }
    operation, _ = timed(lambda: boto3.client("sts", endpoint_url="http://localhost:9000",
        aws_access_key_id=base.S3["key"], aws_secret_access_key=base.S3["secret"], region_name=base.S3["region"]).assume_role(
            RoleArn="arn:aws:iam::000000000000:role/tenant-a", RoleSessionName="tenant-a-check", DurationSeconds=900))
    result["cases"]["sts-assume-role"] = operation


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
        for key, sql in (("view", "SELECT * FROM tenant_marker"),
                         ("zone", "SET spark.sql.session.timeZone"),
                         ("context", "SELECT current_catalog(), current_database()"),
                         ("udf", "SELECT tenant_increment(1)")):
            op(result, f"session-{name}-{key}", sql, execute)


def fairness(result, label, a, b, expected):
    def sample():
        record, rows = timed(lambda: collect(b, "SELECT 1", 15))
        record["matches_spark"] = rows is not None and rows[0][0] == 1
        return record

    # Keep each trial independent so the server cleanup after a timed-out workload is observable.
    idle = [sample() for _ in range(40)]
    load = {}

    def background():
        record, rows = timed(lambda: collect(a, HEAVY, 90))
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

    result["cases"][label] = {"idle": idle, "under_load": overlap, "heavy": load,
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

    def flight_test(*_):
        for actor in TENANTS:
            client = flight.FlightClient("grpc://127.0.0.1:32010")
            opts = flight.FlightCallOptions(timeout=35, headers=[(b"authorization", f"Bearer {secrets[actor]}".encode())])
            def execute(sql):
                info = client.get_flight_info(flight.FlightDescriptor.for_command(any_cmd_statement_query(sql)), opts)
                return [list(row.values()) for ep in info.endpoints for row in client.do_get(ep.ticket, opts).read_all().to_pylist()]
            cross_actions(result, "flight", actor, execute)
        client.close()

    group(result, "flight-global", flight_test, "flight", extra=catalog_env())
    for mode, mem in (("local", "8g"), ("local-cluster", "8g"), ("local", "2g")):
        trial_labels = []
        for rep in range(3):
            port = 50500 + rep
            label = f"fairness-{mode}-{mem}-{rep}"
            def run(*_):
                sessions = [named_session(port, name, secrets[name]) for name in TENANTS]
                fairness(result, label, *sessions, expected)
            group(result, label, run, "sail", port=port, memory=mem, mode=mode, extra=catalog_env())
            trial_labels.append(label)
        samples = [result["cases"].get(k, {}) for k in trial_labels[1:]]
        if all(s.get("p99_under_load_s") is not None for s in samples):
            result.setdefault("fairness_medians", {})[f"{mode}-{mem}"] = {
                name: statistics.median(s[name] for s in samples) for name in ("p99_idle_s", "p99_under_load_s")
            }
    result["source_candidates"] = {
        "flight_session": SOURCE + "crates/sail-flight/src/service.rs#L54",
        "spark_session": SOURCE + "crates/sail-session/src/session_manager/actor/handler.rs#L45",
    }
    save(result)


if __name__ == "__main__":
    main()
