"""Measure the operator-visible surface and lifecycle of the pinned image."""

import gzip
import json
import socket
import ssl
import statistics
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import requests

from harness.p3.common import PIN, UPSTREAM, base, begin, cmd, collect, fingerprint, group, save, sail_state, timed
from harness.p3.tenancy import named_session
from harness.suite_flight import any_cmd_statement_query

SINK_PORT = 14318
WORK = "SELECT SUM(pmod(a.id*b.id,4301)) FROM range(32000) a CROSS JOIN range(24000) b"


@contextmanager
def telemetry_sink(result):
    from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
    from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
    from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

    messages = {"traces": (ExportTraceServiceRequest, "resource_spans", "scope_spans", "spans"),
                "metrics": (ExportMetricsServiceRequest, "resource_metrics", "scope_metrics", "metrics"),
                "logs": (ExportLogsServiceRequest, "resource_logs", "scope_logs", "log_records")}
    evidence = result["telemetry"] = {}

    class OtlpRequest(BaseHTTPRequestHandler):
        def do_POST(self):
            kind = self.path.rsplit("/", 1)[-1]
            if kind not in messages:
                self.send_error(404)
                return
            data = self.rfile.read(int(self.headers["Content-Length"]))
            if self.headers.get("Content-Encoding") == "gzip":
                data = gzip.decompress(data)
            cls, resource_attr, scope_attr, events_attr = messages[kind]
            envelope = cls.FromString(data)
            record = evidence.setdefault(kind, {"requests": 0, "wire_bytes": 0, "events": 0, "examples": []})
            record["requests"] += 1
            record["wire_bytes"] += len(data)
            for resource in getattr(envelope, resource_attr):
                for scope in getattr(resource, scope_attr):
                    for event in getattr(scope, events_attr):
                        record["events"] += 1
                        if len(record["examples"]) < 40:
                            if kind == "logs":
                                example = {"level": event.severity_text, "body": event.body.string_value[:220]}
                            elif kind == "traces":
                                example = {"name": event.name, "duration_ns": event.end_time_unix_nano - event.start_time_unix_nano}
                            else:
                                example = {"name": event.name, "unit": event.unit, "data_type": event.WhichOneof("data")}
                            record["examples"].append(example)
            self.send_response(200)
            self.send_header("Content-Type", "application/x-protobuf")
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("0.0.0.0", SINK_PORT), OtlpRequest)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def sql_probe(result, label, session, sql):
    rec, rows = timed(lambda: collect(session, sql, 90))
    if rows is not None:
        rec["result"] = [list(row) for row in rows[:30]]
    result["cases"][label] = {"sql": sql} | rec


def tls_handshake(port):
    context = ssl.create_default_context()
    with socket.create_connection(("127.0.0.1", port), timeout=3) as sock:
        with context.wrap_socket(sock, server_hostname="localhost") as secure:
            return secure.version()


def flight_query(sql, bearer=None):
    import pyarrow.flight as flight

    headers = [(b"authorization", bearer.encode())] if bearer else []
    opts = flight.FlightCallOptions(timeout=5, headers=headers)
    with flight.FlightClient("grpc://127.0.0.1:32010") as client:
        info = client.get_flight_info(flight.FlightDescriptor.for_command(any_cmd_statement_query(sql)), opts)
        return [list(row.values()) for ep in info.endpoints for row in client.do_get(ep.ticket, opts).read_all().to_pylist()]


def flight_probe(result, *_):
    for attempt in range(10):
        rec, rows = timed(lambda: flight_query("SELECT 1"))
        if rows is not None:
            break
        time.sleep(.4)
    result["cases"]["flight-cleartext"] = rec | {"rows": rows}
    rec, rows = timed(lambda: flight_query("SELECT 1", "Bearer invalid-synthetic-token"))
    result["cases"]["flight-invalid-bearer"] = rec | {"rows": rows}
    rec, negotiated = timed(lambda: tls_handshake(32010))
    result["cases"]["flight-tls"] = rec | {"negotiated": negotiated}


def observe(result, session, _, name):
    for header in (None, "invalid-synthetic-token"):
        key = "connect-invalid-bearer" if header else "connect-cleartext"
        client = named_session(50051, "synthetic_probe", header) if header else session
        sql_probe(result, key, client, "SELECT 1")
    rec, negotiated = timed(lambda: tls_handshake(50051))
    result["cases"]["connect-tls"] = rec | {"negotiated": negotiated}
    for method in ("EXPLAIN", "EXPLAIN FORMATTED", "EXPLAIN ANALYZE"):
        sql_probe(result, f"sail {method}", session, method + " SELECT id % 2 AS g, sum(id) FROM range(1000) GROUP BY 1")
    for relation in ("SHOW TABLES IN system.runtime", "SHOW TABLES IN system", "SHOW NAMESPACES IN system"):
        sql_probe(result, f"sail {relation}", session, relation)
    for endpoint in ("/metrics", "/health", "/healthz"):
        label = f"connect-http{endpoint}"
        rec, response = timed(lambda: requests.get(f"http://127.0.0.1:50051{endpoint}", timeout=3))
        result["cases"][label] = rec | ({"http": response.status_code, "body": response.text[:200]} if response else {})
    result["listeners"] = cmd("docker", "exec", name, "cat", "/proc/net/tcp", "/proc/net/tcp6", check=False)
    # Give periodic export a chance to flush after the operation and plan probes.
    time.sleep(12)


def lifecycle(result, signal, trial, expected):
    key = f"signal-{signal.lower()}-{trial}"

    def exercise(session, stats, container):
        clock = time.perf_counter()
        query = {}
        def run():
            rec, rows = timed(lambda: collect(session, WORK, 95))
            query.update(rec | {"matches_spark": rows is not None and fingerprint(rows) == expected})

        pending = threading.Thread(target=run, daemon=True)
        pending.start()
        base_cpu = stats.cpu_seconds()
        limit = time.perf_counter() + 5
        while pending.is_alive() and time.perf_counter() < limit and stats.cpu_seconds() - base_cpu < .2:
            time.sleep(.05)
        active = pending.is_alive()
        before_signal_cpu = stats.cpu_seconds() - base_cpu
        response = cmd("docker", "kill", "--signal", signal, container, check=False, timeout=10)
        pending.join(100 if signal == "HUP" else 30)
        result["cases"][key] = {"in_flight_at_signal": active, "cpu_s_before_signal": before_signal_cpu,
                                "seconds_to_observation": time.perf_counter() - clock, "docker_response": response,
                                "after_signal": sail_state(container), "client": query, "client_pending": pending.is_alive()}
        if signal == "HUP" and result["cases"][key]["after_signal"].get("Running"):
            sql_probe(result, key + "-subsequent-query", session, "SELECT 1")

    group(result, key, exercise, "sail", extra={"RUST_LOG": "info"})


def main():
    result = begin("ops")
    image_info = json.loads(cmd("docker", "image", "inspect", base.SAIL_IMAGE))[0]
    result["image"] = {field: image_info[field] for field in ("Id", "Size", "RepoDigests")}
    result["cli"] = {entry: cmd("docker", "run", "--rm", base.SAIL_IMAGE, entry, "server", "--help", check=False)
                     for entry in ("spark", "flight")}

    def spark_reference(session, *_):
        for method in ("EXPLAIN", "EXPLAIN FORMATTED", "EXPLAIN ANALYZE"):
            sql_probe(result, f"spark {method}", session, method + " SELECT id % 2 AS g, sum(id) FROM range(1000) GROUP BY 1")
        return fingerprint(collect(session, "SELECT 1")), fingerprint(collect(session, WORK, 180))

    reference = group(result, "spark-reference", spark_reference, "spark")
    extra = {"RUST_LOG": "info", "SAIL_TELEMETRY__EXPORT_TRACES": "true",
             "SAIL_TELEMETRY__EXPORT_METRICS": "true", "SAIL_TELEMETRY__EXPORT_LOGS": "true",
             "SAIL_TELEMETRY__EXPORTER__SYSTEM__ENABLED": "true",
             "SAIL_TELEMETRY__EXPORTER__OTLP__ENDPOINT": f"http://host.docker.internal:{SINK_PORT}",
             "SAIL_TELEMETRY__EXPORTER__OTLP__PROTOCOL": "http-binary"}
    with telemetry_sink(result):
        group(result, "telemetry", lambda *args: observe(result, *args), "sail", extra=extra,
              options=("--add-host=host.docker.internal:host-gateway",))
    group(result, "flight", lambda *args: flight_probe(result, *args), "flight", extra={"RUST_LOG": "info"})
    result["startup"] = []
    for rep in range(3):
        start = time.perf_counter()
        label = f"cold-{rep}"

        def cold(session, *_):
            rows = collect(session, "SELECT 1", 60)
            result["startup"].append({"label": label, "launch_and_first_query_s": time.perf_counter() - start,
                                      "matches_spark": reference is not None and fingerprint(rows) == reference[0]})
        group(result, label, cold, "sail", ready=False)
    if len(result["startup"]) == 3 and all(r["matches_spark"] for r in result["startup"]):
        result["median_fresh_start_s"] = statistics.median(r["launch_and_first_query_s"] for r in result["startup"][1:])
    if reference:
        for signal, reps in (("TERM", 3), ("INT", 3), ("HUP", 1)):
            for rep in range(reps):
                lifecycle(result, signal, rep, reference[1])
    source = f"{UPSTREAM}/blob/{PIN}/crates/"
    result["source"] = {
        "config_at_startup": source + "sail-common/src/config/application.rs#L76-L84",
        "connect_signal": source + "sail-cli/src/spark/server.rs#L13-L29",
        "flight_signal": source + "sail-cli/src/flight/server.rs#L13-L19",
        "connect_listener": source + "sail-cli/src/spark/server.rs#L85-L100",
        "flight_listener": source + "sail-flight/src/entrypoint.rs#L25-L41",
        "telemetry": source + "sail-common/src/config/application.yaml#L1125-L1187",
    }
    save(result)


if __name__ == "__main__":
    main()
