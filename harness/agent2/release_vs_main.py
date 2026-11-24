"""Use the SF10 benchmark loop to compare the upstream wheel and prebuilt main."""

import statistics
import threading
from contextlib import contextmanager, nullcontext
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import requests

from harness import common as base, tpch
from harness.agent2.common import attempt, begin, bounded, collect, command, engine, load_h, save
from harness.suite_bench import bench_engine

TASK = "release-vs-main"
RELEASE_IMAGE = "agent2-release:0.7.3"
RELEASE_SHA = "ac4690078021cf2a5402dc5c7b38a886f1916c73"
TIMEOUT = 240
REPS = 3


def runner(spark, query, namespace):
    return bounded(lambda: tpch.run_query(spark, query, namespace), TIMEOUT, spark.interruptAll)


def summarize(case):
    case["validated_metrics"] = {}
    for query, runs in case.get("queries", {}).items():
        if len(runs) != REPS or any(run.get("matches_spark") is not True for run in runs):
            continue
        case["validated_metrics"][query] = {
            field: statistics.median(run[field] for run in runs[1:]) if all(
                run.get(field) is not None for run in runs[1:]
            ) else None
            for field in ("s", "cpu_s", "peak_mb")
        }


def check_delegation(result):
    route = f"{base.POLARIS}/api/catalog/v1/poc/namespaces/tpch_sf10/tables/orders"
    headers = {"Authorization": f"Bearer {base.polaris_token()}", "Polaris-Realm": base.REALM}
    result["direct_catalog_probe"] = observed = {}
    for label, extra in (("default", {}), ("vended", {"X-Iceberg-Access-Delegation": "vended-credentials"})):
        response = requests.get(route, headers=headers | extra, timeout=15)
        observed[label] = {"status": response.status_code, "error": response.text[:400] if not response.ok else None}
    return observed["default"]["status"] == 200 and observed["vended"]["status"] == 400


@contextmanager
def catalog_proxy():
    """Give both builds the same read-only catalog path when vending is unavailable."""
    calls = []
    port = 18181

    class Proxy(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def forward(self):
            headers = {}
            for key in ("Authorization", "Polaris-Realm", "Accept", "User-Agent"):
                if key in self.headers:
                    headers[key] = self.headers[key]
            response = requests.request(self.command, base.POLARIS + self.path,
                                        headers=headers, timeout=20, allow_redirects=False)
            calls.append({"method": self.command, "status": response.status_code,
                          "removed_delegation": self.headers.get("X-Iceberg-Access-Delegation") is not None})
            self.send_response(response.status_code)
            self.send_header("Content-Type", response.headers.get("Content-Type", "application/json"))
            self.send_header("Content-Length", str(len(response.content)))
            self.end_headers()
            if self.command == "GET":
                self.wfile.write(response.content)

        do_HEAD = do_GET = forward

    with ThreadingHTTPServer(("0.0.0.0", port), Proxy) as proxy:
        thread = threading.Thread(target=proxy.serve_forever, daemon=True)
        thread.start()
        try:
            yield calls
        finally:
            proxy.shutdown()
            thread.join()


def main():
    result = begin(TASK)
    result["versions"]["release"] = {
        "version": "0.7.3", "upstream_tag_commit": RELEASE_SHA, "profile": "fat LTO",
        "image": RELEASE_IMAGE,
    }
    result["versions"]["main_profile"] = "thin LTO"
    command("docker", "build", "-t", RELEASE_IMAGE, "docker/agent2-release", timeout=900)
    release = result["versions"]["release"]
    release["wheel_sha256"] = command("docker", "run", "--rm", "--entrypoint", "cat", RELEASE_IMAGE, "/wheel.sha256")
    release["image_id"] = command("docker", "image", "inspect", "--format", "{{.Id}}", RELEASE_IMAGE)
    release["python"] = command("docker", "run", "--rm", "--entrypoint", "python", RELEASE_IMAGE, "--version")
    result["versions"]["main_python"] = command("docker", "run", "--rm", "--entrypoint", "python",
                                               base.SAIL_IMAGE, "--version")
    reference = {}

    def benchmark(name, spark, stats, case):
        def checkpoint(output):
            case.update(output)
            save(TASK, result)

        case.update(bench_engine(name, spark, stats, "tpch_sf10", REPS, reference,
                                 query_runner=runner, validate_all=True, on_sample=checkpoint))
        if name == "spark":
            for query, records in case["queries"].items():
                records[0]["matches_spark"] = query in reference
        summarize(case)

    with engine(result, "spark", output_name=TASK) as (case, spark, stats, _):
        result["data_load"] = load_h(spark, 10)
        benchmark("spark", spark, stats, case)
    if not result.get("data_load"):
        return
    needs_proxy = check_delegation(result)
    with engine(result, "sail", label="release-direct-probe", image=RELEASE_IMAGE,
                output_name=TASK) as (case, spark, _, _):
        sql = "SELECT count(*) FROM polaris.tpch_sf10.nation"
        record, rows = attempt(lambda: collect(spark, sql, 60))
        case["direct_read"] = record | {"expected_rows": rows is not None and rows[0][0] == 25}

    extra = None
    run_options = None
    if needs_proxy:
        extra = {"SAIL_CATALOG__LIST": base.sail_env()["SAIL_CATALOG__LIST"].replace(
            "http://polaris:8181", "http://host.docker.internal:18181"
        )}
        run_options = ["--add-host=host.docker.internal:host-gateway"]
    result["catalog_routing"] = "shared read-only proxy omitting delegation headers" if needs_proxy else "direct"
    with (catalog_proxy() if needs_proxy else nullcontext([])) as calls:
        for label, image in (("release-0.7.3", RELEASE_IMAGE), ("main", None)):
            with engine(result, "sail", label=label, image=image, extra=extra, run_options=run_options,
                        output_name=TASK) as (case, spark, stats, _):
                offset = len(calls)
                benchmark("sail", spark, stats, case)
                case["catalog_requests"] = len(calls[offset:]) if needs_proxy else None
                case["catalog_requests_with_delegation_removed"] = sum(c["removed_delegation"] for c in calls[offset:])
    tested = result["engines"]
    main_metrics = tested["main"].get("validated_metrics", {})
    release_metrics = tested["release-0.7.3"].get("validated_metrics", {})
    paired = sorted(main_metrics.keys() & release_metrics.keys())
    result["paired_queries"] = paired
    if paired:
        result["paired_comparison"] = {
            engine_name: {"wall_s": sum(metrics[q]["s"] for q in paired),
                          "cpu_s": sum(metrics[q]["cpu_s"] for q in paired)
                          if all(metrics[q]["cpu_s"] is not None for q in paired) else None,
                          "peak_mib": max((metrics[q]["peak_mb"] for q in paired if metrics[q]["peak_mb"] is not None),
                                          default=None)}
            for engine_name, metrics in (("main", main_metrics), ("release", release_metrics))
        }
    save(TASK, result)


if __name__ == "__main__":
    main()
