"""Render task results from their published branches and commit the reports."""

import collections
import json
import os
import re
import statistics
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
REPOSITORY = os.getenv("GITHUB_REPOSITORY", "unikdahal/poc")
REPO = f"https://github.com/{REPOSITORY}"
TASKS = ("tpcds", "ingest", "memory", "concurrency", "udf", "catalog", "flight-leak", "celeborn", "release-vs-main")
PLAIN = "790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f"
CURRENT = {}


def git(*args):
    return subprocess.check_output(["git", "-C", str(ROOT), *args], text=True, stderr=subprocess.DEVNULL)


def read(branch, path):
    try:
        return git("show", f"{branch}:{path}")
    except subprocess.CalledProcessError:
        return ""


def text(value):
    if value is None:
        return "—"
    return str(value).replace("|", "&#124;").replace("\n", " ")


def fmt(value, digits=3):
    return f"{value:,.{digits}f}" if isinstance(value, (int, float)) else "—"


def kib(value):
    return fmt(value / 2**20, 1) if isinstance(value, (int, float)) and value >= 0 else "—"


def parts(record):
    record = record or {}
    return "/".join((fmt(record.get("median_wall_s")), fmt(record.get("median_cpu_s")), kib(record.get("median_peak_bytes"))))


def table(cols, rows):
    def line(row):
        return "| " + " | ".join(map(text, row)) + " |"
    return "\n".join([line(cols), line(("---",) * len(cols)), *(line(row) for row in rows)])


def headline(d, name):
    versions = d.get("versions", {})
    cmd = list(d.get("command", ["python", "-m", f"harness.agent2.{name.replace('-', '_')}"]))
    for i, arg in enumerate(cmd):
        if arg.startswith("/home/runner/work/_temp/"):
            cmd[i] = '"$RUNNER_TEMP/' + arg.rsplit("/_temp/", 1)[-1] + '"'
    source = "Command `" + " ".join(cmd) + "`."
    ref = f"Workflow source revision `{d.get('revision', 'unavailable')}`. Sail image revision `{versions.get('sail_sha', PLAIN)}`."
    deps = ", ".join(f"{key} {versions[key]}" for key in ("spark", "iceberg", "comet", "python", "duckdb", "adbc")
                     if key in versions)
    return "\n\n".join([source, ref, f"Recorded versions include {deps}." if deps else "",
                       "The raw JSON records engine settings, runner details and individual attempts."]).strip()


def anomalies(d, extra=()):
    observations = list(extra)
    seen = set(observations)
    for label, case in d.get("engines", {}).items():
        if not isinstance(case, dict):
            continue
        if case.get("error"):
            observations.append(f"{label} | {case['error']}")
        for line in case.get("log_excerpt", "").splitlines():
            if re.search(r"\b(error|exception|panicked|killed|failed)\b", line, re.I) and line not in seen:
                seen.add(line)
                observations.append(f"{label} | {line.strip()[:280]}")
                break
    if not observations:
        return ""
    return "\n".join(["", "## Anomalies and log excerpts", "", "```text",
                      *[str(line).strip()[:500] for line in observations[:15]], "```", ""])


def tpcds(d):
    queries = d.get("queries", {})
    rows = []
    excerpts = []
    for i in range(1, 100):
        q = queries.get(str(i), {})
        metrics = q.get("validated_metrics", {})
        runs = q.get("runs", {})
        rows.append((i, q.get("verdict", "unverified"), q.get("comet_verdict", "match" if metrics.get("comet") else "—"),
                     parts(metrics.get("spark")), parts(metrics.get("comet")), parts(metrics.get("sail"))))
        for key, samples in runs.items():
            for sample in samples:
                if sample.get("error"):
                    excerpts.append(f"Q{i} {key} | {sample['error'].splitlines()[0]}")
                    break
    votes = collections.Counter(row[1] for row in rows)
    CURRENT["tpcds"] = ", ".join(f"{value} {label}" for label, value in votes.items())
    intro = ("The suite generates 24 synthetic SF1 tables with the signed TPC-DS extension and loads "
             "Parquet into format v2 tables. Each timing cell contains median warm wall seconds, CPU-seconds "
             "and sampled peak MiB after comparison with Spark. Dashes indicate unavailable measurements.")
    return "\n\n".join([intro, f"**Verdicts.** {CURRENT['tpcds']}.",
        table(("Query", "Sail verdict", "Comet verdict", "Spark wall/CPU/MiB", "Spark+Comet wall/CPU/MiB",
               "Sail wall/CPU/MiB"), rows),
        "The reports cover the 99 SF1 query files and compare the rows returned by each statement. "
        "Cases without a validated warm timing have an empty timing cell. Larger scale factors were outside this run."]) + anomalies(d, excerpts)


def ingest(d):
    rows = []
    errors = []
    settings = (("ctas_lineitem", "lineitem"), ("ctas_orders", "orders")) + tuple(
        (f"merge_v{version}_{mode}", f"orders v{version} {mode}")
        for version in (2, 3) for mode in ("copy-on-write", "merge-on-read"))
    for key, label in settings:
        for name in ("spark", "comet", "sail", "sail-cluster"):
            runs = d.get("cases", {}).get(key, {}).get(name, [])
            validated = d.get("validated_metrics", {}).get(key, {}).get(name)
            status = "equal" if validated else "unverified"
            written = []
            for i, record in enumerate(runs):
                if record.get("error"):
                    errors.append(f"{label}, {name}, sample {i + 1} | {record['error'].splitlines()[0]}")
                if record["status"] != "ok":
                    status = record["status"]
                delta = record.get("comparison", {})
                if "extra" in delta and any(delta.values()):
                    status = f"differs ({delta['extra']} extra, {delta['missing']} missing)"
                files = record.get("files", {}).get("added")
                if files is not None and i > 0:
                    written.append((sum(c["files"] for c in files.values()), sum(c["bytes"] for c in files.values())))
            sizes = f"{fmt(statistics.median(x[0] for x in written), 1)} / {kib(statistics.median(x[1] for x in written))}" if len(written) == 2 else "—"
            rows.append((label, name, status, parts(validated), sizes))
    CURRENT["ingest"] = f"{sum(r[2] == 'equal' for r in rows)}/{len(rows)} engine-operation cases with validated warm timings"
    return "\n\n".join([
        f"The suite prepared {text(d.get('orders_row_count'))} orders with operation counts `{d.get('merge_source_counts', {})}`. "
        "Spark checked table content with `EXCEPT ALL` in both directions. The files column gives medians "
        "over the two warm outputs from the reference engine's `.files` metadata.",
        table(("Operation", "Engine", "Equality", "Warm wall s/CPU s/peak MiB", "Added files/MiB"), rows),
        "Rows without an equality verdict or warm timing lack complete verification. File-system write amplification beyond the Iceberg metadata was outside this run."
    ]) + anomalies(d, errors)


def memory(d):
    rows = []
    errors = []
    for sf in (10, 30):
        for eng, pools in (("sail", ("unbounded", "fair")), ("comet", ("driver",))):
            for cap in (2, 4, 8):
                for pool in pools:
                    for q in (9, 18, 21):
                        label = f"{eng}-{cap}g-{pool}-sf{sf}-q{q}"
                        case = d.get("engines", {}).get(label, {})
                        samples = case.get("samples", [])
                        for record in samples:
                            if record.get("error"):
                                errors.append(f"{label} | {record['error'].splitlines()[0]}")
                        spills = [r.get("local_files", {}) for r in samples]
                        spark_spill = case.get("spark_spill_including_startup", {}).get("disk_bytes_spilled")
                        rows.append((sf, q, eng, cap, pool,
                                     ", ".join(r["status"] for r in samples) or ("startup error" if case.get("error") else "unverified"),
                                     case.get("container_state", {}).get("OOMKilled"),
                                     sum(r.get("matches_spark") is True for r in samples),
                                     parts(case.get("validated_metrics")), max((s.get("observed_files", 0) for s in spills), default=None),
                                     kib(max((s.get("sampled_peak_bytes", 0) for s in spills), default=None)), kib(spark_spill)))
    CURRENT["memory"] = f"{sum(r[5] not in ('unverified', 'startup error') for r in rows)}/{len(rows)} configurations attempted, {sum(r[6] is True for r in rows)} container OOM kills"
    return "\n\n".join([
        "Each row uses a fresh server. The fair pool is limited to half the container limit. "
        "Comet uses the indicated driver heap plus one quarter as configured off-heap memory.",
        table(("SF", "Q", "Engine", "GiB", "Pool", "Attempts", "OOMKilled", "Spark matches",
               "Warm wall s/CPU s/peak MiB", "Local files", "Peak local MiB", "Spark disk-spill MiB"), rows),
        "Local spill sampling can miss temporary files created and removed between observations. The Spark "
        "driver runs on the host, where the configured heap does not impose a container memory limit."
    ]) + anomalies(d, errors)


def concurrency(d):
    rows = []
    errors = []
    for name in ("sail", "sail-cluster", "comet"):
        for n in (1, 4, 8, 16):
            case = d.get("engines", {}).get(f"{name}-{n}", {})
            trials = case.get("trials", [])
            warm = case.get("warm_medians", {})
            for trial in trials:
                for stream in trial.get("stream_results", []):
                    for row in stream:
                        if row.get("error"):
                            errors.append(f"{name}/{n} Q{row['query']} | {row['error'].splitlines()[0]}")
            rows.append((name, n, len(trials),
                         sum(t["validated"] for t in trials), sum(t["errors"] for t in trials),
                         sum(t["mismatches"] for t in trials), sum(t["unverified"] for t in trials),
                         fmt(warm.get("queries_per_hour"), 1), fmt(warm.get("p50_s")), fmt(warm.get("p95_s")),
                         fmt(warm.get("cpu_s")), kib(warm.get("peak_bytes"))))
    CURRENT["concurrency"] = f"{sum(r[7] != '—' for r in rows)}/12 stream configurations with two complete trials"
    return "\n\n".join([
        "Each session warmed with Q6 and then ran the same 22 queries in a deterministic shuffled order. "
        "Queries/hour uses Spark-matching results and divides by full trial wall time. P50 and p95 "
        "use the validated results. The table shows the median across two trials.",
        table(("Engine", "Streams", "Trials", "Validated", "Errors", "Mismatches", "Unverified",
               "Queries/hour", "P50 s", "P95 s", "CPU s", "Peak MiB"), rows),
        "Incomplete trials and completed executions without a Spark reference are visible in the raw results."
    ]) + anomalies(d, errors)


def udf(d):
    rows = []
    hol = []
    errors = []
    for name in ("spark", "sail"):
        case = d.get("engines", {}).get(name, {})
        for variant in ("scalar", "pandas", "arrow", "udtf"):
            samples = case.get("variants", {}).get(variant, [])
            valid = case.get("validated_metrics", {}).get(variant, {})
            for sample in samples:
                if sample.get("error"):
                    errors.append(f"{name} {variant} | {sample['error'].splitlines()[0]}")
            rows.append((name, variant, "/".join(r["status"] for r in samples) or "unverified",
                         sum(sample["matches_spark"] for sample in samples), fmt(valid.get("input_rows_per_s"), 0), parts(valid)))
        interference = case.get("interference", {})
        heavy = interference.get("heavy", {})
        if heavy.get("error"):
            errors.append(f"{name} heavy UDF | {heavy['error'].splitlines()[0]}")
        for phase in ("idle", "overlap"):
            metrics = interference.get(phase + "_latency", {}) if phase == "idle" or heavy.get("matches_spark") else {}
            hol.append((name, phase, len(interference.get(phase, [])), fmt(metrics.get("p50_s"), 6), fmt(metrics.get("p95_s"), 6),
                        heavy.get("matches_spark") if phase == "overlap" else None))
    if d.get("sail_client_error"):
        errors.append(d["sail_client_error"])
    CURRENT["udf"] = f"{sum(r[4] != '—' for r in rows)}/8 engine/UDF pairs with validated warm timings"
    return "\n\n".join([
        "Each variant processes 10 million synthetic inputs and checks the result count, sums and "
        "per-row value against Spark's native expression. The Sail UDF client uses the embedded runtime's Python minor version.",
        table(("Engine", "Variant", "Attempts", "Spark matches", "Input rows/s", "Warm wall s/CPU s/peak MiB"), rows),
        "Separate sessions measured `SELECT 1` idle and during the CPU-heavy scalar query.",
        table(("Engine", "Phase", "Samples", "P50 s", "P95 s", "Heavy matches Spark"), hol),
        "Cells without a warm timing have insufficient successful and validated samples."
    ]) + anomalies(d, errors)


def catalog(d):
    size = sum(d.get("table_counts", {}).values())
    states = d.get("engines", {})
    token = d.get("token_probe", {})
    CURRENT["catalog"] = f"{size:,} tables; expired-token retry " + ("verified" if token.get("retry_verified") else "unverified")
    counts = []
    for i in range(10):
        samples = [state.get("request_matrix", [])[i:i+1] for state in
                   [states.get("spark", {}), states.get("sail-none", {}), states.get("sail-global", {})]]
        flattened = [s[0] if s else {} for s in samples]
        counts.append((i + 1, next((s["sql"] for s in flattened if "sql" in s), "—"),
                       *(sum(request["path"] != "/api/catalog/v1/oauth/tokens" for request in s["requests"])
                         if s.get("request_count") is not None else None for s in flattened)))
    latency_rows = []
    for name in ("spark", "sail-none", "sail-global"):
        ops = states.get(name, {}).get("operations", {})
        for kind, phases in ops.items():
            for phase, records in phases.items():
                valid = [r for r in records if r.get("matches_spark")]
                metrics = states[name]["latencies"][kind][phase]
                cpu = [r["cpu_s"] for r in valid if r.get("cpu_s") is not None]
                peaks = [r["peak_bytes"] for r in valid if r.get("peak_bytes") is not None]
                ms = lambda s: fmt(s * 1000) if s is not None else "—"
                latency_rows.append((name, kind, phase, len(valid), ms(metrics.get("p50_s")), ms(metrics.get("p95_s")),
                                     ms(statistics.median(cpu)) if cpu else "—", kib(statistics.median(peaks)) if peaks else "—"))
    errors = []
    trace = [" ".join((str(r["status"]), r["path"], "old bearer" if r["old_token"] else "rotated bearer"))
             for r in token.get("requests", [])]
    if token.get("after_expiry", {}).get("error"):
        errors.append(token["after_expiry"]["error"])
    return "\n\n".join([
        f"Spark created {size:,} tables in {len(d.get('table_counts', {}))} namespaces. One table in each "
        "namespace contains a single synthetic row. Listing-cache scope varied while database and view "
        "listing caches remained disabled.",
        table(("Engine/cache", "Operation", "Phase", "Spark matches", "P50 ms", "P95 ms", "Median CPU ms",
               "Median sampled peak MiB"), latency_rows),
        table(("Statement", "SQL", "Spark HTTP requests", "Sail cache off", "Sail global"), counts),
        "The request matrix excludes background token issuance. The raw trace includes any such requests.",
        f"The token endpoint advertised a {text(token.get('expires_in_s'))}-second lifespan. A direct request "
        f"after waiting returned HTTP {text(token.get('direct_status_after_wait'))}. "
        f"The same-statement retry check was `{text(token.get('retry_verified'))}`.",
        "Token values are absent from the captured HTTP trace. Auth behavior outside the tested expiry and rotation sequence was outside this run."
    ]) + anomalies(d, [*errors, *trace])


def flight_leak(d):
    tickets = d.get("unfetched", {})
    memory_rows = [(fmt(row["elapsed_s"], 1), row["issuance_done"], kib(row["bytes"])) for row in tickets.get("memory", [])]
    rows = []
    excerpts = []
    for name in ("flight_cancel", "connect_interrupt"):
        case = d.get(name, {})
        rows.append((name, case.get("query_active_at_cancel"), case.get("cancel_call", {}).get("status"),
                     case.get("idle", {}).get("cpu_s"), case.get("query_before_cancel", {}).get("cpu_s"),
                     case.get("after_cancel", {}).get("cpu_s"), case.get("after_one_minute", {}).get("cpu_s"),
                     case.get("client_active_after_70s")))
        for rec in (case, case.get("cancel_call", {}), case.get("query_result", {})):
            if rec.get("error"):
                excerpts.append(rec["error"])
    if tickets.get("late_fetch", {}).get("error"):
        excerpts.append(tickets["late_fetch"]["error"])
    if tickets.get("errors"):
        excerpts.extend(row["error"] for row in tickets["errors"])
    CURRENT["flight-leak"] = f"{tickets.get('issued', 0)}/300 tickets issued; {len(memory_rows)} memory samples"
    return "\n\n".join([
        "Flight SQL uses a 120-second session timeout. The synthetic query returns 20,000 rows. "
        "A fetched control was compared with Spark before issuing the unfetched tickets.",
        f"Issued {text(tickets.get('issued'))} tickets, of which {text(tickets.get('unique_tickets'))} had distinct ticket bytes. "
        f"The initial and fresh controls matched Spark `{text(tickets.get('control_matches_spark'))}` "
        f"and `{text(tickets.get('fresh_fetch_matches_spark'))}`. The late fetch status was "
        f"`{text(tickets.get('late_fetch', {}).get('status'))}`.",
        table(("Seconds from issuance start", "Issuance complete", "Cgroup MiB"), memory_rows),
        table(("Protocol", "Query active", "Cancel RPC", "Idle CPU s/10 s", "Before CPU s/5 s",
               "After CPU s/10 s", "Later CPU s/10 s", "Client active at last sample"), rows),
        "Cgroup memory contains all server allocations and cache. Ticket expiry and memory returning to "
        "baseline address different aspects of result cleanup. The test observes one six-minute window."
    ]) + anomalies(d, excerpts)


def celeborn(d):
    rows = []
    errors = []
    scopes = d.get("engines", {})
    metrics = {name: case.get("validated_metrics", {}) for name, case in scopes.items() if name in d.get("treatments", {})}
    baseline = metrics.get("cluster-flight", {})
    for name, options in d.get("treatments", {}).items():
        m = metrics.get(name, {})
        pair = sorted(baseline.keys() & m.keys(), key=int)
        w = sum(m[q]["median_wall_s"] for q in pair) if pair else None
        b = sum(baseline[q]["median_wall_s"] for q in pair) if pair else None
        cpu = sum(m[q]["median_cpu_s"] for q in pair) if pair and all(m[q]["median_cpu_s"] is not None for q in pair) else None
        peak = max((m[q]["median_peak_bytes"] for q in pair if m[q]["median_peak_bytes"] is not None), default=None)
        stages = sum(len(run.get("stages", [])) for rec in scopes[name].get("queries", {}).values()
                     for run in rec if isinstance(run.get("stages"), list)) if name in scopes else 0
        rows.append((name, options["workers"], options["compression"], options["partition_split_bytes"],
                     len(pair), fmt(w), fmt(cpu), kib(peak), fmt(w / b) if b else "—", stages))
        for q, samples in scopes.get(name, {}).get("queries", {}).items():
            for sample in samples:
                if sample.get("error"):
                    errors.append(f"{name} Q{q} | {sample['error'].splitlines()[0]}")
                if isinstance(sample.get("stages"), dict):
                    errors.append(f"{name} Q{q} stage snapshot | {sample['stages'].get('error')}")
    fault = d.get("worker_kill", {})
    CURRENT["celeborn"] = f"{len(rows)} shuffle treatments attempted; worker loss " + (
        fault.get("query_result", {}).get("status", "unverified"))
    default = next((row for row in rows if row[0] == "celeborn-0.6.3-default"), None)
    if default and default[8] != "—":
        CURRENT["celeborn"] += f"; {default[8]}× Flight shuffle on {default[4]} paired queries"
    return "\n\n".join([
        "All timings use paired queries that matched Spark on both the treatment and the local-cluster "
        "Flight shuffle control. CPU and sampled peak memory include the Celeborn processes where applicable.",
        table(("Treatment", "Workers", "Codec", "Split bytes", "Paired queries", "Warm wall s",
               "Warm CPU s", "Peak MiB", "Wall / Flight", "Stage records"), rows),
        f"Worker kill probe. Active at injection `{text(fault.get('query_active_at_kill'))}`. "
        f"Query status `{text(fault.get('query_result', {}).get('status'))}`. Subsequent SELECT 1 correct "
        f"`{text(fault.get('subsequent_query', {}).get('correct'))}`.",
        "The info-level logs and worker metric snapshots are linked with the raw results. "
        "The stage records include planner timestamps and task start-to-stop proxies for Q5, Q9, Q18 and Q21. "
        "The comparison varies one setting at a time. Untested interactions and traces without stage evidence remain unverified."
    ]) + anomalies(d, [*d.get("case_errors", {}).values(), *errors])


def release_vs_main(d):
    rows = []
    errors = []
    versions = d.get("versions", {}).get("release", {})
    for name in ("main", "release-0.7.3", "spark"):
        case = d.get("engines", {}).get(name, {})
        for q, samples in case.get("queries", {}).items():
            for sample in samples:
                if sample.get("error"):
                    errors.append(f"{name} Q{q} | {sample['error']}")
        metrics = d.get("paired_comparison", {}).get("release" if name.startswith("release") else name, {})
        rows.append((name, len(case.get("validated_metrics", {})),
                     fmt(metrics.get("wall_s")), fmt(metrics.get("cpu_s")), fmt(metrics.get("peak_mib"), 1)))
    paired = d.get("paired_queries", [])
    pair = d.get("paired_comparison", {})
    ratio = pair.get("release", {}).get("wall_s", 0) / pair.get("main", {}).get("wall_s", 1) if paired else None
    CURRENT["release-vs-main"] = f"{len(paired)}/22 queries validated on both builds"
    if ratio is not None:
        CURRENT["release-vs-main"] += f"; release/main wall {ratio:.3f}×"
    return "\n\n".join([
        f"Release tag commit `{versions.get('upstream_tag_commit', 'unavailable')}`. Wheel "
        f"`{versions.get('wheel_sha256', 'unavailable')}`. The benchmark uses the same SF10 "
        "tables, resource settings and query loop for the release wheel and main image.",
        table(("Engine", "Validated warm queries", "Paired wall s", "Paired CPU s", "Paired peak MiB"), rows),
        f"The release/main paired wall ratio was {fmt(ratio)}× across {len(paired)} queries."
        if paired else "The run has no complete paired comparison.",
        "Source revision and LTO profile both differ. Their individual effects require a same-revision build comparison. "
        "Queries with missing or unequal results are excluded from the paired totals."
    ]) + anomalies(d, errors)


def result_ref(task):
    return f"refs/remotes/origin/results/agent2-{task}"


def run_status(run):
    status = subprocess.run(["gh", "api", f"repos/{REPOSITORY}/actions/runs/{run}",
                             "--jq", "[.status,.conclusion] | @tsv"], text=True, capture_output=True, timeout=30)
    return status.stdout.strip().split("\t") if status.returncode == 0 else []


def render(task, run):
    branch = result_ref(task)
    folder = f"agent2-{task}"
    manifest = read(branch, f"{folder}/run.json")
    report = ROOT / "results-agent2" / f"{task}.md"
    if not manifest or str(json.loads(manifest)["run_id"]) != run:
        status = run_status(run)
        if not status or status[0] != "completed":
            return False
        CURRENT[task] = f"Run {status[-1]} without publishing its raw results"
        # Preserve prior measurements if a later rerun could not publish.
        body = report.read_text()
        entry = f"Run {run} ended without publishing its raw results."
        if entry not in body:
            report.write_text(body.rstrip() + f"\n\n{entry} [Workflow run]({REPO}/actions/runs/{run})\n")
        return True
    revision = git("rev-parse", branch).strip()
    raw = read(branch, f"{folder}/{task}.json")
    header = report.read_text().splitlines()[0]
    if raw:
        result = json.loads(raw)
        body = globals()[task.replace("-", "_")](result)
        preamble = headline(result, task)
    else:
        result = json.loads(manifest)
        CURRENT[task] = "Published logs with no suite JSON"
        preamble = headline({
            "revision": result.get("sha"), "versions": json.loads(read(branch, f"{folder}/versions.json") or "{}"),
        }, task)
        body = "The workflow published its diagnostics. The suite produced no result JSON, so no outcomes or timings were verified."
    if revision not in report.read_text():
        links = f"[Result files]({REPO}/tree/{revision}/{folder}) · [Workflow run]({REPO}/actions/runs/{run})"
        if raw:
            links = f"[Raw JSON]({REPO}/blob/{revision}/{folder}/{task}.json) · " + links
        report.write_text("\n\n".join([header, preamble, body.strip(), links]) + "\n")
    return True


def update_reports(runs):
    for _ in range(5):
        git("fetch", "-q", "origin", "refs/heads/agent2:refs/remotes/origin/agent2",
            "refs/heads/results/agent2-*:refs/remotes/origin/results/agent2-*")
        git("reset", "--hard", "refs/remotes/origin/agent2")
        CURRENT.clear()
        complete = []
        for task, run in runs.items():
            if render(task, run):
                complete.append(task)
        summary = ["# Engine experiments", "",
                   "Runs use Spark 3.5 for correctness. Validated timings use warm samples "
                   "on the same runner with one engine active at a time.", "",
                   table(("Task", "Run outcome", "Hosted run"),
                         [(f"[{task}]({task}.md)", CURRENT.get(task, "Running or awaiting results"),
                           f"[Run]({REPO}/actions/runs/{runs[task]})") for task in TASKS]), ""]
        (ROOT / "results-agent2" / "SUMMARY.md").write_text("\n".join(summary))
        if not git("status", "--porcelain", "--", "results-agent2"):
            return complete
        git("add", "--", "results-agent2")
        git("commit", "-m", "Update experiment reports")
        try:
            git("push", "origin", "HEAD:refs/heads/agent2")
            return complete
        except subprocess.CalledProcessError:
            time.sleep(3)
    raise RuntimeError("Could not publish reports after five fast-forward attempts")


def main():
    raw = json.loads(os.environ["AGENT2_REPORT_RUNS"])
    if set(raw) != set(TASKS) or any(not re.fullmatch(r"[1-9]\d*", str(run)) for run in raw.values()):
        raise ValueError("Supply one numeric workflow run ID for every task")
    if os.getenv("GITHUB_REF") != "refs/heads/agent2":
        raise RuntimeError("Report publication requires an agent2 workflow checkout")
    runs = {task: str(raw[task]) for task in TASKS}
    git("config", "user.name", "agent2-results")
    git("config", "user.email", "agent2-results@localhost")
    while True:
        try:
            completed = update_reports(runs)
            print(f"Reports published for {', '.join(completed) or 'no finished runs yet'}", flush=True)
            if len(completed) == len(TASKS):
                return
        except (subprocess.SubprocessError, OSError) as exc:
            print(f"Report update will retry: {exc}", flush=True)
        time.sleep(60)


if __name__ == "__main__":
    main()
