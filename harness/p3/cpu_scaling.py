"""Run the complete SF10 query set at four CPU allocations on each runner."""

import json
import statistics
import subprocess
import sys
from pathlib import Path

from harness import common as base, tpch
from harness.p3.common import begin, cmd, fingerprint, group, load_tpch, median_if_valid, sail_state, save, timed, tpch_rows

TASK = "cpu-scaling"
QUERIES = range(1, 23)


def read_result():
    return json.loads((base.OUT / "result.json").read_text())


def cpu_cgroup(cores):
    return Path(f"/sys/fs/cgroup/p3-comet-cpu-{cores}")


def treatment(name, cores):
    result = read_result()
    reference = result["cases"]["reference"]
    label = f"{name}-{cores}"
    cgroup = cpu_cgroup(cores) if name == "comet" else None

    def run(session, stats, _):
        if cgroup is not None:
            cmd("sudo", "mkdir", str(cgroup), timeout=8)
            cmd("sudo", "tee", str(cgroup / "cpu.max"), input=f"{cores * 100000} 100000\n", timeout=8)
            for proc in [stats.proc, *stats.proc.children(recursive=True)]:
                cmd("sudo", "tee", str(cgroup / "cgroup.procs"), input=f"{proc.pid}\n", timeout=8)
            result["engines"][label]["cpu_cgroup"] = {"cpu_max": (cgroup / "cpu.max").read_text().strip(),
                                                       "startup_outside_cgroup": True}
            save(result)
        cases = result["cases"][label] = {}
        for q in QUERIES:
            print(f"{label} Q{q}", flush=True)
            samples = []
            for _ in range(3):
                record, rows = timed(lambda: tpch_rows(session, "tpch_sf10", q, 150), stats)
                record["matches_spark"] = rows is not None and fingerprint(rows, tpch.ordered(q)) == reference.get(str(q), {}).get("result")
                samples.append(record)
                if record["status"] == "timeout":
                    break
            cases[str(q)] = {"samples": samples, "warm_median": median_if_valid(samples)}
            save(result)
            if samples[-1]["status"] == "timeout" and "active=True" in samples[-1]["error"]:
                break

    group(result, label, run, name, cores=cores,
          port=51000 + (10 if name == "comet" else 0) + cores,
          extra={"SAIL_EXECUTION__DEFAULT_PARALLELISM": str(cores)})
    if cgroup is not None and (cgroup / "cpu.stat").exists():
        stats = dict(line.split() for line in (cgroup / "cpu.stat").read_text().splitlines())
        result["engines"][label].setdefault("cpu_cgroup", {})["cpu_stat"] = stats
        cmd("sudo", "rmdir", str(cgroup), check=False, timeout=8)
        save(result)


def main():
    result = begin(TASK)

    def prepare(session, *_):
        result["data_load"] = load_tpch(session, 10)
        for q in QUERIES:
            record, rows = timed(lambda: tpch_rows(session, "tpch_sf10", q))
            result["cases"].setdefault("reference", {})[str(q)] = {
                "status": record["status"], "error": record.get("error"),
                "result": fingerprint(rows, tpch.ordered(q)) if rows is not None else None}
            save(result)

    group(result, "spark-reference", prepare, "spark")
    if not result["cases"].get("reference"):
        return
    base.start_token_refresher()
    for name in ("sail", "comet"):
        for cores in range(1, 5):
            label = f"{name}-{cores}"
            try:
                proc = subprocess.run([sys.executable, "-u", "-m", "harness.p3.cpu_scaling", "--treatment",
                                       name, str(cores)], timeout=33 * 60)
                result = read_result()
                result["engines"].setdefault(label, {})["harness_exit_code"] = proc.returncode
            except subprocess.TimeoutExpired:
                result = read_result()
                record = result["engines"].setdefault(label, {})
                record["error"] = "Treatment client exceeded the 33-minute top-level deadline"
                if name == "sail":
                    container = f"p3-{label}"
                    cmd("docker", "stop", "-t", "8", container, check=False, timeout=20)
                    record["exit_state"] = sail_state(container)
                    cmd("docker", "rm", "-f", container, check=False)
                elif (cgroup := cpu_cgroup(cores)).exists():
                    cmd("sudo", "tee", str(cgroup / "cgroup.kill"), input="1\n", check=False, timeout=8)
                    cmd("sudo", "rmdir", str(cgroup), check=False, timeout=8)
            save(result)
        matches = [str(q) for q in QUERIES if all(
            result["cases"].get(f"{name}-{c}", {}).get(str(q), {}).get("warm_median") for c in range(1, 5))]
        if not matches:
            continue
        curve = result.setdefault("curves", {})[name] = {"paired_queries": matches}
        base_time = sum(result["cases"][f"{name}-1"][q]["warm_median"]["elapsed_s"] for q in matches)
        for cores in range(1, 5):
            samples = [result["cases"][f"{name}-{cores}"][q]["warm_median"] for q in matches]
            wall = sum(row["elapsed_s"] for row in samples)
            cpu = sum(row["cpu_s"] for row in samples) if all("cpu_s" in row for row in samples) else None
            curve[str(cores)] = {"wall_s": wall, "cpu_s": cpu, "speedup": base_time / wall,
                "parallel_efficiency": (base_time / wall) / cores,
                "cpu_utilization_fraction": cpu / (cores * wall) if cpu is not None else None,
                "median_query_wall_s": statistics.median(row["elapsed_s"] for row in samples)}
        save(result)


if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[1] == "--treatment":
        treatment(sys.argv[2], int(sys.argv[3]))
    else:
        main()
