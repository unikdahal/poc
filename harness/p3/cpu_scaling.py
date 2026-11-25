"""Run the complete SF10 query set at four CPU allocations on each runner."""

import statistics

from harness import common as base, tpch
from harness.p3.common import begin, fingerprint, group, load_tpch, median_if_valid, save, timed, tpch_rows

TASK = "cpu-scaling"
QUERIES = range(1, 23)


def main():
    result = begin(TASK)
    reference = {}

    def prepare(session, *_):
        result["data_load"] = load_tpch(session, 10)
        for q in QUERIES:
            record, rows = timed(lambda: tpch_rows(session, "tpch_sf10", q))
            if rows is not None:
                reference[q] = fingerprint(rows, tpch.ordered(q))
            result["cases"].setdefault("reference", {})[str(q)] = {
                "status": record["status"], "error": record.get("error"), "result": reference.get(q)}
    group(result, "spark-reference", prepare, "spark")
    if not reference:
        return
    base.start_token_refresher()

    def run(name, cores, session, stats, _):
        treatment = result["cases"][f"{name}-{cores}"] = {}
        for q in QUERIES:
            samples = []
            for _ in range(3):
                record, rows = timed(lambda: tpch_rows(session, "tpch_sf10", q, 240), stats)
                record["matches_spark"] = rows is not None and fingerprint(rows, tpch.ordered(q)) == reference.get(q)
                samples.append(record)
                if record["status"] == "timeout":
                    break
            treatment[str(q)] = {"samples": samples, "warm_median": median_if_valid(samples)}
            save(result)
            if samples[-1]["status"] == "timeout" and "active=True" in samples[-1]["error"]:
                break

    for index, name in enumerate(("sail", "comet")):
        for cores in range(1, 5):
            group(result, f"{name}-{cores}", lambda *args: run(name, cores, *args), name, cores=cores,
                  port=51000 + index * 10 + cores,
                  extra={"SAIL_EXECUTION__DEFAULT_PARALLELISM": str(cores)})
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
    main()
