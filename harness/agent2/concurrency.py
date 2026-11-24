"""Measure validated goodput and latency across independent query streams."""

import random
import statistics
import threading

from harness import common as base, tpch
from harness.agent2.common import attempt, begin, engine, fingerprint, latency, query_h, save, tpch_reference, wait_ready

TASK = "concurrency"
QUERIES = tuple(range(1, 23))
SETS = (1, 4, 8, 16)
DEADLINE = 180


def run_trial(sessions, stats, reference, seed):
    results = [[] for _ in sessions]
    rendezvous = threading.Barrier(len(sessions))

    def stream(index, spark):
        order = list(QUERIES)
        random.Random(seed + index).shuffle(order)
        rendezvous.wait()
        for query in order:
            record, rows = attempt(lambda: query_h(spark, "tpch_sf10", query, DEADLINE))
            record["query"] = query
            if rows is not None:
                record["matches_spark"] = None if query not in reference else fingerprint(
                    rows, tpch.ordered(query)
                ) == reference[query]
            results[index].append(record)
            if record["status"] == "timeout" and "client still active: True" in record["error"]:
                break

    def run():
        threads = [threading.Thread(target=stream, args=(index, spark))
                   for index, spark in enumerate(sessions)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    record, _ = attempt(run, stats)
    executions = [row for stream_results in results for row in stream_results]
    valid = [row for row in executions if row.get("matches_spark") is True]
    expected = len(sessions) * len(QUERIES)
    record.update({
        "seed": seed, "stream_results": results, "expected": expected, "attempted": len(executions),
        "validated": len(valid), "errors": sum(row["status"] != "ok" for row in executions),
        "mismatches": sum(row.get("matches_spark") is False for row in executions),
        "unverified": sum(row["status"] == "ok" and row.get("matches_spark") is None for row in executions),
        "queries_per_hour": len(valid) / record["wall_s"] * 3600,
        **latency(row["wall_s"] for row in valid),
    })
    return record


def main():
    result = begin(TASK)
    result["measurement"].update({"query_timeout_s": DEADLINE, "warm_samples": 2})
    reference = tpch_reference(result, 10, QUERIES, TASK)
    if not result.get("data_load"):
        return
    for name in ("sail", "sail-cluster", "comet"):
        for streams in SETS:
            label = f"{name}-{streams}"
            with engine(result, name, label=label, output_name=TASK) as (case, first, stats, port):
                sessions = [first] + [base.connect(port) for _ in range(streams - 1)]
                for session in sessions:
                    wait_ready(session)
                    query_h(session, "tpch_sf10", 6, DEADLINE)
                case["trials"] = trials = []
                for index in range(2):
                    trial = run_trial(sessions, stats, reference, 2179 + index * 100)
                    trials.append(trial)
                    save(TASK, result)
                    if any(row["status"] == "timeout" and "client still active: True" in row["error"]
                           for stream_results in trial["stream_results"] for row in stream_results):
                        case["trial_stopped_after_timeout"] = True
                        break
                if len(trials) == 2 and all(trial["attempted"] == trial["expected"] for trial in trials):
                    keys = ("wall_s", "cpu_s", "peak_bytes", "queries_per_hour", "p50_s", "p95_s")
                    case["warm_medians"] = {
                        key: statistics.median(trial[key] for trial in trials)
                        if all(trial[key] is not None for trial in trials) else None
                        for key in keys
                    }
    save(TASK, result)


if __name__ == "__main__":
    main()
