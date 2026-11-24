"""Use the SF10 benchmark loop to compare the upstream wheel and prebuilt main."""

import statistics

from harness import common as base, tpch
from harness.agent2.common import begin, bounded, command, engine, load_h, save
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
    for name, label, image in (("spark", "spark", None), ("sail", "release-0.7.3", RELEASE_IMAGE),
                               ("sail", "main", None)):
        with engine(result, name, label=label, image=image, output_name=TASK) as (case, spark, stats, _):
            if name == "spark":
                result["data_load"] = load_h(spark, 10)
            if not result.get("data_load"):
                raise RuntimeError("SF10 reference data load incomplete")

            def checkpoint(output):
                case.update(output)
                save(TASK, result)

            case.update(bench_engine(name, spark, stats, "tpch_sf10", REPS, reference,
                                     query_runner=runner, validate_all=True, on_sample=checkpoint))
            if name == "spark":
                for query, records in case["queries"].items():
                    records[0]["matches_spark"] = query in reference
            summarize(case)

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
