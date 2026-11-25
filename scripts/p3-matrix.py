"""Emit the requested independent hosted treatments."""
import json
import os

variants = {
    "tenancy": ("isolation",),
    "k8s-scale": ("flight-1", "flight-2", "flight-4", "storage-1", "storage-2", "storage-4",
                  "map-kill", "reduce-kill", "drain", "netem", "pod-oom", "driver-restart", "burst"),
    "soak": ("local", "local-cluster"),
    "iceberg-deep": ("interop", "evolution", "maintenance", "conflict", "conflict-snapshot", "layout"),
    "cpu-scaling": ("runner-1", "runner-2"),
    "faults-storage": ("storage", "catalog"),
    "api-coverage": ("python35", "python4", "jvm35", "jvm4"),
    "ops": ("surface", "term", "int", "hup"),
    "scale-up": ("tpch50", "tpcds10-1", "tpcds10-2", "tpcds10-3"),
}

if __name__ == "__main__":
    selected = {task.strip() for task in os.environ.get("P3_TASKS", "").split(",")}
    matrix = [{"task": task, "variant": variant, "step_timeout": 342 if task == "soak" else 310}
              for task, cases in variants.items() for variant in cases
              if {task, f"{task}/{variant}", "all"} & selected]
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        print(f"matrix={json.dumps({'include': matrix}, separators=(',', ':'))}", file=output)
        print(f"enabled={str(bool(matrix)).lower()}", file=output)
