"""Emit the requested independent hosted treatments."""
import json
import os

variants = {
    "tenancy": ("isolation",),
    "k8s-scale": ("flight-1", "flight-2", "flight-4", "storage-1", "storage-2", "storage-4", "failure"),
    "soak": ("local", "local-cluster"),
    "iceberg-deep": ("interop", "evolution", "maintenance", "conflict"),
    "cpu-scaling": ("runner-1", "runner-2"),
    "faults-storage": ("storage", "catalog"),
    "api-coverage": ("python35", "python4", "jvm35", "jvm4"),
    "ops": ("operator",),
    "scale-up": ("tpch50", "tpcds10"),
}

if __name__ == "__main__":
    selected = os.environ.get("P3_TASKS", "").split(",")
    matrix = [{"task": task, "variant": variant, "step_timeout": 342 if task == "soak" else 310}
              for task, cases in variants.items() if task in selected or "all" in selected
              for variant in cases]
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        print(f"matrix={json.dumps({'include': matrix}, separators=(',', ':'))}", file=output)
        print(f"enabled={str(bool(matrix)).lower()}", file=output)
