"""Fills k8s/sail-poc.yaml with values for the kind-based test in .github/workflows/k8s-manifest-test.yml.
Test-only differences from a real deployment: static S3 keys and a local S3 endpoint instead of an
IAM role, no node selectors, small pods. Everything else is applied exactly as written."""
import os
import re
import sys

import yaml

v = {
    "NAMESPACE": "sail-poc", "POLARIS_URI": f"http://{os.environ['POLARIS_IP']}:8181/api/catalog",
    "POLARIS_CATALOG": "poc", "POLARIS_SCOPE": "PRINCIPAL_ROLE:ALL",
    "POLARIS_CLIENT_ID": os.environ["CLIENT_ID"], "POLARIS_CLIENT_SECRET": os.environ["CLIENT_SECRET"],
    "AWS_REGION": "us-east-1", "WAREHOUSE_BUCKET": "warehouse", "TOOLS_IMAGE": "poc-tools:latest",
    "MAX_WORKERS": "3", "MAX_WORKERS_PLUS_5": "8", "WORKER_CPU": "2", "WORKER_MEM": "3Gi",
    "WORKER_POOL_BYTES": "2147483648", "QUOTA_CPU": "20", "QUOTA_MEMORY": "40Gi",
    "POC_OWNER_IDENTITY": "poc-owner", "S3_ROLE_ARN": "unused-in-test",
}
src = open(sys.argv[1]).read()
src = src.replace("<REGISTRY>:790c622e5c4d", os.environ["SAIL_IMAGE"])
src = src.replace("{<ONDEMAND_NODE_SELECTOR>}", "{}").replace("{<WORKER_NODE_SELECTOR_JSON>}", "{}")
src = src.replace("[<WORKER_TOLERATIONS_JSON>]", "[]").replace("[<CLIENT_SOURCE_SELECTOR>]", "[{ipBlock: {cidr: 0.0.0.0/0}}]")
src = re.sub(r"<([A-Z_]+)>", lambda m: v[m.group(1)], src)
docs = [d for d in yaml.safe_load_all(src) if d]
for d in docs:
    if d["kind"] == "ConfigMap" and d["metadata"]["name"] == "sail-common":
        d["data"].update({"AWS_ACCESS_KEY_ID": "admin", "AWS_SECRET_ACCESS_KEY": "password",
                          "AWS_ENDPOINT": f"http://{os.environ['S3_IP']}:9000", "AWS_ALLOW_HTTP": "true"})
    if d["kind"] == "Deployment":
        d["spec"]["template"]["spec"]["containers"][0]["resources"] = {"requests": {"cpu": "1", "memory": "2Gi"},
                                                                     "limits": {"memory": "4Gi"}}
        d["spec"]["template"]["spec"]["containers"][0]["imagePullPolicy"] = "IfNotPresent"
print(yaml.safe_dump_all(docs, sort_keys=False))
