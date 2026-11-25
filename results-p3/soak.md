# Long-running workload and session churn

Reproduce with `p3.yml`, `tasks=soak`. The suite command is `python -m harness.p3.soak` with `P3_VARIANT=local` or `local-cluster`.
The pinned Sail commit is `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`. The correctness reference is Spark 3.5.10 with Iceberg 1.12.0. Each raw file records the job revision, image digests, configuration and engine exit state where available.

The target schedule is 5 h 05 min of active workload and 25 min idle. Sampling is every 30 s. The mix uses six SF1 TPC-H queries, point lookups, small MERGEs and newly created sessions. The full runs used an 11 GiB container limit and a 120 s session timeout. The independent PySpark Connect 4.0.1 churn client checks `SELECT 1` against Spark and requires a successful `ReleaseSession` response. [Pinned release handler](https://github.com/lakehq/sail/blob/790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f/crates/sail-spark-connect/src/server.rs#L505-L526)

### Timed windows and run coverage

| Mode | Observed seconds | Samples | Matched / attempted | RSS first / last active / last idle MiB | RSS MiB/h [95% interval] | Evidence |
| --- | --- | --- | --- | --- | --- | --- |
| local | 390.0 | 14 | merge 68/94 · point 1127/1128 · session_churn 313/313 · tpch 275/282 | 177.1 / 786.1 / — | — | [Raw JSON](https://github.com/unikdahal/poc/blob/d829d4dc5a1ec93594dcec6df0327727525fdf38/local/result.json) · [Run 45553300746](https://github.com/unikdahal/poc/actions/runs/45553300746) |
| local | — | 0 | — | — / — / — | — | [Raw JSON](https://github.com/unikdahal/poc/blob/8d1a070faa0bfe654b5922f7ded0f4dd7d65ce49/local/result.json) · [Run 45555344538](https://github.com/unikdahal/poc/actions/runs/45555344538) |
| local | 19801.3 | 661 | merge 337/565 · point 6772/6772 · session_churn 14638/14638 · tpch 1693/1693 | 171.1 / 550.8 / 507.9 | 27.99 [19.62, 37.73] | [Raw JSON](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local/result.json) · [Run 45557039053](https://github.com/unikdahal/poc/actions/runs/45557039053) |
| local-cluster | 540.0 | 19 | merge 8/277 · point 3315/3320 · session_churn 433/433 · tpch 815/830 | 192.9 / 1415.0 / — | — | [Raw JSON](https://github.com/unikdahal/poc/blob/d829d4dc5a1ec93594dcec6df0327727525fdf38/local-cluster/result.json) · [Run 45553300746](https://github.com/unikdahal/poc/actions/runs/45553300746) |
| local-cluster | — | 0 | — | — / — / — | — | [Raw JSON](https://github.com/unikdahal/poc/blob/8d1a070faa0bfe654b5922f7ded0f4dd7d65ce49/local-cluster/result.json) · [Run 45555344538](https://github.com/unikdahal/poc/actions/runs/45555344538) |
| local-cluster | 19800.3 | 661 | merge 32/6646 · point 79744/79744 · session_churn 14638/14638 · tpch 19936/19936 | 200.7 / 9657.6 / 11217.9 | 1774.56 [1070.17, 2335.50] | [Raw JSON](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local-cluster/result.json) · [Run 45557039053](https://github.com/unikdahal/poc/actions/runs/45557039053) |

**FACT.** The first jobs produced short active windows and ended before the idle phase. They captured `Pool not running` mixed-client errors and a local-cluster slot limit failure, `task region requires 9 worker task slots, but the configured maximum is 8`. The repeat with the isolated churn client initially failed resolving Spark dependencies. [First local](https://github.com/unikdahal/poc/blob/d829d4dc5a1ec93594dcec6df0327727525fdf38/local/result.json) · [First cluster](https://github.com/unikdahal/poc/blob/d829d4dc5a1ec93594dcec6df0327727525fdf38/local-cluster/result.json)

RSS, cgroup memory, open file descriptors, thread count, local disk usage, S3 object counts and correctness-gated latency windows are in the raw JSON. The fit uses adjacent 30-minute block medians and a block bootstrap. This estimates trend over the actual observed window. The first two short windows cannot support a leak rate or a post-idle conclusion.

Tokio task counts were unavailable from the container process telemetry. The phase labels follow the scheduled cutoff. The raw latency windows include the final client responses as the workload drained.

### Completed-run baseline and idle comparison

| Mode | Counter | Unit | First ten active samples | Last ten active samples | Last ten idle samples | Idle minus starting baseline | Evidence |
| --- | --- | --- | --- | --- | --- | --- | --- |
| local | rss_bytes | MiB | 675.51 | 547.66 | 507.89 | -167.62 | [Raw JSON](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local/result.json) · [Run 45557039053](https://github.com/unikdahal/poc/actions/runs/45557039053) |
| local | cgroup_bytes | MiB | 593.33 | 449.60 | 398.08 | -195.24 | [Raw JSON](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local/result.json) · [Run 45557039053](https://github.com/unikdahal/poc/actions/runs/45557039053) |
| local | fd_count | count | 73.00 | 497.50 | 14.00 | -59.00 | [Raw JSON](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local/result.json) · [Run 45557039053](https://github.com/unikdahal/poc/actions/runs/45557039053) |
| local | threads | count | 19.00 | 10.00 | 10.00 | -9.00 | [Raw JSON](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local/result.json) · [Run 45557039053](https://github.com/unikdahal/poc/actions/runs/45557039053) |
| local | local_disk_used_bytes | MiB | 45861.17 | 59725.89 | 60458.82 | 14597.65 | [Raw JSON](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local/result.json) · [Run 45557039053](https://github.com/unikdahal/poc/actions/runs/45557039053) |
| local | workload_s3_objects | count | 343.00 | 3064.00 | 3073.00 | 2730.00 | [Raw JSON](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local/result.json) · [Run 45557039053](https://github.com/unikdahal/poc/actions/runs/45557039053) |
| local-cluster | rss_bytes | MiB | 1158.61 | 9621.94 | 11217.77 | 10059.15 | [Raw JSON](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local-cluster/result.json) · [Run 45557039053](https://github.com/unikdahal/poc/actions/runs/45557039053) |
| local-cluster | cgroup_bytes | MiB | 1191.07 | 11181.82 | 11255.28 | 10064.21 | [Raw JSON](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local-cluster/result.json) · [Run 45557039053](https://github.com/unikdahal/poc/actions/runs/45557039053) |
| local-cluster | fd_count | count | 65.00 | 65.50 | 17.00 | -48.00 | [Raw JSON](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local-cluster/result.json) · [Run 45557039053](https://github.com/unikdahal/poc/actions/runs/45557039053) |
| local-cluster | threads | count | 49.00 | 49.00 | 10.00 | -39.00 | [Raw JSON](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local-cluster/result.json) · [Run 45557039053](https://github.com/unikdahal/poc/actions/runs/45557039053) |
| local-cluster | local_disk_used_bytes | MiB | 45857.83 | 46249.35 | 46255.60 | 397.77 | [Raw JSON](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local-cluster/result.json) · [Run 45557039053](https://github.com/unikdahal/poc/actions/runs/45557039053) |
| local-cluster | workload_s3_objects | count | 205.00 | 205.00 | 205.00 | 0.00 | [Raw JSON](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local-cluster/result.json) · [Run 45557039053](https://github.com/unikdahal/poc/actions/runs/45557039053) |

Each entry summarizes the median of the displayed time window. The idle difference reports how closely the counter returned to the starting baseline.

| Mode | Counter | Rate MiB/h | 95% interval or fit limit | Evidence |
| --- | --- | --- | --- | --- |
| local | rss_bytes | 27.990 | 19.618, 37.731 | [Raw JSON](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local/result.json) · [Run 45557039053](https://github.com/unikdahal/poc/actions/runs/45557039053) |
| local | cgroup_bytes | 23.363 | 17.027, 29.827 | [Raw JSON](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local/result.json) · [Run 45557039053](https://github.com/unikdahal/poc/actions/runs/45557039053) |
| local-cluster | rss_bytes | 1774.564 | 1070.168, 2335.497 | [Raw JSON](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local-cluster/result.json) · [Run 45557039053](https://github.com/unikdahal/poc/actions/runs/45557039053) |
| local-cluster | cgroup_bytes | 2082.089 | 1396.450, 2671.499 | [Raw JSON](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local-cluster/result.json) · [Run 45557039053](https://github.com/unikdahal/poc/actions/runs/45557039053) |

**FACT.** Local RSS ended 167.62 MiB below the first five-minute median. Local-cluster RSS ended 10,059.15 MiB above its starting median. Its cgroup memory ended at 11,255.28 MiB against an 11,264 MiB limit. Threads and file descriptors fell in both modes after the workload stopped. The rates describe active-workload drift over one run per mode. These measurements leave the underlying heap allocations unclassified. [Local raw JSON](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local/result.json) · [Local-cluster raw JSON](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local-cluster/result.json)

### Observed latency windows

| Mode | Workload | Validated windows | Median window p50, ms | Median window p99, ms | Evidence |
| --- | --- | --- | --- | --- | --- |
| local | merge | 164 | 11670.25 | 11686.70 | [Raw JSON](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local/result.json) · [Run 45557039053](https://github.com/unikdahal/poc/actions/runs/45557039053) |
| local | point | 412 | 12.44 | 16.24 | [Raw JSON](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local/result.json) · [Run 45557039053](https://github.com/unikdahal/poc/actions/runs/45557039053) |
| local | session_churn | 610 | 37.80 | 70.22 | [Raw JSON](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local/result.json) · [Run 45557039053](https://github.com/unikdahal/poc/actions/runs/45557039053) |
| local | tpch | 412 | 202.87 | 280.08 | [Raw JSON](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local/result.json) · [Run 45557039053](https://github.com/unikdahal/poc/actions/runs/45557039053) |
| local-cluster | merge | 4 | 377.66 | 521.44 | [Raw JSON](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local-cluster/result.json) · [Run 45557039053](https://github.com/unikdahal/poc/actions/runs/45557039053) |
| local-cluster | point | 610 | 17.58 | 28.26 | [Raw JSON](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local-cluster/result.json) · [Run 45557039053](https://github.com/unikdahal/poc/actions/runs/45557039053) |
| local-cluster | session_churn | 611 | 28.12 | 192.24 | [Raw JSON](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local-cluster/result.json) · [Run 45557039053](https://github.com/unikdahal/poc/actions/runs/45557039053) |
| local-cluster | tpch | 610 | 304.03 | 462.12 | [Raw JSON](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local-cluster/result.json) · [Run 45557039053](https://github.com/unikdahal/poc/actions/runs/45557039053) |

The median across window percentiles describes a typical window. The raw data retains individual window counts and percentiles.

### Session churn and container exit

| Mode | Active samples | Idle samples | Validated churn | Failed churn | Churn client exit code | Sail OOMKilled | Sail exit code | Evidence |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| local | 610 | 51 | 14638 | 0 | 0 | False | 137 | [Raw JSON](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local/result.json) · [Run 45557039053](https://github.com/unikdahal/poc/actions/runs/45557039053) |
| local-cluster | 610 | 51 | 14638 | 0 | 0 | False | 137 | [Raw JSON](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local-cluster/result.json) · [Run 45557039053](https://github.com/unikdahal/poc/actions/runs/45557039053) |

Both containers were still running when the harness started teardown. Docker recorded exit code 137 after `docker stop -t 8` and `OOMKilled=false` for both. [Local Docker state](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local/result.json) · [Local-cluster Docker state](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local-cluster/result.json)

### MERGE failures

**FACT.** Local verified 337 of 565 MERGEs. The remaining attempts included 56 MERGE timeouts, 169 acknowledged MERGEs whose verification read timed out, and three readbacks that advanced by two where a single increment was expected. Earlier verification timeouts leave the individual commits behind those three jumps unattributed. Workload S3 objects increased from a starting median of 343 to an idle median of 3,073. Public searches found no exact issue or PR for the verification timeout pattern. [Local raw JSON](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local/result.json)

**FACT.** Local-cluster verified 32 of 6,646 MERGEs. The first 300 retained failures all reported `task region requires 33 worker task slots, but the configured maximum is 32`, and their readbacks stayed at 32. The scheduler rejects a region whose number of worker tasks exceeds the configured worker count times slots per worker. The same guard exists on the newer SHA. Public searches found no exact issue or PR for this Iceberg workload. [Local-cluster raw JSON](https://github.com/unikdahal/poc/blob/d3b242e88e84c695dfebea2b3dc3c0cc49c5f6bf/local-cluster/result.json) · [Pinned slot check](https://github.com/lakehq/sail/blob/790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f/crates/sail-execution/src/driver/task_assigner/core.rs#L95-L111)

### Session retention in the pinned code

**FACT from code.** Release and idle expiry replace the running state with `Deleted` and remove its driver registration. The server keeps the client-to-server ID mapping and the session entry so it can reject reuse. The same maps and deletion paths are present on the newer revision tested for correctness. [Registry insert](https://github.com/lakehq/sail/blob/790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f/crates/sail-session/src/session_manager/actor/mod.rs#L20-L38) · [Idle release](https://github.com/lakehq/sail/blob/790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f/crates/sail-session/src/session_manager/actor/handler.rs#L287-L310) · [Explicit release](https://github.com/lakehq/sail/blob/790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f/crates/sail-session/src/session_manager/actor/handler.rs#L315-L348)

**HYPOTHESIS.** Retained session IDs can contribute to process memory after churn. Allocator retention, catalog state and execution history are other possible contributors. The runs did not profile heap ownership or isolate churn from the mixed workload. Upstream clone-session design discussion already calls out bounding deleted session IDs for long-lived servers. [Upstream design discussion](https://github.com/lakehq/sail/issues/2512)
