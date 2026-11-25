# Long-running workload and session churn

Reproduce with `p3.yml`, `tasks=soak`. The suite command is `python -m harness.p3.soak` with `P3_VARIANT=local` or `local-cluster`.
The pinned Sail commit is `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`. The correctness reference is Spark 3.5.10 with Iceberg 1.12.0. Each raw file records the job revision, image digests, configuration and engine exit state where available.

The target schedule is 5 h 05 min of active workload and 25 min idle. Sampling is every 30 s. The mix uses six SF1 TPC-H queries, point lookups, small MERGEs and newly created sessions. The revised churn client sends ReleaseSession independently of the workload client. [Pinned release handler](https://github.com/lakehq/sail/blob/790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f/crates/sail-spark-connect/src/server.rs#L505)

### Timed windows and run coverage

| Mode | Observed seconds | Samples | Matched / attempted | RSS first / last active / last idle MiB | RSS MiB/h [95% interval] | Evidence |
| --- | --- | --- | --- | --- | --- | --- |
| local | 390.0 | 14 | merge 68/94 · point 1127/1128 · session_churn 313/313 · tpch 275/282 | 177.1 / 786.1 / — | — | [Raw JSON](https://github.com/unikdahal/poc/blob/d829d4dc5a1ec93594dcec6df0327727525fdf38/local/result.json) · [Run 45553300746](https://github.com/unikdahal/poc/actions/runs/45553300746) |
| local | — | 0 | — | — / — / — | — | [Raw JSON](https://github.com/unikdahal/poc/blob/8d1a070faa0bfe654b5922f7ded0f4dd7d65ce49/local/result.json) · [Run 45555344538](https://github.com/unikdahal/poc/actions/runs/45555344538) |
| local-cluster | 540.0 | 19 | merge 8/277 · point 3315/3320 · session_churn 433/433 · tpch 815/830 | 192.9 / 1415.0 / — | — | [Raw JSON](https://github.com/unikdahal/poc/blob/d829d4dc5a1ec93594dcec6df0327727525fdf38/local-cluster/result.json) · [Run 45553300746](https://github.com/unikdahal/poc/actions/runs/45553300746) |
| local-cluster | — | 0 | — | — / — / — | — | [Raw JSON](https://github.com/unikdahal/poc/blob/8d1a070faa0bfe654b5922f7ded0f4dd7d65ce49/local-cluster/result.json) · [Run 45555344538](https://github.com/unikdahal/poc/actions/runs/45555344538) |

**FACT.** The first jobs produced short active windows and ended before the idle phase. They captured `Pool not running` mixed-client errors and a local-cluster slot limit failure, `task region requires 9 worker task slots, but the configured maximum is 8`. The repeat with the isolated churn client initially failed resolving Spark dependencies. [First local](https://github.com/unikdahal/poc/blob/d829d4dc5a1ec93594dcec6df0327727525fdf38/local/result.json) · [First cluster](https://github.com/unikdahal/poc/blob/d829d4dc5a1ec93594dcec6df0327727525fdf38/local-cluster/result.json)

RSS, cgroup memory, open file descriptors, thread count, local disk usage, S3 object counts and correctness-gated latency windows are in the raw JSON. The fit uses adjacent 30-minute block medians and a block bootstrap. This estimates trend over the actual observed window. The first two short windows cannot support a leak rate or a post-idle conclusion.

Tokio task counts were unavailable from the container process telemetry. Repeated sessions were used only for the churn test and each independently checked the known Spark literal result.

The full-duration repeat is scheduled separately for both execution modes. [Hosted run](https://github.com/unikdahal/poc/actions/runs/45557039053)
