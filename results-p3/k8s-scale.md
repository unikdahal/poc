# Multi-node execution on kind

Reproduce with `p3.yml`, `tasks=k8s-scale`. The suite command is `python -u -m harness.p3.k8s_scale` with one variant per hosted job. The kind cluster uses a control plane and three worker nodes.
The pinned Sail commit is `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`. The correctness reference is Spark 3.5.10 with Iceberg 1.12.0. Each raw file records the job revision, image digests, configuration and engine exit state where available.

The normal placement requested 250m CPU and 256 MiB for the server and capped it at one CPU and 3 GiB. Workers normally have one CPU and 2 GiB limits. Failure treatments include a separate 40 MiB worker cap. The burst uses a 250m CPU and 512 MiB worker limit.

### TPC-H SF10

Warm wall and first-row figures require two valid warm samples and a matching first execution. The totals use the intersection of all measured worker counts in each shuffle backend. Scaling efficiency divides speedup by the configured worker count. A hosted worker count is a logical allocation on the same physical runner.

| Shuffle and workers | Queries timed | Paired queries | Paired wall, s | Speedup | Efficiency | Error / timeout / mismatch attempts / final worker OOM | Evidence |
| --- | --- | --- | --- | --- | --- | --- | --- |
| flight-1 | 8 | 8 | 27.804 | 1.000 | 1.000 | 36 / 0 / 6 / 3 | [Raw JSON](https://github.com/unikdahal/poc/blob/dc89e55895f3fb0d930ad27d7ac63d8ca9d19dac/flight-1/result.json) · [Run 45553201626](https://github.com/unikdahal/poc/actions/runs/45553201626) |
| flight-2 | 14 | 8 | 16.952 | 1.640 | 0.820 | 15 / 0 / 9 / 1 | [Raw JSON](https://github.com/unikdahal/poc/blob/dc89e55895f3fb0d930ad27d7ac63d8ca9d19dac/flight-2/result.json) · [Run 45553201626](https://github.com/unikdahal/poc/actions/runs/45553201626) |
| flight-4 | 19 | 8 | 22.029 | 1.262 | 0.316 | 0 / 0 / 9 / 0 | [Raw JSON](https://github.com/unikdahal/poc/blob/dc89e55895f3fb0d930ad27d7ac63d8ca9d19dac/flight-4/result.json) · [Run 45553201626](https://github.com/unikdahal/poc/actions/runs/45553201626) |
| storage-1 | 17 | 0 | — | — | — | 0 / 2 / 9 / 7 | [Raw JSON](https://github.com/unikdahal/poc/blob/dc89e55895f3fb0d930ad27d7ac63d8ca9d19dac/storage-1/result.json) · [Run 45553201626](https://github.com/unikdahal/poc/actions/runs/45553201626) |
| storage-2 | 0 | 0 | — | — | — | 0 / 0 / 0 / 0 | [Raw JSON](https://github.com/unikdahal/poc/blob/dc89e55895f3fb0d930ad27d7ac63d8ca9d19dac/storage-2/result.json) · [Run 45553201626](https://github.com/unikdahal/poc/actions/runs/45553201626) |
| storage-4 | 19 | 0 | — | — | — | 0 / 0 / 9 / 0 | [Raw JSON](https://github.com/unikdahal/poc/blob/dc89e55895f3fb0d930ad27d7ac63d8ca9d19dac/storage-4/result.json) · [Run 45553201626](https://github.com/unikdahal/poc/actions/runs/45553201626) |

Correctness-gated warm medians per query in seconds. Each populated cell is wall / first row.

| Query | flight-1 | flight-2 | flight-4 | storage-1 | storage-2 | storage-4 |
| --- | --- | --- | --- | --- | --- | --- |
| Q1 | — | — | — | — | — | — |
| Q2 | 1.050 / — | 0.587 / — | 0.578 / — | 0.852 / — | — | 0.711 / — |
| Q3 | 3.788 / — | 2.142 / — | 3.559 / — | 5.208 / — | — | 3.937 / — |
| Q4 | 2.100 / — | 0.958 / — | 1.069 / — | 1.900 / — | — | 1.248 / — |
| Q5 | 5.475 / — | 3.902 / — | 5.086 / — | 9.721 / — | — | 7.250 / — |
| Q6 | 2.344 / — | 0.800 / — | 0.915 / — | 1.351 / — | — | 0.838 / — |
| Q7 | 4.417 / — | 1.933 / — | 2.309 / — | 3.781 / — | — | 2.639 / — |
| Q8 | — | — | — | — | — | — |
| Q9 | — | 7.199 / — | 8.838 / — | — | — | 13.147 / — |
| Q10 | — | 2.216 / — | 2.675 / — | 4.692 / — | — | 3.229 / — |
| Q11 | — | 0.300 / — | 0.339 / — | 0.483 / — | — | 0.382 / — |
| Q12 | — | 1.717 / — | 1.730 / — | 3.190 / — | — | 1.983 / — |
| Q13 | — | 1.666 / — | 2.036 / — | 2.759 / — | — | 2.034 / — |
| Q14 | — | — | — | — | — | — |
| Q15 | — | 1.842 / — | 2.153 / — | 3.255 / — | — | 2.156 / — |
| Q16 | 0.984 / — | 0.685 / — | 0.946 / — | 1.258 / — | — | 1.201 / — |
| Q17 | 7.646 / — | 5.944 / — | 7.566 / — | 13.492 / — | — | 10.297 / — |
| Q18 | — | — | 7.839 / — | 14.638 / — | — | 9.304 / — |
| Q19 | — | — | 2.060 / — | 3.514 / — | — | 2.028 / — |
| Q20 | — | — | 2.207 / — | 3.640 / — | — | 2.536 / — |
| Q21 | — | — | 7.023 / — | — | — | 8.617 / — |
| Q22 | — | — | 0.645 / — | 1.188 / — | — | 0.836 / — |

### Worker lifecycle and failure matrix

| Fault or pattern | Trial | Injection | Rows match | Client observation | Evidence |
| --- | --- | --- | --- | --- | --- |
| map-kill | 0 | pending | — | — | [Run](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| reduce-kill | 0 | pending | — | — | [Run](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| drain | 0 | pending | — | — | [Run](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| netem | 0 | pending | — | — | [Run](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| pod-oom | 0 | pending | — | — | [Run](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| driver-restart | 0 | pending | — | — | [Run](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| burst | 0 | pending | — | — | [Run](https://github.com/unikdahal/poc/actions/runs/45556141965) |

**FACT.** The first two-worker Flight job recorded a worker in `OOMKilled` with exit code 137. After that worker stopped, Q18–Q22 returned `Connection refused (os error 111)`. That job opened a separate observer session and created a second worker pool, which made the configured count unsuitable as a physical cluster capacity result. [Initial raw JSON](https://github.com/unikdahal/poc/blob/dc89e55895f3fb0d930ad27d7ac63d8ca9d19dac/flight-2/result.json)

**HYPOTHESIS.** The worker loss may have left the old worker address eligible for query dispatch until heartbeat expiry. The fault trials test client outcomes explicitly. The worker pool schedules expiry checks from the last heartbeat. [Pinned heartbeat code](https://github.com/lakehq/sail/blob/790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f/crates/sail-execution/src/driver/worker_pool/core.rs#L567-L583)

### Coverage limits

Stage creation and stop timestamps are lifecycle intervals. They can overlap and include scheduling time. The raw JSON retains stage inputs and placement when those columns were available. Kubernetes container states expose OOMKilled and exit status through the runtime's pod status. Docker inspect records the kind nodes. Storage samples count retained shuffle objects at the end of a query. The current Flight instrumentation has no transport-byte measurement. The network treatment filters cross-node worker destinations. Same-node traffic does not pass through that filter.
