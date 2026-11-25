# Multi-node execution on kind

Reproduce with `p3.yml`, `tasks=k8s-scale`. The suite command is `python -m harness.p3.k8s_scale` with one variant per hosted job. The kind cluster uses a control plane and three worker nodes.
The pinned Sail commit is `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`. The correctness reference is Spark 3.5.10 with Iceberg 1.12.0. Each raw file records the job revision, image digests, configuration and engine exit state where available.

The normal placement requested 250m CPU and 256 MiB for the server and capped it at one CPU and 3 GiB. Workers normally have one CPU and 2 GiB limits. Failure treatments include a separate 40 MiB worker cap. The burst uses a 250m CPU and 512 MiB worker limit.

### TPC-H SF10

Warm wall and first-row figures require two valid warm samples and a matching first execution. The totals use the intersection of all measured worker counts in each shuffle backend. Scaling efficiency divides speedup by the configured worker count. A hosted worker count is a logical allocation on the same physical runner.

| Shuffle and workers | Queries timed | Paired queries | Paired wall, s | Speedup | Efficiency | Error / timeout / mismatch attempts / final worker OOM | Evidence |
| --- | --- | --- | --- | --- | --- | --- | --- |
| flight-1 | 6 | 6 | 14.635 | 1.000 | 1.000 | 41 / 0 / 6 / 1 | [Raw JSON](https://github.com/unikdahal/poc/blob/5fb0dd6888b2b225d1d6c57db29cff33fc6024f8/flight-1/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| flight-2 | 14 | 6 | 13.849 | 1.057 | 0.528 | 15 / 0 / 9 / 2 | [Raw JSON](https://github.com/unikdahal/poc/blob/5fb0dd6888b2b225d1d6c57db29cff33fc6024f8/flight-2/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| flight-4 | 19 | 6 | 12.398 | 1.180 | 0.295 | 0 / 0 / 9 / 0 | [Raw JSON](https://github.com/unikdahal/poc/blob/5fb0dd6888b2b225d1d6c57db29cff33fc6024f8/flight-4/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| storage-1 | 17 | 17 | 74.920 | 1.000 | 1.000 | 0 / 2 / 9 / 7 | [Raw JSON](https://github.com/unikdahal/poc/blob/5fb0dd6888b2b225d1d6c57db29cff33fc6024f8/storage-1/result.json) · [Run 45553201626](https://github.com/unikdahal/poc/actions/runs/45553201626) |
| storage-2 | 19 | 17 | 64.753 | 1.157 | 0.579 | 0 / 0 / 9 / 0 | [Raw JSON](https://github.com/unikdahal/poc/blob/5fb0dd6888b2b225d1d6c57db29cff33fc6024f8/storage-2/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| storage-4 | 19 | 17 | 57.635 | 1.300 | 0.325 | 0 / 0 / 9 / 0 | [Raw JSON](https://github.com/unikdahal/poc/blob/5fb0dd6888b2b225d1d6c57db29cff33fc6024f8/storage-4/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |

Correctness-gated warm medians per query in seconds. Each populated cell is wall / first row.

| Query | flight-1 | flight-2 | flight-4 | storage-1 | storage-2 | storage-4 |
| --- | --- | --- | --- | --- | --- | --- |
| Q1 | — | — | — | — | — | — |
| Q2 | 0.769 / 0.769 | 0.794 / 0.793 | 0.643 / 0.642 | 0.852 / — | 0.823 / 0.822 | 0.831 / 0.830 |
| Q3 | 2.940 / 2.940 | 2.988 / 2.987 | 3.117 / 3.116 | 5.208 / — | 4.478 / 4.477 | 4.453 / 4.453 |
| Q4 | 1.593 / 1.592 | 1.216 / 1.216 | 1.013 / 1.012 | 1.900 / — | 1.587 / 1.586 | 1.315 / 1.313 |
| Q5 | 4.285 / 4.284 | 5.220 / 5.219 | 4.652 / 4.652 | 9.721 / — | 8.881 / 8.880 | 7.946 / 7.945 |
| Q6 | 1.754 / 1.753 | 1.116 / 1.115 | 0.831 / 0.831 | 1.351 / — | 1.031 / 1.031 | 0.899 / 0.899 |
| Q7 | 3.295 / 3.294 | 2.516 / 2.515 | 2.142 / 2.141 | 3.781 / — | 3.074 / 3.073 | 2.785 / 2.784 |
| Q8 | — | — | — | — | — | — |
| Q9 | — | 9.371 / 9.371 | 8.437 / 8.436 | — | 16.055 / 16.054 | 14.242 / 14.241 |
| Q10 | — | 2.925 / 2.925 | 2.444 / 2.444 | 4.692 / — | 4.014 / 4.013 | 3.476 / 3.474 |
| Q11 | — | 0.377 / — | 0.330 / — | 0.483 / — | 0.404 / — | 0.362 / — |
| Q12 | — | 2.404 / 2.404 | 1.672 / 1.671 | 3.190 / — | 2.533 / 2.531 | 2.143 / 2.142 |
| Q13 | — | 2.360 / 2.359 | 1.818 / 1.818 | 2.759 / — | 2.453 / 2.451 | 2.172 / 2.170 |
| Q14 | — | — | — | — | — | — |
| Q15 | — | 2.457 / 2.448 | 2.068 / 2.058 | 3.255 / — | 2.498 / 2.485 | 2.238 / 2.226 |
| Q16 | — | 0.900 / 0.834 | 0.840 / 0.785 | 1.258 / — | 1.335 / 1.274 | 1.216 / 1.159 |
| Q17 | — | 8.247 / 8.246 | 6.721 / 6.721 | 13.492 / — | 12.821 / 12.820 | 11.435 / 11.434 |
| Q18 | — | — | 7.040 / 7.039 | 14.638 / — | 12.280 / 12.279 | 10.673 / 10.672 |
| Q19 | — | — | 1.969 / 1.968 | 3.514 / — | 2.519 / 2.518 | 2.115 / 2.114 |
| Q20 | — | — | 2.024 / 2.023 | 3.640 / — | 3.023 / 3.022 | 2.697 / 2.695 |
| Q21 | — | — | 6.354 / 6.353 | — | 11.534 / 11.532 | 9.090 / 9.089 |
| Q22 | — | — | 0.603 / 0.602 | 1.188 / — | 0.999 / 0.998 | 0.880 / 0.879 |

### Worker lifecycle and failure matrix

| Fault or pattern | Trial | Injection | Rows match | Client observation | Evidence |
| --- | --- | --- | --- | --- | --- |
| map-kill | 1 | ok, stage 4 | False | SparkRuntimeException: Connection refused (os error 111) | [Raw JSON](https://github.com/unikdahal/poc/blob/5fb0dd6888b2b225d1d6c57db29cff33fc6024f8/map-kill/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| map-kill | 2 | ok, stage 4 | False | SparkRuntimeException: h2 protocol error: error reading a body from connection | [Raw JSON](https://github.com/unikdahal/poc/blob/5fb0dd6888b2b225d1d6c57db29cff33fc6024f8/map-kill/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| map-kill | 3 | ok, stage 4 | False | SparkRuntimeException: Connection refused (os error 111) | [Raw JSON](https://github.com/unikdahal/poc/blob/5fb0dd6888b2b225d1d6c57db29cff33fc6024f8/map-kill/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| reduce-kill | 1 | not observed | True | ok | [Raw JSON](https://github.com/unikdahal/poc/blob/5fb0dd6888b2b225d1d6c57db29cff33fc6024f8/reduce-kill/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| reduce-kill | 2 | not observed | True | ok | [Raw JSON](https://github.com/unikdahal/poc/blob/5fb0dd6888b2b225d1d6c57db29cff33fc6024f8/reduce-kill/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| reduce-kill | 3 | not observed | True | ok | [Raw JSON](https://github.com/unikdahal/poc/blob/5fb0dd6888b2b225d1d6c57db29cff33fc6024f8/reduce-kill/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| drain | 1 | ok | True | ok | [Raw JSON](https://github.com/unikdahal/poc/blob/5fb0dd6888b2b225d1d6c57db29cff33fc6024f8/drain/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| drain | 2 | ok | True | ok | [Raw JSON](https://github.com/unikdahal/poc/blob/5fb0dd6888b2b225d1d6c57db29cff33fc6024f8/drain/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| drain | 3 | ok | True | ok | [Raw JSON](https://github.com/unikdahal/poc/blob/5fb0dd6888b2b225d1d6c57db29cff33fc6024f8/drain/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| netem | 1 | ok | True | ok | [Raw JSON](https://github.com/unikdahal/poc/blob/5fb0dd6888b2b225d1d6c57db29cff33fc6024f8/netem/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| netem | 2 | ok | False | TimeoutError: 150s deadline, client active=False | [Raw JSON](https://github.com/unikdahal/poc/blob/5fb0dd6888b2b225d1d6c57db29cff33fc6024f8/netem/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| netem | 3 | ok | False | TimeoutError: 150s deadline, client active=False | [Raw JSON](https://github.com/unikdahal/poc/blob/5fb0dd6888b2b225d1d6c57db29cff33fc6024f8/netem/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| pod-oom | 1 | configured | False | SparkRuntimeException: stream closed because of a broken pipe | [Raw JSON](https://github.com/unikdahal/poc/blob/5fb0dd6888b2b225d1d6c57db29cff33fc6024f8/pod-oom/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| pod-oom | 2 | configured | False | AnalysisException: task failed for lost worker | [Raw JSON](https://github.com/unikdahal/poc/blob/5fb0dd6888b2b225d1d6c57db29cff33fc6024f8/pod-oom/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| pod-oom | 3 | configured | False | SparkRuntimeException: connection reset | [Raw JSON](https://github.com/unikdahal/poc/blob/5fb0dd6888b2b225d1d6c57db29cff33fc6024f8/pod-oom/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| driver-restart | 1 | ok | True | ok | [Raw JSON](https://github.com/unikdahal/poc/blob/5fb0dd6888b2b225d1d6c57db29cff33fc6024f8/driver-restart/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| driver-restart | 2 | ok | True | ok | [Raw JSON](https://github.com/unikdahal/poc/blob/5fb0dd6888b2b225d1d6c57db29cff33fc6024f8/driver-restart/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| driver-restart | 3 | ok | True | ok | [Raw JSON](https://github.com/unikdahal/poc/blob/5fb0dd6888b2b225d1d6c57db29cff33fc6024f8/driver-restart/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| burst | 1 workload | Seed matched=True, reaped=True | 20/20 | p50 21.117 s, p95 22.517 s | [Raw JSON](https://github.com/unikdahal/poc/blob/5fb0dd6888b2b225d1d6c57db29cff33fc6024f8/burst/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |

**FACT.** The burst had 20 correct responses. The final observation contained 21 Succeeded pods, with cleanup checkpoints at 70 s only. [Raw JSON](https://github.com/unikdahal/poc/blob/5fb0dd6888b2b225d1d6c57db29cff33fc6024f8/burst/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965)

**FACT.** The first two-worker Flight job recorded a worker in `OOMKilled` with exit code 137. After that worker stopped, Q18–Q22 returned `Connection refused (os error 111)`. That job opened a separate observer session and created a second worker pool, which made the configured count unsuitable as a physical cluster capacity result. [Initial raw JSON](https://github.com/unikdahal/poc/blob/dc89e55895f3fb0d930ad27d7ac63d8ca9d19dac/flight-2/result.json)

**HYPOTHESIS.** The worker loss may have left the old worker address eligible for query dispatch until heartbeat expiry. The fault trials test client outcomes explicitly. The worker pool schedules expiry checks from the last heartbeat. [Pinned heartbeat code](https://github.com/lakehq/sail/blob/790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f/crates/sail-execution/src/driver/worker_pool/core.rs#L567-L583) · [Related normal worker cleanup report #2391](https://github.com/lakehq/sail/issues/2391)

### Coverage limits

Stage creation and stop timestamps are lifecycle intervals. They can overlap and include scheduling time. The raw JSON retains stage inputs and placement when those columns were available. Kubernetes container states expose OOMKilled and exit status through the runtime's pod status. Docker inspect records the kind nodes. Storage samples count retained shuffle objects at the end of a query. The current Flight instrumentation has no transport-byte measurement. The network treatment filters cross-node worker destinations. Same-node traffic does not pass through that filter.
