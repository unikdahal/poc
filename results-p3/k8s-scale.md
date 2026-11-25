# Multi-node execution on kind

Reproduce with `p3.yml`, `tasks=k8s-scale`. The suite command is `python -m harness.p3.k8s_scale` with one variant per hosted job. The kind cluster uses a control plane and three worker nodes.
The pinned Sail commit is `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`. The correctness reference is Spark 3.5.10 with Iceberg 1.12.0. Each raw file records the job revision, image digests, configuration and engine exit state where available.

The normal placement requested 250m CPU and 256 MiB for the server and capped it at one CPU and 3 GiB. Workers normally have one CPU and 2 GiB limits. Failure treatments include a separate 40 MiB worker cap. The burst uses a 250m CPU and 512 MiB worker limit.

### TPC-H SF10

Warm wall and first-row figures require two valid warm samples and a matching first execution. The totals use the intersection of all measured worker counts in each shuffle backend. Scaling efficiency divides speedup by the configured worker count. A hosted worker count is a logical allocation on the same physical runner.

| Shuffle and workers | Queries timed | Paired queries | Paired wall, s | Speedup | Efficiency | Error / timeout / mismatch attempts / final worker OOM | Evidence |
| --- | --- | --- | --- | --- | --- | --- | --- |
| flight-1 | 6 | 6 | 14.635 | 1.000 | 1.000 | 41 / 0 / 6 / 1 | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/flight-1/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| flight-2 | 14 | 6 | 13.849 | 1.057 | 0.528 | 15 / 0 / 9 / 2 | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/flight-2/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| flight-4 | 19 | 6 | 12.398 | 1.180 | 0.295 | 0 / 0 / 9 / 0 | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/flight-4/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| storage-1 | 17 | 17 | 97.254 | 1.000 | 1.000 | 0 / 2 / 9 / 6 | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/storage-1/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| storage-2 | 19 | 17 | 64.753 | 1.502 | 0.751 | 0 / 0 / 9 / 0 | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/storage-2/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| storage-4 | 19 | 17 | 57.635 | 1.687 | 0.422 | 0 / 0 / 9 / 0 | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/storage-4/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |

Correctness-gated warm medians per query in seconds. Each populated cell is wall / first row.

| Query | flight-1 | flight-2 | flight-4 | storage-1 | storage-2 | storage-4 |
| --- | --- | --- | --- | --- | --- | --- |
| Q1 | — | — | — | — | — | — |
| Q2 | 0.769 / 0.769 | 0.794 / 0.793 | 0.643 / 0.642 | 1.085 / 1.084 | 0.823 / 0.822 | 0.831 / 0.830 |
| Q3 | 2.940 / 2.940 | 2.988 / 2.987 | 3.117 / 3.116 | 6.858 / 6.857 | 4.478 / 4.477 | 4.453 / 4.453 |
| Q4 | 1.593 / 1.592 | 1.216 / 1.216 | 1.013 / 1.012 | 2.412 / 2.411 | 1.587 / 1.586 | 1.315 / 1.313 |
| Q5 | 4.285 / 4.284 | 5.220 / 5.219 | 4.652 / 4.652 | 12.812 / 12.811 | 8.881 / 8.880 | 7.946 / 7.945 |
| Q6 | 1.754 / 1.753 | 1.116 / 1.115 | 0.831 / 0.831 | 1.774 / 1.773 | 1.031 / 1.031 | 0.899 / 0.899 |
| Q7 | 3.295 / 3.294 | 2.516 / 2.515 | 2.142 / 2.141 | 5.019 / 5.018 | 3.074 / 3.073 | 2.785 / 2.784 |
| Q8 | — | — | — | — | — | — |
| Q9 | — | 9.371 / 9.371 | 8.437 / 8.436 | — | 16.055 / 16.054 | 14.242 / 14.241 |
| Q10 | — | 2.925 / 2.925 | 2.444 / 2.444 | 6.034 / 6.033 | 4.014 / 4.013 | 3.476 / 3.474 |
| Q11 | — | 0.377 / — | 0.330 / — | 0.600 / — | 0.404 / — | 0.362 / — |
| Q12 | — | 2.404 / 2.404 | 1.672 / 1.671 | 4.044 / 4.043 | 2.533 / 2.531 | 2.143 / 2.142 |
| Q13 | — | 2.360 / 2.359 | 1.818 / 1.818 | 3.858 / 3.857 | 2.453 / 2.451 | 2.172 / 2.170 |
| Q14 | — | — | — | — | — | — |
| Q15 | — | 2.457 / 2.448 | 2.068 / 2.058 | 4.159 / 4.146 | 2.498 / 2.485 | 2.238 / 2.226 |
| Q16 | — | 0.900 / 0.834 | 0.840 / 0.785 | 1.584 / 1.527 | 1.335 / 1.274 | 1.216 / 1.159 |
| Q17 | — | 8.247 / 8.246 | 6.721 / 6.721 | 18.114 / 18.113 | 12.821 / 12.820 | 11.435 / 11.434 |
| Q18 | — | — | 7.040 / 7.039 | 18.149 / 18.148 | 12.280 / 12.279 | 10.673 / 10.672 |
| Q19 | — | — | 1.969 / 1.968 | 4.476 / 4.476 | 2.519 / 2.518 | 2.115 / 2.114 |
| Q20 | — | — | 2.024 / 2.023 | 4.780 / 4.780 | 3.023 / 3.022 | 2.697 / 2.695 |
| Q21 | — | — | 6.354 / 6.353 | — | 11.534 / 11.532 | 9.090 / 9.089 |
| Q22 | — | — | 0.603 / 0.602 | 1.494 / 1.492 | 0.999 / 0.998 | 0.880 / 0.879 |

### Worker lifecycle and failure matrix

| Fault or pattern | Trial | Injection | Rows match | Observed elapsed s | Client observation | Evidence |
| --- | --- | --- | --- | --- | --- | --- |
| map-kill | 1 | ok, stage 4 | False | 6.716 | SparkRuntimeException: Connection refused (os error 111) | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/map-kill/result.json) · [Run 45558181996](https://github.com/unikdahal/poc/actions/runs/45558181996) |
| map-kill | 2 | ok, stage 2 | False | 121.433 | SparkRuntimeException: h2 protocol error: error reading a body from connection | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/map-kill/result.json) · [Run 45558181996](https://github.com/unikdahal/poc/actions/runs/45558181996) |
| map-kill | 3 | ok, stage 4 | False | 5.266 | SparkRuntimeException: Connection refused (os error 111) | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/map-kill/result.json) · [Run 45558181996](https://github.com/unikdahal/poc/actions/runs/45558181996) |
| reduce-kill | 1 | ok, stage 5 | False | 6.148 | SparkRuntimeException: Connection refused (os error 111) | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/reduce-kill/result.json) · [Run 45560553970](https://github.com/unikdahal/poc/actions/runs/45560553970) |
| reduce-kill | 2 | ok, stage 5 | False | 5.414 | SparkRuntimeException: h2 protocol error: error reading a body from connection | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/reduce-kill/result.json) · [Run 45560553970](https://github.com/unikdahal/poc/actions/runs/45560553970) |
| reduce-kill | 3 | ok, stage 5 | False | 5.218 | SparkRuntimeException: Connection refused (os error 111) | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/reduce-kill/result.json) · [Run 45560553970](https://github.com/unikdahal/poc/actions/runs/45560553970) |
| drain | 1 | ok | True | 10.851 | ok | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/drain/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| drain | 2 | ok | True | 9.047 | ok | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/drain/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| drain | 3 | ok | True | 10.716 | ok | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/drain/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| netem | 1 | ok | False | 150.008 | TimeoutError: 150s deadline, client active=False | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/netem/result.json) · [Run 45560024043](https://github.com/unikdahal/poc/actions/runs/45560024043) |
| netem | 2 | ok | False | 150.008 | TimeoutError: 150s deadline, client active=False | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/netem/result.json) · [Run 45560024043](https://github.com/unikdahal/poc/actions/runs/45560024043) |
| netem | 3 | ok | False | 150.007 | TimeoutError: 150s deadline, client active=False | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/netem/result.json) · [Run 45560024043](https://github.com/unikdahal/poc/actions/runs/45560024043) |
| pod-oom | 1 | configured | False | 0.231 | SparkRuntimeException: stream closed because of a broken pipe | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/pod-oom/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| pod-oom | 2 | configured | False | 120.528 | AnalysisException: task failed for lost worker | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/pod-oom/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| pod-oom | 3 | configured | False | 0.183 | SparkRuntimeException: connection reset | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/pod-oom/result.json) · [Run 45556141965](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| driver-restart | 1 | ok | False | 153.002 | TimeoutError: 150s deadline, client active=True | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/driver-restart/result.json) · [Run 45557689391](https://github.com/unikdahal/poc/actions/runs/45557689391) |
| driver-restart | 2 | ok | False | 153.001 | TimeoutError: 150s deadline, client active=True | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/driver-restart/result.json) · [Run 45557689391](https://github.com/unikdahal/poc/actions/runs/45557689391) |
| driver-restart | 3 | ok | False | 153.001 | TimeoutError: 150s deadline, client active=True | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/driver-restart/result.json) · [Run 45557689391](https://github.com/unikdahal/poc/actions/runs/45557689391) |
| burst | 1 workload | Seed matched=True, reaped=True | 20/20 | — | p50 21.498 s, p95 22.505 s | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/burst/result.json) · [Run 45558533058](https://github.com/unikdahal/poc/actions/runs/45558533058) |

### Worker startup during cold Q6

| Treatment | Trials | Spark matches | Median wall seconds | Time through last observed pod start / wall | Evidence |
| --- | --- | --- | --- | --- | --- |
| startup-flight-1 | 3 | 3 | 3.045 | 0.502 | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/startup/result.json) · [Run 45562553646](https://github.com/unikdahal/poc/actions/runs/45562553646) |
| startup-flight-2 | 3 | 3 | 3.184 | 0.577 | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/startup/result.json) · [Run 45562553646](https://github.com/unikdahal/poc/actions/runs/45562553646) |
| startup-flight-4 | 3 | 3 | 3.337 | 0.641 | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/startup/result.json) · [Run 45562553646](https://github.com/unikdahal/poc/actions/runs/45562553646) |
| startup-storage-1 | 3 | 3 | 3.066 | 0.615 | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/startup/result.json) · [Run 45562553646](https://github.com/unikdahal/poc/actions/runs/45562553646) |
| startup-storage-2 | 3 | 3 | 3.172 | 0.695 | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/startup/result.json) · [Run 45562553646](https://github.com/unikdahal/poc/actions/runs/45562553646) |
| startup-storage-4 | 3 | 3 | 3.354 | 0.676 | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/startup/result.json) · [Run 45562553646](https://github.com/unikdahal/poc/actions/runs/45562553646) |

### Cross-node worker traffic during Q9

| Backend | Workers | Median egress bytes | Median egress packets | Retained S3 shuffle delta bytes | Evidence |
| --- | --- | --- | --- | --- | --- |
| flight | 2 | 489866719.5 | 338680.0 | — | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/shuffle-traffic/result.json) · [Run 45561940195](https://github.com/unikdahal/poc/actions/runs/45561940195) |
| storage | 2 | 2775658241.5 | 1902176.5 | 0.0 | [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/shuffle-traffic/result.json) · [Run 45561940195](https://github.com/unikdahal/poc/actions/runs/45561940195) |

**FACT.** The bidirectional queue counters counted traffic between the two worker pod IPs. Idle controls counted zero bytes. These are wire counters including protocol overhead. The retained S3 delta was zero in this fixture, which leaves total S3 bytes written unmeasured.

**FACT.** The burst had 20 correct responses. The final observation contained 0 Succeeded pods, with cleanup checkpoints [(70, 20), (130, 0), (190, 0)]. [Raw JSON](https://github.com/unikdahal/poc/blob/43980b533ca7ff7853c8f6f7b36584ecb78f8d39/burst/result.json) · [Run 45558533058](https://github.com/unikdahal/poc/actions/runs/45558533058)

**FACT.** The first two-worker Flight job recorded a worker in `OOMKilled` with exit code 137. After that worker stopped, Q18–Q22 returned `Connection refused (os error 111)`. That job opened a separate observer session and created a second worker pool, which made the configured count unsuitable as a physical cluster capacity result. [Initial raw JSON](https://github.com/unikdahal/poc/blob/dc89e55895f3fb0d930ad27d7ac63d8ca9d19dac/flight-2/result.json)

**HYPOTHESIS.** The worker loss may have left the old worker address eligible for query dispatch until heartbeat expiry. The fault trials test client outcomes explicitly. The worker pool schedules expiry checks from the last heartbeat. [Pinned heartbeat code](https://github.com/lakehq/sail/blob/790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f/crates/sail-execution/src/driver/worker_pool/core.rs#L567-L583) · [Related normal worker cleanup report #2391](https://github.com/lakehq/sail/issues/2391)

### Coverage limits

Stage creation and stop timestamps are lifecycle intervals. They can overlap and include scheduling time. The raw JSON retains stage inputs and placement when those columns were available. Kubernetes container states expose OOMKilled and exit status through the runtime's pod status. Docker inspect records the kind nodes. Storage samples count retained shuffle objects at the end of a query. The network counters cover cross-node worker paths, and the stage tables expose lifecycle intervals and stage edges. Exact shuffle payload bytes for each stage were unavailable.
