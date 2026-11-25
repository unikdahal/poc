# CPU scaling at TPC-H SF10

Reproduce with `p3.yml`, `tasks=cpu-scaling`. The suite command is `python -m harness.p3.cpu_scaling` with two independent hosted runners, each sweeping 1, 2, 3 and 4 cores.
The pinned Sail commit is `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`. The correctness reference is Spark 3.5.10 with Iceberg 1.12.0. Each raw file records the job revision, image digests, configuration and engine exit state where available.

Sail uses a CPU quota and matching default parallelism. Comet runs with a matching Spark master core count. Results compare query sets that match Spark at every core count for that engine and runner. CPU utilization divides CPU-seconds by elapsed time and the requested CPU count.

### Paired totals

| Runner | Engine | Cores | Paired queries | Wall s | CPU s | Speedup | Efficiency | CPU utilization | Evidence |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| runner-1 | — | — | — | — | — | — | — | — | [Run](https://github.com/unikdahal/poc/actions/runs/45553585916) |
| runner-2 | — | — | — | — | — | — | — | — | [Raw JSON](https://github.com/unikdahal/poc/blob/e56e268140cf137a98f2b5ae600d7cbbf7fd6a13/runner-2/result.json) · [Run 45553585916](https://github.com/unikdahal/poc/actions/runs/45553585916) |

### runner-2

Each number is the correctness-gated warm wall time in seconds. Blank entries had a mismatch, failure, timeout or an incomplete sample set.

| Query | sail-1 | sail-2 | sail-3 | sail-4 | comet-1 | comet-2 | comet-3 | comet-4 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Q1 | — | — | — | — | — | — | — | — |
| Q2 | 1.032 | 0.664 | — | — | — | — | — | — |
| Q3 | 3.189 | 2.112 | — | — | — | — | — | — |
| Q4 | 1.933 | 1.272 | — | — | — | — | — | — |
| Q5 | 4.676 | 3.111 | — | — | — | — | — | — |
| Q6 | 2.259 | 1.307 | — | — | — | — | — | — |
| Q7 | 4.067 | 2.464 | — | — | — | — | — | — |
| Q8 | — | — | — | — | — | — | — | — |
| Q9 | 8.494 | 5.198 | — | — | — | — | — | — |
| Q10 | 4.034 | 2.525 | — | — | — | — | — | — |
| Q11 | 0.787 | 0.504 | — | — | — | — | — | — |
| Q12 | 3.404 | 2.638 | — | — | — | — | — | — |
| Q13 | 4.412 | 2.680 | — | — | — | — | — | — |
| Q14 | — | — | — | — | — | — | — | — |
| Q15 | 4.743 | — | — | — | — | — | — | — |
| Q16 | 0.820 | — | — | — | — | — | — | — |
| Q17 | 11.309 | — | — | — | — | — | — | — |
| Q18 | 9.438 | — | — | — | — | — | — | — |
| Q19 | 5.168 | — | — | — | — | — | — | — |
| Q20 | 4.137 | — | — | — | — | — | — | — |
| Q21 | 9.425 | — | — | — | — | — | — | — |
| Q22 | 0.915 | — | — | — | — | — | — | — |

Attempt counts by status. `sail-1` {'mismatch': 9, 'ok': 57} · `sail-2` {'mismatch': 6, 'ok': 33, 'timeout': 1}

[Raw JSON](https://github.com/unikdahal/poc/blob/e56e268140cf137a98f2b5ae600d7cbbf7fd6a13/runner-2/result.json) · [Run 45553585916](https://github.com/unikdahal/poc/actions/runs/45553585916)

### Coverage limits

Per-query runs use local execution on the hosted CPU model. CPU-seconds are reported by the Sail cgroup and the Spark JVM process tree. These changes are core allocations within a four-vCPU runner. The experiment does not measure scaling beyond four cores.
