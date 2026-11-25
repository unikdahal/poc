# CPU scaling at TPC-H SF10

Reproduce with `p3.yml`, `tasks=cpu-scaling`. The suite command is `python -u -m harness.p3.cpu_scaling` with two independent hosted runners, each sweeping 1, 2, 3 and 4 cores.
The pinned Sail commit is `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`. The correctness reference is Spark 3.5.10 with Iceberg 1.12.0. Each raw file records the job revision, image digests, configuration and engine exit state where available.

Sail uses a CPU quota and matching default parallelism. Comet runs with a matching Spark master core count. Results compare query sets that match Spark at every core count for that engine and runner. CPU utilization divides CPU-seconds by elapsed time and the requested CPU count.

### Paired totals

| Runner | Engine | Cores | Paired queries | Wall s | CPU s | Speedup | Efficiency | CPU utilization | Evidence |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| runner-1 | — | — | — | — | — | — | — | — | [Run](https://github.com/unikdahal/poc/actions/runs/45553585916) |
| runner-2 | — | — | — | — | — | — | — | — | [Run](https://github.com/unikdahal/poc/actions/runs/45553585916) |

### Coverage limits

Per-query runs use local execution on the hosted CPU model. CPU-seconds are reported by the Sail cgroup and the Spark JVM process tree. These changes are core allocations within a four-vCPU runner. The experiment does not measure scaling beyond four cores.
