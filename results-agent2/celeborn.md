# SF1 Celeborn shuffle

Command `/opt/hostedtoolcache/Python/3.11.18/x64/bin/python -m harness.agent2.celeborn`.

Workflow source revision `c25b0d80b191c0199e4a005648b25dfd1ba535d4`. Sail image revision `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`.

Recorded versions include spark 3.5.10, iceberg 1.12.0, comet 1.2.0, python 3.11.18, adbc 1.13.0.

The raw JSON records engine settings, runner details and individual attempts.

All timings use paired queries that matched Spark on both the treatment and the local-cluster Flight shuffle control. CPU and sampled peak memory include the Celeborn processes where applicable.

| Treatment | Workers | Codec | Split bytes | Paired queries | Warm wall s | Warm CPU s | Peak MiB | Wall / Flight | Stage records |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| local | 0 | — | — | 18 | 5.068 | 8.823 | 699.9 | 0.917 | 0 |
| cluster-flight | 0 | — | — | 18 | 5.525 | 13.287 | 1,126.6 | 1.000 | 49 |
| celeborn-0.6.3-default | 1 | lz4 | 1073741824 | 18 | 42.946 | 21.056 | 1,842.0 | 7.773 | 49 |
| celeborn-0.6.3-uncompressed | 1 | none | 1073741824 | 18 | 40.128 | 19.541 | 2,606.9 | 7.263 | 49 |
| celeborn-0.6.3-small-split | 1 | lz4 | 16777216 | 18 | 42.764 | 20.904 | 1,893.5 | 7.740 | 49 |
| celeborn-0.6.3-two-workers | 2 | lz4 | 1073741824 | 18 | 39.469 | 23.348 | 2,364.0 | 7.144 | 49 |
| celeborn-0.7.0-default | 1 | lz4 | 1073741824 | 18 | 42.657 | 20.287 | 2,051.0 | 7.721 | 49 |

Q1, Q8, Q14 and Q17 differed from Spark and were excluded. Across the
remaining 18 queries, the default 0.6.3 run used 42.946 s wall time
and 21.056 CPU-seconds, compared with 5.525 s wall and 13.287
CPU-seconds for Flight. An initial run with less instrumentation measured
9.08× the Flight control. [Initial matched timings](https://github.com/unikdahal/poc/blob/68f208d1cfa9ca0073fc83dbe363dfb512758e67/agent2-celeborn/celeborn.json)

| Query | Flight blocking / total stages | Celeborn blocking / total stages | Flight longest stage s | Celeborn longest stage s |
| --- | --- | --- | --- | --- |
| Q5 | 0 / 12 | 6 / 12 | 0.436 | 1.479 |
| Q9 | 0 / 13 | 7 / 13 | 0.359 | 2.446 |
| Q18 | 0 / 11 | 5 / 11 | 0.503 | 1.042 |
| Q21 | 0 / 13 | 7 / 13 | 0.460 | 2.861 |

The stage table uses first task creation to last task stop in the final
warm sample. The planner pipelines Flight shuffle output. For Celeborn
it makes shuffle output blocking. Producers serialize and push batches,
then signal mapper completion before downstream consumption. This reduces
stage overlap and extends the critical path. Compression, a smaller split
threshold, an extra worker and version 0.7.0 all retained those blocking
stages. [Stage planner](https://github.com/lakehq/sail/blob/790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f/crates/sail-execution/src/job_graph/planner.rs#L1213) ·
[Shuffle writer](https://github.com/lakehq/sail/blob/790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f/crates/sail-execution/src/stream/celeborn/core.rs#L182)

| Default 0.6.3 worker timer | P50 ms | P95 ms |
| --- | --- | --- |
| Commit files | 5.95 | 9.31 |
| Fetch chunk | 0.39 | 4.45 |
| Flush data | 0.12 | 0.18 |
| Primary push | 0.01 | 0.05 |

The worker snapshot shows short individual operations relative to the
stage durations. These are worker-side timers. Per-operation network
waits and the individual costs of serialization, scheduling and I/O
remain unverified. [Worker metrics](https://github.com/unikdahal/poc/blob/de93cb5a934a7492df09e859934700fc2b266628/agent2-celeborn/celeborn-0.6.3-default-celeborn-worker-1-after.prom)

The worker was killed 0.501 seconds into Q9 and exited with code 137.
Q9 failed with `Temporary failure in name resolution`. A subsequent
`SELECT 1` succeeded. Recovery of a distributed query after worker loss
was outside the probe.

The info-level logs and worker metric snapshots are linked with the raw results. The stage records include planner timestamps and task start-to-stop proxies for Q5, Q9, Q18 and Q21. The comparison varies one setting at a time. Untested interactions and traces without stage evidence remain unverified.

[Raw JSON](https://github.com/unikdahal/poc/blob/de93cb5a934a7492df09e859934700fc2b266628/agent2-celeborn/celeborn.json) · [Result files](https://github.com/unikdahal/poc/tree/de93cb5a934a7492df09e859934700fc2b266628/agent2-celeborn) · [Workflow run](https://github.com/unikdahal/poc/actions/runs/45494813690)
