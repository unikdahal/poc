# SF1 Celeborn shuffle

Command `/opt/hostedtoolcache/Python/3.11.18/x64/bin/python -m harness.agent2.celeborn`.

Workflow source revision `b0039138cd4b1b699c9a905473026d41840d1a36`. Sail image revision `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`.

Recorded versions include spark 3.5.10, iceberg 1.12.0, comet 1.2.0, python 3.11.18, adbc 1.13.0.

The raw JSON records engine settings, runner details and individual attempts.

All timings use paired queries that matched Spark on both the treatment and the local-cluster Flight shuffle control. CPU and sampled peak memory include the Celeborn processes where applicable.

| Treatment | Workers | Codec | Split bytes | Paired queries | Warm wall s | Warm CPU s | Peak MiB | Wall / Flight | Stage records |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| local | 0 | — | — | 18 | 4.186 | 5.578 | 657.8 | 0.914 | 0 |
| cluster-flight | 0 | — | — | 18 | 4.578 | 8.375 | 1,193.2 | 1.000 | 0 |
| celeborn-0.6.3-default | 1 | lz4 | 1073741824 | 18 | 41.570 | 13.639 | 1,888.4 | 9.080 | 0 |
| celeborn-0.6.3-uncompressed | 1 | none | 1073741824 | 18 | 39.266 | 13.936 | 2,749.2 | 8.577 | 0 |
| celeborn-0.6.3-small-split | 1 | lz4 | 16777216 | 18 | 41.263 | 13.378 | 1,880.8 | 9.013 | 0 |
| celeborn-0.6.3-two-workers | 2 | lz4 | 1073741824 | 18 | 39.053 | 14.669 | 2,353.0 | 8.531 | 0 |
| celeborn-0.7.0-default | 1 | lz4 | 1073741824 | 18 | 41.059 | 12.191 | 1,980.3 | 8.969 | 0 |

Celeborn's default wall time was 9.08× the Flight control, while total
engine-plus-shuffle CPU was 1.63× the Flight CPU across the same 18 queries.
Removing compression or adding a worker reduced some wall time, and a
16 MiB split threshold changed little. Q1, Q8, Q14 and Q17 were excluded
because at least one result differed from Spark.

The pinned planner makes Celeborn shuffle output blocking. Producers
serialize RecordBatches, wait for push acknowledgments and report mapper
completion before dependent reducers can start. Flight shuffle pipelines
the producer and consumer stages. A sample of 175 matching worker log
entries put the median slot-reservation-to-commit-start interval at
166 ms and the median final commit at 5 ms. The former includes upstream
work and transfer time, so the measurements leave the individual costs
of those operations unresolved.
[Stage planner](https://github.com/lakehq/sail/blob/790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f/crates/sail-execution/src/job_graph/planner.rs#L1213) ·
[Shuffle writer](https://github.com/lakehq/sail/blob/790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f/crates/sail-execution/src/stream/celeborn/core.rs#L182)

Worker kill probe. Active at injection `True`. Query status `error`. Subsequent SELECT 1 correct `True`.

The worker returned exit code 137 immediately after the intentional kill.
Q9 failed with `Connection refused (os error 111)`. A subsequent
`SELECT 1` succeeded. A distributed query after worker loss remains unverified.

The original instrumentation returned no stage rows and HTTP 404 for
the worker metrics endpoint. A rerun enables the metrics servlet and
resolves the client session ID to the server session ID before querying
for stage events.

[Raw JSON](https://github.com/unikdahal/poc/blob/68f208d1cfa9ca0073fc83dbe363dfb512758e67/agent2-celeborn/celeborn.json) · [Result files](https://github.com/unikdahal/poc/tree/68f208d1cfa9ca0073fc83dbe363dfb512758e67/agent2-celeborn) · [Workflow run](https://github.com/unikdahal/poc/actions/runs/45491448770)
