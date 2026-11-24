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

Worker kill probe. Active at injection `True`. Query status `error`. Subsequent SELECT 1 correct `True`.

The info-level logs and worker metric snapshots are linked with the raw results. The stage records include planner timestamps and task start-to-stop proxies for Q5, Q9, Q18 and Q21. The comparison varies one setting at a time. Untested interactions and traces without stage evidence remain unverified.

[Raw JSON](https://github.com/unikdahal/poc/blob/de93cb5a934a7492df09e859934700fc2b266628/agent2-celeborn/celeborn.json) · [Result files](https://github.com/unikdahal/poc/tree/de93cb5a934a7492df09e859934700fc2b266628/agent2-celeborn) · [Workflow run](https://github.com/unikdahal/poc/actions/runs/45494813690)
