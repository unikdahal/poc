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

Worker kill probe. Active at injection `True`. Query status `error`. Subsequent SELECT 1 correct `True`.

The info-level logs and worker metric snapshots are linked with the raw results. The stage records include planner timestamps and task start-to-stop proxies for Q5, Q9, Q18 and Q21. The comparison varies one setting at a time. Untested interactions and traces without stage evidence remain unverified.

[Raw JSON](https://github.com/unikdahal/poc/blob/68f208d1cfa9ca0073fc83dbe363dfb512758e67/agent2-celeborn/celeborn.json) · [Result files](https://github.com/unikdahal/poc/tree/68f208d1cfa9ca0073fc83dbe363dfb512758e67/agent2-celeborn) · [Workflow run](https://github.com/unikdahal/poc/actions/runs/45491448770)
