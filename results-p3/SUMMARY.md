| Task | Key measured result | Status | Run link |
| --- | --- | --- | --- |
| [rootcause](rootcause.md) | 44 correctness failures and one equal-key ordering difference | Measured | [Raw JSON](https://github.com/unikdahal/poc/blob/04520e9d67c74b8cf43ee6d5dcfa6fa3f9f3f565/pinned/result.json) · [Run 45555635573](https://github.com/unikdahal/poc/actions/runs/45555635573) · [Raw JSON](https://github.com/unikdahal/poc/blob/04520e9d67c74b8cf43ee6d5dcfa6fa3f9f3f565/newer/result.json) · [Run 45555635573](https://github.com/unikdahal/poc/actions/runs/45555635573) |
| [tenancy](tenancy.md) | Both protocols accepted cross-tenant changes under the configured identity. Vended storage access was table-scoped. | Measured | [Raw JSON](https://github.com/unikdahal/poc/blob/c3b649375515aa3cd5c4539ddaad182f0ef0c201/isolation/result.json) · [Run 45555042191](https://github.com/unikdahal/poc/actions/runs/45555042191) |
| [k8s-scale](k8s-scale.md) | 6/6 scaling cells ran queries. A Flight worker exited OOMKilled in the initial sweep. | Partial | [Run](https://github.com/unikdahal/poc/actions/runs/45556141965) |
| [soak](soak.md) | First attempts published short active windows only | Partial | [Run](https://github.com/unikdahal/poc/actions/runs/45555344538) |
| [iceberg-deep](iceberg-deep.md) | 500 commits measured. Mixed-engine format and delete results are in the task report. | Partial | [Raw JSON](https://github.com/unikdahal/poc/blob/e83bf9e764a79f3fafdbf5f8d1bcfa84d63a1376/maintenance/result.json) · [Run 45553493787](https://github.com/unikdahal/poc/actions/runs/45553493787) |
| [cpu-scaling](cpu-scaling.md) | 0/2 runners published scaling curves | Partial | [Run](https://github.com/unikdahal/poc/actions/runs/45553585916) |
| [faults-storage](faults-storage.md) | S3 scan reset recovered. Warm medians 0.351 s baseline and 1.939 s with reset. | Partial | [Raw JSON](https://github.com/unikdahal/poc/blob/30107d1357a0811a76944c776e96953c38c979ca/storage/result.json) · [Run 45556883387](https://github.com/unikdahal/poc/actions/runs/45556883387) · [Raw JSON](https://github.com/unikdahal/poc/blob/30107d1357a0811a76944c776e96953c38c979ca/catalog/result.json) · [Run 45556883387](https://github.com/unikdahal/poc/actions/runs/45556883387) |
| [api-coverage](api-coverage.md) | Python 3.5.10 matched 35 cases. Both Java versions matched 9 when the 4.0.1 runner completed. | Measured | [Raw JSON](https://github.com/unikdahal/poc/blob/733189a51af9914996b03e2b108176d981753cac/python35/result.json) · [Run 45555832626](https://github.com/unikdahal/poc/actions/runs/45555832626) · [Raw JSON](https://github.com/unikdahal/poc/blob/733189a51af9914996b03e2b108176d981753cac/jvm35/result.json) · [Run 45555832626](https://github.com/unikdahal/poc/actions/runs/45555832626) |
| [ops](ops.md) | Operator probes are running | Partial | [Run](https://github.com/unikdahal/poc/actions/runs/45554682627) |
| [scale-up](scale-up.md) | TPC-DS SF10 warm results matched Spark for 1 Sail queries and 12 Comet queries in the initial subset | Partial | [Raw JSON](https://github.com/unikdahal/poc/blob/504ebf41b652d65f00ab67a01f99a833c9e12767/tpcds10/result.json) · [Run 45553585916](https://github.com/unikdahal/poc/actions/runs/45553585916) |

## New bugs found

The reduced cases with no exact public Sail issue or PR match include decimal AVG truncation, decimal bround ties, scientific-notation casts, unary LOG, initcap, regex lookaround, SQL PIVOT syntax, value-mapping DECODE and the Iceberg Etc/UTC CTAS failure. Public search cannot establish that a bug has never been filed. [Individual repros and searches](rootcause.md)

## Bugs fixed on newer main

The alternate SHA `da1e7351d28046452f0ec3e8232aee28458f3852` resolved none of the 0 reduced failures that reproduced on the pinned build. The equal-key ordering difference is permitted.

## Blockers for a multi-tenant replacement of Spark, Kyuubi and Celeborn

- The default CLI accepted both tenants' requests under the configured catalog credential. Flight SQL also shared temporary view and timezone state across the two clients. [Tenancy evidence](tenancy.md)
- Query parity depends on Spark-compatible arithmetic and function semantics. Decimal division and AVG affected original benchmark results and the reduced cases. [Correctness evidence](rootcause.md)
- The tested shared-resource configurations increased point-query tail latency under load. The isolation experiment did not attribute independent CPU shares or demonstrate a tenant memory guarantee. A gateway would need authenticated routing and admission policies. [Interference evidence](tenancy.md)
- The Python client and embedded Python versions must agree for the tested Python plans. [Client evidence](api-coverage.md)

## What still couldn't be tested on hosted runners and why

- The system process counters did not expose Tokio task counts. The soak report tracks duration, session counts and any minimum-observation limit for trend estimates. [Soak evidence](soak.md)
- The Flight shutdown path exposes no transport-byte count to the stage table. The kind report uses retained object sizes for storage and records the scope of cross-node network faulting. [kind evidence](k8s-scale.md)
- A per-process JVM heap and off-heap budget are different from an 8 GiB container envelope. The scale-up report records the configured memory method and any disk-related limits. [Scale evidence](scale-up.md)
- The published JSON and logs retain individual failures, timeouts, mismatched outputs and container status. The summary uses only correctness-gated timings.
