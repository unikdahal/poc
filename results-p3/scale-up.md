# Larger public data sets

Reproduce with `p3.yml`, `tasks=scale-up`. The suite command is `python -m harness.p3.scale_up` with TPC-H SF50 and TPC-DS SF10 on hosted runners.
The pinned Sail commit is `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`. The correctness reference is Spark 3.5.10 with Iceberg 1.12.0. Each raw file records the job revision, image digests, configuration and engine exit state where available.

Sail runs under an 8 GiB container cap with a 4 GiB fair pool and a mounted spill directory. Comet is assigned a 6 GiB heap plus 2 GiB off-heap. A planned JVM allocation is distinct from a process-level memory cap. Peak spill is the largest sampled total of files visible in the mounted directory. Spill can be created and removed between samples.

### Data load and validation

| Variant | Loaded tables | Engine | Queries attempted | Matched warm query medians | Peak sampled spill bytes | Evidence |
| --- | --- | --- | --- | --- | --- | --- |
| tpcds10 | 24 | sail | 12 | 1 | 0 | [Raw JSON](https://github.com/unikdahal/poc/blob/504ebf41b652d65f00ab67a01f99a833c9e12767/tpcds10/result.json) · [Run 45553585916](https://github.com/unikdahal/poc/actions/runs/45553585916) |
| tpcds10 | 24 | comet | 12 | 12 | 1231287600 | [Raw JSON](https://github.com/unikdahal/poc/blob/504ebf41b652d65f00ab67a01f99a833c9e12767/tpcds10/result.json) · [Run 45553585916](https://github.com/unikdahal/poc/actions/runs/45553585916) |

### tpcds10 by query

| Query | Reference | Sail | Comet |
| --- | --- | --- | --- |
| 7 | ok | {'mismatch': 3} | match, 5.516 s |
| 9 | ok | {'mismatch': 3} | match, 7.868 s |
| 12 | ok | {'mismatch': 3} | match, 0.975 s |
| 18 | ok | {'mismatch': 3} | match, 4.251 s |
| 20 | ok | {'mismatch': 3} | match, 1.214 s |
| 27 | ok | {'mismatch': 3} | match, 4.855 s |
| 36 | ok | {'mismatch': 3} | match, 2.068 s |
| 49 | ok | {'mismatch': 3} | match, 4.691 s |
| 66 | ok | {'mismatch': 3} | match, 2.747 s |
| 77 | ok | match, 0.760 s | match, 2.930 s |
| 83 | ok | {'mismatch': 3} | match, 1.568 s |
| 98 | ok | {'mismatch': 3} | match, 1.853 s |

### Coverage limits

Disk space is measured during generation and execution. The sample is a filesystem gauge. Table data, shuffle caches and spill contribute to the total. Failure classification is reported from query and container diagnostics, including data generation failures, errors and OOM kills when present.
