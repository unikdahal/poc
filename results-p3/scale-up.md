# Larger public data sets

Reproduce with `p3.yml`, `tasks=scale-up`. The suite command is `python -m harness.p3.scale_up` with TPC-H SF50 and TPC-DS SF10 on hosted runners.
The pinned Sail commit is `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`. The correctness reference is Spark 3.5.10 with Iceberg 1.12.0. Each raw file records the job revision, image digests, configuration and engine exit state where available.

Sail runs under an 8 GiB container cap with a 4 GiB fair pool and a mounted spill directory. Comet is assigned a 6 GiB heap plus 2 GiB off-heap. A planned JVM allocation is distinct from a process-level memory cap. Peak spill is the largest sampled total of files visible in the mounted directory. Spill can be created and removed between samples.

### Data load and validation

| Variant | Loaded tables | Engine | Queries attempted | Matched warm query medians | Peak sampled spill bytes | Evidence |
| --- | --- | --- | --- | --- | --- | --- |
| tpch50 | 8 | sail | 22 | 15 | 1820514192 | [Raw JSON](https://github.com/unikdahal/poc/blob/d0a077c3b3d4a780b09109c3c2d9fc0189fa729d/tpch50/result.json) · [Run 45553585916](https://github.com/unikdahal/poc/actions/runs/45553585916) |
| tpch50 | 8 | comet | 22 | 22 | 12845001007 | [Raw JSON](https://github.com/unikdahal/poc/blob/d0a077c3b3d4a780b09109c3c2d9fc0189fa729d/tpch50/result.json) · [Run 45553585916](https://github.com/unikdahal/poc/actions/runs/45553585916) |
| tpcds10 | 24 | sail | 12 | 1 | 0 | [Raw JSON](https://github.com/unikdahal/poc/blob/d0a077c3b3d4a780b09109c3c2d9fc0189fa729d/tpcds10/result.json) · [Run 45553585916](https://github.com/unikdahal/poc/actions/runs/45553585916) |
| tpcds10 | 24 | comet | 12 | 12 | 1231287600 | [Raw JSON](https://github.com/unikdahal/poc/blob/d0a077c3b3d4a780b09109c3c2d9fc0189fa729d/tpcds10/result.json) · [Run 45553585916](https://github.com/unikdahal/poc/actions/runs/45553585916) |

### Disk and memory envelope

| Variant | Free bytes before generation | Lowest sampled free bytes loading | Free bytes after run | Sail OOMKilled | Sail exit code | Comet process memory cap in bytes | Evidence |
| --- | --- | --- | --- | --- | --- | --- | --- |
| tpch50 | 110796386304 | 77908295680 | 98311729152 | False | 137 | JVM budget only | [Raw JSON](https://github.com/unikdahal/poc/blob/d0a077c3b3d4a780b09109c3c2d9fc0189fa729d/tpch50/result.json) · [Run 45553585916](https://github.com/unikdahal/poc/actions/runs/45553585916) |
| tpcds10 | 107345383424 | 99646783488 | 103998939136 | False | 137 | JVM budget only | [Raw JSON](https://github.com/unikdahal/poc/blob/d0a077c3b3d4a780b09109c3c2d9fc0189fa729d/tpcds10/result.json) · [Run 45553585916](https://github.com/unikdahal/poc/actions/runs/45553585916) |

### tpch50 by query

| Query | Reference | Sail | Comet |
| --- | --- | --- | --- |
| 1 | ok | {'mismatch': 3} | match, 18.310 s |
| 2 | ok | match, 1.514 s | match, 5.108 s |
| 3 | ok | match, 5.900 s | match, 24.430 s |
| 4 | ok | match, 3.336 s | match, 11.984 s |
| 5 | ok | match, 7.998 s | match, 45.700 s |
| 6 | ok | match, 3.584 s | match, 3.482 s |
| 7 | ok | match, 6.769 s | match, 14.228 s |
| 8 | ok | {'mismatch': 3} | match, 37.297 s |
| 9 | ok | {'error': 3} AnalysisException: Failed to allocate additional 459.0 KB for HashJoinInput[0] with 258.8 MB already | match, 59.366 s |
| 10 | ok | match, 6.688 s | match, 19.148 s |
| 11 | ok | match, 1.237 s | match, 2.178 s |
| 12 | ok | match, 6.719 s | match, 13.058 s |
| 13 | ok | match, 5.923 s | match, 13.641 s |
| 14 | ok | {'mismatch': 3} | match, 5.699 s |
| 15 | ok | match, 8.173 s | match, 12.123 s |
| 16 | ok | match, 1.782 s | match, 3.911 s |
| 17 | ok | {'mismatch': 3} | match, 47.103 s |
| 18 | ok | {'error': 3} AnalysisException: Failed to allocate additional 678.8 KB for HashJoinInput[1] with 933.9 MB already | match, 47.778 s |
| 19 | ok | match, 7.698 s | match, 9.341 s |
| 20 | ok | match, 9.519 s | match, 8.784 s |
| 21 | ok | {'error': 3} AnalysisException: Failed to allocate additional 1325.3 MB for HashJoinInput[2] with 723.4 MB alread | match, 112.861 s |
| 22 | ok | match, 1.429 s | match, 5.563 s |

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
