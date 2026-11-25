# Larger public data sets

Reproduce with `p3.yml`, `tasks=scale-up`. The suite command is `python -m harness.p3.scale_up` with TPC-H SF50 and TPC-DS SF10 on hosted runners.
The pinned Sail commit is `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`. The correctness reference is Spark 3.5.10 with Iceberg 1.12.0. Each raw file records the job revision, image digests, configuration and engine exit state where available.

Sail runs under an 8 GiB container cap with a 4 GiB fair pool and a mounted spill directory. Comet is assigned a 6 GiB heap plus 2 GiB off-heap. A planned JVM allocation is distinct from a process-level memory cap. Peak spill is the largest sampled total of files visible in the mounted directory. Spill can be created and removed between samples.

### Data load and validation

| Variant | Loaded tables | Engine | Queries attempted | Matched warm query medians | Peak sampled spill bytes | Evidence |
| --- | --- | --- | --- | --- | --- | --- |
| tpch50 | 8 | sail | 22 | 15 | 1820514192 | [Raw JSON](https://github.com/unikdahal/poc/blob/596866003464267f0cf8be3d0a1c744e9acf5689/tpch50/result.json) · [Run 45553585916](https://github.com/unikdahal/poc/actions/runs/45553585916) |
| tpch50 | 8 | comet | 22 | 22 | 12845001007 | [Raw JSON](https://github.com/unikdahal/poc/blob/596866003464267f0cf8be3d0a1c744e9acf5689/tpch50/result.json) · [Run 45553585916](https://github.com/unikdahal/poc/actions/runs/45553585916) |
| tpcds10 | 24 | sail | 12 | 1 | 0 | [Raw JSON](https://github.com/unikdahal/poc/blob/596866003464267f0cf8be3d0a1c744e9acf5689/tpcds10/result.json) · [Run 45553585916](https://github.com/unikdahal/poc/actions/runs/45553585916) |
| tpcds10 | 24 | comet | 12 | 12 | 1231287600 | [Raw JSON](https://github.com/unikdahal/poc/blob/596866003464267f0cf8be3d0a1c744e9acf5689/tpcds10/result.json) · [Run 45553585916](https://github.com/unikdahal/poc/actions/runs/45553585916) |
| tpcds10-1 | 24 | sail | 33 | 23 | 830326224 | [Raw JSON](https://github.com/unikdahal/poc/blob/596866003464267f0cf8be3d0a1c744e9acf5689/tpcds10-1/result.json) · [Run 45557918310](https://github.com/unikdahal/poc/actions/runs/45557918310) |
| tpcds10-1 | 24 | comet | 15 | 13 | 2463775510 | [Raw JSON](https://github.com/unikdahal/poc/blob/596866003464267f0cf8be3d0a1c744e9acf5689/tpcds10-1/result.json) · [Run 45557918310](https://github.com/unikdahal/poc/actions/runs/45557918310) |
| tpcds10-2 | 24 | sail | 33 | 22 | 0 | [Raw JSON](https://github.com/unikdahal/poc/blob/596866003464267f0cf8be3d0a1c744e9acf5689/tpcds10-2/result.json) · [Run 45557918310](https://github.com/unikdahal/poc/actions/runs/45557918310) |
| tpcds10-2 | 24 | comet | 31 | 30 | 2516394628 | [Raw JSON](https://github.com/unikdahal/poc/blob/596866003464267f0cf8be3d0a1c744e9acf5689/tpcds10-2/result.json) · [Run 45557918310](https://github.com/unikdahal/poc/actions/runs/45557918310) |
| tpcds10-3 | 24 | sail | 33 | 29 | 814317088 | [Raw JSON](https://github.com/unikdahal/poc/blob/596866003464267f0cf8be3d0a1c744e9acf5689/tpcds10-3/result.json) · [Run 45557918310](https://github.com/unikdahal/poc/actions/runs/45557918310) |
| tpcds10-3 | 24 | comet | 7 | 5 | 1845450276 | [Raw JSON](https://github.com/unikdahal/poc/blob/596866003464267f0cf8be3d0a1c744e9acf5689/tpcds10-3/result.json) · [Run 45557918310](https://github.com/unikdahal/poc/actions/runs/45557918310) |
| tpcds10-tail2 | 24 | comet | 2 | 2 | 553879647 | [Raw JSON](https://github.com/unikdahal/poc/blob/596866003464267f0cf8be3d0a1c744e9acf5689/tpcds10-tail2/result.json) · [Run 45562958964](https://github.com/unikdahal/poc/actions/runs/45562958964) |
| tpcds10-tail3 | 24 | comet | 26 | 25 | 1959191042 | [Raw JSON](https://github.com/unikdahal/poc/blob/596866003464267f0cf8be3d0a1c744e9acf5689/tpcds10-tail3/result.json) · [Run 45562958964](https://github.com/unikdahal/poc/actions/runs/45562958964) |

### Disk and memory envelope

| Variant | Free bytes before generation | Lowest sampled free bytes loading | Free bytes after run | Sail OOMKilled | Sail exit code | Comet process memory cap in bytes | Evidence |
| --- | --- | --- | --- | --- | --- | --- | --- |
| tpch50 | 110796386304 | 77908295680 | 98311729152 | False | 137 | JVM budget only | [Raw JSON](https://github.com/unikdahal/poc/blob/596866003464267f0cf8be3d0a1c744e9acf5689/tpch50/result.json) · [Run 45553585916](https://github.com/unikdahal/poc/actions/runs/45553585916) |
| tpcds10 | 107345383424 | 99646783488 | 103998939136 | False | 137 | JVM budget only | [Raw JSON](https://github.com/unikdahal/poc/blob/596866003464267f0cf8be3d0a1c744e9acf5689/tpcds10/result.json) · [Run 45553585916](https://github.com/unikdahal/poc/actions/runs/45553585916) |
| tpcds10-1 | 107349692416 | 99616784384 | 103825539072 | False | 137 | 8589934592 | [Raw JSON](https://github.com/unikdahal/poc/blob/596866003464267f0cf8be3d0a1c744e9acf5689/tpcds10-1/result.json) · [Run 45557918310](https://github.com/unikdahal/poc/actions/runs/45557918310) |
| tpcds10-2 | 110795759616 | 103108694016 | 107281281024 | False | 137 | 8589934592 | [Raw JSON](https://github.com/unikdahal/poc/blob/596866003464267f0cf8be3d0a1c744e9acf5689/tpcds10-2/result.json) · [Run 45557918310](https://github.com/unikdahal/poc/actions/runs/45557918310) |
| tpcds10-3 | 107349655552 | 99709095936 | 103857729536 | False | 137 | 8589934592 | [Raw JSON](https://github.com/unikdahal/poc/blob/596866003464267f0cf8be3d0a1c744e9acf5689/tpcds10-3/result.json) · [Run 45557918310](https://github.com/unikdahal/poc/actions/runs/45557918310) |
| tpcds10-tail2 | 110795710464 | 103103025152 | 107463352320 | None | None | 8589934592 | [Raw JSON](https://github.com/unikdahal/poc/blob/596866003464267f0cf8be3d0a1c744e9acf5689/tpcds10-tail2/result.json) · [Run 45562958964](https://github.com/unikdahal/poc/actions/runs/45562958964) |
| tpcds10-tail3 | 110795718656 | 103195107328 | 107292848128 | None | None | 8589934592 | [Raw JSON](https://github.com/unikdahal/poc/blob/596866003464267f0cf8be3d0a1c744e9acf5689/tpcds10-tail3/result.json) · [Run 45562958964](https://github.com/unikdahal/poc/actions/runs/45562958964) |

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

### tpcds10-1 by query

| Query | Reference | Sail | Comet |
| --- | --- | --- | --- |
| 1 | ok | match, 0.190 s | match, 2.258 s |
| 2 | ok | match, 0.384 s | match, 2.741 s |
| 3 | ok | match, 0.636 s | match, 1.469 s |
| 4 | ok | match, 5.836 s | match, 28.715 s |
| 5 | ok | match, 0.905 s | match, 4.458 s |
| 6 | ok | match, 0.525 s | match, 2.891 s |
| 7 | ok | {'mismatch': 3} | match, 4.977 s |
| 8 | ok | match, 0.467 s | match, 1.486 s |
| 9 | ok | {'mismatch': 3} | match, 6.645 s |
| 10 | ok | match, 0.460 s | match, 2.302 s |
| 11 | ok | match, 3.638 s | match, 15.085 s |
| 12 | ok | {'mismatch': 3} | match, 0.686 s |
| 13 | ok | {'mismatch': 3} | match, 2.097 s |
| 14 | ok | match, 7.524 s | {'match': 2, 'error': 1} SparkConnectGrpcException: <_MultiThreadedRendezvous of RPC that terminated with: 	status = StatusCo |
| 15 | ok | match, 0.118 s | {'timeout': 1} TimeoutError: 30s deadline, client active=True |
| 16 | ok | match, 0.329 s | — |
| 17 | ok | match, 0.888 s | — |
| 18 | ok | {'mismatch': 3} | — |
| 19 | ok | match, 0.688 s | — |
| 20 | ok | {'mismatch': 3} | — |
| 21 | ok | match, 0.268 s | — |
| 22 | ok | match, 4.165 s | — |
| 23 | ok | match, 19.345 s | — |
| 24 | ok | match, 2.920 s | — |
| 25 | ok | match, 0.865 s | — |
| 26 | ok | {'mismatch': 3} | — |
| 27 | ok | {'mismatch': 3} | — |
| 28 | ok | {'mismatch': 3} | — |
| 29 | ok | match, 0.949 s | — |
| 30 | ok | match, 0.195 s | — |
| 31 | ok | {'mismatch': 3} | — |
| 32 | ok | match, 0.098 s | — |
| 33 | ok | match, 0.742 s | — |

### tpcds10-2 by query

| Query | Reference | Sail | Comet |
| --- | --- | --- | --- |
| 34 | ok | match, 0.441 s | match, 2.017 s |
| 35 | ok | match, 0.466 s | match, 2.560 s |
| 36 | ok | {'mismatch': 3} | match, 1.755 s |
| 37 | ok | match, 0.272 s | match, 3.891 s |
| 38 | ok | match, 0.713 s | match, 2.706 s |
| 39 | ok | {'mismatch': 3} | match, 12.698 s |
| 40 | ok | match, 0.138 s | match, 3.165 s |
| 41 | ok | match, 0.064 s | match, 0.380 s |
| 42 | ok | match, 0.376 s | match, 0.784 s |
| 43 | ok | match, 0.329 s | match, 0.821 s |
| 44 | ok | match, 0.858 s | match, 0.869 s |
| 45 | ok | match, 0.201 s | match, 0.964 s |
| 46 | ok | match, 0.748 s | match, 1.859 s |
| 47 | ok | {'mismatch': 3} | match, 2.727 s |
| 48 | ok | match, 0.839 s | match, 1.960 s |
| 49 | ok | {'mismatch': 3} | match, 3.105 s |
| 50 | ok | match, 0.562 s | match, 5.678 s |
| 51 | ok | match, 2.731 s | match, 3.950 s |
| 52 | ok | match, 0.391 s | match, 0.793 s |
| 53 | ok | {'mismatch': 3} | match, 1.075 s |
| 54 | ok | match, 0.620 s | match, 2.832 s |
| 55 | ok | match, 0.384 s | match, 0.774 s |
| 56 | ok | match, 0.620 s | match, 1.979 s |
| 57 | ok | {'mismatch': 3} | match, 1.840 s |
| 58 | ok | {'mismatch': 3} | match, 2.161 s |
| 59 | ok | {'mismatch': 3} | match, 1.674 s |
| 60 | ok | match, 0.614 s | match, 1.965 s |
| 61 | ok | {'mismatch': 3} | match, 2.692 s |
| 62 | ok | match, 0.232 s | match, 0.731 s |
| 63 | ok | {'mismatch': 3} | match, 1.017 s |
| 64 | ok | match, 2.825 s | {'match': 1, 'error': 1, 'timeout': 1} SparkConnectGrpcException: <_MultiThreadedRendezvous of RPC that terminated with: 	status = StatusCo |
| 65 | ok | match, 1.346 s | — |
| 66 | ok | {'mismatch': 3} | — |

### tpcds10-3 by query

| Query | Reference | Sail | Comet |
| --- | --- | --- | --- |
| 67 | ok | match, 11.911 s | match, 11.170 s |
| 68 | ok | match, 0.808 s | match, 2.294 s |
| 69 | ok | match, 0.362 s | match, 1.915 s |
| 70 | ok | match, 0.917 s | match, 2.128 s |
| 71 | ok | match, 0.641 s | match, 1.992 s |
| 72 | ok | match, 4.325 s | {'match': 2, 'error': 1} SparkConnectGrpcException: <_MultiThreadedRendezvous of RPC that terminated with: 	status = StatusCo |
| 73 | ok | match, 0.447 s | {'timeout': 1} TimeoutError: 30s deadline, client active=True |
| 74 | ok | match, 1.817 s | — |
| 75 | ok | match, 2.535 s | — |
| 76 | ok | match, 0.761 s | — |
| 77 | ok | match, 0.567 s | — |
| 78 | ok | match, 8.477 s | — |
| 79 | ok | match, 0.749 s | — |
| 80 | ok | match, 1.745 s | — |
| 81 | ok | match, 0.166 s | — |
| 82 | ok | match, 0.383 s | — |
| 83 | ok | {'mismatch': 3} | — |
| 84 | ok | match, 0.140 s | — |
| 85 | ok | match, 0.420 s | — |
| 86 | ok | match, 0.205 s | — |
| 87 | ok | match, 0.733 s | — |
| 88 | ok | match, 1.754 s | — |
| 89 | ok | {'mismatch': 3} | — |
| 90 | ok | {'mismatch': 3} | — |
| 91 | ok | match, 0.152 s | — |
| 92 | ok | match, 0.226 s | — |
| 93 | ok | match, 0.983 s | — |
| 94 | ok | match, 0.263 s | — |
| 95 | ok | match, 1.184 s | — |
| 96 | ok | match, 0.262 s | — |
| 97 | ok | match, 0.708 s | — |
| 98 | ok | {'mismatch': 3} | — |
| 99 | ok | match, 0.367 s | — |

### tpcds10-tail2 by query

| Query | Reference | Sail | Comet |
| --- | --- | --- | --- |
| 65 | ok | — | match, 4.302 s |
| 66 | ok | — | match, 2.838 s |

### tpcds10-tail3 by query

| Query | Reference | Sail | Comet |
| --- | --- | --- | --- |
| 74 | ok | — | match, 9.102 s |
| 75 | ok | — | match, 6.291 s |
| 76 | ok | — | match, 1.643 s |
| 77 | ok | — | match, 2.357 s |
| 78 | ok | — | match, 14.892 s |
| 79 | ok | — | match, 1.640 s |
| 80 | ok | — | match, 17.072 s |
| 81 | ok | — | match, 1.090 s |
| 82 | ok | — | {'error': 1, 'timeout': 1} SparkConnectGrpcException: <_MultiThreadedRendezvous of RPC that terminated with: 	status = StatusCo |
| 83 | ok | — | match, 1.978 s |
| 84 | ok | — | match, 1.010 s |
| 85 | ok | — | match, 1.597 s |
| 86 | ok | — | match, 0.694 s |
| 87 | ok | — | match, 2.425 s |
| 88 | ok | — | match, 5.330 s |
| 89 | ok | — | match, 1.170 s |
| 90 | ok | — | match, 0.673 s |
| 91 | ok | — | match, 0.705 s |
| 92 | ok | — | match, 0.656 s |
| 93 | ok | — | match, 6.664 s |
| 94 | ok | — | match, 2.215 s |
| 95 | ok | — | match, 3.675 s |
| 96 | ok | — | match, 0.791 s |
| 97 | ok | — | match, 2.169 s |
| 98 | ok | — | match, 1.345 s |
| 99 | ok | — | match, 1.076 s |

### Comet memory-controller outcomes

| Variant | Driver attempt | Cgroup max bytes | Cgroup OOM kills | Setup error | Evidence |
| --- | --- | --- | --- | --- | --- |
| tpcds10-1 | comet | 8589934592 | 1 | — | [Raw JSON](https://github.com/unikdahal/poc/blob/596866003464267f0cf8be3d0a1c744e9acf5689/tpcds10-1/result.json) · [Run 45557918310](https://github.com/unikdahal/poc/actions/runs/45557918310) |
| tpcds10-2 | comet | 8589934592 | 1 | — | [Raw JSON](https://github.com/unikdahal/poc/blob/596866003464267f0cf8be3d0a1c744e9acf5689/tpcds10-2/result.json) · [Run 45557918310](https://github.com/unikdahal/poc/actions/runs/45557918310) |
| tpcds10-3 | comet | 8589934592 | 1 | — | [Raw JSON](https://github.com/unikdahal/poc/blob/596866003464267f0cf8be3d0a1c744e9acf5689/tpcds10-3/result.json) · [Run 45557918310](https://github.com/unikdahal/poc/actions/runs/45557918310) |
| tpcds10-tail2 | comet | 8589934592 | 0 | — | [Raw JSON](https://github.com/unikdahal/poc/blob/596866003464267f0cf8be3d0a1c744e9acf5689/tpcds10-tail2/result.json) · [Run 45562958964](https://github.com/unikdahal/poc/actions/runs/45562958964) |
| tpcds10-tail3 | comet | 8589934592 | 1 | — | [Raw JSON](https://github.com/unikdahal/poc/blob/596866003464267f0cf8be3d0a1c744e9acf5689/tpcds10-tail3/result.json) · [Run 45562958964](https://github.com/unikdahal/poc/actions/runs/45562958964) |
| tpcds10-tail3 | comet-restart-1 | 8589934592 | 0 | — | [Raw JSON](https://github.com/unikdahal/poc/blob/596866003464267f0cf8be3d0a1c744e9acf5689/tpcds10-tail3/result.json) · [Run 45562958964](https://github.com/unikdahal/poc/actions/runs/45562958964) |

### Coverage limits

Disk space is measured during generation and execution. The sample is a filesystem gauge. Table data, shuffle caches and spill contribute to the total. Failure classification is reported from query and container diagnostics, including data generation failures, errors and OOM kills when present.
