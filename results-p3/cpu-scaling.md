# CPU scaling at TPC-H SF10

Reproduce with `p3.yml`, `tasks=cpu-scaling`. The suite command is `python -m harness.p3.cpu_scaling` with two independent hosted runners, each sweeping 1, 2, 3 and 4 cores.
The pinned Sail commit is `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`. The correctness reference is Spark 3.5.10 with Iceberg 1.12.0. Each raw file records the job revision, image digests, configuration and engine exit state where available.

Sail uses a CPU quota and matching default parallelism. Comet runs with a matching Spark master core count. Results compare query sets that match Spark at every core count for that engine and runner. CPU utilization divides CPU-seconds by elapsed time and the requested CPU count.

### Paired totals

| Runner | Engine | Cores | Paired queries | Wall s | CPU s | Speedup | Efficiency | CPU utilization | Evidence |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| runner-1 | sail | 1 | 19 | 57.852 | 53.087 | 1.000 | 1.000 | 0.918 | [Raw JSON](https://github.com/unikdahal/poc/blob/6ccd9b089e799e83945f074d85c81f5646b59517/runner-1/result.json) · [Run 45553585916](https://github.com/unikdahal/poc/actions/runs/45553585916) |
| runner-1 | sail | 2 | 19 | 38.085 | 65.345 | 1.519 | 0.760 | 0.858 | [Raw JSON](https://github.com/unikdahal/poc/blob/6ccd9b089e799e83945f074d85c81f5646b59517/runner-1/result.json) · [Run 45553585916](https://github.com/unikdahal/poc/actions/runs/45553585916) |
| runner-1 | sail | 3 | 19 | 31.804 | 79.176 | 1.819 | 0.606 | 0.830 | [Raw JSON](https://github.com/unikdahal/poc/blob/6ccd9b089e799e83945f074d85c81f5646b59517/runner-1/result.json) · [Run 45553585916](https://github.com/unikdahal/poc/actions/runs/45553585916) |
| runner-1 | sail | 4 | 19 | 28.980 | 89.482 | 1.996 | 0.499 | 0.772 | [Raw JSON](https://github.com/unikdahal/poc/blob/6ccd9b089e799e83945f074d85c81f5646b59517/runner-1/result.json) · [Run 45553585916](https://github.com/unikdahal/poc/actions/runs/45553585916) |
| runner-1 | comet | 1 | 22 | 233.432 | 255.135 | 1.000 | 1.000 | 1.093 | [Raw JSON](https://github.com/unikdahal/poc/blob/6ccd9b089e799e83945f074d85c81f5646b59517/runner-1/result.json) · [Run 45553585916](https://github.com/unikdahal/poc/actions/runs/45553585916) |
| runner-1 | comet | 2 | 22 | 131.005 | 266.335 | 1.782 | 0.891 | 1.017 | [Raw JSON](https://github.com/unikdahal/poc/blob/6ccd9b089e799e83945f074d85c81f5646b59517/runner-1/result.json) · [Run 45553585916](https://github.com/unikdahal/poc/actions/runs/45553585916) |
| runner-1 | comet | 3 | 22 | 112.804 | 311.735 | 2.069 | 0.690 | 0.921 | [Raw JSON](https://github.com/unikdahal/poc/blob/6ccd9b089e799e83945f074d85c81f5646b59517/runner-1/result.json) · [Run 45553585916](https://github.com/unikdahal/poc/actions/runs/45553585916) |
| runner-1 | comet | 4 | 22 | 102.837 | 339.690 | 2.270 | 0.567 | 0.826 | [Raw JSON](https://github.com/unikdahal/poc/blob/6ccd9b089e799e83945f074d85c81f5646b59517/runner-1/result.json) · [Run 45553585916](https://github.com/unikdahal/poc/actions/runs/45553585916) |
| runner-2 | — | — | — | — | — | — | — | — | [Raw JSON](https://github.com/unikdahal/poc/blob/6ccd9b089e799e83945f074d85c81f5646b59517/runner-2/result.json) · [Run 45553585916](https://github.com/unikdahal/poc/actions/runs/45553585916) |

### runner-1

Each number is the correctness-gated warm wall time in seconds. Blank entries had a mismatch, failure, timeout or an incomplete sample set.

| Query | sail-1 | sail-2 | sail-3 | sail-4 | comet-1 | comet-2 | comet-3 | comet-4 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Q1 | — | — | — | — | 8.879 | 5.040 | 4.494 | 4.239 |
| Q2 | 0.768 | 0.492 | 0.448 | 0.379 | 3.042 | 2.332 | 2.346 | 2.275 |
| Q3 | 2.393 | 1.597 | 1.354 | 1.248 | 10.296 | 6.402 | 4.662 | 4.361 |
| Q4 | 1.488 | 0.988 | 0.826 | 0.733 | 4.906 | 2.941 | 2.477 | 2.267 |
| Q5 | 3.334 | 2.244 | 1.968 | 1.739 | 19.318 | 11.026 | 9.443 | 8.816 |
| Q6 | 1.766 | 1.015 | 0.862 | 0.787 | 1.586 | 1.058 | 1.088 | 0.919 |
| Q7 | 3.106 | 1.873 | 1.623 | 1.486 | 17.695 | 8.667 | 7.253 | 6.572 |
| Q8 | — | — | — | — | 14.577 | 8.491 | 7.302 | 6.847 |
| Q9 | 5.876 | 3.621 | 2.979 | 2.734 | 21.970 | 12.121 | 10.743 | 10.184 |
| Q10 | 3.067 | 1.946 | 1.673 | 1.471 | 8.317 | 4.894 | 4.237 | 3.844 |
| Q11 | 0.587 | 0.357 | 0.347 | 0.293 | 1.292 | 0.977 | 0.943 | 0.905 |
| Q12 | 2.678 | 1.867 | 1.380 | 1.275 | 4.627 | 2.983 | 2.430 | 2.270 |
| Q13 | 2.675 | 1.633 | 1.322 | 1.276 | 5.879 | 3.442 | 2.985 | 2.943 |
| Q14 | — | — | — | — | 2.882 | 1.807 | 1.525 | 1.420 |
| Q15 | 3.753 | 2.256 | 1.960 | 1.802 | 5.658 | 3.513 | 3.067 | 2.863 |
| Q16 | 0.600 | 0.575 | 0.555 | 0.503 | 2.075 | 1.396 | 1.386 | 1.274 |
| Q17 | 5.317 | 3.589 | 3.118 | 2.796 | 16.960 | 9.104 | 7.958 | 7.693 |
| Q18 | 5.577 | 4.858 | 3.581 | 3.334 | 19.948 | 10.223 | 8.905 | 7.376 |
| Q19 | 4.111 | 2.342 | 1.941 | 1.735 | 4.808 | 2.816 | 2.441 | 2.147 |
| Q20 | 3.085 | 1.841 | 1.544 | 1.406 | 4.631 | 2.976 | 2.539 | 2.389 |
| Q21 | 7.022 | 4.547 | 3.944 | 3.626 | 51.107 | 26.950 | 23.077 | 19.731 |
| Q22 | 0.648 | 0.444 | 0.379 | 0.357 | 2.983 | 1.847 | 1.501 | 1.501 |

Attempt counts by status. `sail-1` {'mismatch': 9, 'ok': 57} · `sail-2` {'mismatch': 9, 'ok': 57} · `sail-3` {'mismatch': 9, 'ok': 57} · `sail-4` {'mismatch': 9, 'ok': 57} · `comet-1` {'ok': 66} · `comet-2` {'ok': 66} · `comet-3` {'ok': 66} · `comet-4` {'ok': 66}

[Raw JSON](https://github.com/unikdahal/poc/blob/6ccd9b089e799e83945f074d85c81f5646b59517/runner-1/result.json) · [Run 45553585916](https://github.com/unikdahal/poc/actions/runs/45553585916)

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

[Raw JSON](https://github.com/unikdahal/poc/blob/6ccd9b089e799e83945f074d85c81f5646b59517/runner-2/result.json) · [Run 45553585916](https://github.com/unikdahal/poc/actions/runs/45553585916)

### Coverage limits

Per-query runs use local execution on the hosted CPU model. CPU-seconds are reported by the Sail cgroup and the Spark JVM process tree. These changes are core allocations within a four-vCPU runner. The experiment does not measure scaling beyond four cores.
