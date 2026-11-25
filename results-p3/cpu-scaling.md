# CPU scaling at TPC-H SF10

Reproduce with `p3.yml`, `tasks=cpu-scaling`. The suite command is `python -m harness.p3.cpu_scaling` with two independent hosted runners, each sweeping 1, 2, 3 and 4 cores.
The pinned Sail commit is `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`. The correctness reference is Spark 3.5.10 with Iceberg 1.12.0. Each raw file records the job revision, image digests, configuration and engine exit state where available.

Sail uses a CPU quota and matching default parallelism. Comet runs with a matching Spark master core count. Results compare query sets that match Spark at every core count for that engine and runner. CPU utilization divides CPU-seconds by elapsed time and the requested CPU count.

### Paired totals

| Runner | Engine | Cores | Paired queries | Wall s | CPU s | Speedup | Efficiency | CPU utilization | Evidence |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| runner-1 | comet | 1 | 22 | 223.155 | 245.320 | 1.000 | 1.000 | 1.099 | [Raw JSON](https://github.com/unikdahal/poc/blob/45eb83798dc29c38ae823e4fd5cfcbb5ac9c4566/runner-1/result.json) · [Run 45557839275](https://github.com/unikdahal/poc/actions/runs/45557839275) |
| runner-1 | comet | 2 | 22 | 127.112 | 260.535 | 1.756 | 0.878 | 1.025 | [Raw JSON](https://github.com/unikdahal/poc/blob/45eb83798dc29c38ae823e4fd5cfcbb5ac9c4566/runner-1/result.json) · [Run 45557839275](https://github.com/unikdahal/poc/actions/runs/45557839275) |
| runner-1 | comet | 3 | 22 | 108.910 | 300.555 | 2.049 | 0.683 | 0.920 | [Raw JSON](https://github.com/unikdahal/poc/blob/45eb83798dc29c38ae823e4fd5cfcbb5ac9c4566/runner-1/result.json) · [Run 45557839275](https://github.com/unikdahal/poc/actions/runs/45557839275) |
| runner-1 | comet | 4 | 22 | 98.381 | 330.665 | 2.268 | 0.567 | 0.840 | [Raw JSON](https://github.com/unikdahal/poc/blob/45eb83798dc29c38ae823e4fd5cfcbb5ac9c4566/runner-1/result.json) · [Run 45557839275](https://github.com/unikdahal/poc/actions/runs/45557839275) |
| runner-1 | sail | 1 | 19 | 54.045 | 49.951 | 1.000 | 1.000 | 0.924 | [Raw JSON](https://github.com/unikdahal/poc/blob/45eb83798dc29c38ae823e4fd5cfcbb5ac9c4566/runner-1/result.json) · [Run 45557839275](https://github.com/unikdahal/poc/actions/runs/45557839275) |
| runner-1 | sail | 2 | 19 | 35.271 | 61.081 | 1.532 | 0.766 | 0.866 | [Raw JSON](https://github.com/unikdahal/poc/blob/45eb83798dc29c38ae823e4fd5cfcbb5ac9c4566/runner-1/result.json) · [Run 45557839275](https://github.com/unikdahal/poc/actions/runs/45557839275) |
| runner-1 | sail | 3 | 19 | 29.606 | 74.198 | 1.825 | 0.608 | 0.835 | [Raw JSON](https://github.com/unikdahal/poc/blob/45eb83798dc29c38ae823e4fd5cfcbb5ac9c4566/runner-1/result.json) · [Run 45557839275](https://github.com/unikdahal/poc/actions/runs/45557839275) |
| runner-1 | sail | 4 | 19 | 27.529 | 84.774 | 1.963 | 0.491 | 0.770 | [Raw JSON](https://github.com/unikdahal/poc/blob/45eb83798dc29c38ae823e4fd5cfcbb5ac9c4566/runner-1/result.json) · [Run 45557839275](https://github.com/unikdahal/poc/actions/runs/45557839275) |
| runner-2 | comet | 1 | 22 | 295.106 | 328.035 | 1.000 | 1.000 | 1.112 | [Raw JSON](https://github.com/unikdahal/poc/blob/45eb83798dc29c38ae823e4fd5cfcbb5ac9c4566/runner-2/result.json) · [Run 45557839275](https://github.com/unikdahal/poc/actions/runs/45557839275) |
| runner-2 | comet | 2 | 22 | 165.141 | 339.515 | 1.787 | 0.893 | 1.028 | [Raw JSON](https://github.com/unikdahal/poc/blob/45eb83798dc29c38ae823e4fd5cfcbb5ac9c4566/runner-2/result.json) · [Run 45557839275](https://github.com/unikdahal/poc/actions/runs/45557839275) |
| runner-2 | comet | 3 | 22 | 143.886 | 399.930 | 2.051 | 0.684 | 0.926 | [Raw JSON](https://github.com/unikdahal/poc/blob/45eb83798dc29c38ae823e4fd5cfcbb5ac9c4566/runner-2/result.json) · [Run 45557839275](https://github.com/unikdahal/poc/actions/runs/45557839275) |
| runner-2 | comet | 4 | 22 | 130.734 | 433.830 | 2.257 | 0.564 | 0.830 | [Raw JSON](https://github.com/unikdahal/poc/blob/45eb83798dc29c38ae823e4fd5cfcbb5ac9c4566/runner-2/result.json) · [Run 45557839275](https://github.com/unikdahal/poc/actions/runs/45557839275) |
| runner-2 | sail | 1 | 19 | 76.033 | 71.549 | 1.000 | 1.000 | 0.941 | [Raw JSON](https://github.com/unikdahal/poc/blob/45eb83798dc29c38ae823e4fd5cfcbb5ac9c4566/runner-2/result.json) · [Run 45557839275](https://github.com/unikdahal/poc/actions/runs/45557839275) |
| runner-2 | sail | 2 | 19 | 49.893 | 88.200 | 1.524 | 0.762 | 0.884 | [Raw JSON](https://github.com/unikdahal/poc/blob/45eb83798dc29c38ae823e4fd5cfcbb5ac9c4566/runner-2/result.json) · [Run 45557839275](https://github.com/unikdahal/poc/actions/runs/45557839275) |
| runner-2 | sail | 3 | 19 | 41.754 | 106.731 | 1.821 | 0.607 | 0.852 | [Raw JSON](https://github.com/unikdahal/poc/blob/45eb83798dc29c38ae823e4fd5cfcbb5ac9c4566/runner-2/result.json) · [Run 45557839275](https://github.com/unikdahal/poc/actions/runs/45557839275) |
| runner-2 | sail | 4 | 19 | 38.331 | 121.287 | 1.984 | 0.496 | 0.791 | [Raw JSON](https://github.com/unikdahal/poc/blob/45eb83798dc29c38ae823e4fd5cfcbb5ac9c4566/runner-2/result.json) · [Run 45557839275](https://github.com/unikdahal/poc/actions/runs/45557839275) |

### runner-1

Each number is the correctness-gated warm wall time in seconds. Blank entries had a mismatch, failure, timeout or an incomplete sample set.

| Query | sail-1 | sail-2 | sail-3 | sail-4 | comet-1 | comet-2 | comet-3 | comet-4 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Q1 | — | — | — | — | 8.775 | 4.969 | 4.561 | 4.185 |
| Q2 | 0.731 | 0.445 | 0.422 | 0.371 | 2.836 | 2.212 | 2.141 | 2.172 |
| Q3 | 2.255 | 1.525 | 1.280 | 1.189 | 10.246 | 6.097 | 4.459 | 4.191 |
| Q4 | 1.360 | 0.908 | 0.756 | 0.689 | 4.700 | 2.824 | 2.446 | 2.195 |
| Q5 | 3.186 | 2.138 | 1.850 | 1.686 | 18.474 | 10.769 | 9.266 | 8.545 |
| Q6 | 1.688 | 0.967 | 0.816 | 0.752 | 1.405 | 1.058 | 0.942 | 0.853 |
| Q7 | 2.934 | 1.796 | 1.524 | 1.428 | 16.912 | 8.526 | 7.013 | 6.222 |
| Q8 | — | — | — | — | 14.082 | 8.415 | 7.151 | 6.436 |
| Q9 | 5.562 | 3.509 | 2.824 | 2.610 | 21.176 | 11.821 | 10.418 | 9.659 |
| Q10 | 2.932 | 1.850 | 1.585 | 1.415 | 7.719 | 4.648 | 3.959 | 3.661 |
| Q11 | 0.559 | 0.335 | 0.331 | 0.280 | 1.164 | 0.929 | 0.891 | 0.813 |
| Q12 | 2.563 | 1.773 | 1.358 | 1.223 | 4.258 | 2.759 | 2.313 | 2.241 |
| Q13 | 2.362 | 1.424 | 1.196 | 1.222 | 5.838 | 3.375 | 2.920 | 2.813 |
| Q14 | — | — | — | — | 2.612 | 1.649 | 1.518 | 1.360 |
| Q15 | 3.511 | 2.068 | 1.846 | 1.737 | 5.107 | 3.382 | 2.905 | 2.700 |
| Q16 | 0.623 | 0.510 | 0.508 | 0.476 | 1.919 | 1.305 | 1.236 | 1.231 |
| Q17 | 4.611 | 3.018 | 2.643 | 2.430 | 14.399 | 8.491 | 7.431 | 7.031 |
| Q18 | 5.175 | 4.353 | 3.204 | 3.123 | 19.114 | 9.840 | 8.519 | 7.090 |
| Q19 | 3.979 | 2.262 | 1.876 | 1.693 | 4.625 | 2.878 | 2.316 | 2.102 |
| Q20 | 2.850 | 1.702 | 1.471 | 1.366 | 4.398 | 2.806 | 2.335 | 2.281 |
| Q21 | 6.553 | 4.281 | 3.770 | 3.494 | 50.516 | 26.544 | 22.751 | 19.240 |
| Q22 | 0.611 | 0.409 | 0.348 | 0.346 | 2.880 | 1.813 | 1.417 | 1.359 |

Attempt counts by status. `sail-1` {'mismatch': 9, 'ok': 57} · `sail-2` {'mismatch': 9, 'ok': 57} · `sail-3` {'mismatch': 9, 'ok': 57} · `sail-4` {'mismatch': 9, 'ok': 57} · `comet-1` {'ok': 66} · `comet-2` {'ok': 66} · `comet-3` {'ok': 66} · `comet-4` {'ok': 66}

[Raw JSON](https://github.com/unikdahal/poc/blob/45eb83798dc29c38ae823e4fd5cfcbb5ac9c4566/runner-1/result.json) · [Run 45557839275](https://github.com/unikdahal/poc/actions/runs/45557839275)

### runner-2

Each number is the correctness-gated warm wall time in seconds. Blank entries had a mismatch, failure, timeout or an incomplete sample set.

| Query | sail-1 | sail-2 | sail-3 | sail-4 | comet-1 | comet-2 | comet-3 | comet-4 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Q1 | — | — | — | — | 12.236 | 6.896 | 6.266 | 6.001 |
| Q2 | 0.980 | 0.604 | 0.577 | 0.480 | 4.204 | 3.032 | 3.024 | 2.812 |
| Q3 | 2.935 | 1.900 | 1.674 | 1.611 | 13.047 | 8.113 | 5.881 | 5.394 |
| Q4 | 1.860 | 1.191 | 1.040 | 0.897 | 5.974 | 3.694 | 3.399 | 2.949 |
| Q5 | 4.125 | 2.659 | 2.541 | 2.180 | 24.298 | 13.939 | 12.384 | 11.343 |
| Q6 | 2.181 | 1.245 | 1.106 | 0.982 | 2.033 | 1.335 | 1.217 | 1.122 |
| Q7 | 3.747 | 2.243 | 1.966 | 1.828 | 21.627 | 10.742 | 9.001 | 8.147 |
| Q8 | — | — | — | — | 17.399 | 9.798 | 8.873 | 8.070 |
| Q9 | 7.346 | 4.506 | 3.808 | 3.542 | 26.732 | 14.375 | 13.012 | 12.392 |
| Q10 | 3.790 | 2.333 | 2.084 | 1.845 | 9.151 | 5.621 | 5.023 | 4.641 |
| Q11 | 0.741 | 0.462 | 0.447 | 0.391 | 1.551 | 1.216 | 1.225 | 1.090 |
| Q12 | 3.325 | 2.571 | 1.838 | 1.715 | 5.067 | 3.434 | 2.903 | 2.571 |
| Q13 | 3.991 | 2.392 | 1.963 | 1.944 | 7.468 | 4.294 | 3.638 | 3.604 |
| Q14 | — | — | — | — | 3.419 | 2.154 | 1.896 | 1.816 |
| Q15 | 4.621 | 2.731 | 2.423 | 2.257 | 6.456 | 4.199 | 3.754 | 3.610 |
| Q16 | 0.795 | 0.714 | 0.706 | 0.632 | 2.498 | 1.795 | 1.793 | 1.664 |
| Q17 | 9.178 | 5.892 | 4.636 | 4.205 | 23.293 | 12.503 | 10.693 | 9.942 |
| Q18 | 8.211 | 7.357 | 5.266 | 4.743 | 27.117 | 13.689 | 11.891 | 9.536 |
| Q19 | 5.014 | 2.847 | 2.381 | 2.183 | 5.770 | 3.625 | 2.989 | 2.723 |
| Q20 | 3.835 | 2.230 | 1.933 | 1.785 | 5.381 | 3.445 | 3.091 | 2.864 |
| Q21 | 8.555 | 5.466 | 4.904 | 4.667 | 66.917 | 35.120 | 30.163 | 26.738 |
| Q22 | 0.803 | 0.551 | 0.461 | 0.445 | 3.468 | 2.124 | 1.773 | 1.705 |

Attempt counts by status. `sail-1` {'mismatch': 9, 'ok': 57} · `sail-2` {'mismatch': 9, 'ok': 57} · `sail-3` {'mismatch': 9, 'ok': 57} · `sail-4` {'mismatch': 9, 'ok': 57} · `comet-1` {'ok': 66} · `comet-2` {'ok': 66} · `comet-3` {'ok': 66} · `comet-4` {'ok': 66}

[Raw JSON](https://github.com/unikdahal/poc/blob/45eb83798dc29c38ae823e4fd5cfcbb5ac9c4566/runner-2/result.json) · [Run 45557839275](https://github.com/unikdahal/poc/actions/runs/45557839275)

### Scaling across runners

**FACT.** runner-1 comet cut paired wall time by 43.0%, 14.3%, 9.7% for the successive 1→2, 2→3 and 3→4 core steps. Four-core speedup was 2.268×. [Raw JSON](https://github.com/unikdahal/poc/blob/45eb83798dc29c38ae823e4fd5cfcbb5ac9c4566/runner-1/result.json) · [Run 45557839275](https://github.com/unikdahal/poc/actions/runs/45557839275)

**FACT.** runner-1 sail cut paired wall time by 34.7%, 16.1%, 7.0% for the successive 1→2, 2→3 and 3→4 core steps. Four-core speedup was 1.963×. [Raw JSON](https://github.com/unikdahal/poc/blob/45eb83798dc29c38ae823e4fd5cfcbb5ac9c4566/runner-1/result.json) · [Run 45557839275](https://github.com/unikdahal/poc/actions/runs/45557839275)

**FACT.** runner-2 comet cut paired wall time by 44.0%, 12.9%, 9.1% for the successive 1→2, 2→3 and 3→4 core steps. Four-core speedup was 2.257×. [Raw JSON](https://github.com/unikdahal/poc/blob/45eb83798dc29c38ae823e4fd5cfcbb5ac9c4566/runner-2/result.json) · [Run 45557839275](https://github.com/unikdahal/poc/actions/runs/45557839275)

**FACT.** runner-2 sail cut paired wall time by 34.4%, 16.3%, 8.2% for the successive 1→2, 2→3 and 3→4 core steps. Four-core speedup was 1.984×. [Raw JSON](https://github.com/unikdahal/poc/blob/45eb83798dc29c38ae823e4fd5cfcbb5ac9c4566/runner-2/result.json) · [Run 45557839275](https://github.com/unikdahal/poc/actions/runs/45557839275)


### Comparisons on the common Spark-matching query set

| Runner | Cores | Common queries | Sail wall s | Comet wall s | Comet wall / Sail wall | Evidence |
| --- | --- | --- | --- | --- | --- | --- |
| runner-1 | 1 | 19 | 54.045 | 197.686 | 3.658 | [Raw JSON](https://github.com/unikdahal/poc/blob/45eb83798dc29c38ae823e4fd5cfcbb5ac9c4566/runner-1/result.json) · [Run 45557839275](https://github.com/unikdahal/poc/actions/runs/45557839275) |
| runner-1 | 2 | 19 | 35.271 | 112.079 | 3.178 | [Raw JSON](https://github.com/unikdahal/poc/blob/45eb83798dc29c38ae823e4fd5cfcbb5ac9c4566/runner-1/result.json) · [Run 45557839275](https://github.com/unikdahal/poc/actions/runs/45557839275) |
| runner-1 | 3 | 19 | 29.606 | 95.681 | 3.232 | [Raw JSON](https://github.com/unikdahal/poc/blob/45eb83798dc29c38ae823e4fd5cfcbb5ac9c4566/runner-1/result.json) · [Run 45557839275](https://github.com/unikdahal/poc/actions/runs/45557839275) |
| runner-1 | 4 | 19 | 27.529 | 86.400 | 3.138 | [Raw JSON](https://github.com/unikdahal/poc/blob/45eb83798dc29c38ae823e4fd5cfcbb5ac9c4566/runner-1/result.json) · [Run 45557839275](https://github.com/unikdahal/poc/actions/runs/45557839275) |
| runner-2 | 1 | 19 | 76.033 | 262.052 | 3.447 | [Raw JSON](https://github.com/unikdahal/poc/blob/45eb83798dc29c38ae823e4fd5cfcbb5ac9c4566/runner-2/result.json) · [Run 45557839275](https://github.com/unikdahal/poc/actions/runs/45557839275) |
| runner-2 | 2 | 19 | 49.893 | 146.292 | 2.932 | [Raw JSON](https://github.com/unikdahal/poc/blob/45eb83798dc29c38ae823e4fd5cfcbb5ac9c4566/runner-2/result.json) · [Run 45557839275](https://github.com/unikdahal/poc/actions/runs/45557839275) |
| runner-2 | 3 | 19 | 41.754 | 126.851 | 3.038 | [Raw JSON](https://github.com/unikdahal/poc/blob/45eb83798dc29c38ae823e4fd5cfcbb5ac9c4566/runner-2/result.json) · [Run 45557839275](https://github.com/unikdahal/poc/actions/runs/45557839275) |
| runner-2 | 4 | 19 | 38.331 | 114.848 | 2.996 | [Raw JSON](https://github.com/unikdahal/poc/blob/45eb83798dc29c38ae823e4fd5cfcbb5ac9c4566/runner-2/result.json) · [Run 45557839275](https://github.com/unikdahal/poc/actions/runs/45557839275) |

The run JSON contains the validated per-query medians. The report reconstructs the Sail aggregate from those medians because an intermediate report write in the harness omitted that aggregate.

### Coverage limits

Per-query runs use local execution on the hosted CPU model. CPU-seconds are reported by the Sail cgroup and the Spark JVM process tree. These changes are core allocations within a four-vCPU runner. The experiment does not measure scaling beyond four cores.
