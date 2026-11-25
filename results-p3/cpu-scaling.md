# CPU scaling at TPC-H SF10

Reproduce with `p3.yml`, `tasks=cpu-scaling`. The suite command is `python -m harness.p3.cpu_scaling` with two independent hosted runners, each sweeping 1, 2, 3 and 4 cores.
The pinned Sail commit is `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`. The correctness reference is Spark 3.5.10 with Iceberg 1.12.0. Each raw file records the job revision, image digests, configuration and engine exit state where available.

Sail uses a CPU quota and matching default parallelism. Comet runs with `local[n]` and a cgroup CPU quota matching Sail's Docker CPU limit. Results compare query sets that match Spark at every core count for that engine and runner. CPU utilization divides CPU-seconds by elapsed time and the requested CPU count.

### Paired totals

| Runner | Engine | Cores | Paired queries | Wall s | CPU s | Speedup | Efficiency | CPU utilization | Evidence |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| runner-1 | sail | 1 | 19 | 77.664 | 73.075 | 1.000 | 1.000 | 0.941 | [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-1/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949) |
| runner-1 | sail | 2 | 19 | 49.938 | 87.966 | 1.555 | 0.778 | 0.881 | [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-1/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949) |
| runner-1 | sail | 3 | 19 | 41.416 | 105.845 | 1.875 | 0.625 | 0.852 | [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-1/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949) |
| runner-1 | sail | 4 | 19 | 37.999 | 120.385 | 2.044 | 0.511 | 0.792 | [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-1/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949) |
| runner-1 | comet | 1 | 22 | 337.713 | 331.835 | 1.000 | 1.000 | 0.983 | [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-1/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949) |
| runner-1 | comet | 2 | 22 | 183.443 | 352.075 | 1.841 | 0.920 | 0.960 | [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-1/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949) |
| runner-1 | comet | 3 | 22 | 151.016 | 410.705 | 2.236 | 0.745 | 0.907 | [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-1/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949) |
| runner-1 | comet | 4 | 22 | 133.289 | 437.935 | 2.534 | 0.633 | 0.821 | [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-1/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949) |
| runner-2 | sail | 1 | 19 | 58.506 | 53.688 | 1.000 | 1.000 | 0.918 | [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-2/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949) |
| runner-2 | sail | 2 | 19 | 38.068 | 65.291 | 1.537 | 0.768 | 0.858 | [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-2/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949) |
| runner-2 | sail | 3 | 19 | 32.693 | 81.394 | 1.790 | 0.597 | 0.830 | [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-2/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949) |
| runner-2 | sail | 4 | 19 | 29.685 | 91.574 | 1.971 | 0.493 | 0.771 | [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-2/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949) |
| runner-2 | comet | 1 | 22 | 260.774 | 256.325 | 1.000 | 1.000 | 0.983 | [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-2/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949) |
| runner-2 | comet | 2 | 22 | 137.218 | 263.705 | 1.900 | 0.950 | 0.961 | [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-2/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949) |
| runner-2 | comet | 3 | 22 | 111.544 | 304.230 | 2.338 | 0.779 | 0.909 | [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-2/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949) |
| runner-2 | comet | 4 | 22 | 100.021 | 333.225 | 2.607 | 0.652 | 0.833 | [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-2/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949) |

### runner-1

Each number is the correctness-gated warm wall time in seconds. Blank entries had a mismatch, failure, timeout or an incomplete sample set.

| Query | sail-1 | sail-2 | sail-3 | sail-4 | comet-1 | comet-2 | comet-3 | comet-4 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Q1 | — | — | — | — | 14.167 | 7.336 | 6.452 | 6.157 |
| Q2 | 1.004 | 0.643 | 0.582 | 0.526 | 8.841 | 4.528 | 3.388 | 3.085 |
| Q3 | 2.961 | 1.972 | 1.702 | 1.604 | 15.804 | 9.238 | 6.147 | 5.409 |
| Q4 | 1.832 | 1.201 | 1.001 | 0.924 | 7.503 | 3.913 | 3.308 | 2.952 |
| Q5 | 4.161 | 2.784 | 2.463 | 2.283 | 30.141 | 16.236 | 12.747 | 11.386 |
| Q6 | 2.154 | 1.268 | 1.077 | 0.995 | 2.602 | 1.509 | 1.318 | 1.131 |
| Q7 | 3.843 | 2.343 | 1.987 | 1.852 | 26.217 | 11.732 | 9.528 | 8.243 |
| Q8 | — | — | — | — | 19.657 | 10.655 | 9.129 | 8.472 |
| Q9 | 7.691 | 4.702 | 3.827 | 3.553 | 28.698 | 15.776 | 13.784 | 12.699 |
| Q10 | 3.837 | 2.352 | 2.070 | 1.834 | 10.749 | 6.125 | 5.245 | 4.855 |
| Q11 | 0.763 | 0.464 | 0.440 | 0.371 | 2.251 | 1.299 | 1.262 | 1.177 |
| Q12 | 3.520 | 2.582 | 1.849 | 1.678 | 6.075 | 3.703 | 3.034 | 2.654 |
| Q13 | 4.130 | 2.439 | 1.937 | 1.920 | 7.710 | 4.589 | 3.858 | 3.603 |
| Q14 | — | — | — | — | 3.597 | 2.327 | 1.959 | 1.756 |
| Q15 | 4.744 | 2.801 | 2.394 | 2.259 | 7.640 | 4.943 | 3.782 | 3.630 |
| Q16 | 0.840 | 0.732 | 0.686 | 0.626 | 3.464 | 2.190 | 1.916 | 1.532 |
| Q17 | 9.293 | 5.479 | 4.569 | 4.121 | 24.577 | 13.976 | 11.691 | 10.127 |
| Q18 | 8.097 | 7.154 | 5.070 | 4.629 | 29.850 | 15.342 | 12.436 | 9.566 |
| Q19 | 5.334 | 2.853 | 2.386 | 2.174 | 6.388 | 3.378 | 3.039 | 2.785 |
| Q20 | 3.805 | 2.212 | 1.913 | 1.688 | 6.457 | 3.931 | 3.116 | 2.860 |
| Q21 | 8.817 | 5.405 | 4.979 | 4.516 | 71.591 | 38.457 | 31.921 | 27.497 |
| Q22 | 0.838 | 0.553 | 0.484 | 0.444 | 3.734 | 2.261 | 1.956 | 1.715 |

Attempt counts by status. `sail-1` {'mismatch': 9, 'ok': 57} · `sail-2` {'mismatch': 9, 'ok': 57} · `sail-3` {'mismatch': 9, 'ok': 57} · `sail-4` {'mismatch': 9, 'ok': 57} · `comet-1` {'ok': 66} · `comet-2` {'ok': 66} · `comet-3` {'ok': 66} · `comet-4` {'ok': 66}

[Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-1/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949)

### runner-2

Each number is the correctness-gated warm wall time in seconds. Blank entries had a mismatch, failure, timeout or an incomplete sample set.

| Query | sail-1 | sail-2 | sail-3 | sail-4 | comet-1 | comet-2 | comet-3 | comet-4 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Q1 | — | — | — | — | 10.015 | 5.264 | 4.386 | 4.125 |
| Q2 | 0.759 | 0.472 | 0.487 | 0.395 | 6.587 | 3.193 | 2.401 | 2.212 |
| Q3 | 2.365 | 1.607 | 1.398 | 1.260 | 12.617 | 7.080 | 4.688 | 4.169 |
| Q4 | 1.498 | 0.990 | 0.857 | 0.753 | 5.883 | 3.151 | 2.489 | 2.156 |
| Q5 | 3.337 | 2.248 | 2.024 | 1.791 | 22.601 | 12.606 | 9.341 | 8.581 |
| Q6 | 1.778 | 1.031 | 0.883 | 0.806 | 1.822 | 1.106 | 0.929 | 0.947 |
| Q7 | 3.151 | 1.884 | 1.644 | 1.489 | 20.585 | 9.218 | 7.175 | 6.331 |
| Q8 | — | — | — | — | 15.682 | 8.773 | 7.237 | 6.651 |
| Q9 | 5.914 | 3.653 | 3.089 | 2.777 | 23.237 | 12.815 | 10.623 | 9.932 |
| Q10 | 3.102 | 1.951 | 1.709 | 1.478 | 9.088 | 4.905 | 4.167 | 3.806 |
| Q11 | 0.575 | 0.367 | 0.354 | 0.292 | 1.901 | 0.984 | 0.917 | 0.846 |
| Q12 | 2.675 | 1.886 | 1.426 | 1.280 | 4.901 | 2.880 | 2.457 | 2.164 |
| Q13 | 2.690 | 1.625 | 1.390 | 1.344 | 6.208 | 3.445 | 2.926 | 2.775 |
| Q14 | — | — | — | — | 3.015 | 1.701 | 1.511 | 1.358 |
| Q15 | 3.744 | 2.249 | 1.994 | 1.852 | 6.430 | 3.488 | 3.049 | 2.749 |
| Q16 | 0.610 | 0.562 | 0.565 | 0.491 | 2.539 | 1.473 | 1.333 | 1.190 |
| Q17 | 5.578 | 3.584 | 3.246 | 2.943 | 16.569 | 8.858 | 7.775 | 7.407 |
| Q18 | 5.615 | 4.705 | 3.781 | 3.440 | 22.014 | 10.701 | 8.799 | 7.267 |
| Q19 | 4.115 | 2.353 | 1.968 | 1.753 | 5.218 | 2.769 | 2.475 | 2.135 |
| Q20 | 3.150 | 1.865 | 1.566 | 1.458 | 5.087 | 2.809 | 2.429 | 2.267 |
| Q21 | 7.175 | 4.583 | 3.938 | 3.717 | 55.224 | 28.179 | 22.969 | 19.527 |
| Q22 | 0.676 | 0.451 | 0.373 | 0.365 | 3.552 | 1.821 | 1.468 | 1.424 |

Attempt counts by status. `sail-1` {'mismatch': 9, 'ok': 57} · `sail-2` {'mismatch': 9, 'ok': 57} · `sail-3` {'mismatch': 9, 'ok': 57} · `sail-4` {'mismatch': 9, 'ok': 57} · `comet-1` {'ok': 66} · `comet-2` {'ok': 66} · `comet-3` {'ok': 66} · `comet-4` {'ok': 66}

[Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-2/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949)

### Scaling across runners

**FACT.** runner-1 sail cut paired wall time by 35.7%, 17.1%, 8.3% for the successive 1→2, 2→3 and 3→4 core steps. Four-core speedup was 2.044×. [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-1/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949)

**FACT.** runner-1 comet cut paired wall time by 45.7%, 17.7%, 11.7% for the successive 1→2, 2→3 and 3→4 core steps. Four-core speedup was 2.534×. [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-1/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949)

**FACT.** runner-2 sail cut paired wall time by 34.9%, 14.1%, 9.2% for the successive 1→2, 2→3 and 3→4 core steps. Four-core speedup was 1.971×. [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-2/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949)

**FACT.** runner-2 comet cut paired wall time by 47.4%, 18.7%, 10.3% for the successive 1→2, 2→3 and 3→4 core steps. Four-core speedup was 2.607×. [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-2/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949)


### Comparisons on the common Spark-matching query set

| Runner | Cores | Common queries | Sail wall s | Comet wall s | Comet wall / Sail wall | Evidence |
| --- | --- | --- | --- | --- | --- | --- |
| runner-1 | 1 | 19 | 77.664 | 300.293 | 3.867 | [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-1/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949) |
| runner-1 | 2 | 19 | 49.938 | 163.125 | 3.267 | [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-1/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949) |
| runner-1 | 3 | 19 | 41.416 | 133.475 | 3.223 | [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-1/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949) |
| runner-1 | 4 | 19 | 37.999 | 116.904 | 3.077 | [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-1/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949) |
| runner-2 | 1 | 19 | 58.506 | 232.062 | 3.966 | [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-2/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949) |
| runner-2 | 2 | 19 | 38.068 | 121.480 | 3.191 | [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-2/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949) |
| runner-2 | 3 | 19 | 32.693 | 98.410 | 3.010 | [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-2/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949) |
| runner-2 | 4 | 19 | 29.685 | 87.886 | 2.961 | [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-2/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949) |

The earlier run applied a Spark scheduler limit. It preserved the per-query Sail medians but omitted the Sail aggregate during checkpointing. The follow-up applies the CPU ceilings before timing and publishes both aggregates directly. [Raw JSON](https://github.com/unikdahal/poc/blob/45eb83798dc29c38ae823e4fd5cfcbb5ac9c4566/runner-1/result.json) · [Run 45557839275](https://github.com/unikdahal/poc/actions/runs/45557839275) · [Raw JSON](https://github.com/unikdahal/poc/blob/45eb83798dc29c38ae823e4fd5cfcbb5ac9c4566/runner-2/result.json) · [Run 45557839275](https://github.com/unikdahal/poc/actions/runs/45557839275)

### Comet cgroup counters

| Runner | Cores | cpu.max | Quota periods | Throttled periods | Evidence |
| --- | --- | --- | --- | --- | --- |
| runner-1 | 1 | `100000 100000` | 10346 | 5735 | [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-1/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949) |
| runner-1 | 2 | `200000 100000` | 5651 | 2940 | [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-1/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949) |
| runner-1 | 3 | `300000 100000` | 4656 | 1440 | [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-1/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949) |
| runner-1 | 4 | `400000 100000` | 4127 | 10 | [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-1/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949) |
| runner-2 | 1 | `100000 100000` | 8004 | 4304 | [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-2/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949) |
| runner-2 | 2 | `200000 100000` | 4256 | 2121 | [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-2/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949) |
| runner-2 | 3 | `300000 100000` | 3447 | 1143 | [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-2/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949) |
| runner-2 | 4 | `400000 100000` | 3088 | 20 | [Raw JSON](https://github.com/unikdahal/poc/blob/11d0878947aac81915c18adcc52a9e34c35bf402/runner-2/result.json) · [Run 45582149949](https://github.com/unikdahal/poc/actions/runs/45582149949) |

### Coverage limits

Per-query runs use local execution on the hosted CPU model. CPU-seconds are reported by the Sail cgroup and the Spark JVM process tree. These changes are core allocations within a four-vCPU runner. The experiment does not measure scaling beyond four cores.
