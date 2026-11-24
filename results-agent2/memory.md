# Memory pressure

Command `/opt/hostedtoolcache/Python/3.11.18/x64/bin/python -m harness.agent2.memory`.

Workflow source revision `9341458023748f2ea5a946109d30e5f51dbba7cb`. Sail image revision `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`.

Recorded versions include spark 3.5.10, iceberg 1.12.0, comet 1.2.0, python 3.11.18, adbc 1.13.0.

The raw JSON records engine settings, runner details and individual attempts.

Each row uses a fresh server. The fair pool is limited to half the container limit. Comet uses the indicated driver heap plus one quarter as configured off-heap memory.

The runner attempted all 54 cases. Forty-one produced three results
that matched Spark, including all 18 Spark+Comet cases. Four
unbounded Sail containers were OOM-killed on SF30 Q18 and Q21 at
2 GiB and 4 GiB. The client hit its 300-second deadline in all four
cases, and Docker's `OOMKilled` field distinguishes the server exit
from the client timeout.

The fair pool returned nine allocation errors and had zero recorded
container OOM kills. At SF30, Q18 completed under the 8 GiB container
with a 4 GiB fair pool and sampled up to 1,041.7 MiB of spill files.
Q21 still failed allocation in the same fair pool. With an unbounded
pool at 8 GiB, all three SF30 queries completed.

| SF | Q | Engine | GiB | Pool | Attempts | OOMKilled | Spark matches | Warm wall s/CPU s/peak MiB | Local files | Peak local MiB | Spark disk-spill MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 10 | 9 | sail | 2 | unbounded | ok, ok, ok | False | 3 | 4.570/11.742/1,557.2 | 0 | 0.0 | — |
| 10 | 18 | sail | 2 | unbounded | ok, ok, ok | False | 3 | 19.564/24.634/2,048.0 | 0 | 0.0 | — |
| 10 | 21 | sail | 2 | unbounded | ok, ok, ok | False | 3 | 27.196/25.428/2,048.4 | 0 | 0.0 | — |
| 10 | 9 | sail | 2 | fair | ok, ok, ok | False | 3 | 3.614/11.553/1,541.8 | 0 | 0.0 | — |
| 10 | 18 | sail | 2 | fair | error | False | 0 | —/—/— | 16 | 347.2 | — |
| 10 | 21 | sail | 2 | fair | error | False | 0 | —/—/— | 0 | 0.0 | — |
| 10 | 9 | sail | 4 | unbounded | ok, ok, ok | False | 3 | 3.615/11.550/1,565.9 | 0 | 0.0 | — |
| 10 | 18 | sail | 4 | unbounded | ok, ok, ok | False | 3 | 4.719/16.493/3,205.5 | 0 | 0.0 | — |
| 10 | 21 | sail | 4 | unbounded | ok, ok, ok | False | 3 | 4.770/15.268/3,345.2 | 0 | 0.0 | — |
| 10 | 9 | sail | 4 | fair | ok, ok, ok | False | 3 | 3.615/11.683/1,514.9 | 0 | 0.0 | — |
| 10 | 18 | sail | 4 | fair | ok, ok, ok | False | 3 | 5.472/18.520/3,108.8 | 8 | 347.2 | — |
| 10 | 21 | sail | 4 | fair | ok, ok, ok | False | 3 | 4.668/15.060/3,242.2 | 0 | 0.0 | — |
| 10 | 9 | sail | 8 | unbounded | ok, ok, ok | False | 3 | 3.664/11.568/1,544.7 | 0 | 0.0 | — |
| 10 | 18 | sail | 8 | unbounded | ok, ok, ok | False | 3 | 4.665/16.305/3,355.9 | 0 | 0.0 | — |
| 10 | 21 | sail | 8 | unbounded | ok, ok, ok | False | 3 | 4.669/15.108/3,159.4 | 0 | 0.0 | — |
| 10 | 9 | sail | 8 | fair | ok, ok, ok | False | 3 | 3.614/11.618/1,591.4 | 0 | 0.0 | — |
| 10 | 18 | sail | 8 | fair | ok, ok, ok | False | 3 | 4.666/16.318/3,361.8 | 0 | 0.0 | — |
| 10 | 21 | sail | 8 | fair | ok, ok, ok | False | 3 | 4.671/15.152/3,342.3 | 0 | 0.0 | — |
| 10 | 9 | comet | 2 | driver | ok, ok, ok | — | 3 | 14.765/48.880/3,298.2 | 310 | 2,081.5 | 3,183.8 |
| 10 | 18 | comet | 2 | driver | ok, ok, ok | — | 3 | 15.195/52.830/2,790.7 | 451 | 1,057.2 | 2,844.2 |
| 10 | 21 | comet | 2 | driver | ok, ok, ok | — | 3 | 29.305/103.550/2,562.0 | 489 | 1,468.4 | 2,431.8 |
| 10 | 9 | comet | 4 | driver | ok, ok, ok | — | 3 | 13.458/44.270/4,184.4 | 260 | 1,908.7 | 0.0 |
| 10 | 18 | comet | 4 | driver | ok, ok, ok | — | 3 | 12.692/43.970/3,220.9 | 386 | 1,057.2 | 1,126.9 |
| 10 | 21 | comet | 4 | driver | ok, ok, ok | — | 3 | 29.474/104.585/2,803.2 | 434 | 1,468.3 | 2,431.8 |
| 10 | 9 | comet | 8 | driver | ok, ok, ok | — | 3 | 13.598/45.550/4,217.1 | 260 | 1,908.7 | 0.0 |
| 10 | 18 | comet | 8 | driver | ok, ok, ok | — | 3 | 12.042/41.730/3,300.2 | 351 | 1,005.4 | 816.3 |
| 10 | 21 | comet | 8 | driver | ok, ok, ok | — | 3 | 28.788/102.050/3,690.1 | 393 | 1,350.9 | 1,727.3 |
| 30 | 9 | sail | 2 | unbounded | ok, ok, ok | False | 3 | 102.186/71.277/2,057.1 | 0 | 0.0 | — |
| 30 | 18 | sail | 2 | unbounded | timeout | True | 0 | —/—/— | 0 | 0.0 | — |
| 30 | 21 | sail | 2 | unbounded | timeout | True | 0 | —/—/— | 0 | 0.0 | — |
| 30 | 9 | sail | 2 | fair | error | False | 0 | —/—/— | 0 | 0.0 | — |
| 30 | 18 | sail | 2 | fair | error | False | 0 | —/—/— | 44 | 1,041.7 | — |
| 30 | 21 | sail | 2 | fair | error | False | 0 | —/—/— | 0 | 0.0 | — |
| 30 | 9 | sail | 4 | unbounded | ok, ok, ok | False | 3 | 10.392/35.444/3,971.9 | 0 | 0.0 | — |
| 30 | 18 | sail | 4 | unbounded | timeout | True | 0 | —/—/— | 0 | 0.0 | — |
| 30 | 21 | sail | 4 | unbounded | timeout | True | 0 | —/—/— | 0 | 0.0 | — |
| 30 | 9 | sail | 4 | fair | error | False | 0 | —/—/— | 0 | 0.0 | — |
| 30 | 18 | sail | 4 | fair | error | False | 0 | —/—/— | 24 | 1,041.7 | — |
| 30 | 21 | sail | 4 | fair | error | False | 0 | —/—/— | 0 | 0.0 | — |
| 30 | 9 | sail | 8 | unbounded | ok, ok, ok | False | 3 | 10.035/34.502/3,783.4 | 0 | 0.0 | — |
| 30 | 18 | sail | 8 | unbounded | ok, ok, ok | False | 3 | 16.005/57.672/7,833.3 | 0 | 0.0 | — |
| 30 | 21 | sail | 8 | unbounded | ok, ok, ok | False | 3 | 14.012/45.846/8,108.5 | 0 | 0.0 | — |
| 30 | 9 | sail | 8 | fair | ok, ok, ok | False | 3 | 10.087/34.430/3,817.3 | 0 | 0.0 | — |
| 30 | 18 | sail | 8 | fair | ok, ok, ok | False | 3 | 16.661/60.122/8,026.0 | 12 | 1,041.7 | — |
| 30 | 21 | sail | 8 | fair | error | False | 0 | —/—/— | 0 | 0.0 | — |
| 30 | 9 | comet | 2 | driver | ok, ok, ok | — | 3 | 39.735/133.280/4,685.2 | 718 | 5,627.7 | 10,178.0 |
| 30 | 18 | comet | 2 | driver | ok, ok, ok | — | 3 | 38.927/136.050/4,048.7 | 1096 | 2,732.4 | 8,381.6 |
| 30 | 21 | comet | 2 | driver | ok, ok, ok | — | 3 | 89.577/314.145/3,188.0 | 1282 | 4,136.6 | 8,140.6 |
| 30 | 9 | comet | 4 | driver | ok, ok, ok | — | 3 | 36.657/126.285/4,336.9 | 615 | 5,470.5 | 1,666.6 |
| 30 | 18 | comet | 4 | driver | ok, ok, ok | — | 3 | 39.096/136.510/3,841.1 | 941 | 2,738.5 | 6,579.7 |
| 30 | 21 | comet | 4 | driver | ok, ok, ok | — | 3 | 88.819/313.580/3,362.9 | 1170 | 4,136.5 | 7,575.3 |
| 30 | 9 | comet | 8 | driver | ok, ok, ok | — | 3 | 39.403/125.140/5,608.1 | 575 | 5,365.5 | 0.0 |
| 30 | 18 | comet | 8 | driver | ok, ok, ok | — | 3 | 40.241/140.650/4,754.0 | 869 | 2,738.5 | 6,579.6 |
| 30 | 21 | comet | 8 | driver | ok, ok, ok | — | 3 | 89.249/314.935/4,144.2 | 1122 | 4,136.6 | 7,575.2 |

Local spill sampling can miss temporary files created and removed between observations.
Spark local-directory files can also contain ordinary shuffle data.
The event-log column uses Spark's reported disk-spill bytes. The Spark driver runs on the host, where the configured heap does not impose a container memory limit.
## Anomalies and log excerpts

```text
sail-2g-fair-sf10-q18 | AnalysisException: Failed to allocate additional 136.0 MB for HashJoinInput[0] with 207.5 MB already allocated for this reservation - 129.3 MB remain available for the total memory pool: fair(pool_size: 1024.0 MB)
sail-2g-fair-sf10-q21 | AnalysisException: Failed to allocate additional 265.0 MB for HashJoinInput[1] with 144.6 MB already allocated for this reservation - 59.6 MB remain available for the total memory pool: fair(pool_size: 1024.0 MB)
sail-2g-unbounded-sf30-q18 | TimeoutError: deadline of 300s exceeded (client still active: True)
sail-2g-unbounded-sf30-q21 | TimeoutError: deadline of 300s exceeded (client still active: True)
sail-2g-fair-sf30-q9 | AnalysisException: Failed to allocate additional 136.0 MB for HashJoinInput[2] with 183.2 MB already allocated for this reservation - 2.6 MB remain available for the total memory pool: fair(pool_size: 1024.0 MB)
sail-2g-fair-sf30-q18 | AnalysisException: Failed to allocate additional 678.8 KB for HashJoinInput[2] with 201.7 MB already allocated for this reservation - 76.8 KB remain available for the total memory pool: fair(pool_size: 1024.0 MB)
sail-2g-fair-sf30-q21 | AnalysisException: Failed to allocate additional 255.1 KB for HashJoinInput[1] with 255.5 MB already allocated for this reservation - 243.6 KB remain available for the total memory pool: fair(pool_size: 1024.0 MB)
sail-4g-unbounded-sf30-q18 | TimeoutError: deadline of 300s exceeded (client still active: True)
sail-4g-unbounded-sf30-q21 | TimeoutError: deadline of 300s exceeded (client still active: True)
sail-4g-fair-sf30-q9 | AnalysisException: Failed to allocate additional 68.0 MB for HashJoinInput[2] with 168.2 MB already allocated for this reservation - 12.7 MB remain available for the total memory pool: fair(pool_size: 2.0 GB)
sail-4g-fair-sf30-q18 | AnalysisException: Failed to allocate additional 849.2 KB for HashJoinInput[2] with 457.9 MB already allocated for this reservation - 66.9 KB remain available for the total memory pool: fair(pool_size: 2.0 GB)
sail-4g-fair-sf30-q21 | AnalysisException: Failed to allocate additional 795.1 MB for HashJoinInput[2] with 434.1 MB already allocated for this reservation - 745.6 MB remain available for the total memory pool: fair(pool_size: 2.0 GB)
sail-8g-fair-sf30-q21 | AnalysisException: Failed to allocate additional 795.2 MB for HashJoinInput[3] with 434.2 MB already allocated for this reservation - 769.0 MB remain available for the total memory pool: fair(pool_size: 4.0 GB)
```

[Raw JSON](https://github.com/unikdahal/poc/blob/9a00d91fd64053f4e6b9769630a498f32bd1850a/agent2-memory/memory.json) · [Result files](https://github.com/unikdahal/poc/tree/9a00d91fd64053f4e6b9769630a498f32bd1850a/agent2-memory) · [Workflow run](https://github.com/unikdahal/poc/actions/runs/45490267308)
