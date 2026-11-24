# Python UDF throughput and session latency

Command `/opt/hostedtoolcache/Python/3.11.18/x64/bin/python -m harness.agent2.udf --sail-python "$RUNNER_TEMP/agent2-udf/bin/python"`.

Workflow source revision `ce63ff3b49ef24444b790dd4a2d64a017b0ca5fa`. Sail image revision `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`.

Recorded versions include spark 3.5.10, iceberg 1.12.0, comet 1.2.0, python 3.11.18, adbc 1.13.0.

The raw JSON records engine settings, runner details and individual attempts.

Each variant processes 10 million synthetic inputs and checks the result count, sums and per-row value against Spark's native expression. The Sail UDF client uses the embedded runtime's Python minor version.

| Engine | Variant | Attempts | Spark matches | Input rows/s | Warm wall s/CPU s/peak MiB |
| --- | --- | --- | --- | --- | --- |
| spark | scalar | ok/ok/ok | 3 | 1,308,574 | 7.642/27.755/1,894.9 |
| spark | pandas | ok/ok/ok | 3 | 5,431,651 | 1.841/6.945/2,328.8 |
| spark | arrow | ok/ok/ok | 3 | 2,745,755 | 3.642/13.160/2,343.2 |
| spark | udtf | ok/ok/ok | 3 | 314,352 | 31.812/118.095/2,328.2 |
| sail | scalar | ok/ok/ok | 3 | 590,219 | 16.943/18.100/143.2 |
| sail | pandas | ok/ok/ok | 3 | 1,477,711 | 6.767/8.699/143.6 |
| sail | arrow | ok/ok/ok | 3 | 2,139,933 | 4.673/5.933/143.8 |
| sail | udtf | timeout | 0 | — | —/—/— |

Separate sessions measured `SELECT 1` idle and during the CPU-heavy scalar query.
The Sail p95 moved from 6.558 ms to 6.781 ms across 57 probes that
started while the heavy query was active. Spark's corresponding p95
moved from 103.888 ms to 11,453.662 ms with six overlapping probes.
The heavy query matched Spark across engines. That query took 12.191 s
wall and 45.240 CPU-seconds on Spark and 37.622 s wall and 38.306
CPU-seconds on Sail. The single overlap trial per engine limits how
far the latency result can be generalized.

| Engine | Phase | Samples | P50 s | P95 s | Heavy matches Spark |
| --- | --- | --- | --- | --- | --- |
| spark | idle | 30 | 0.072395 | 0.103888 | — |
| spark | overlap | 6 | 0.103836 | 11.453662 | True |
| sail | idle | 30 | 0.006033 | 0.006558 | — |
| sail | overlap | 57 | 0.005752 | 0.006781 | True |

Both engines matched the Spark reference on a 1,000-row UDTF.
The 10-million-row Sail UDTF hit the 180-second timeout, leaving
throughput at that scale unverified.

## Anomalies and log excerpts

```text
sail udtf | TimeoutError: deadline of 180s exceeded (client still active: False)
sail | TimeoutError: TimeoutError: deadline of 180s exceeded (client still active: False)
```

[Raw JSON](https://github.com/unikdahal/poc/blob/137486c15dfdaeddd606810c6935d0e7cb38b492/agent2-udf/udf.json) · [Result files](https://github.com/unikdahal/poc/tree/137486c15dfdaeddd606810c6935d0e7cb38b492/agent2-udf) · [Workflow run](https://github.com/unikdahal/poc/actions/runs/45494537059)
