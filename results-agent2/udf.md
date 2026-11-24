# Python UDF throughput and session latency

Command `/opt/hostedtoolcache/Python/3.11.18/x64/bin/python -m harness.agent2.udf --sail-python "$RUNNER_TEMP/agent2-udf/bin/python"`.

Workflow source revision `406d043d02ed6fc593cb670129b47dd335e3e214`. Sail image revision `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`.

Recorded versions include spark 3.5.10, iceberg 1.12.0, comet 1.2.0, python 3.11.18, adbc 1.13.0.

The raw JSON records engine settings, runner details and individual attempts.

Each variant processes 10 million synthetic inputs and checks the result count, sums and per-row value against Spark's native expression. The Sail UDF client uses the embedded runtime's Python minor version.

| Engine | Variant | Attempts | Spark matches | Input rows/s | Warm wall s/CPU s/peak MiB |
| --- | --- | --- | --- | --- | --- |
| spark | scalar | ok/ok/ok | 3 | 1,409,535 | 7.095/27.120/2,092.5 |
| spark | pandas | ok/ok/ok | 3 | 6,225,600 | 1.606/6.040/2,497.3 |
| spark | arrow | ok/ok/ok | 3 | 3,075,536 | 3.251/11.895/2,517.2 |
| spark | udtf | ok/ok/ok | 3 | 398,655 | 25.084/94.840/2,512.9 |
| sail | scalar | ok/ok/ok | 3 | 706,942 | 14.145/15.236/153.0 |
| sail | pandas | ok/ok/ok | 3 | 1,553,634 | 6.437/7.985/154.4 |
| sail | arrow | ok/ok/ok | 3 | 2,711,916 | 3.687/4.604/155.6 |
| sail | udtf | timeout | 0 | — | —/—/— |

Separate sessions measured `SELECT 1` idle and during the CPU-heavy scalar query.

| Engine | Phase | Samples | P50 s | P95 s | Heavy matches Spark |
| --- | --- | --- | --- | --- | --- |
| spark | idle | 30 | 0.075133 | 0.105315 | — |
| spark | overlap | 7 | 0.116928 | 9.966971 | True |
| sail | idle | 30 | 0.006007 | 0.007053 | — |
| sail | overlap | 80 | 0.005885 | 0.006941 | True |

Support was also tested with a 1,000-row UDTF before the full case. The small-case Spark match was spark `True`, sail `True`. Cells without a warm timing have insufficient successful and validated samples.
## Anomalies and log excerpts

```text
sail udtf | TimeoutError: deadline of 900s exceeded (client still active: False)
sail | TimeoutError: TimeoutError: deadline of 900s exceeded (client still active: False)
```

[Raw JSON](https://github.com/unikdahal/poc/blob/207444f141fe392a6fe4d985a162afbd5c5385b2/agent2-udf/udf.json) · [Result files](https://github.com/unikdahal/poc/tree/207444f141fe392a6fe4d985a162afbd5c5385b2/agent2-udf) · [Workflow run](https://github.com/unikdahal/poc/actions/runs/45498374264)
