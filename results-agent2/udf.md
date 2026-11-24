# Python UDF throughput and session latency

Command `/opt/hostedtoolcache/Python/3.11.18/x64/bin/python -m harness.agent2.udf --sail-python "$RUNNER_TEMP/agent2-udf/bin/python"`.

Workflow source revision `d65c398e3f09b82bd1b44ce4681929986b95f798`. Sail image revision `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`.

Recorded versions include spark 3.5.10, iceberg 1.12.0, comet 1.2.0, python 3.11.18, adbc 1.13.0.

The raw JSON records engine settings, runner details and individual attempts.

Each variant processes 10 million synthetic inputs and checks the result count, sums and per-row value against Spark's native expression. The Sail UDF client uses the embedded runtime's Python minor version.

| Engine | Variant | Attempts | Spark matches | Input rows/s | Warm wall s/CPU s/peak MiB |
| --- | --- | --- | --- | --- | --- |
| spark | scalar | ok/ok/ok | 3 | 1,325,050 | 7.547/27.570/2,478.1 |
| spark | pandas | ok/ok/ok | 3 | 5,210,420 | 1.919/6.990/2,927.5 |
| spark | arrow | ok/ok/ok | 3 | 2,788,430 | 3.586/12.800/2,944.5 |
| spark | udtf | ok/ok/ok | 3 | 321,936 | 31.062/114.435/2,963.9 |
| sail | scalar | ok/ok/ok | 3 | 576,711 | 17.340/18.413/139.0 |
| sail | pandas | ok/ok/ok | 3 | 1,434,707 | 6.970/8.870/147.2 |
| sail | arrow | ok/ok/ok | 3 | 2,265,819 | 4.413/5.706/150.9 |
| sail | udtf | timeout | 0 | — | —/—/— |

Separate sessions measured `SELECT 1` idle and during the CPU-heavy scalar query.

| Engine | Phase | Samples | P50 s | P95 s | Heavy matches Spark |
| --- | --- | --- | --- | --- | --- |
| spark | idle | 30 | 0.048124 | 0.079114 | — |
| spark | overlap | 8 | 0.064497 | 10.047072 | True |
| sail | idle | 0 | — | — | — |
| sail | overlap | 0 | — | — | — |

Cells without a warm timing have insufficient successful and validated samples.
## Anomalies and log excerpts

```text
sail udtf | TimeoutError: deadline of 180s exceeded (client still active: False)
sail | TimeoutError: TimeoutError: deadline of 180s exceeded (client still active: False)
```

[Raw JSON](https://github.com/unikdahal/poc/blob/19730ea9eac9a4fee91a452e7b51c35c651d0047/agent2-udf/udf.json) · [Result files](https://github.com/unikdahal/poc/tree/19730ea9eac9a4fee91a452e7b51c35c651d0047/agent2-udf) · [Workflow run](https://github.com/unikdahal/poc/actions/runs/45490669445)
