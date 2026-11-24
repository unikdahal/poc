# Python UDF throughput and session latency

Run `python -m harness.agent2.udf --sail-python "$RUNNER_TEMP/agent2-udf/bin/python"`
with the pinned image `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`. The Spark
client uses Python 3.11 and the Sail client uses Python 3.14 to match its
embedded runtime. Each variant evaluates 10 million deterministic inputs
and checks the output against Spark's native expression. A separate
session measures `SELECT 1` before and during the CPU-heavy scalar UDF.

| Evaluation path | Spark | Sail |
| --- | --- | --- |
| Scalar, pandas, Arrow-optimized scalar, UDTF | Pending runner | Pending runner |
| Idle and overlapping SELECT 1 latency | Pending runner | Pending runner |

Correctness, throughput, UDTF support and log excerpts are pending.
[Raw results](https://github.com/unikdahal/poc/tree/results/agent2-udf/agent2-udf)
