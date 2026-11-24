# SF1 TPC-DS

The `agent2-tpcds` job installs DuckDB 1.4.4 and its TPC-DS extension, then runs
`python -m harness.agent2.tpcds`. It generates the 24 synthetic tables,
exports Parquet, and loads format v2 tables through the Spark 3.5 reference.
The query sources and prebuilt image are pinned to
`790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`.

| Scope | Verdict | Warm wall, CPU and peak memory |
| --- | --- | --- |
| Queries 1–99 | Pending runner | Unverified |

No execution results or log excerpts are available yet. The run records the
resolved dependencies and engine configurations with the per-query verdicts.
[Raw results](https://github.com/unikdahal/poc/tree/results/agent2-tpcds/agent2-tpcds)

Two earlier runs stopped before data generation. The first extension installer
failed while fetching `httpfs`. A direct download received HTTP 403 from the
hosted runner. The current run installs the pinned packaged extension.
[First run](https://github.com/unikdahal/poc/actions/runs/45489901367) ·
[Second run](https://github.com/unikdahal/poc/actions/runs/45491870821)
