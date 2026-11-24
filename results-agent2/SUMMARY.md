# Engine experiments

Each suite runs on its own hosted runner. Spark 3.5 is the reference.
Warm timing summaries require matching results. The pinned image reports
revision `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`.

| Task | Report | Hosted run |
| --- | --- | --- |
| TPC-DS | [SF1 query comparison](tpcds.md) | [Run](https://github.com/unikdahal/poc/actions/runs/45492749749) |
| Ingest | [Iceberg write paths](ingest.md) | [Run](https://github.com/unikdahal/poc/actions/runs/45490127635) |
| Memory | [Limits and spill](memory.md) | [Run](https://github.com/unikdahal/poc/actions/runs/45490267308) |
| Concurrency | [SF10 query streams](concurrency.md) | [Run](https://github.com/unikdahal/poc/actions/runs/45490383284) |
| Python UDFs | [Evaluation and session latency](udf.md) | [Run](https://github.com/unikdahal/poc/actions/runs/45490669445) |
| Catalog | [Metadata and tokens](catalog.md) | [Run](https://github.com/unikdahal/poc/actions/runs/45492229100) |
| Flight SQL | [Tickets and cancellation](flight-leak.md) | [Run](https://github.com/unikdahal/poc/actions/runs/45492104693) |
| Celeborn | [Shuffle tuning and worker loss](celeborn.md) | [Run](https://github.com/unikdahal/poc/actions/runs/45491448770) |
| Release versus main | [SF10 build comparison](release-vs-main.md) | [Run](https://github.com/unikdahal/poc/actions/runs/45491701924) |

Per-task reports link to the corresponding `results/agent2-<task>` branches.
