# Engine experiments

Each comparison used one hosted runner and ran the engines sequentially.
Spark 3.5 was the correctness reference. Per-task reports give the commands,
versions, configurations, warm medians, failures and links to raw JSON on
the `results/agent2-<task>` branches.

| Task | Measured outcome | Hosted run |
| --- | --- | --- |
| [TPC-DS](tpcds.md) | Sail matched Spark on 78/99 queries. Spark+Comet matched on 98/99. | [Run](https://github.com/unikdahal/poc/actions/runs/45496222083) |
| [Ingest](ingest.md) | 23/24 engine-operation cases had an output equal to the reference, and 21 had validated warm timings. | [Run](https://github.com/unikdahal/poc/actions/runs/45534164779) |
| [Memory](memory.md) | 54 cases ran. Four unbounded containers were OOM-killed at SF30. | [Run](https://github.com/unikdahal/poc/actions/runs/45490267308) |
| [Concurrency](concurrency.md) | All 12 configurations were reached. Eight yielded two usable warm trials. Both Sail modes were OOM-killed at 16 streams. | [Run](https://github.com/unikdahal/poc/actions/runs/45501158882) |
| [Python UDFs](udf.md) | Seven of eight engine and UDF pairs yielded validated warm timings. Sail's 10-million-row UDTF timed out after 900 seconds. | [Run](https://github.com/unikdahal/poc/actions/runs/45498374264) |
| [Catalog](catalog.md) | Created 2,000 tables and verified same-statement retry after an expired token returned HTTP 401. | [Run](https://github.com/unikdahal/poc/actions/runs/45492229100) |
| [Flight SQL](flight-leak.md) | The oldest of 300 tickets remained fetchable after six minutes. Both cancellation tests showed continued server CPU use. | [Run](https://github.com/unikdahal/poc/actions/runs/45494537059) |
| [Celeborn](celeborn.md) | Default shuffle took 7.77× the Flight wall time on 18 paired queries in the stage-instrumented run. | [Run](https://github.com/unikdahal/poc/actions/runs/45494813690) |
| [Release versus main](release-vs-main.md) | The release took 1.122× main's wall time on 19 paired queries through the shared catalog proxy. | [Run](https://github.com/unikdahal/poc/actions/runs/45495413240) |
