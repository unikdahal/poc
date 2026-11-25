# Engine experiments

Runs use Spark 3.5 for correctness. Validated timings use warm samples on the same runner with one engine active at a time.

| Task | Run outcome | Hosted run |
| --- | --- | --- |
| [tpcds](tpcds.md) | 78 match, 21 values differ | [Run](https://github.com/unikdahal/poc/actions/runs/45496222083) |
| [ingest](ingest.md) | Running or awaiting results | [Run](https://github.com/unikdahal/poc/actions/runs/45534164779) |
| [memory](memory.md) | 54/54 configurations attempted, 4 container OOM kills | [Run](https://github.com/unikdahal/poc/actions/runs/45490267308) |
| [concurrency](concurrency.md) | Running or awaiting results | [Run](https://github.com/unikdahal/poc/actions/runs/45501158882) |
| [udf](udf.md) | 7/8 engine/UDF pairs with validated warm timings | [Run](https://github.com/unikdahal/poc/actions/runs/45498374264) |
| [catalog](catalog.md) | 2,000 tables; expired-token retry verified | [Run](https://github.com/unikdahal/poc/actions/runs/45492229100) |
| [flight-leak](flight-leak.md) | 300/300 tickets issued; 37 memory samples | [Run](https://github.com/unikdahal/poc/actions/runs/45494537059) |
| [celeborn](celeborn.md) | 7 shuffle treatments attempted; worker loss error; 7.773× Flight shuffle on 18 paired queries | [Run](https://github.com/unikdahal/poc/actions/runs/45494813690) |
| [release-vs-main](release-vs-main.md) | 19/22 queries validated on both builds; release/main wall 1.122× | [Run](https://github.com/unikdahal/poc/actions/runs/45495413240) |
