# Spark Connect client coverage

Reproduce with `p3.yml`, `tasks=api-coverage`. The suite command is `python -m harness.p3.api_coverage` with Python and Java clients at 3.5.10 and 4.0.1.
The pinned Sail commit is `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`. The correctness reference is Spark 3.5.10 with Iceberg 1.12.0. Each raw file records the job revision, image digests, configuration and engine exit state where available.

Each treatment uses a fresh Spark reference and a fresh Sail server. The Python suites compare unordered, typed row fingerprints and include sample rows in the raw evidence. The Java probes compare sorted row representations. Python 4.0.1 uses the Spark 3.5.10 Python reference client for shared operations.

### Client verdicts

| Client | Match | Differs | Unsupported | Reference error | Evidence |
| --- | --- | --- | --- | --- | --- |
| python35 | 35 | 0 | 2 | 2 | [Raw JSON](https://github.com/unikdahal/poc/blob/733189a51af9914996b03e2b108176d981753cac/python35/result.json) · [Run 45555832626](https://github.com/unikdahal/poc/actions/runs/45555832626) |
| python4 | 35 | 0 | 2 | 2 | [Raw JSON](https://github.com/unikdahal/poc/blob/733189a51af9914996b03e2b108176d981753cac/python4/result.json) · [Run 45555832626](https://github.com/unikdahal/poc/actions/runs/45555832626) |
| jvm35 | 9 | 0 | 0 | 0 | [Raw JSON](https://github.com/unikdahal/poc/blob/733189a51af9914996b03e2b108176d981753cac/jvm35/result.json) · [Run 45555832626](https://github.com/unikdahal/poc/actions/runs/45555832626) |
| jvm4 | 9 | 0 | 0 | 0 | [Raw JSON](https://github.com/unikdahal/poc/blob/733189a51af9914996b03e2b108176d981753cac/jvm4/result.json) · [Run 45556517946](https://github.com/unikdahal/poc/actions/runs/45556517946) |

### Python APIs

| API | python35 | python4 | Error excerpt |
| --- | --- | --- | --- |
| aggregate | match | match | — |
| applyInPandas | unsupported | unsupported | python35 `IllegalArgumentException: invalid argument: Python version used to compile the UDF (3.11) does not match the Python version at runtime (3.14.10 (main, Nov 24 2026, 04:58:` |
| approx_count_distinct | match | match | — |
| approx_percentile | match | match | — |
| cache | match | match | — |
| checkpoint | reference error | reference error | python35 `PySparkNotImplementedError: [NOT_IMPLEMENTED] checkpoint() is not implemented.` |
| cube | match | match | — |
| date_arithmetic | match | match | — |
| day_time_interval | match | match | — |
| explode | match | match | — |
| explode_outer | match | match | — |
| filter_exists_forall | match | match | — |
| from_to_csv | match | match | — |
| from_to_json | match | match | — |
| get_json_object | match | match | — |
| grouping_sets_sql | match | match | — |
| json_tuple_path | match | match | — |
| local_checkpoint | reference error | reference error | python35 `PySparkNotImplementedError: [NOT_IMPLEMENTED] localCheckpoint() is not implemented.` |
| mapInPandas | unsupported | unsupported | python35 `IllegalArgumentException: invalid argument: Python version used to compile the UDF (3.11) does not match the Python version at runtime (3.14.10 (main, Nov 24 2026, 04:58:` |
| map_filter_transform | match | match | — |
| map_keys_entries_access | match | match | — |
| percentile | match | match | — |
| pivot_discovery | match | match | — |
| pivot_values | match | match | — |
| posexplode | match | match | — |
| regexp_count | match | match | — |
| regexp_extract_replace | match | match | — |
| rollup | match | match | — |
| row_number_rank_lag | match | match | — |
| struct_fields | match | match | — |
| timestamp_ntz | match | match | — |
| transform | match | match | — |
| unpivot | match | match | — |
| window_range | match | match | — |
| window_rows | match | match | — |
| writer_iceberg | match | match | — |
| writer_parquet_options | match | match | — |
| year_month_interval | match | match | — |
| zip_with | match | match | — |

### Java APIs

| API | jvm35 | jvm4 | Error excerpt |
| --- | --- | --- | --- |
| approx | match | match | — |
| explode | match | match | — |
| groupby_sum | match | match | — |
| higher_order | match | match | — |
| json_regex | match | match | — |
| range_filter | match | match | — |
| select_literal | match | match | — |
| timestamp_ntz | match | match | — |
| window | match | match | — |

**FACT.** The two pandas operations from the pinned Python 3.11 clients fail the Sail server's version guard because the image embeds Python 3.14.10. [Version guard](https://github.com/lakehq/sail/blob/790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f/crates/sail-python-udf/src/cereal/mod.rs#L49-L57) · [Python 3.5 run](https://github.com/unikdahal/poc/blob/733189a51af9914996b03e2b108176d981753cac/python35/result.json)

### Coverage limits

The matrix uses the same Spark 3.5.10 correctness reference for both client versions. Checkpoint method availability differs by client. The raw output distinguishes a reference error from a Sail error. The JVM cases cover public Java APIs distributed in the same JVM artifact used by Scala applications.
