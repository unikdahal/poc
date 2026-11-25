# Iceberg reader and writer behavior

Reproduce with `p3.yml`, `tasks=iceberg-deep`. The suite command is `python -u -m harness.p3.iceberg_deep` with the interop, evolution, maintenance and conflict matrix variants.
The pinned Sail commit is `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`. The correctness reference is Spark 3.5.10 with Iceberg 1.12.0. Each raw file records the job revision, image digests, configuration and engine exit state where available.

### Format and delete interoperability

[Raw JSON](https://github.com/unikdahal/poc/blob/c2594a7c9bc38ad4c75843f84bc7afc59851b604/interop/result.json) · [Run 45553493787](https://github.com/unikdahal/poc/actions/runs/45553493787)

| Writer and fixture | Spark 3.5.10 | Sail | PyIceberg 0.10.0 | Flink 1.20.4 |
| --- | --- | --- | --- | --- |
| pyiceberg-v2 | differs | differs | BadRequestError: IllegalArgumentException: Credential vending was requested for table p3_iceberg.pyice | — |
| pyiceberg-v3 | differs | differs | BadRequestError: IllegalArgumentException: Credential vending was requested for table p3_iceberg.pyice | — |
| sail-v2 | match | match | BadRequestError: IllegalArgumentException: Credential vending was requested for table p3_iceberg.sail_ | — |
| sail-v3 | match | match | BadRequestError: IllegalArgumentException: Credential vending was requested for table p3_iceberg.sail_ | — |
| spark-v2 | match | match | BadRequestError: IllegalArgumentException: Credential vending was requested for table p3_iceberg.spark | — |
| spark-v3 | match | match | BadRequestError: IllegalArgumentException: Credential vending was requested for table p3_iceberg.spark | — |

Position deletes and deletion vectors are distinguished using the content type, file format and offsets in Spark's metadata table. An equality-delete claim requires file content value 2 and readback of the upsert fixture. The raw file records writer failures independently from reader results.

### Schema and partition evolution

**FACT.** Nested changes applied 9/9 statements. Readback matched Spark for True. [Raw JSON](https://github.com/unikdahal/poc/blob/c2594a7c9bc38ad4c75843f84bc7afc59851b604/evolution/result.json) · [Run 45553493787](https://github.com/unikdahal/poc/actions/runs/45553493787)

**FACT.** Partition evolution applied 6/6 statements. Readback matched Spark for True. [Raw JSON](https://github.com/unikdahal/poc/blob/c2594a7c9bc38ad4c75843f84bc7afc59851b604/evolution/result.json) · [Run 45553493787](https://github.com/unikdahal/poc/actions/runs/45553493787)

The partition metadata lists [{'spec_id': 0, 'files': 16}, {'spec_id': 2, 'files': 1}]. The Sail analyzed plan enumerated three Parquet file ranges across the old and evolved specs for `id=42 AND v='abcdef'`. [Spec IDs and analyzed plan](https://github.com/unikdahal/poc/blob/c2594a7c9bc38ad4c75843f84bc7afc59851b604/evolution/result.json)

### Write amplification and maintenance

| MERGE rounds | Data files | Positional delete files | Sail median read s | Spark median read s |
| --- | --- | --- | --- | --- |
| 1 | 5 | 1 | 0.029 | 0.352 |
| 5 | 9 | 5 | 0.042 | 0.463 |
| 20 | 24 | 20 | 0.165 | 0.713 |
| 50 | 54 | 50 | 0.746 | 1.051 |

**FACT.** The metadata growth fixture completed 500 commits. The checkpoint records current metadata bytes and current manifest count. The MERGE fixture uses hash distribution and a 1 MiB target size. [Raw JSON](https://github.com/unikdahal/poc/blob/c2594a7c9bc38ad4c75843f84bc7afc59851b604/maintenance/result.json) · [Run 45553493787](https://github.com/unikdahal/poc/actions/runs/45553493787)

| Commit | metadata.json bytes | Manifest count |
| --- | --- | --- |
| 1 | 1910 | 1 |
| 50 | 57854 | 50 |
| 100 | 114976 | 1 |
| 250 | 262663 | 52 |
| 500 | 509033 | 5 |

**FACT.** The compaction treatments called `rewrite_data_files` and `rewrite_position_delete_files`. The after-compaction metadata and the final read are in the raw evidence. [Maintenance raw JSON](https://github.com/unikdahal/poc/blob/c2594a7c9bc38ad4c75843f84bc7afc59851b604/maintenance/result.json)

### Overlapping MERGEs

| Isolation, peer and trial | Sail | Peer | Readback accounts for successful clients | Row values and counts | Explicit retry |
| --- | --- | --- | --- | --- | --- |
| serializable_spark_0 | ok | error | True | [{'v': 1, 'n': 100}] | ok |
| serializable_spark_1 | ok | error | True | [{'v': 1, 'n': 100}] | ok |
| serializable_spark_2 | ok | error | True | [{'v': 1, 'n': 100}] | ok |
| serializable_sail_0 | ok | error | True | [{'v': 1, 'n': 100}] | ok |
| serializable_sail_1 | error | ok | True | [{'v': 1, 'n': 100}] | ok |
| serializable_sail_2 | ok | error | True | [{'v': 1, 'n': 100}] | ok |
| snapshot_spark_0 | ok | error | True | [{'v': 1, 'n': 100}] | ok |

[Raw JSON](https://github.com/unikdahal/poc/blob/c2594a7c9bc38ad4c75843f84bc7afc59851b604/conflict/result.json) · [Run 45553493787](https://github.com/unikdahal/poc/actions/runs/45553493787)

### Coverage limits

The nested evolution treatment applied Spark DDL, then read old data with both engines. The maintenance evidence includes reported sort-order IDs and object sizes. It cannot generalize file layout to larger partitions or establish global range distribution. Serializable and snapshot results apply to the synthetic overlapping predicates used here.
