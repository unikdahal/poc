# Iceberg reader and writer behavior

Reproduce with `p3.yml`, `tasks=iceberg-deep`. The suite command is `python -u -m harness.p3.iceberg_deep` with the interop, evolution, maintenance and conflict matrix variants.
The pinned Sail commit is `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`. The correctness reference is Spark 3.5.10 with Iceberg 1.12.0. Each raw file records the job revision, image digests, configuration and engine exit state where available.

### Format and delete interoperability

[Raw JSON](https://github.com/unikdahal/poc/blob/e83bf9e764a79f3fafdbf5f8d1bcfa84d63a1376/interop/result.json) · [Run 45556400465](https://github.com/unikdahal/poc/actions/runs/45556400465)

| Writer and fixture | Spark 3.5.10 | Sail | PyIceberg 0.10.0 | Flink 1.20.4 |
| --- | --- | --- | --- | --- |
| flink_equality_v2 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `polaris`.`p3_iceberg`.`flink_equality_ | AnalysisException: Table not found: [TABLE_OR_VIEW_NOT_FOUND] Table or view not found: p3_iceberg.flin | NoSuchTableError: NoSuchTableException: Table does not exist: p3_iceberg.flink_equality_v2 | — |
| flink_v2 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `polaris`.`p3_iceberg`.`flink_v2` canno | AnalysisException: Table not found: [TABLE_OR_VIEW_NOT_FOUND] Table or view not found: p3_iceberg.flin | NoSuchTableError: NoSuchTableException: Table does not exist: p3_iceberg.flink_v2 | — |
| flink_v3 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `polaris`.`p3_iceberg`.`flink_v3` canno | AnalysisException: Table not found: [TABLE_OR_VIEW_NOT_FOUND] Table or view not found: p3_iceberg.flin | NoSuchTableError: NoSuchTableException: Table does not exist: p3_iceberg.flink_v3 | — |
| pyiceberg_v2 | match | match | match | — |
| pyiceberg_v3 | differs | differs | differs | — |
| sail_delete_v2 | match | match | match | — |
| sail_delete_v3 | match | match | match | — |
| sail_v2 | match | match | match | — |
| sail_v3 | match | match | match | — |
| spark_delete_v2 | match | match | match | — |
| spark_delete_v3 | match | match | match | — |
| spark_v2 | match | match | match | — |
| spark_v3 | match | match | match | — |

Position deletes and deletion vectors are distinguished using the content type, file format and offsets in Spark's metadata table. The raw file records writer failures independently from reader results.

| Fixture | Delete records in .files | Formats | Content codes |
| --- | --- | --- | --- |
| spark_delete_v2 | 2 | ['PARQUET'] | [1] |
| spark_delete_v3 | 2 | ['PUFFIN'] | [1] |
| sail_delete_v2 | 1 | ['PARQUET'] | [1] |
| sail_delete_v3 | 1 | ['PUFFIN'] | [1] |
| flink_equality_v2 | 0 | [] | [] |


**FACT.** Flink setup returned `TimeoutExpired: Command '('curl', '-fsSL', '--retry', '3', 'https://archive.apache.org/dist/flink/flink-1.20.4/flink-1.20.4-bin-scala_2.12.tgz', '-o', '/tmp/p3-flink-dxvqg3wi/flink.tgz')' timed out after 300 seconds`. [Raw JSON](https://github.com/unikdahal/poc/blob/e83bf9e764a79f3fafdbf5f8d1bcfa84d63a1376/interop/result.json) · [Run 45556400465](https://github.com/unikdahal/poc/actions/runs/45556400465)

### Schema and partition evolution

**FACT.** Nested changes applied 9/9 statements. Readback matched Spark for True. [Raw JSON](https://github.com/unikdahal/poc/blob/e83bf9e764a79f3fafdbf5f8d1bcfa84d63a1376/evolution/result.json) · [Run 45553493787](https://github.com/unikdahal/poc/actions/runs/45553493787)

**FACT.** Partition evolution applied 6/6 statements. Readback matched Spark for True. [Raw JSON](https://github.com/unikdahal/poc/blob/e83bf9e764a79f3fafdbf5f8d1bcfa84d63a1376/evolution/result.json) · [Run 45553493787](https://github.com/unikdahal/poc/actions/runs/45553493787)

The partition metadata lists [{'spec_id': 0, 'files': 16}, {'spec_id': 2, 'files': 1}]. The Sail analyzed plan enumerated three Parquet file ranges across the old and evolved specs for `id=42 AND v='abcdef'`. [Spec IDs and analyzed plan](https://github.com/unikdahal/poc/blob/e83bf9e764a79f3fafdbf5f8d1bcfa84d63a1376/evolution/result.json)

### Write amplification and maintenance

| MERGE rounds | Data files | Positional delete files | Sail median read s | Spark median read s |
| --- | --- | --- | --- | --- |
| 1 | 5 | 1 | 0.029 | 0.352 |
| 5 | 9 | 5 | 0.042 | 0.463 |
| 20 | 24 | 20 | 0.165 | 0.713 |
| 50 | 54 | 50 | 0.746 | 1.051 |

**FACT.** The metadata growth fixture completed 500 commits. The checkpoint records current metadata bytes and current manifest count. The MERGE fixture uses hash distribution and a 1 MiB target size. [Raw JSON](https://github.com/unikdahal/poc/blob/e83bf9e764a79f3fafdbf5f8d1bcfa84d63a1376/maintenance/result.json) · [Run 45553493787](https://github.com/unikdahal/poc/actions/runs/45553493787)

| Commit | metadata.json bytes | Manifest count |
| --- | --- | --- |
| 1 | 1910 | 1 |
| 50 | 57854 | 50 |
| 100 | 114976 | 1 |
| 250 | 262663 | 52 |
| 500 | 509033 | 5 |

**FACT.** The compaction treatments called `rewrite_data_files` and `rewrite_position_delete_files`. The after-compaction metadata and the final read are in the raw evidence. [Maintenance raw JSON](https://github.com/unikdahal/poc/blob/e83bf9e764a79f3fafdbf5f8d1bcfa84d63a1376/maintenance/result.json)

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
| snapshot_spark_1 | None | None | None | None | — |
| snapshot_spark_2 | None | None | None | None | — |
| snapshot_sail_0 | None | None | None | None | — |
| snapshot_sail_1 | None | None | None | None | — |
| snapshot_sail_2 | None | None | None | None | — |

[Raw JSON](https://github.com/unikdahal/poc/blob/e83bf9e764a79f3fafdbf5f8d1bcfa84d63a1376/conflict/result.json) · [Run 45553493787](https://github.com/unikdahal/poc/actions/runs/45553493787)

### Coverage limits

The nested evolution treatment applied Spark DDL, then read old data with both engines. The maintenance evidence includes reported sort-order IDs and object sizes. It cannot generalize file layout to larger partitions or establish global range distribution. Serializable and snapshot results apply to the synthetic overlapping predicates used here.
