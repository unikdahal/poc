# Iceberg reader and writer behavior

Reproduce with `p3.yml`, `tasks=iceberg-deep`. The suite command is `python -m harness.p3.iceberg_deep` with the interop, evolution, maintenance and conflict matrix variants.
The pinned Sail commit is `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`. The correctness reference is Spark 3.5.10 with Iceberg 1.12.0. Each raw file records the job revision, image digests, configuration and engine exit state where available.

### Format and delete interoperability

[Raw JSON](https://github.com/unikdahal/poc/blob/89c31bc4e602959784e9d21763767b0c1afc975f/interop/result.json) · [Run 45558181996](https://github.com/unikdahal/poc/actions/runs/45558181996)

| Writer and fixture | Spark 3.5.10 | Sail | PyIceberg 0.10.0 | Flink 1.20.4 |
| --- | --- | --- | --- | --- |
| flink_equality_v2 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `polaris`.`p3_iceberg`.`flink_equality_ | — | NoSuchTableError: NoSuchTableException: Table does not exist: p3_iceberg.flink_equality_v2 | RuntimeError: WARNING: Unknown module: jdk.compiler specified to --add-exports WARNING: Unknown module |
| flink_v2 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `polaris`.`p3_iceberg`.`flink_v2` canno | — | NoSuchTableError: NoSuchTableException: Table does not exist: p3_iceberg.flink_v2 | RuntimeError: WARNING: Unknown module: jdk.compiler specified to --add-exports WARNING: Unknown module |
| flink_v3 | AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] The table or view `polaris`.`p3_iceberg`.`flink_v3` canno | — | NoSuchTableError: NoSuchTableException: Table does not exist: p3_iceberg.flink_v3 | RuntimeError: WARNING: Unknown module: jdk.compiler specified to --add-exports WARNING: Unknown module |
| pyiceberg_v2 | match | — | match | RuntimeError: WARNING: Unknown module: jdk.compiler specified to --add-exports WARNING: Unknown module |
| pyiceberg_v3 | differs | — | differs | RuntimeError: WARNING: Unknown module: jdk.compiler specified to --add-exports WARNING: Unknown module |
| sail_delete_v2 | match | — | match | RuntimeError: WARNING: Unknown module: jdk.compiler specified to --add-exports WARNING: Unknown module |
| sail_delete_v3 | match | — | match | RuntimeError: WARNING: Unknown module: jdk.compiler specified to --add-exports WARNING: Unknown module |
| sail_v2 | match | — | match | RuntimeError: WARNING: Unknown module: jdk.compiler specified to --add-exports WARNING: Unknown module |
| sail_v3 | match | — | match | RuntimeError: WARNING: Unknown module: jdk.compiler specified to --add-exports WARNING: Unknown module |
| spark_delete_v2 | match | — | match | RuntimeError: WARNING: Unknown module: jdk.compiler specified to --add-exports WARNING: Unknown module |
| spark_delete_v3 | match | — | match | RuntimeError: WARNING: Unknown module: jdk.compiler specified to --add-exports WARNING: Unknown module |
| spark_v2 | match | — | match | RuntimeError: WARNING: Unknown module: jdk.compiler specified to --add-exports WARNING: Unknown module |
| spark_v3 | match | — | match | RuntimeError: WARNING: Unknown module: jdk.compiler specified to --add-exports WARNING: Unknown module |

Position deletes and deletion vectors are distinguished using the content type, file format and offsets in Spark's metadata table. The raw file records writer failures independently from reader results.

| Fixture | Delete records in .files | Formats | Content codes |
| --- | --- | --- | --- |
| spark_delete_v2 | 2 | ['PARQUET'] | [1] |
| spark_delete_v3 | 2 | ['PUFFIN'] | [1] |
| sail_delete_v2 | 1 | ['PARQUET'] | [1] |
| sail_delete_v3 | 1 | ['PUFFIN'] | [1] |
| flink_equality_v2 | 0 | [] | [] |

### Schema and partition evolution

**FACT.** Nested changes applied 9/9 statements. Readback matched Spark for True. [Raw JSON](https://github.com/unikdahal/poc/blob/89c31bc4e602959784e9d21763767b0c1afc975f/evolution/result.json) · [Run 45553493787](https://github.com/unikdahal/poc/actions/runs/45553493787)

**FACT.** Partition evolution applied 6/6 statements. Readback matched Spark for True. [Raw JSON](https://github.com/unikdahal/poc/blob/89c31bc4e602959784e9d21763767b0c1afc975f/evolution/result.json) · [Run 45553493787](https://github.com/unikdahal/poc/actions/runs/45553493787)

The partition metadata lists [{'spec_id': 0, 'files': 16}, {'spec_id': 2, 'files': 1}]. The Sail analyzed plan enumerated three Parquet file ranges across the old and evolved specs for `id=42 AND v='abcdef'`. [Spec IDs and analyzed plan](https://github.com/unikdahal/poc/blob/89c31bc4e602959784e9d21763767b0c1afc975f/evolution/result.json)

### Write amplification and maintenance

| MERGE rounds | Data files | Positional delete files | Sail median read s | Spark median read s |
| --- | --- | --- | --- | --- |
| 1 | 5 | 1 | 0.029 | 0.352 |
| 5 | 9 | 5 | 0.042 | 0.463 |
| 20 | 24 | 20 | 0.165 | 0.713 |
| 50 | 54 | 50 | 0.746 | 1.051 |

**FACT.** The metadata growth fixture completed 500 commits. The checkpoint records current metadata bytes and current manifest count. The MERGE fixture uses hash distribution and a 1 MiB target size. [Raw JSON](https://github.com/unikdahal/poc/blob/89c31bc4e602959784e9d21763767b0c1afc975f/maintenance/result.json) · [Run 45553493787](https://github.com/unikdahal/poc/actions/runs/45553493787)

| Commit | metadata.json bytes | Manifest count |
| --- | --- | --- |
| 1 | 1910 | 1 |
| 50 | 57854 | 50 |
| 100 | 114976 | 1 |
| 250 | 262663 | 52 |
| 500 | 509033 | 5 |

**FACT.** The compaction treatments called `rewrite_data_files` and `rewrite_position_delete_files`. The after-compaction metadata and the final read are in the raw evidence. [Maintenance raw JSON](https://github.com/unikdahal/poc/blob/89c31bc4e602959784e9d21763767b0c1afc975f/maintenance/result.json)

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
| snapshot_spark_1 | ok | error | True | [{'v': 1, 'n': 100}] | ok |
| snapshot_spark_2 | ok | error | True | [{'v': 1, 'n': 100}] | ok |
| snapshot_sail_0 | ok | error | True | [{'v': 1, 'n': 100}] | ok |
| snapshot_sail_1 | ok | error | True | [{'v': 1, 'n': 100}] | ok |
| snapshot_sail_2 | error | ok | True | [{'v': 1, 'n': 100}] | ok |

[Raw JSON](https://github.com/unikdahal/poc/blob/89c31bc4e602959784e9d21763767b0c1afc975f/conflict/result.json) · [Run 45553493787](https://github.com/unikdahal/poc/actions/runs/45553493787) · [Raw JSON](https://github.com/unikdahal/poc/blob/89c31bc4e602959784e9d21763767b0c1afc975f/conflict-snapshot/result.json) · [Run 45559915681](https://github.com/unikdahal/poc/actions/runs/45559915681)

### Physical file ordering

**FACT.** The layout fixture observed 16 files, with 16 files sorted by id and 16 files matching their declared partition. File sizes ranged from 310512 to 589586 bytes. Logical readback matched Spark as `True`. [Raw JSON](https://github.com/unikdahal/poc/blob/89c31bc4e602959784e9d21763767b0c1afc975f/layout/result.json) · [Run 45558533058](https://github.com/unikdahal/poc/actions/runs/45558533058)

### Coverage limits

The nested evolution treatment applied Spark DDL, then read old data with both engines. The maintenance evidence includes reported sort-order IDs and object sizes. Physical ordering was limited to the partitioned layout fixture. Serializable and snapshot results apply to the synthetic overlapping predicates used here.
