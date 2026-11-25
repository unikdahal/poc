# Storage and catalog faults

Reproduce with `p3.yml`, `tasks=faults-storage`. The suite command is `python -u -m harness.p3.faults_storage` with `P3_VARIANT=storage` or `catalog`.
The pinned Sail commit is `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`. The correctness reference is Spark 3.5.10 with Iceberg 1.12.0. Each raw file records the job revision, image digests, configuration and engine exit state where available.

The workload scans 1.8 million synthetic rows from an Iceberg table. The commit treatment adds a second disjoint block of 10,000 rows per fixture. Toxiproxy 2.12.0 applies downstream delay and TCP resets. The HTTP relay counts calls and returns an S3 503 on each 100th matching request. A separate Spark process reads the table after each commit treatment.

### Scan outcomes

| Treatment | Trials | Profiles matched Spark | S3 requests | Synthetic 503s | Injection observed | Matched warm median s | Evidence |
| --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | 3 | 3 | 460 | 0 | True | 0.351 | [Raw JSON](https://github.com/unikdahal/poc/blob/30107d1357a0811a76944c776e96953c38c979ca/storage/result.json) · [Run 45556883387](https://github.com/unikdahal/poc/actions/runs/45556883387) |
| latency-20 | 3 | 3 | 460 | 0 | True | 0.965 | [Raw JSON](https://github.com/unikdahal/poc/blob/30107d1357a0811a76944c776e96953c38c979ca/storage/result.json) · [Run 45556883387](https://github.com/unikdahal/poc/actions/runs/45556883387) |
| latency-100 | 3 | 3 | 460 | 0 | True | 3.571 | [Raw JSON](https://github.com/unikdahal/poc/blob/30107d1357a0811a76944c776e96953c38c979ca/storage/result.json) · [Run 45556883387](https://github.com/unikdahal/poc/actions/runs/45556883387) |
| http-503 | 3 | 3 | 464 | 4 | True | 0.361 | [Raw JSON](https://github.com/unikdahal/poc/blob/30107d1357a0811a76944c776e96953c38c979ca/storage/result.json) · [Run 45556883387](https://github.com/unikdahal/poc/actions/runs/45556883387) |
| scan-reset | 3 | 3 | 526 | 0 | True | 1.939 | [Raw JSON](https://github.com/unikdahal/poc/blob/30107d1357a0811a76944c776e96953c38c979ca/storage/result.json) · [Run 45556883387](https://github.com/unikdahal/poc/actions/runs/45556883387) |

### Commit and token outcomes

| Table | Fault observed | Client | Spark row state | Snapshot chain valid | Orphan data candidates | Client excerpt | Evidence |
| --- | --- | --- | --- | --- | --- | --- | --- |
| p3fault.test.commit_reset_0 | True | error | before | True | 1 | SparkRuntimeException: commit state unknown: Iceberg REST catalog commit for test.commit_reset_0 may have succeeded | [Raw JSON](https://github.com/unikdahal/poc/blob/30107d1357a0811a76944c776e96953c38c979ca/storage/result.json) · [Run 45556883387](https://github.com/unikdahal/poc/actions/runs/45556883387) |
| p3fault.test.commit_reset_1 | True | error | before | True | 1 | SparkRuntimeException: commit state unknown: Iceberg REST catalog commit for test.commit_reset_1 may have succeeded | [Raw JSON](https://github.com/unikdahal/poc/blob/30107d1357a0811a76944c776e96953c38c979ca/storage/result.json) · [Run 45556883387](https://github.com/unikdahal/poc/actions/runs/45556883387) |
| p3fault.test.commit_reset_2 | True | error | before | True | 1 | SparkRuntimeException: commit state unknown: Iceberg REST catalog commit for test.commit_reset_2 may have succeeded | [Raw JSON](https://github.com/unikdahal/poc/blob/30107d1357a0811a76944c776e96953c38c979ca/storage/result.json) · [Run 45556883387](https://github.com/unikdahal/poc/actions/runs/45556883387) |
| p3fault.test.catalog_down_0 | True | error | before | True | 1 | SparkRuntimeException: commit state unknown: Iceberg REST catalog commit for test.catalog_down_0 may have succeeded | [Raw JSON](https://github.com/unikdahal/poc/blob/30107d1357a0811a76944c776e96953c38c979ca/catalog/result.json) · [Run 45556883387](https://github.com/unikdahal/poc/actions/runs/45556883387) |
| p3fault.test.catalog_down_1 | True | error | before | True | 1 | SparkRuntimeException: commit state unknown: Iceberg REST catalog commit for test.catalog_down_1 may have succeeded | [Raw JSON](https://github.com/unikdahal/poc/blob/30107d1357a0811a76944c776e96953c38c979ca/catalog/result.json) · [Run 45556883387](https://github.com/unikdahal/poc/actions/runs/45556883387) |
| p3fault.test.catalog_down_2 | True | error | before | True | 1 | SparkRuntimeException: commit state unknown: Iceberg REST catalog commit for test.catalog_down_2 may have succeeded | [Raw JSON](https://github.com/unikdahal/poc/blob/30107d1357a0811a76944c776e96953c38c979ca/catalog/result.json) · [Run 45556883387](https://github.com/unikdahal/poc/actions/runs/45556883387) |

**FACT.** The token service reported a 120 s TTL. The direct REST control returned 200 before expiry and 401 after expiry. An active long-query client at expiry was observed as `False`. Rotated-token readback matched Spark as `True`. The long-query result was `error`. [Raw JSON](https://github.com/unikdahal/poc/blob/30107d1357a0811a76944c776e96953c38c979ca/catalog/result.json) · [Run 45556883387](https://github.com/unikdahal/poc/actions/runs/45556883387)

The pinned REST provider constructs each request from current credentials and retries once on HTTP 401. [REST retry code](https://github.com/lakehq/sail/blob/790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f/crates/sail-catalog-iceberg/src/provider.rs#L258-L281)

### Coverage limits

Snapshot verification compares the final aggregate and the snapshot parent chain. An orphan candidate is a data object absent from all retained file references. The procedure does not prove that every metadata or manifest object is collectible. The network faults target the pin's container and synthetic services.
