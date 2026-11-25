# Tenant identities and session isolation

Reproduce with `p3.yml`, `tasks=tenancy`. The suite command is `python -m harness.p3.tenancy` with `P3_VARIANT=isolation`.
The pinned Sail commit is `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`. The correctness reference is Spark 3.5.10 with Iceberg 1.12.0. Each raw file records the job revision, image digests, configuration and engine exit state where available.

**FACT.** The two Polaris principals each had a role and a separate catalog, namespace and bucket. The Sail tests exercised one configured identity and a second configuration with two preconfigured catalog aliases. Each action used its own synthetic fixture. [Raw JSON](https://github.com/unikdahal/poc/blob/fadd0372c13476283dc5866ca82714af62457ce0/isolation/result.json) · [Run 45557689391](https://github.com/unikdahal/poc/actions/runs/45557689391)

### Catalog and storage authorization

| Protocol / treatment | Session header | Target catalog | Read | Insert | Drop |
| --- | --- | --- | --- | --- | --- |
| connect | tenant_a | tenant_a | ok | ok | ok |
| connect | tenant_a | tenant_b | 403 | 403 | 403 |
| connect | tenant_b | tenant_a | ok | ok | ok |
| connect | tenant_b | tenant_b | 403 | 403 | 403 |
| flight | tenant_a | tenant_a | ok | ok | ok |
| flight | tenant_a | tenant_b | 403 | 403 | 403 |
| flight | tenant_b | tenant_a | ok | ok | ok |
| flight | tenant_b | tenant_b | 403 | 403 | 403 |
| aliases | tenant_a | tenant_a | ok | ok | ok |
| aliases | tenant_a | tenant_b | ok | ok | ok |
| aliases | tenant_b | tenant_a | ok | ok | ok |
| aliases | tenant_b | tenant_b | ok | ok | ok |

**FACT.** A tenant B session with a tenant B header could read, insert and drop tenant A's fixture through both protocols when the server was configured with tenant A's credential. Tenant B’s own catalog returned HTTP 403. Alias-specific credentials let both test sessions reach both catalogs. Session options and bearer metadata did not supply an independent catalog credential in these treatments. [Raw JSON](https://github.com/unikdahal/poc/blob/fadd0372c13476283dc5866ca82714af62457ce0/isolation/result.json) · [Run 45557689391](https://github.com/unikdahal/poc/actions/runs/45557689391)

**FACT.** Polaris REST returned 200 for each principal against its own table with vending, and 403 against the other principal's table. The vended credentials read a four-byte range from their own table's Parquet file (206), and returned 403 for the other table in the same bucket and for both target objects in the other bucket. Both direct authorized reads returned the Parquet prefix. [Raw JSON](https://github.com/unikdahal/poc/blob/fadd0372c13476283dc5866ca82714af62457ce0/isolation/result.json) · [Run 45557689391](https://github.com/unikdahal/poc/actions/runs/45557689391)

### Session state and interference

| Protocol | Probe | Session A | Session B |
| --- | --- | --- | --- |
| Connect | Temp view | [[71]] | AnalysisException |
| Connect | Session timezone | Pacific/Honolulu | Etc/UTC |
| Connect | Catalog and namespace | [['tenant_a', 'isolated']] | [['tenant_a', 'default']] |
| Connect | Python UDF | IllegalArgumentException | UnsupportedOperationException |
| Flight SQL | Temp view | [[71]] | [[71]] |
| Flight SQL | Timezone | [['Pacific/Honolulu']] | [['Pacific/Honolulu']] |

**FACT.** Every heavy query used for these timings matched the Spark control. The values are medians of the warm per-trial p99 statistics for tenant B's `SELECT 1`. The raw file records the number of overlapped samples in each trial. Some local-cluster trials had only seven overlapped samples, making their p99 the sample maximum. [Raw JSON](https://github.com/unikdahal/poc/blob/c3b649375515aa3cd5c4539ddaad182f0ef0c201/isolation/result.json) · [Run 45555042191](https://github.com/unikdahal/poc/actions/runs/45555042191)

| Mode and container memory | Idle p99, ms | Under load p99, ms | Ratio |
| --- | --- | --- | --- |
| local-8g | 14.14 | 168.36 | 11.91 |
| local-cluster-8g | 15.28 | 223.15 | 14.61 |
| local-2g | 14.52 | 125.91 | 8.67 |

**FACT.** Per-session Spark catalog options produced the following reads: `AnalysisException: Table not found: [TABLE_OR_VIEW_NOT_FOUND] Table or view not found: per_session.isolated.anchor` · `AnalysisException: Table not found: [TABLE_OR_VIEW_NOT_FOUND] Table or view not found: per_session.isolated.anchor`. [Raw JSON](https://github.com/unikdahal/poc/blob/fadd0372c13476283dc5866ca82714af62457ce0/isolation/result.json) · [Run 45557689391](https://github.com/unikdahal/poc/actions/runs/45557689391)

**FACT.** For the Sail vending test the REST server used table-scoped credentials, then Sail was run with valid and invalid ambient object-store keys. sail-vending-invalid-tenant_a error · sail-vending-invalid-tenant_b error · sail-vending-valid-tenant_a ok · sail-vending-valid-tenant_b ok. [Raw JSON](https://github.com/unikdahal/poc/blob/fadd0372c13476283dc5866ca82714af62457ce0/isolation/result.json) · [Run 45557689391](https://github.com/unikdahal/poc/actions/runs/45557689391)

### High-cardinality aggregation interference

| Trial | Heavy result | Matches Spark | Peak memory bytes | OOMKilled | Other-session errors | Matched p99 ms | Evidence |
| --- | --- | --- | --- | --- | --- | --- | --- |
| memory-2g-rep-0 | ok | True | 1751138304 | False | 0 | 23.718 | [Raw JSON](https://github.com/unikdahal/poc/blob/fadd0372c13476283dc5866ca82714af62457ce0/memory-2g/result.json) · [Run 45561269348](https://github.com/unikdahal/poc/actions/runs/45561269348) |
| memory-2g-rep-1 | ok | True | 1566281728 | False | 0 | 26.258 | [Raw JSON](https://github.com/unikdahal/poc/blob/fadd0372c13476283dc5866ca82714af62457ce0/memory-2g/result.json) · [Run 45561269348](https://github.com/unikdahal/poc/actions/runs/45561269348) |
| memory-2g-rep-2 | ok | True | 1650446336 | False | 0 | 25.773 | [Raw JSON](https://github.com/unikdahal/poc/blob/fadd0372c13476283dc5866ca82714af62457ce0/memory-2g/result.json) · [Run 45561269348](https://github.com/unikdahal/poc/actions/runs/45561269348) |
| memory-8g-rep-0 | ok | True | 1482076160 | False | 0 | 16.022 | [Raw JSON](https://github.com/unikdahal/poc/blob/fadd0372c13476283dc5866ca82714af62457ce0/memory-8g/result.json) · [Run 45561269348](https://github.com/unikdahal/poc/actions/runs/45561269348) |
| memory-8g-rep-1 | ok | True | 1629298688 | False | 0 | 21.806 | [Raw JSON](https://github.com/unikdahal/poc/blob/fadd0372c13476283dc5866ca82714af62457ce0/memory-8g/result.json) · [Run 45561269348](https://github.com/unikdahal/poc/actions/runs/45561269348) |
| memory-8g-rep-2 | ok | True | 1410768896 | False | 0 | 15.910 | [Raw JSON](https://github.com/unikdahal/poc/blob/fadd0372c13476283dc5866ca82714af62457ce0/memory-8g/result.json) · [Run 45561269348](https://github.com/unikdahal/poc/actions/runs/45561269348) |
| memory-heavy-2g-rep-0 | ok | True | 2147483648 | False | 0 | 31.548 | [Raw JSON](https://github.com/unikdahal/poc/blob/fadd0372c13476283dc5866ca82714af62457ce0/memory-heavy-2g/result.json) · [Run 45561829768](https://github.com/unikdahal/poc/actions/runs/45561829768) |
| memory-heavy-2g-rep-1 | ok | True | 2147483648 | False | 0 | 13.976 | [Raw JSON](https://github.com/unikdahal/poc/blob/fadd0372c13476283dc5866ca82714af62457ce0/memory-heavy-2g/result.json) · [Run 45561829768](https://github.com/unikdahal/poc/actions/runs/45561829768) |
| memory-heavy-2g-rep-2 | ok | True | 2147483648 | False | 0 | 17.128 | [Raw JSON](https://github.com/unikdahal/poc/blob/fadd0372c13476283dc5866ca82714af62457ce0/memory-heavy-2g/result.json) · [Run 45561829768](https://github.com/unikdahal/poc/actions/runs/45561829768) |
| memory-heavy-8g-rep-0 | ok | True | 2995286016 | False | 0 | 58.965 | [Raw JSON](https://github.com/unikdahal/poc/blob/fadd0372c13476283dc5866ca82714af62457ce0/memory-heavy-8g/result.json) · [Run 45561829768](https://github.com/unikdahal/poc/actions/runs/45561829768) |
| memory-heavy-8g-rep-1 | ok | True | 2806157312 | False | 0 | 30.663 | [Raw JSON](https://github.com/unikdahal/poc/blob/fadd0372c13476283dc5866ca82714af62457ce0/memory-heavy-8g/result.json) · [Run 45561829768](https://github.com/unikdahal/poc/actions/runs/45561829768) |
| memory-heavy-8g-rep-2 | ok | True | 2970058752 | False | 0 | 22.217 | [Raw JSON](https://github.com/unikdahal/poc/blob/fadd0372c13476283dc5866ca82714af62457ce0/memory-heavy-8g/result.json) · [Run 45561829768](https://github.com/unikdahal/poc/actions/runs/45561829768) |

**FACT from code.** Flight SQL resolves requests to a fixed session and user ID. Spark Connect resolves a session from the request. The REST client builds the outgoing Authorization header from configured catalog credentials. [Flight](https://github.com/lakehq/sail/blob/790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f/crates/sail-flight/src/service.rs#L58-L69) · [Connect session actor](https://github.com/lakehq/sail/blob/790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f/crates/sail-session/src/session_manager/actor/handler.rs#L45) · [REST provider](https://github.com/lakehq/sail/blob/790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f/crates/sail-catalog-iceberg/src/provider.rs#L179-L193)

**FACT from code.** The server session factory builds a new runtime environment for each session. The runtime allocates its memory pool using the server configuration. In local-cluster mode the session also gets a local worker manager. All of these run within the tested server container's CPU and memory budget, which was shared by the two Connect sessions. [Session runtime](https://github.com/lakehq/sail/blob/790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f/crates/sail-session/src/session_factory/server.rs#L153-L162) · [Pool construction](https://github.com/lakehq/sail/blob/790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f/crates/sail-session/src/runtime.rs#L65-L83) · [Local-cluster session worker manager](https://github.com/lakehq/sail/blob/790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f/crates/sail-session/src/session_factory/job_runner.rs#L103-L118)

Upstream session-defined catalogs were proposed in [#2929](https://github.com/lakehq/sail/pull/2929) and use of vended S3 keys in execution was proposed in [#2932](https://github.com/lakehq/sail/pull/2932). Both proposals were closed without merge. The pinned provider disables default credential delegation because it does not use vended credentials. [Catalog initialization](https://github.com/lakehq/sail/blob/790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f/crates/sail-session/src/catalog.rs#L28-L98) · [Delegation default](https://github.com/lakehq/sail/blob/790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f/crates/sail-catalog-iceberg/src/provider.rs#L106-L119)

**HYPOTHESIS.** A gateway for the target stack would need an authenticated tenant-to-catalog binding and a resource admission policy. Separate Sail instances would allow independent credentials and resource limits. The latency experiment measures interference under one query shape and leaves per-tenant CPU-share attribution open.

### Coverage limits

The direct S3 tests establish the scope of Polaris-issued credentials. The invalid-key control failed both Sail reads at the object store, showing that enabling the delegation header still depended on the server's ambient storage credentials. UDF registration and name visibility were probed. UDF execution from the 3.11 client encountered the pinned image's 3.14 version guard. Authorization on all Flight APIs and network-level tenant isolation had no separate treatment.
