# Tenant identities and session isolation

Reproduce with `p3.yml`, `tasks=tenancy`. The suite command is `python -u -m harness.p3.tenancy` with `P3_VARIANT=isolation`.
The pinned Sail commit is `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`. The correctness reference is Spark 3.5.10 with Iceberg 1.12.0. Each raw file records the job revision, image digests, configuration and engine exit state where available.

**FACT.** The two Polaris principals each had a role and a separate catalog, namespace and bucket. The Sail tests exercised one configured identity and a second configuration with two preconfigured catalog aliases. Each action used its own synthetic fixture. [Raw JSON](https://github.com/unikdahal/poc/blob/c3b649375515aa3cd5c4539ddaad182f0ef0c201/isolation/result.json) · [Run 45555042191](https://github.com/unikdahal/poc/actions/runs/45555042191)

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

**FACT.** A tenant B session with a tenant B header could read, insert and drop tenant A's fixture through both protocols when the server was configured with tenant A's credential. Tenant B’s own catalog returned HTTP 403. Alias-specific credentials let both test sessions reach both catalogs. Session options and bearer metadata did not supply an independent catalog credential in these treatments. [Raw JSON](https://github.com/unikdahal/poc/blob/c3b649375515aa3cd5c4539ddaad182f0ef0c201/isolation/result.json) · [Run 45555042191](https://github.com/unikdahal/poc/actions/runs/45555042191)

**FACT.** Polaris REST returned 200 for each principal against its own table with vending, and 403 against the other principal's table. The vended credentials read a four-byte range from their own table's Parquet file (206), and returned 403 for the other table in the same bucket and for both target objects in the other bucket. Both direct authorized reads returned the Parquet prefix. [Raw JSON](https://github.com/unikdahal/poc/blob/c3b649375515aa3cd5c4539ddaad182f0ef0c201/isolation/result.json) · [Run 45555042191](https://github.com/unikdahal/poc/actions/runs/45555042191)

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

**FACT from code.** Flight SQL resolves requests to a fixed session and user ID. Spark Connect resolves a session from the request. The REST client builds the outgoing Authorization header from configured catalog credentials. [Flight](https://github.com/lakehq/sail/blob/790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f/crates/sail-flight/src/service.rs#L58-L69) · [Connect session actor](https://github.com/lakehq/sail/blob/790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f/crates/sail-session/src/session_manager/actor/handler.rs#L45) · [REST provider](https://github.com/lakehq/sail/blob/790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f/crates/sail-catalog-iceberg/src/provider.rs#L179-L193)

**HYPOTHESIS.** A gateway for the target stack would need an authenticated tenant-to-catalog binding and a resource admission policy. Separate Sail instances would allow independent credentials and resource limits. The latency experiment measures interference under one query shape and leaves per-tenant CPU-share attribution open.

### Coverage limits

The direct S3 tests establish the scope of Polaris-issued credentials. The Sail tests used a configured server credential and did not establish runtime use of separate per-session vended credentials. UDF registration and name visibility were probed. UDF execution from the 3.11 client encountered the pinned image's 3.14 version guard. Authorization on all Flight APIs and network-level tenant isolation had no separate treatment.
