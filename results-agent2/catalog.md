# Catalog scale and token rotation

Run `POLARIS_ACCESS_LOG=true POLARIS_TOKEN_LIFETIME=PT90S python -m harness.agent2.catalog`
with those environment variables also set when starting infrastructure and with
the pinned image `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`. Spark creates 100
tables in each of 20 namespaces and writes one row to each namespace's probe
table. The suite times first and warm operations across sessions. It counts
catalog endpoint requests from Polaris access logs for ten identical statements.

| Scope | Treatment | Observations |
| --- | --- | --- |
| SHOW TABLES, DESCRIBE, SELECT | Listing cache off and global | Pending runner |
| Ten statements | Spark and both Sail settings | Pending runner |
| Expired bearer token | Real HTTP 401 with atomic file rotation before retry | Pending runner |

The token probe requires a successful retry on the same endpoint within the
same statement. Execution outcomes and log excerpts are pending.
[Raw results](https://github.com/unikdahal/poc/tree/results/agent2-catalog/agent2-catalog)

The initial run created 2,000 tables. The ten replayed statements generated
25 catalog requests from Spark, 14 from Sail with caching disabled, and 10
with the global listing cache. The expiry probe observed a 401 followed by a
successful retry for the views listing. The parallel tables request failed
with `error sending request for url` before the full statement completed.
Corrected latency measurements and the full expired-token retry are pending.
[Initial raw results](https://github.com/unikdahal/poc/blob/559940e67204b7a77647890922cce628ec85fcf2/agent2-catalog/catalog.json)
