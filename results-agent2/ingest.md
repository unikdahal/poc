# SF10 Iceberg writes

Run `python -m harness.agent2.ingest` with the pinned main image
`790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f` and the reference versions recorded
by setup. The suite times CTAS and MERGE separately. It prepares a deterministic
source with two update buckets, one delete bucket and one insert bucket out of
twenty equally sized buckets. The reference checks both directions with
`EXCEPT ALL` and reads the Iceberg `.files` metadata for current, added and
removed file counts and bytes.

| Operation | Configurations | Warm timings, files and equality |
| --- | --- | --- |
| CTAS | SF10 lineitem and orders, four engine configurations | Pending runner |
| MERGE | SF10 orders, CoW and MoR for v2 and v3 | Pending runner |

Execution, log excerpts and cross-engine equality are pending.
[Raw results](https://github.com/unikdahal/poc/tree/results/agent2-ingest/agent2-ingest)
