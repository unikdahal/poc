# SF10 Iceberg writes

Command `/opt/hostedtoolcache/Python/3.11.18/x64/bin/python -m harness.agent2.ingest`.

Workflow source revision `e1a382c1e2ae0dca85f40cfb507f9a95c0033663`. Sail image revision `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`.

Recorded versions include spark 3.5.10, iceberg 1.12.0, comet 1.2.0, python 3.11.18, adbc 1.13.0.

The raw JSON records engine settings, runner details and individual attempts.

The suite prepared 15000000 orders with operation counts `{'delete': 750000, 'insert': 750000, 'update': 1500000}`. Spark checked table content with `EXCEPT ALL` in both directions. The files column gives medians over the two warm outputs from the reference engine's `.files` metadata.

| Operation | Engine | Equality | Warm wall s/CPU s/peak MiB | Added files/MiB |
| --- | --- | --- | --- | --- |
| lineitem | spark | equal | 54.241/181.505/3,331.3 | 19.0 / 1,502.1 |
| lineitem | comet | equal | 51.004/169.580/3,929.9 | 19.0 / 1,502.1 |
| lineitem | sail | equal | 19.818/65.401/1,642.0 | 4.0 / 1,650.0 |
| lineitem | sail-cluster | unverified | —/—/— | — |
| orders | spark | unverified | —/—/— | — |
| orders | comet | unverified | —/—/— | — |
| orders | sail | unverified | —/—/— | — |
| orders | sail-cluster | unverified | —/—/— | — |
| orders v2 copy-on-write | spark | unverified | —/—/— | — |
| orders v2 copy-on-write | comet | unverified | —/—/— | — |
| orders v2 copy-on-write | sail | error | —/—/— | — |
| orders v2 copy-on-write | sail-cluster | unverified | —/—/— | — |
| orders v2 merge-on-read | spark | unverified | —/—/— | — |
| orders v2 merge-on-read | comet | unverified | —/—/— | — |
| orders v2 merge-on-read | sail | error | —/—/— | — |
| orders v2 merge-on-read | sail-cluster | unverified | —/—/— | — |
| orders v3 copy-on-write | spark | unverified | —/—/— | — |
| orders v3 copy-on-write | comet | unverified | —/—/— | — |
| orders v3 copy-on-write | sail | error | —/—/— | — |
| orders v3 copy-on-write | sail-cluster | error | —/—/— | — |
| orders v3 merge-on-read | spark | unverified | —/—/— | — |
| orders v3 merge-on-read | comet | unverified | —/—/— | — |
| orders v3 merge-on-read | sail | error | —/—/— | — |
| orders v3 merge-on-read | sail-cluster | error | —/—/— | — |

Rows without an equality verdict or warm timing lack complete verification. File-system write amplification beyond the Iceberg metadata was outside this run.
## Anomalies and log excerpts

```text
orders v2 copy-on-write, sail, sample 1 | SparkConnectGrpcException: <_MultiThreadedRendezvous of RPC that terminated with:
orders v2 copy-on-write, sail, sample 2 | SparkConnectGrpcException: <_MultiThreadedRendezvous of RPC that terminated with:
orders v2 merge-on-read, sail, sample 1 | SparkConnectGrpcException: <_MultiThreadedRendezvous of RPC that terminated with:
orders v3 copy-on-write, sail, sample 1 | SparkConnectGrpcException: <_MultiThreadedRendezvous of RPC that terminated with:
orders v3 copy-on-write, sail-cluster, sample 1 | SparkConnectGrpcException: <_MultiThreadedRendezvous of RPC that terminated with:
orders v3 copy-on-write, sail-cluster, sample 2 | SparkConnectGrpcException: <_MultiThreadedRendezvous of RPC that terminated with:
orders v3 merge-on-read, sail, sample 1 | SparkConnectGrpcException: <_MultiThreadedRendezvous of RPC that terminated with:
orders v3 merge-on-read, sail-cluster, sample 1 | SparkConnectGrpcException: <_MultiThreadedRendezvous of RPC that terminated with:
```

[Raw JSON](https://github.com/unikdahal/poc/blob/3c1ff1909f6d0feaabe6a95565cba0c5ec9fe2ea/agent2-ingest/ingest.json) · [Result files](https://github.com/unikdahal/poc/tree/3c1ff1909f6d0feaabe6a95565cba0c5ec9fe2ea/agent2-ingest) · [Workflow run](https://github.com/unikdahal/poc/actions/runs/45490127635)
