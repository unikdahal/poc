# Concurrent SF10 query streams

Run `python -m harness.agent2.concurrency` using the main image at
`790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`. Each stream has a separate
session and deterministic shuffled query order. Sessions warm with Q6 before
two full trials. Queries/hour counts only results matching Spark and uses
the full trial duration. Latency percentiles use the same validated results.

| Configuration | Streams | Queries/hour, p50/p95, CPU and peak memory |
| --- | --- | --- |
| Sail local | 1, 4, 8, 16 | Pending runner |
| Sail local-cluster | 1, 4, 8, 16 | Pending runner |
| Spark+Comet | 1, 4, 8, 16 | Pending runner |

Execution, errors, mismatches and log excerpts are pending.
[Raw results](https://github.com/unikdahal/poc/tree/results/agent2-concurrency/agent2-concurrency)
