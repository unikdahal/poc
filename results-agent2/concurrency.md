# Concurrent SF10 query streams

Command `/opt/hostedtoolcache/Python/3.11.18/x64/bin/python -m harness.agent2.concurrency`.

Workflow source revision `800678056eea18d9122d9d7c7d5a96696fa19c34`. Sail image revision `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`.

Recorded versions include spark 3.5.10, iceberg 1.12.0, comet 1.2.0, python 3.11.18, adbc 1.13.0.

The raw JSON records engine settings, runner details and individual attempts.

Each session warmed with Q6 and then ran the same 22 queries in a deterministic shuffled order. Queries/hour uses Spark-matching results and divides by full trial wall time. P50 and p95 use the validated results. The table shows the median across two trials.

| Engine | Streams | Trials | Validated | Errors | Mismatches | Unverified | Queries/hour | P50 s | P95 s | CPU s | Peak MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| sail | 1 | 2 | 38 | 0 | 6 | 0 | 1,823.7 | 1.481 | 4.577 | 115.197 | 3,379.0 |
| sail | 4 | 2 | 152 | 0 | 24 | 0 | 1,932.2 | 4.447 | 18.777 | 495.961 | 8,107.0 |
| sail | 8 | 2 | 304 | 0 | 48 | 0 | 1,850.6 | 7.558 | 45.167 | 1,018.967 | 12,288.1 |
| sail | 16 | 1 | 15 | 16 | 3 | 0 | — | — | — | — | — |
| sail-cluster | 1 | 2 | 38 | 0 | 6 | 0 | 1,298.7 | 1.793 | 6.263 | 183.035 | 5,646.7 |
| sail-cluster | 4 | 2 | 152 | 0 | 24 | 0 | 1,331.1 | 7.039 | 20.161 | 739.782 | 11,153.9 |
| sail-cluster | 8 | 2 | 303 | 1 | 48 | 0 | 1,237.1 | 14.666 | 45.868 | 1,497.608 | 12,291.7 |
| sail-cluster | 16 | 1 | 0 | 16 | 2 | 0 | — | — | — | — | — |
| comet | 1 | 1 | 10 | 1 | 0 | 0 | — | — | — | — | — |
| comet | 4 | 0 | 0 | 0 | 0 | 0 | — | — | — | — | — |
| comet | 8 | 0 | 0 | 0 | 0 | 0 | — | — | — | — | — |
| comet | 16 | 0 | 0 | 0 | 0 | 0 | — | — | — | — | — |

Incomplete trials and completed executions without a Spark reference are visible in the raw results.
## Anomalies and log excerpts

```text
sail/16 Q21 | TimeoutError: deadline of 180s exceeded (client still active: True)
sail/16 Q5 | TimeoutError: deadline of 180s exceeded (client still active: True)
sail/16 Q4 | TimeoutError: deadline of 180s exceeded (client still active: True)
sail/16 Q3 | TimeoutError: deadline of 180s exceeded (client still active: True)
sail/16 Q8 | TimeoutError: deadline of 180s exceeded (client still active: True)
sail/16 Q9 | TimeoutError: deadline of 180s exceeded (client still active: True)
sail/16 Q3 | TimeoutError: deadline of 180s exceeded (client still active: True)
sail/16 Q18 | TimeoutError: deadline of 180s exceeded (client still active: True)
sail/16 Q9 | TimeoutError: deadline of 180s exceeded (client still active: True)
sail/16 Q9 | TimeoutError: deadline of 180s exceeded (client still active: True)
sail/16 Q21 | TimeoutError: deadline of 180s exceeded (client still active: True)
sail/16 Q18 | TimeoutError: deadline of 180s exceeded (client still active: True)
sail/16 Q21 | TimeoutError: deadline of 180s exceeded (client still active: True)
sail/16 Q21 | TimeoutError: deadline of 180s exceeded (client still active: True)
sail/16 Q5 | TimeoutError: deadline of 180s exceeded (client still active: True)
```

[Raw JSON](https://github.com/unikdahal/poc/blob/8dff639c62a924019135e7feccf2c44ff511fe43/agent2-concurrency/concurrency.json) · [Result files](https://github.com/unikdahal/poc/tree/8dff639c62a924019135e7feccf2c44ff511fe43/agent2-concurrency) · [Workflow run](https://github.com/unikdahal/poc/actions/runs/45490383284)
