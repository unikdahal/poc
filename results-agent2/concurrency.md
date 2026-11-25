# Concurrent SF10 query streams

Command `/opt/hostedtoolcache/Python/3.11.18/x64/bin/python -m harness.agent2.concurrency`.

Workflow source revision `4651a1c51eed03ceffada6a4a4ade59d535b045e`. Sail image revision `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`.

Recorded versions include spark 3.5.10, iceberg 1.12.0, comet 1.2.0, python 3.11.18, adbc 1.13.0.

The raw JSON records engine settings, runner details and individual attempts.

Each session warmed with Q6 and then ran the same 22 queries in a deterministic shuffled order. Queries/hour uses Spark-matching results and divides by full trial wall time. P50 and p95 use the validated results. The table shows the median across two trials.

| Engine | Streams | Trials | Validated | Errors | Mismatches | Unverified | Queries/hour | P50 s | P95 s | CPU s | Peak MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| sail | 1 | 2 | 38 | 0 | 6 | 0 | 1,517.9 | 1.766 | 5.035 | 139.312 | 3,407.9 |
| sail | 4 | 2 | 152 | 0 | 24 | 0 | 1,618.5 | 5.141 | 23.083 | 595.664 | 7,598.9 |
| sail | 8 | 2 | 304 | 0 | 48 | 0 | 1,587.5 | 9.537 | 53.333 | 1,206.933 | 12,288.0 |
| sail | 16 | 1 | 15 | 16 | 3 | 0 | — | — | — | — | — |
| sail-cluster | 1 | 2 | 38 | 0 | 6 | 0 | 1,072.3 | 2.205 | 7.723 | 223.865 | 5,527.3 |
| sail-cluster | 4 | 2 | 25 | 148 | 3 | 0 | — | — | — | — | — |
| sail-cluster | 8 | 0 | 0 | 0 | 0 | 0 | — | — | — | — | — |
| sail-cluster | 16 | 1 | 0 | 16 | 0 | 0 | — | — | — | — | — |
| comet | 1 | 2 | 44 | 0 | 0 | 0 | 597.6 | 3.974 | 13.122 | 434.385 | 7,030.9 |
| comet | 4 | 2 | 176 | 0 | 0 | 0 | 634.2 | 20.162 | 44.398 | 1,653.290 | 10,276.2 |
| comet | 8 | 2 | 352 | 0 | 0 | 0 | 633.7 | 43.009 | 84.562 | 3,230.365 | 11,716.9 |
| comet | 16 | 2 | 683 | 21 | 0 | 0 | 597.1 | 86.773 | 157.036 | 6,367.080 | 13,396.7 |

Incomplete trials and completed executions without a Spark reference are visible in the raw results.
The fresh-port rerun reached all 12 configurations before its
310-minute suite deadline. Spark+Comet completed both trials at every
stream count and recorded 21 query timeouts at 16 streams. Sail local
completed both trials through eight streams. Docker reported an OOM
kill for both Sail modes at 16 streams. Q1, Q8 and Q14 differed from
Spark in both Sail modes.

The clustered four-stream mode lost its worker pool after returning
25 validated results in the first trial. The second trial returned
zero validated results, so the report omits those warm medians.
The clustered eight-stream mode failed the readiness check with
`Pool not running`. The earlier run completed these two configurations
at 1,331.1 and 1,237.1 validated queries/hour respectively. Those
observations come from a separate runner and leave a same-runner
comparison at four and eight clustered streams unverified.
[First concurrency run](https://github.com/unikdahal/poc/blob/8dff639c62a924019135e7feccf2c44ff511fe43/agent2-concurrency/concurrency.json)

## Anomalies and log excerpts

```text
Docker | sail-16 and sail-cluster-16 OOMKilled=true, exit 137
Workflow | suite step timed out after 310 minutes
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

[Raw JSON](https://github.com/unikdahal/poc/blob/45d97647b9abc6528477f315fcc2c26b361d8cb9/agent2-concurrency/concurrency.json) · [Result files](https://github.com/unikdahal/poc/tree/45d97647b9abc6528477f315fcc2c26b361d8cb9/agent2-concurrency) · [Workflow run](https://github.com/unikdahal/poc/actions/runs/45501158882)
