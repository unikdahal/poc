# SF10 Iceberg writes

Command `/opt/hostedtoolcache/Python/3.11.18/x64/bin/python -m harness.agent2.ingest`.

Workflow source revision `368ce7fcd213faa230e1a69daee180e9a5c75ce8`. Sail image revision `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`.

Recorded versions include spark 3.5.10, iceberg 1.12.0, comet 1.2.0, python 3.11.18, adbc 1.13.0.

The raw JSON records engine settings, runner details and individual attempts.

The MERGE source applied 1,500,000 updates, 750,000 deletes and 750,000
inserts against 15,000,000 SF10 orders. Spark checked the final
successful output against the source for CTAS and an independently
derived expected table for MERGE. The check validated unique reference
keys, total row counts and all column values through a full outer join.
The files column gives medians over the two warm outputs from Spark's
`.files` metadata.

Twenty-three of the 24 engine-operation pairs produced an output that
matched the reference. Twenty-one completed all three executions and
have warm medians. Both Sail write modes completed the v2 and v3
merge-on-read cases. Three copy-on-write cases hit the client deadline
and recorded a container OOM kill.

| Operation | Engine | Equality | Warm wall s/CPU s/peak MiB | Added files/MiB |
| --- | --- | --- | --- | --- |
| lineitem | spark | equal | 39.442/125.900/4,227.2 | 19.0 / 1,502.1 |
| lineitem | comet | equal | 38.069/119.110/4,137.1 | 19.0 / 1,502.1 |
| lineitem | sail | equal | 15.245/50.684/1,423.6 | 4.0 / 1,650.0 |
| lineitem | sail-cluster | equal | 15.611/50.525/2,243.9 | 8.0 / 1,651.4 |
| orders | spark | equal | 12.569/25.470/3,555.9 | 6.0 / 362.9 |
| orders | comet | equal | 8.303/22.630/3,334.5 | 6.0 / 362.9 |
| orders | sail | equal | 3.752/9.797/1,314.1 | 4.0 / 399.9 |
| orders | sail-cluster | equal | 3.238/10.184/1,430.3 | 6.0 / 400.5 |
| orders v2 copy-on-write | spark | equal | 21.618/65.430/8,174.2 | 8.0 / 367.0 |
| orders v2 copy-on-write | comet | equal | 23.429/74.195/8,219.8 | 8.0 / 367.0 |
| orders v2 copy-on-write | sail | Earlier output equal, warm attempt timed out | —/—/— | — |
| orders v2 copy-on-write | sail-cluster | equal | 23.969/55.008/12,288.0 | 5.0 / 417.5 |
| orders v2 merge-on-read | spark | equal | 12.643/37.220/7,179.3 | 7.0 / 58.8 |
| orders v2 merge-on-read | comet | equal | 12.210/36.165/5,629.9 | 7.0 / 58.8 |
| orders v2 merge-on-read | sail | equal | 4.027/12.168/11,520.2 | 5.5 / 83.6 |
| orders v2 merge-on-read | sail-cluster | equal | 6.086/21.975/10,724.1 | 10.0 / 87.0 |
| orders v3 copy-on-write | spark | equal | 22.912/68.175/8,382.5 | 8.0 / 383.4 |
| orders v3 copy-on-write | comet | equal | 24.329/79.120/8,268.3 | 8.0 / 383.4 |
| orders v3 copy-on-write | sail | Cold output equal, warm attempt timed out | —/—/— | — |
| orders v3 copy-on-write | sail-cluster | First attempt timed out | —/—/— | — |
| orders v3 merge-on-read | spark | equal | 12.806/36.150/7,577.5 | 3.5 / 60.3 |
| orders v3 merge-on-read | comet | equal | 13.966/38.670/5,614.7 | 3.5 / 60.3 |
| orders v3 merge-on-read | sail | equal | 30.429/22.591/12,288.0 | 6.5 / 73.0 |
| orders v3 merge-on-read | sail-cluster | equal | 38.981/36.235/12,288.0 | 11.5 / 77.6 |

Warm copy-on-write timings remain unverified for the three failed cases.
The first run hit its 310-minute suite limit during verification. This
rerun started a fresh server for each case and verified the final successful
output. File-system write amplification beyond Iceberg's metadata
was outside the run. [Initial partial run](https://github.com/unikdahal/poc/actions/runs/45490127635)

## Anomalies and log excerpts

```text
orders v2 copy-on-write, sail, sample 3 | TimeoutError: deadline of 360s exceeded (client still active: True)
orders v3 copy-on-write, sail, sample 2 | TimeoutError: deadline of 360s exceeded (client still active: True)
orders v3 copy-on-write, sail-cluster, sample 1 | TimeoutError: deadline of 360s exceeded (client still active: True)
sail-merge_v2_copy-on-write | Docker OOMKilled=true, exit 137
sail-merge_v3_copy-on-write | Docker OOMKilled=true, exit 137
sail-cluster-merge_v3_copy-on-write | Docker OOMKilled=true, exit 137
```

[Raw JSON](https://github.com/unikdahal/poc/blob/ddf9395c3547dce6d7b73967d7820eaae66b5771/agent2-ingest/ingest.json) · [Result files](https://github.com/unikdahal/poc/tree/ddf9395c3547dce6d7b73967d7820eaae66b5771/agent2-ingest) · [Workflow run](https://github.com/unikdahal/poc/actions/runs/45534164779)
