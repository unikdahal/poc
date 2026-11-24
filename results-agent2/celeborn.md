# SF1 Celeborn shuffle

Run `python -m harness.agent2.celeborn` with
`790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f` and the default Flight shuffle
and local-mode controls. The newest stable worker image was 0.7.0 in the
public registry when this comparison was prepared. The suite records
`RUST_LOG=info` over the full runs and reads per-stage timestamps from
`system.execution` for Q5, Q9, Q18 and Q21. Wall, CPU and peak memory
include the relevant server processes for each backend. Worker Prometheus
snapshots bracket each treatment.

| Treatment | Version | Observations |
| --- | --- | --- |
| Local and Flight shuffle controls | Pinned main | Pending runner |
| LZ4 vs. none, 1 GiB vs. 16 MiB splits, one vs. two workers | 0.6.3 | Pending runner |
| Default settings | 0.7.0 | Pending runner |
| Worker terminated during Q9 | 0.6.3 | Pending runner |

The cause of the previously observed slowdown, worker-loss outcome and log
excerpts are pending. Stage timings include scheduling where labeled.
[Raw results](https://github.com/unikdahal/poc/tree/results/agent2-celeborn/agent2-celeborn)
