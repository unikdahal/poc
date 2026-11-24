# Memory pressure

Run `python -m harness.agent2.memory` against the pinned image
`790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`. Each Q9, Q18 and Q21 case starts
a fresh server. The fair pool is limited to half the container limit. The
Spark+Comet cases set the JVM heap to the corresponding size and configure
off-heap memory separately at one quarter of the heap size.

| Scale factors | Limits (GiB) | Configurations | Outcomes |
| --- | --- | --- | --- |
| 10 and 30 | 2, 4, 8 | Sail unbounded and fair, Spark+Comet driver heap | Pending runner |

The job records container OOM status, sampled local spill files, Spark task
spill counters, timings and comparisons with Spark. Spark runs as a host
process, so the reported driver heap sizes do not impose a container limit.
Execution outcomes and log excerpts are pending.
[Raw results](https://github.com/unikdahal/poc/tree/results/agent2-memory/agent2-memory)
