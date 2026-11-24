# Collected results

Copied from the `results/*` branches after each CI run (JSON only; logs stay on the branches).

| Folder | Workflow run | What |
| --- | --- | --- |
| functional | 45477188179 | Iceberg differential suite, SQL compatibility, Flight SQL (Python), ADBC Java probe |
| cluster-run1 | 45472084098 | First shuffle-backend run (storage backend hung on q12 rep 2; job cancelled) |
| cluster-run2 | 45479879004 | Shuffle backends, overhead and head-of-line blocking |
| cluster-run3 | 45483085716 | Shuffle backends, overhead, Spark head-of-line baseline, storage soak (220 executions) |
| k8s-run1..3 | 45473360107, 45479879004, 45486050961 | Kubernetes (kind) suite; run 3 is the final one (own-worker kill, longer query) |
| bench-sf1 | 45473360107 | TPC-H-derived SF1, 3 reps |
| bench-sf10-run1/run2 | 45473360107, 45480369818 | SF10, 2 reps; run2 adds 4-stream throughput |
| bench-sf30 | 45480369818 | SF30, 2 reps |
