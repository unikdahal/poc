# Operator-visible diagnostics and lifecycle

Reproduce with `p3.yml`, `tasks=ops`. The suite command is `python -u -m harness.p3.ops` with `P3_VARIANT=operator`.
The pinned Sail commit is `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`. The correctness reference is Spark 3.5.10 with Iceberg 1.12.0. Each raw file records the job revision, image digests, configuration and engine exit state where available.

The hosted treatment is still in progress. [Run](https://github.com/unikdahal/poc/actions/runs/45554682627)
