# Sail POC

An end-to-end, reproducible evaluation of [Sail](https://github.com/lakehq/sail) (LakeSail) as a
distributed SQL engine for Iceberg tables behind an Apache Polaris REST catalog, compared with
Apache Spark 3.5 (with and without Apache DataFusion Comet).

Everything runs on GitHub-hosted runners. All data is synthetic (TPC-H generated with
`tpchgen-cli`, plus small hand-written tables). No proprietary queries, data or configuration.

## Layout

| Path | What |
| --- | --- |
| `docker/sail/` | Builds the Sail CLI image from a pinned `lakehq/sail` commit |
| `compose/` | MinIO (S3), Apache Polaris (Iceberg REST catalog), Apache Celeborn |
| `harness/` | Python harness: catalog bootstrap, engine sessions, test suites, benchmarks |
| `k8s/` | Manifests for running Sail in `kubernetes-cluster` mode on kind |
| `.github/workflows/` | `build-sail` (image) and `poc` (all suites) |
| `results/` | Collected results from CI runs |

See `REPORT.md` for findings once runs have completed.

## Report

The written report of everything verified here: https://claude.ai/artifact/QDhSkD5gJQEsoXDE8mSok8 (raw JSON in `results/`).
