# SF10 release versus main

Run `python -m harness.agent2.release_vs_main`. It builds a container around
the published Linux x86-64 `pysail==0.7.3` wheel and checks the wheel's
SHA-256 before installing it. The release tag points to
`ac4690078021cf2a5402dc5c7b38a886f1916c73`. The already built main image is
`790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`. The engines run separately with
the same resource settings and SF10 data. The suite reuses
`harness.suite_bench.bench_engine` for the 22 queries and two warm samples.

| Build | Queries matching Spark | Warm wall, CPU and peak memory |
| --- | --- | --- |
| Release 0.7.3, fat LTO | Pending runner | Pending runner |
| Pinned main, thin LTO | Pending runner | Pending runner |

A measured difference combines source revision and build profile effects.
The individual LTO effect would require an additional same-revision build.
Execution and log excerpts are pending.
[Raw results](https://github.com/unikdahal/poc/tree/results/agent2-release-vs-main/agent2-release-vs-main)
