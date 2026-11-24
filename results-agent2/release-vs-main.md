# SF10 release versus main

Command `/opt/hostedtoolcache/Python/3.11.18/x64/bin/python -m harness.agent2.release_vs_main`.

Workflow source revision `bef24d6c067d7a9fab8975a01e5d5a95d6e790b9`. Sail image revision `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`.

Recorded versions include spark 3.5.10, iceberg 1.12.0, comet 1.2.0, python 3.11.18, adbc 1.13.0.

The raw JSON records engine settings, runner details and individual attempts.

Release tag commit `ac4690078021cf2a5402dc5c7b38a886f1916c73`. Wheel `b9a308f8216b65c15f0e3666482ba602e962ec25408d42570a18b35f090dfdda  pysail-0.7.3-cp38-abi3-manylinux2014_x86_64.manylinux_2_17_x86_64.whl`. The benchmark uses the same SF10 tables, resource settings and query loop for the release wheel and main image.

Both builds used the same read-only catalog proxy for the measured
comparison. The release requests credential vending during table
loads. A direct probe returned HTTP 200 without the delegation header
and HTTP 400 with it. The proxy removed the delegation header and
forwarded the read. The initial direct run failed all 22 release
queries. [Initial direct run](https://github.com/unikdahal/poc/blob/bf0ce21e8c29a5f9b090aebdf70e4dbafd20b672/agent2-release-vs-main/release-vs-main.json)

| Engine | Validated warm queries | Queries with value differences | Paired wall s | Paired CPU s | Paired peak MiB |
| --- | --- | --- | --- | --- | --- |
| main | 19 | 1, 8, 14 | 31.461 | 99.360 | 3,642.5 |
| release-0.7.3 | 19 | 1, 8, 14 | 35.308 | 102.920 | 3,359.0 |
| spark | 22 | — | — | — | — |

The release took 1.122× the main wall time across the 19 paired
queries. While collecting the suite, the proxy recorded 1,606
requests from the release, including 513 that carried the delegation
header, and 496 requests from main. The measured difference includes
the catalog traffic under the common route.

Source revision and LTO profile both differ. Their individual effects require a same-revision build comparison. Queries with missing or unequal results are excluded from the paired totals. Direct release timings remain unverified.

## Anomalies and log excerpts

```text
Catalog response | Credential vending was requested for table tpch_sf10.orders, but no credentials are available.
Release direct catalog read | AnalysisException: Failed to load table tpch_sf10.nation: response error: status code 400 Bad Request
```

[Raw JSON](https://github.com/unikdahal/poc/blob/246c33d085fa0100212acea07123641b84bd64fd/agent2-release-vs-main/release-vs-main.json) · [Result files](https://github.com/unikdahal/poc/tree/246c33d085fa0100212acea07123641b84bd64fd/agent2-release-vs-main) · [Workflow run](https://github.com/unikdahal/poc/actions/runs/45495413240)
