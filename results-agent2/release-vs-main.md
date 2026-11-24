# SF10 release versus main

Command `/opt/hostedtoolcache/Python/3.11.18/x64/bin/python -m harness.agent2.release_vs_main`.

Workflow source revision `93af83585ba1c738e1b25a01dc6d149fa59017e0`. Sail image revision `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`.

Recorded versions include spark 3.5.10, iceberg 1.12.0, comet 1.2.0, python 3.11.18, adbc 1.13.0.

The raw JSON records engine settings, runner details and individual attempts.

Release tag commit `ac4690078021cf2a5402dc5c7b38a886f1916c73`. Wheel `b9a308f8216b65c15f0e3666482ba602e962ec25408d42570a18b35f090dfdda  pysail-0.7.3-cp38-abi3-manylinux2014_x86_64.manylinux_2_17_x86_64.whl`. The benchmark uses the same SF10 tables, resource settings and query loop for the release wheel and main image.

| Engine | Validated warm queries | Paired wall s | Paired CPU s | Paired peak MiB |
| --- | --- | --- | --- | --- |
| main | 19 | — | — | — |
| release-0.7.3 | 0 | — | — | — |
| spark | 22 | — | — | — |

The run has no complete paired comparison.

Source revision and LTO profile both differ. Their individual effects require a same-revision build comparison. Queries with missing or unequal results are excluded from the paired totals.
## Anomalies and log excerpts

```text
release-0.7.3 Q1 | Failed to load table tpch_sf10.lineitem: response error: status code 400 Bad Request
release-0.7.3 Q1 | Failed to load table tpch_sf10.lineitem: response error: status code 400 Bad Request
release-0.7.3 Q1 | Failed to load table tpch_sf10.lineitem: response error: status code 400 Bad Request
release-0.7.3 Q2 | Failed to load table tpch_sf10.part: response error: status code 400 Bad Request
release-0.7.3 Q2 | Failed to load table tpch_sf10.part: response error: status code 400 Bad Request
release-0.7.3 Q2 | Failed to load table tpch_sf10.part: response error: status code 400 Bad Request
release-0.7.3 Q3 | Failed to load table tpch_sf10.customer: response error: status code 400 Bad Request
release-0.7.3 Q3 | Failed to load table tpch_sf10.customer: response error: status code 400 Bad Request
release-0.7.3 Q3 | Failed to load table tpch_sf10.customer: response error: status code 400 Bad Request
release-0.7.3 Q4 | Failed to load table tpch_sf10.orders: response error: status code 400 Bad Request
release-0.7.3 Q4 | Failed to load table tpch_sf10.orders: response error: status code 400 Bad Request
release-0.7.3 Q4 | Failed to load table tpch_sf10.orders: response error: status code 400 Bad Request
release-0.7.3 Q5 | Failed to load table tpch_sf10.customer: response error: status code 400 Bad Request
release-0.7.3 Q5 | Failed to load table tpch_sf10.customer: response error: status code 400 Bad Request
release-0.7.3 Q5 | Failed to load table tpch_sf10.customer: response error: status code 400 Bad Request
```

[Raw JSON](https://github.com/unikdahal/poc/blob/bf0ce21e8c29a5f9b090aebdf70e4dbafd20b672/agent2-release-vs-main/release-vs-main.json) · [Result files](https://github.com/unikdahal/poc/tree/bf0ce21e8c29a5f9b090aebdf70e4dbafd20b672/agent2-release-vs-main) · [Workflow run](https://github.com/unikdahal/poc/actions/runs/45491701924)
