# Catalog scale and token rotation

Command `/opt/hostedtoolcache/Python/3.11.18/x64/bin/python -m harness.agent2.catalog`.

Workflow source revision `f5b2d5b5fa954f919fac5398966b53a303e5fc34`. Sail image revision `790c622e5c4d9a9e2b87bdfb8e5cb5a3a720035f`.

The runner used Spark 3.5.10, Iceberg 1.12.0 and Python 3.11.18. Polaris started with `POLARIS_ACCESS_LOG=true` and `POLARIS_TOKEN_LIFETIME=PT90S`.

The raw JSON records engine settings, runner details and individual attempts.

Spark created 2,000 tables in 20 namespaces. One table in each namespace contains a single synthetic row. Listing-cache scope varied while database and view listing caches remained disabled.

| Engine/cache | Operation | Phase | Spark matches | P50 ms | P95 ms | Median CPU ms | Median sampled peak MiB |
| --- | --- | --- | --- | --- | --- | --- | --- |
| spark | show | first | 20 | 38.781 | 84.030 | 130.000 | 971.2 |
| spark | show | warm_same_session | 20 | 36.293 | 68.499 | 110.000 | 975.8 |
| spark | show | warm_new_session | 20 | 35.359 | 61.224 | 120.000 | 979.7 |
| spark | describe | first | 20 | 43.650 | 61.364 | 125.000 | 982.8 |
| spark | describe | warm_same_session | 20 | 33.758 | 45.065 | 100.000 | 982.1 |
| spark | describe | warm_new_session | 20 | 36.952 | 61.966 | 105.000 | 984.9 |
| spark | select | first | 20 | 99.894 | 166.035 | 270.000 | 989.3 |
| spark | select | warm_same_session | 20 | 94.458 | 173.203 | 275.000 | 1,000.5 |
| spark | select | warm_new_session | 20 | 89.505 | 137.729 | 255.000 | 1,026.5 |
| sail-none | show | first | 20 | 5.773 | 8.328 | 3.092 | 100.4 |
| sail-none | show | warm_same_session | 20 | 5.696 | 7.710 | 2.932 | 100.4 |
| sail-none | show | warm_new_session | 20 | 6.074 | 8.838 | 3.090 | 100.4 |
| sail-none | describe | first | 20 | 7.670 | 10.854 | 2.696 | 100.4 |
| sail-none | describe | warm_same_session | 20 | 7.492 | 9.737 | 2.728 | 100.4 |
| sail-none | describe | warm_new_session | 20 | 7.562 | 8.452 | 2.762 | 100.4 |
| sail-none | select | first | 20 | 10.250 | 14.867 | 3.904 | 100.4 |
| sail-none | select | warm_same_session | 20 | 10.056 | 11.631 | 3.975 | 100.4 |
| sail-none | select | warm_new_session | 20 | 9.715 | 14.189 | 3.733 | 100.4 |
| sail-global | show | first | 20 | 5.232 | 6.593 | 2.905 | 100.4 |
| sail-global | show | warm_same_session | 20 | 4.976 | 5.705 | 2.634 | 100.4 |
| sail-global | show | warm_new_session | 20 | 5.028 | 7.002 | 2.697 | 100.5 |
| sail-global | describe | first | 20 | 6.756 | 7.876 | 2.633 | 100.4 |
| sail-global | describe | warm_same_session | 20 | 6.697 | 8.345 | 2.606 | 100.4 |
| sail-global | describe | warm_new_session | 20 | 6.942 | 8.045 | 2.665 | 100.5 |
| sail-global | select | first | 20 | 9.264 | 12.456 | 3.749 | 100.4 |
| sail-global | select | warm_same_session | 20 | 9.191 | 10.467 | 3.641 | 100.4 |
| sail-global | select | warm_new_session | 20 | 9.093 | 11.037 | 3.636 | 100.4 |

| Statement | SQL | Spark HTTP requests | Sail cache off | Sail global |
| --- | --- | --- | --- | --- |
| 1 | SHOW TABLES IN polaris.agent2_catalog_00 | 1 | 2 | 1 |
| 2 | DESCRIBE TABLE polaris.agent2_catalog_01.t000 | 1 | 1 | 1 |
| 3 | SELECT id FROM polaris.agent2_catalog_02.t000 | 6 | 1 | 1 |
| 4 | SHOW TABLES IN polaris.agent2_catalog_03 | 1 | 2 | 1 |
| 5 | DESCRIBE TABLE polaris.agent2_catalog_04.t000 | 1 | 1 | 1 |
| 6 | SELECT id FROM polaris.agent2_catalog_05.t000 | 6 | 1 | 1 |
| 7 | SHOW TABLES IN polaris.agent2_catalog_06 | 1 | 2 | 1 |
| 8 | DESCRIBE TABLE polaris.agent2_catalog_07.t000 | 1 | 1 | 1 |
| 9 | SELECT id FROM polaris.agent2_catalog_08.t000 | 6 | 1 | 1 |
| 10 | SHOW TABLES IN polaris.agent2_catalog_09 | 1 | 2 | 1 |

The ten queries generated 25 catalog requests from Spark, 14 from Sail with caching off, and 10 with the global cache. An overlapping background token refresh in the seventh window is preserved in the raw trace and excluded from the query count. The warm same-session SHOW TABLES median fell from 5.696 ms to 4.976 ms. Each SELECT used one catalog request in both cache settings.

The token endpoint advertised a 90-second lifespan. A direct request after waiting returned HTTP 401. Both rejected listing requests returned HTTP 200 after Sail reread the rotated file. The statement returned the same 100 table names.

The test covers one expiration and rotation, a single runner and the synthetic catalog.

## Anomalies and log excerpts

```text
200 /api/catalog/v1/config old bearer
200 /api/catalog/v1/poc/namespaces/agent2%5Fcatalog%5F00/views old bearer
200 /api/catalog/v1/poc/namespaces/agent2%5Fcatalog%5F00/tables old bearer
401 /api/catalog/v1/poc/namespaces/agent2%5Fcatalog%5F00/tables old bearer
401 /api/catalog/v1/poc/namespaces/agent2%5Fcatalog%5F00/views old bearer
200 /api/catalog/v1/poc/namespaces/agent2%5Fcatalog%5F00/views rotated bearer
200 /api/catalog/v1/poc/namespaces/agent2%5Fcatalog%5F00/tables rotated bearer
```

[Raw JSON](https://github.com/unikdahal/poc/blob/669e918ca50df7ea2c5cd5f4e275ee400d1c6bf2/agent2-catalog/catalog.json) · [Result files](https://github.com/unikdahal/poc/tree/669e918ca50df7ea2c5cd5f4e275ee400d1c6bf2/agent2-catalog) · [Workflow run](https://github.com/unikdahal/poc/actions/runs/45492229100)
