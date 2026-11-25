# Run the POC on your own Ubuntu machine

One script brings up everything: S3 storage, the Polaris catalog, Spark 3.5 + Iceberg + Comet, and Sail. It then loads TPC-H data and runs a first query on each engine.

## What you need

- Ubuntu 22.04 or 24.04, with at least 4 CPU cores and 16 GB of RAM. SF1 fits in 8 GB if you set `SPARK_MEMORY=2g`.
- About 10 GB free disk for SF1, or about 25 GB for SF10.
- Docker Engine with the compose plugin.
- Java 17, plus curl and jq.

Install the system packages:

```bash
sudo apt update && sudo apt install -y openjdk-17-jdk-headless curl jq git
# Docker: https://docs.docker.com/engine/install/ubuntu/ then
sudo usermod -aG docker $USER && newgrp docker
```

Python 3.11 is set up automatically by `uv` in `.venv`; your system Python is not touched.

## Start everything

```bash
git clone https://github.com/unikdahal/poc && cd poc
scripts/local-up.sh
```

The first run downloads the container images, the Spark jars (Iceberg, Comet, Spark Connect) and generates TPC-H SF1. This takes about 5–10 minutes, and later runs are faster.

| Endpoint | Address | Notes |
| --- | --- | --- |
| Spark 3.5 + Comet | `sc://localhost:15002` | Spark Connect |
| Plain Spark 3.5 | `sc://localhost:15003` | only when `ENGINES` includes `spark` |
| Sail | `sc://localhost:50051` | Spark Connect |
| Sail Flight SQL | `grpc://localhost:32010` | ADBC or the Arrow Flight SQL JDBC driver; no auth |
| S3 (RustFS) | `http://localhost:9000` | `admin` / `password` |
| Polaris | `http://localhost:8181` | `root` / `s3cr3t`; catalog `poc`, SQL catalog name `polaris` |

Tables live in `polaris.tpch_sf1.*` (or `tpch_sf10` and so on).

### Options

Set these as environment variables before the command:

| Variable | Default | Meaning |
| --- | --- | --- |
| `SF` | `1` | TPC-H scale factor (0.1, 1, 10, …) |
| `ENGINES` | `comet,sail` | Any of `comet`, `spark`, `sail` |
| `SPARK_MEMORY` | `4g` | Spark driver heap. Comet adds 4 GB off-heap on top. |
| `SAIL_MEMORY` | unset | Memory cap for the Sail container, e.g. `8g` |
| `SAIL_IMAGE` | `ghcr.io/unikdahal/poc/sail:790c622e5c4d` | Use your own image tag here |

If you already have the Sail image, pass its tag in `SAIL_IMAGE`. If pulling it from GHCR says `unauthorized`, either run `docker login ghcr.io -u <user>` with a token that has `read:packages`, or make the package public in GitHub's package settings.

## Query workbench (UI)

`local-up.sh` also starts a web UI at **http://localhost:8088**. Run it by hand with `python ui/server.py`; add `--host 0.0.0.0` to open it from another machine.

1. Tick the engines to compare: Spark + Comet, plain Spark (when started with `ENGINES=comet,spark,sail`), Sail over Spark Connect, and Sail over Flight SQL. A green dot means the engine is running.
2. Pick a namespace (e.g. `tpch_sf1`) so unqualified table names resolve, or write fully qualified names such as `polaris.tpch_sf1.orders`.
3. Write SQL, or choose a preset (the 22 TPC-H queries and a few probes). Ctrl/⌘ + Enter runs it. Several statements separated by `;` run in order, and the last one's result is compared.
4. Settings:
   - **Runs:** timed repetitions; the median is reported.
   - **Warm-up:** one untimed run first.
   - **Sequential or parallel:** parallel shows how the engines behave when they compete for the machine.
   - **Tolerance:** for numeric differences.
   - **Timeout:** interrupts a query that runs too long.
   - **Plans:** shows `EXPLAIN` output.

For each engine you get median and min wall time, every run, CPU-seconds, peak memory, rows, and how much slower it was than the fastest engine. CPU and memory are read from the Sail container's cgroup and from the Spark JVM's process tree.

The result comparison uses the first engine as the reference. It reports one of: identical, equal within tolerance, or different. Differences show the column types that differ, the largest absolute and relative numeric difference, the cells over tolerance, and rows found in only one engine. Rows are compared in order when the query has `ORDER BY`, and as a multiset otherwise.

**Run TPC-H suite** runs all 22 queries with the current settings and builds a table of time per engine, the speedup, result checks, and totals for wall time, CPU and peak memory. Each run and each suite can be exported as JSON or CSV. The sidebar shows the catalog (click a table for its columns, double-click to insert its name) and recent runs.

## Use it from code

```bash
source .venv/bin/activate && export PYTHONPATH=$PWD

python scripts/local-query.py --query all                 # 22 TPC-H queries, Comet vs Sail, with result check
python scripts/local-query.py --sql "SELECT count(*) FROM polaris.tpch_sf1.lineitem"
```

From your own PySpark code (client 3.5 or newer):

```python
from pyspark.sql import SparkSession
sail  = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
comet = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
sail.sql("SELECT l_returnflag, count(*) FROM polaris.tpch_sf1.lineitem GROUP BY 1").show()
```

Through Flight SQL, with ADBC:

```python
import adbc_driver_flightsql.dbapi as flight
cur = flight.connect("grpc://localhost:32010").cursor()
cur.execute("SELECT count(*) FROM polaris.tpch_sf1.orders"); print(cur.fetchall())
```

## Benchmarks and suites

There are two kinds of suite.

These start their own engines, so stop the dev servers first (`scripts/local-down.sh`, then `docker compose -f compose/docker-compose.yml up -d`) if memory is tight. The benchmark gives Spark 10 GB and Sail 12 GB / 4 CPUs by default.

```bash
python -m harness.suite_bench --sf 1 --engines spark,comet,sail,sail-cluster --reps 3
python -m harness.suite_cluster overhead     # latency and head-of-line blocking
```

These use the running dev servers. They compare Sail on `:50051` against whatever runs on `:15002` (Spark + Comet by default):

```bash
python -m harness.suite_iceberg              # Iceberg DML differential tests
python -m harness.suite_sql                  # SQL compatibility corpus
python -m harness.suite_flight               # Flight SQL probes against :32010
```

Results go to `out/`.

## Stop

```bash
scripts/local-down.sh          # stop; generated data stays in data/
scripts/local-down.sh --wipe   # also delete S3 contents, the catalog and the generated data
```

Polaris keeps its catalog in memory, so a restart forgets the tables. `local-up.sh` detects this, re-creates the catalog and reloads the tables from the Parquet files in `data/tpch` without generating them again.

## If something fails

- **Spark doesn't start:** look in `out/spark-connect-15002.log`. The first start downloads jars from Maven Central, which needs internet access. Spark 3.5 needs Java 17; Java 21 may work but is untested.
- **Sail queries fail with 401:** the Polaris token expired. A background refresher rewrites `/tmp/poc-tokens/polaris` every 10 minutes; restart it with `scripts/local-up.sh`.
- **`URL scheme is not allowed`:** Sail needs `AWS_ALLOW_HTTP=true`, which the harness sets.
- **Out of memory:** use a smaller `SF`, set `SPARK_MEMORY=2g`, or run one engine at a time with `ENGINES=comet` or `ENGINES=sail`.
