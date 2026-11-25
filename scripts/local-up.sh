#!/usr/bin/env bash
# Bring up the whole POC stack on one Ubuntu machine:
#   RustFS (S3) + Polaris (Iceberg REST catalog)            docker compose
#   Spark 3.5 + Iceberg + Comet, Spark Connect on :15002     host JVM
#   Sail, Spark Connect on :50051 and Flight SQL on :32010   docker
# then load TPC-H and run one query on each engine.
#
#   scripts/local-up.sh                 # everything, TPC-H SF1
#   SF=10 scripts/local-up.sh           # bigger data
#   ENGINES=sail scripts/local-up.sh    # skip Spark/Comet (data must already be loaded)
#   SAIL_IMAGE=my/sail:tag scripts/local-up.sh
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD

SF=${SF:-1}
ENGINES=${ENGINES:-comet,sail}             # comet = Spark + Comet; add "spark" for plain Spark on :15003
SPARK_MEMORY=${SPARK_MEMORY:-4g}            # driver heap; Comet adds 4g off-heap
SAIL_MEMORY=${SAIL_MEMORY:-}                # e.g. 8g to cap the Sail container (default: no cap)
export SAIL_IMAGE=${SAIL_IMAGE:-ghcr.io/unikdahal/poc/sail:790c622e5c4d}
export POC_OUT=${POC_OUT:-$ROOT/out}
export PYTHONPATH=$ROOT
VENV=$ROOT/.venv

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
need() { command -v "$1" >/dev/null || { echo "missing: $1 ($2)"; exit 1; }; }

say "checking prerequisites"
need docker "https://docs.docker.com/engine/install/ubuntu/"
docker compose version >/dev/null || { echo "missing: docker compose plugin (apt install docker-compose-plugin)"; exit 1; }
docker info >/dev/null 2>&1 || { echo "docker daemon not reachable: start it, or add yourself to the docker group"; exit 1; }
need curl "apt install curl"
need jq "apt install jq"
if [[ ",$ENGINES," == *,comet,* || ",$ENGINES," == *,spark,* ]]; then
  need java "apt install openjdk-17-jdk-headless"
  jv=$(java -version 2>&1 | awk -F'"' '/version/{print $2}' | cut -d. -f1)
  [[ "$jv" == 17 ]] || echo "warning: Java $jv found; Spark 3.5 + Comet were tested with Java 17"
fi
echo "cpus=$(nproc) mem=$(free -g | awk '/Mem/{print $2}')G disk_free=$(df -h . | awk 'NR==2{print $4}')"

say "python 3.11 environment"
if ! command -v uv >/dev/null; then curl -LsSf https://astral.sh/uv/install.sh | sh; export PATH=$HOME/.local/bin:$PATH; fi
[[ -x $VENV/bin/python ]] || uv venv -q -p 3.11 "$VENV"
# shellcheck disable=SC1091
source "$VENV/bin/activate"
cat > versions.json <<EOF
{"spark": "3.5.10", "iceberg": "1.12.0", "comet": "1.2.0", "adbc": "1.13.0"}
EOF
uv pip install -q "pyspark[connect]==3.5.10" requests psutil pyyaml setuptools boto3 tpchgen-cli \
  "adbc-driver-flightsql==1.13.0" "adbc-driver-manager==1.13.0" "pyarrow>=15,<18" "pandas<2.3" "numpy<2"
mkdir -p "$POC_OUT"

say "object store + catalog (docker compose)"
docker compose -f compose/docker-compose.yml up -d
for _ in $(seq 1 120); do curl -sf http://localhost:8182/q/health >/dev/null && break; sleep 2; done
curl -sf http://localhost:8182/q/health >/dev/null || { docker compose -f compose/docker-compose.yml logs --tail 80; exit 1; }
python -c "from harness.common import polaris_bootstrap; polaris_bootstrap()" >/dev/null
python -m harness.services token
# Polaris tokens expire after an hour; Sail reads the token file, so keep it fresh.
pkill -f "poc-token-refresher" 2>/dev/null || true
nohup bash -c "exec -a poc-token-refresher bash -c 'while sleep 600; do $VENV/bin/python -m harness.services token; done'" \
  >"$POC_OUT/token-refresher.log" 2>&1 &

if [[ ",$ENGINES," == *,comet,* ]]; then
  say "Spark 3.5 + Iceberg + Comet on :15002 (first start downloads jars, a few minutes)"
  python -m harness.services spark --comet --port 15002 --memory "$SPARK_MEMORY"
fi
if [[ ",$ENGINES," == *,spark,* ]]; then
  say "plain Spark 3.5 + Iceberg on :15003"
  python -m harness.services spark --port 15003 --memory "$SPARK_MEMORY"
fi

if [[ ",$ENGINES," == *,comet,* || ",$ENGINES," == *,spark,* ]]; then
  LOADER=15002; [[ ",$ENGINES," == *,comet,* ]] || LOADER=15003
  ns=tpch_sf${SF//./_}
  if python - "$LOADER" "$ns" <<'PY'
import sys
from harness.common import connect
s = connect(int(sys.argv[1]))
sys.exit(0 if s.sql(f"SHOW TABLES IN polaris.{sys.argv[2]}").count() >= 8 else 1)
PY
  then echo "polaris.$ns already loaded"
  else
    say "generating and loading TPC-H SF$SF into polaris.$ns"
    python -m harness.data --sf "$SF" --port "$LOADER" --root "$ROOT/data/tpch"
  fi
fi

if [[ ",$ENGINES," == *,sail,* ]]; then
  say "Sail ($SAIL_IMAGE)"
  docker image inspect "$SAIL_IMAGE" >/dev/null 2>&1 || docker pull "$SAIL_IMAGE"
  mem_arg=()
  [[ -n "$SAIL_MEMORY" ]] && mem_arg=(--memory "$SAIL_MEMORY")
  python -m harness.services sail --name sail --server spark --port 50051 "${mem_arg[@]}"
  python -m harness.services sail --name sail-flight --server flight --port 32010 "${mem_arg[@]}"
fi

say "first query on each engine"
python scripts/local-query.py --sf "$SF" --query 1 --engines "$ENGINES"

cat <<EOF

Ready.
  Spark + Comet   sc://localhost:15002      (pyspark: SparkSession.builder.remote("sc://localhost:15002"))
  Plain Spark     sc://localhost:15003      (only with ENGINES containing spark)
  Sail            sc://localhost:50051
  Sail Flight SQL grpc://localhost:32010    (ADBC / Arrow Flight SQL JDBC, no auth)
  S3 (RustFS)     http://localhost:9000     admin / password
  Polaris         http://localhost:8181     root / s3cr3t, catalog "poc", SQL catalog name "polaris"

  Compare engines: python scripts/local-query.py --sf $SF --query all
  Full benchmark:  python -m harness.suite_bench --sf $SF --engines comet,sail --reps 3
  Stop:            scripts/local-down.sh      (add --wipe to delete data)
Activate the venv first: source .venv/bin/activate && export PYTHONPATH=$ROOT
EOF
