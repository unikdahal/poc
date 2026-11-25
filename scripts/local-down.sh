#!/usr/bin/env bash
# Stop everything local-up.sh started. --wipe also deletes the S3/catalog volumes and generated data.
set -uo pipefail
cd "$(dirname "$0")/.."
export PYTHONPATH=$PWD
[[ -x .venv/bin/python ]] && .venv/bin/python -m harness.services stop-spark
pkill -f poc-token-refresher 2>/dev/null
pkill -f ui/server.py 2>/dev/null
docker rm -f sail sail-flight sail-bench 2>/dev/null
if [[ "${1:-}" == "--wipe" ]]; then
  docker compose -f compose/docker-compose.yml --profile celeborn down -v
  rm -rf data/tpch
else
  # stop, not down: keeps the containers. Polaris keeps its catalog in memory, so after a restart
  # local-up.sh re-creates the catalog and re-loads the tables from data/tpch (no regeneration).
  docker compose -f compose/docker-compose.yml --profile celeborn stop
fi
echo stopped
