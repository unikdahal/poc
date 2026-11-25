#!/bin/bash
# Runs INSIDE the container, as sparkuser (after the root entrypoint). Executes the full test
# runner with the real sandbox: semantics suites + sandbox enforcement suites.
set -uo pipefail
MODE=${1:-spark}           # spark | comet
REPORT=${2:-/out/report-sandbox-$MODE.json}

JARS=$(ls /opt/lcpy/jars/*.jar | tr '\n' ',')
CONF_ARGS=(
  --conf spark.ui.enabled=false
  --conf spark.lcpy.sandbox.mode=bwrap
  --conf spark.lcpy.debug.failOnLeak=true
  --conf "spark.driver.extraJavaOptions=-Dlog4j2.level=WARN -Darrow.memory.debug.allocator=true"
  --conf spark.executorEnv.LCPY_TEST_SECRET=lcpy-topsecret
)
export LCPY_TEST_SECRET=lcpy-topsecret     # must never be visible inside a worker
export AWS_SECRET_ACCESS_KEY=lcpy-topsecret

ARGS=(--registry /opt/lcpy/registry --python /usr/bin/python3 --sandbox bwrap --report "$REPORT")
[[ $MODE == comet ]] && ARGS+=(--comet)

"$SPARK_HOME/bin/spark-submit" --master 'local[2]' --driver-memory 2g \
  --jars "$JARS" "${CONF_ARGS[@]}" \
  --class org.apache.spark.sql.lcpy.LcTestMain /opt/lcpy/jars/lc-python-runner-tests.jar \
  "${ARGS[@]}"
