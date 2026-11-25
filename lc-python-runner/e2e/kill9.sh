#!/bin/bash
# Abrupt JVM termination: SIGKILL the executor JVM while workers run, then check that
#  (1) no worker process survives (bwrap --die-with-parent + PID namespace teardown), and
#  (2) the next JVM's plugin init removes the leftover cgroups.
set -uo pipefail
JARS=$(ls /opt/lcpy/jars/*.jar | tr '\n' ',')
run() {
  "$SPARK_HOME/bin/spark-submit" --master 'local[2]' --jars "$JARS" \
    --conf spark.ui.enabled=false --conf spark.lcpy.sandbox.mode=bwrap \
    --class org.apache.spark.sql.lcpy.LcTestMain /opt/lcpy/jars/lc-python-runner-tests.jar \
    --registry /opt/lcpy/registry --sandbox bwrap --only "$1" &
}
fail=0
run "\\[sandbox\\] task cancellation.*"   # long-ish sleepy query, we kill it mid-flight
pid=$!
for _ in $(seq 120); do
  ls -d /sys/fs/cgroup/lcpy/sandbox/w-* >/dev/null 2>&1 && break; sleep 0.5
done
ls -d /sys/fs/cgroup/lcpy/sandbox/w-* || { echo "no worker started"; exit 1; }
jvm=$(pgrep -f 'org.apache.spark.deploy.SparkSubmit' | head -1)
echo "SIGKILL JVM $jvm with workers: $(pgrep -af lcpy/worker.py | wc -l)"
kill -9 "$jvm"; wait "$pid" 2>/dev/null
sleep 3
left=$(pgrep -af 'lcpy/worker.py|bwrap' | grep -v pgrep || true)
if [[ -n $left ]]; then echo "FAIL: processes survived the JVM: $left"; fail=1; else echo "PASS: no worker survived SIGKILL of the JVM"; fi
stale=$(ls -d /sys/fs/cgroup/lcpy/sandbox/w-* 2>/dev/null | wc -l)
echo "stale cgroups after SIGKILL: $stale (expected > 0 before the next JVM starts)"
# Next JVM: the executor plugin's init removes them (local mode: the driver hosts the executor)
"$SPARK_HOME/bin/spark-submit" --master 'local[1]' --jars "$JARS" \
  --conf spark.ui.enabled=false --conf spark.lcpy.sandbox.mode=bwrap \
  --conf spark.plugins=org.apache.spark.sql.lcpy.LcPythonPlugin \
  --class org.apache.spark.sql.lcpy.LcTestMain /opt/lcpy/jars/lc-python-runner-tests.jar \
  --registry /opt/lcpy/registry --sandbox bwrap --only "\\[sandbox\\] no Arrow.*" >/dev/null 2>&1
stale=$(ls -d /sys/fs/cgroup/lcpy/sandbox/w-* 2>/dev/null | wc -l)
if [[ $stale != 0 ]]; then echo "FAIL: $stale stale cgroups not cleaned"; fail=1; else echo "PASS: stale cgroups removed by the next JVM"; fi
exit $fail
