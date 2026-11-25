#!/bin/bash
# Abrupt JVM termination: SIGKILL the executor JVM while sandboxed workers are running, then check
#  (1) no worker process survives (bwrap --die-with-parent + PID-namespace teardown), and
#  (2) the next JVM's executor-plugin init removes the leftover (empty) cgroups.
set -uo pipefail
JARS=$(ls /opt/lcpy/jars/*.jar | tr '\n' ',')
submit() {
  "$SPARK_HOME/bin/spark-submit" --master "$1" --jars "$JARS" \
    --conf spark.ui.enabled=false --conf spark.lcpy.sandbox.mode=bwrap \
    --class org.apache.spark.sql.lcpy.LcTestMain /opt/lcpy/jars/lc-python-runner-tests.jar \
    --registry /opt/lcpy/registry --python /usr/bin/python3 --sandbox bwrap --only "$2"
}
fail=0
LCPY_KILL9=1 submit 'local[2]' '\[kill9\].*' >/tmp/kill9-run.log 2>&1 &
pid=$!
for _ in $(seq 240); do
  [[ $(pgrep -fc '^/usr/bin/python3 -I -B -u /lcpy/worker.py') -ge 1 ]] && break; sleep 0.5
done
sleep 1
echo "before SIGKILL: $(pgrep -fc '^/usr/bin/python3 -I -B -u /lcpy/worker.py') python worker(s), $(pgrep -fc '^/usr/bin/bwrap') bwrap, cgroups: $(ls -d /sys/fs/cgroup/lcpy/sandbox/w-* 2>/dev/null | wc -l)"
if [[ $(pgrep -fc '^/usr/bin/python3 -I -B -u /lcpy/worker.py') -lt 1 ]]; then
  echo "FAIL: no worker was running"; ps -eo pid,ppid,user,etimes,args | cut -c1-200
  grep -v '^\s*at ' /tmp/kill9-run.log | tail -60; exit 1
fi
jvm=$(pgrep -f 'org.apache.spark.deploy.SparkSubmit' | head -1)
kill -9 "$jvm"; wait "$pid" 2>/dev/null
sleep 2
left=$(pgrep -af 'lcpy/worker.py' | grep -v pgrep || true)
if [[ -n $left ]]; then echo "FAIL: processes survived the JVM: $left"; fail=1
else echo "PASS: no worker survived SIGKILL of the JVM"; fi
stale=$(ls -d /sys/fs/cgroup/lcpy/sandbox/w-* 2>/dev/null | wc -l)
echo "stale cgroups after SIGKILL: $stale (left behind: nobody ran cleanup)"
submit 'local[1]' '\[sandbox\] no Arrow.*' >/dev/null 2>&1
stale=$(ls -d /sys/fs/cgroup/lcpy/sandbox/w-* 2>/dev/null | wc -l)
if [[ $stale != 0 ]]; then echo "FAIL: $stale stale cgroups not cleaned"; fail=1
else echo "PASS: stale cgroups removed by the next JVM's plugin init"; fi
exit $fail
