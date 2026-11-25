#!/bin/bash
# Container CMD for CI: runs as sparkuser after the root entrypoint prepared cgroups + egress.
set -uo pipefail
echo "== whoami: $(id) | cgroup: $(cat /proc/self/cgroup) | bwrap $(bwrap --version)"
# Two "pod-local" services: 127.0.0.2 is allowlisted for workers, 127.0.0.1 is not.
python3 -m http.server 18080 --bind 127.0.0.1 >/dev/null 2>&1 &
python3 -m http.server 18081 --bind 127.0.0.2 >/dev/null 2>&1 &
sleep 1
rc=0
/opt/lcpy/e2e/run-in-container.sh spark /out/report-sandbox-spark.json || rc=$((rc + 1))
if ls /opt/lcpy/jars/comet*.jar >/dev/null 2>&1; then
  /opt/lcpy/e2e/run-in-container.sh comet /out/report-sandbox-comet.json || rc=$((rc + 1))
fi
/opt/lcpy/e2e/kill9.sh 2>&1 | grep -E "PASS|FAIL|SIGKILL|stale|no worker|before" | tee /out/kill9.txt
[[ ${PIPESTATUS[0]} == 0 ]] || rc=$((rc + 1))
echo "== e2e exit code: $rc"
exit $rc
