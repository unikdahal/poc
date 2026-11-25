#!/bin/bash
# Container entrypoint (PID 1). Runs as root ONLY to prepare the sandbox substrate, then drops
# to the unprivileged JVM user for good:
#   1. cgroup v2 delegation without systemd:
#        /                 (container cgroup-namespace root; processes moved out)
#        |- lcpy-init      every pre-existing process (the "no internal processes" rule)
#        `- lcpy           controllers cpu,memory,pids enabled for children
#           |- jvm         the executor JVM (cgroup.procs delegated)
#           `- sandbox     delegated subtree: the JVM user creates w-<id> per worker
#      Moving a launcher from lcpy/jvm to lcpy/sandbox/w-x needs write access to cgroup.procs of
#      the common ancestor (lcpy) -- delegated as well. Nothing above lcpy is writable by the JVM.
#   2. egress policy for sockets created in lcpy/sandbox (iptables cgroup v2 path match).
#      The JVM's cgroup (lcpy/jvm) is not matched: it keeps unrestricted network.
#   3. exec the command as $LCPY_USER with no capabilities.
set -euo pipefail

LCPY_USER=${LCPY_USER:-sparkuser}
CG=/sys/fs/cgroup
log() { echo "[lcpy-entrypoint] $*" >&2; }
die() { log "FATAL: $*"; exit 1; }

if [[ "$(id -u)" != 0 ]]; then
  die "must start as root to delegate cgroups and install the egress policy (privileged pod)"
fi
[[ -f $CG/cgroup.controllers ]] || die "cgroup v2 (unified hierarchy) is required at $CG"
if ! [[ -w $CG/cgroup.subtree_control ]]; then
  mount -o remount,rw "$CG" 2>/dev/null || die "$CG is read-only (pod must be privileged)"
fi
for c in cpu memory pids; do
  grep -qw "$c" "$CG/cgroup.controllers" || die "cgroup controller '$c' is not available"
done

# --- 1. cgroups ------------------------------------------------------------------------------
mkdir -p "$CG/lcpy-init"
# Retry: processes can appear while we move them (e.g. a probe from the runtime).
for _ in 1 2 3; do
  while read -r pid; do
    echo "$pid" > "$CG/lcpy-init/cgroup.procs" 2>/dev/null || true
  done < "$CG/cgroup.procs"
done
echo "+cpu +memory +pids" > "$CG/cgroup.subtree_control"
mkdir -p "$CG/lcpy/jvm" "$CG/lcpy/sandbox"
echo "+cpu +memory +pids" > "$CG/lcpy/cgroup.subtree_control"
echo "+cpu +memory +pids" > "$CG/lcpy/sandbox/cgroup.subtree_control"
# stale worker cgroups from a previous run of this container
for d in "$CG"/lcpy/sandbox/w-*; do
  [[ -d $d ]] || continue
  echo 1 > "$d/cgroup.kill" 2>/dev/null || true
  for _ in $(seq 50); do grep -q "populated 0" "$d/cgroup.events" && break; sleep 0.1; done
  rmdir "$d" 2>/dev/null || log "could not remove stale $d"
done
chown "$LCPY_USER:" "$CG/lcpy/cgroup.procs" "$CG/lcpy/jvm/cgroup.procs" \
  "$CG/lcpy/sandbox" "$CG/lcpy/sandbox/cgroup.procs" \
  "$CG/lcpy/sandbox/cgroup.subtree_control" "$CG/lcpy/sandbox/cgroup.threads"
echo $$ > "$CG/lcpy/jvm/cgroup.procs"
log "cgroups delegated: $CG/lcpy/sandbox -> $LCPY_USER (controllers: $(cat "$CG/lcpy/sandbox/cgroup.subtree_control"))"

# --- 2. egress policy ------------------------------------------------------------------------
/opt/lcpy/bin/lcpy-egress

# --- 3. unprivileged user namespaces must work for bwrap ----------------------------------------
if [[ -r /proc/sys/kernel/apparmor_restrict_unprivileged_userns ]] && \
   [[ "$(cat /proc/sys/kernel/apparmor_restrict_unprivileged_userns)" == 1 ]]; then
  log "WARNING: kernel.apparmor_restrict_unprivileged_userns=1 on this node; bwrap may fail" \
      "unless an AppArmor profile allows userns for /usr/bin/bwrap"
fi

log "dropping to $LCPY_USER: $*"
exec setpriv --reuid="$LCPY_USER" --regid="$LCPY_USER" --init-groups --inh-caps=-all -- "$@"
