# `lc_python_runner`: sandboxed Python inside Spark 3.5 + DataFusion Comet 1.2

Untrusted, registered Python functions called from any Spark SQL query:

```sql
SELECT id, upper(lc_python_runner(my_mod.score(col1, col2 * 1.18, coalesce(col3, 'default'))).label)
FROM t
WHERE lc_python_runner(my_mod.score(col1, col2 * 1.18, coalesce(col3, 'default'))).valid;

SELECT lower(region), lc_python_runner(my_mod.summarize(v), lower(region)).total FROM t;
```

The functions receive Arrow data and return structs. Every invocation runs in a bubblewrap
sandbox with its own cgroup v2 limits, seccomp filter and cgroup-scoped egress allowlist. It
stays inside mixed Spark/Comet physical plans without row/columnar thrashing.

**Status:** everything below is implemented in this directory and exercised on GitHub-hosted
runners by [`.github/workflows/lc-python-runner.yml`](../.github/workflows/lc-python-runner.yml).
Section 7 separates what CI verified from what is design reasoning only.

---

## 1. Architecture at a glance

```
 SQL text / expr()                      Catalyst                               Executor (privileged pod)
 ─────────────────                      ────────                               ─────────────────────────
 lc_python_runner(m.f(a, b*2), k)  ─►  LcParser (wraps Spark's parser)
                                        UnresolvedLcCall
                                     ─► ResolveLcCalls (registry: mode, return struct, code sha256)
                                        LcCall (Unevaluable, never executed)
                                     ─► ExtractLcCalls (post-hoc, analysis time)
                                        LcPythonEval      (1-to-1: child.output ++ result)
                                        LcPythonAggregate (N-to-1: keys ++ results)
                                     ─► PushFiltersBelowLcEval (optimizer)
                                     ─► LcStrategy ──► LcPythonEvalExec / LcPythonAggregateExec
                                        [+ CometSparkToColumnarExec wrapper when Comet is on]
                                     ─► LcColumnarRule (strip transitions around us)
                                                                                JVM (sparkuser, cgroup lcpy/jvm)
                                                                                 ├─ writer thread: upstream batches
                                                                                 │   ─► owned Arrow ─► IPC ─► stdin
                                                                                 ├─ task thread: stdout IPC ─► batches
                                                                                 └─ lcpy-launch ─► bwrap ─► python3 worker.py
                                                                                    (cgroup lcpy/sandbox/w-<task>)
```

| Piece | File |
| --- | --- |
| Parser, unresolved/resolved expressions | `LcParser.scala`, `Expressions.scala` |
| Registry (manifests, code hash) | `Registry.scala` |
| Resolution, extraction, validation, filter pushdown | `Analysis.scala` |
| Logical / physical operators, strategy, columnar rule, Comet bridge | `LogicalNodes.scala`, `Physical.scala` |
| Columnar ingest (Spark Arrow, Comet shaded Arrow, any ColumnVector, rows) | `execution/arrow/LcArrowIngest.scala` |
| Runners: writer/reader threads, back-pressure, teardown, memory | `Runner.scala` |
| Worker process, cgroup teardown, failure reports | `Worker.scala` |
| Extension and executor plugin, shutdown hooks | `LcPythonExtensions.scala` |
| Python worker (in the jar) | `src/main/resources/lcpy/worker.py` |
| Container runtime: entrypoint, launcher, egress, seccomp | `sandbox/` |
| Tests | `src/test/scala/.../LcTests.scala`, `registry/`, `e2e/` |

Enable it with:

```
spark.sql.extensions = org.apache.spark.sql.lcpy.LcPythonExtensions   (order relative to Comet's does not matter)
spark.plugins        = org.apache.spark.sql.lcpy.LcPythonPlugin       (executor lifecycle hooks)
spark.lcpy.registry.dir = /opt/lcpy/registry                           (same path on driver and executors)
```

## 2. SQL surface and Catalyst

### 2.1 Parsing

Spark's grammar already accepts `lc_python_runner(my_mod.score(a, b), k)`, because a qualified
name is a valid function call. Left alone, though, the analyzer would look up `my_mod.score` in
the catalog and fail with `UNRESOLVED_ROUTINE`. `LcParser` delegates to the session parser and
post-processes the tree (including subquery plans, `expr()` / `selectExpr()` and persisted view
text). Each `UnresolvedFunction("lc_python_runner", <UnresolvedFunction(mod.fn, args)> +: groupBy)`
becomes an `UnresolvedLcCall(mod, fn, args, groupBy)`. The rewrite is bottom-up, so nested calls
work at any depth. The arguments stay ordinary Catalyst expressions and resolve like any others.
That is why `col2 * 1.18`, `coalesce(...)` and `date_trunc('day', ts)` need no special handling.

### 2.2 Resolution and the registry

`ResolveLcCalls` (resolution batch) binds a call once its children are resolved. It reads
`<registry>/<module>/manifest.json`:

```json
{"functions": {"score":     {"mode": "row",     "returns": "label string, score double, valid boolean"},
               "summarize": {"mode": "grouped", "returns": "n bigint, total double"}}}
```

`returns` is a Spark DDL struct, so the call's `dataType` is that struct. `.label`, `.score + 10`
and `WHERE ... .valid` then resolve through Spark's normal `ExtractValue` machinery. The driver
also records a **sha256 of the module directory** in the plan. The worker recomputes it inside the
sandbox and refuses to run if the code changed between planning and execution, or differs
between the driver's and an executor's copy of the registry.

How the registry reaches executors (baked into the image, a synced volume, an init container) is
a deployment choice. The code only needs the same path on the driver and executors, and binds it
read-only into the sandbox.

### 2.3 Extraction: nesting, deduplication, cardinality

`ExtractLcCalls` is a **post-hoc resolution** rule, so it runs during analysis, not in the
optimizer. It removes every `LcCall` and replaces it with an attribute produced by a dedicated
logical operator:

* **Innermost first.** `lc(m.outer(lc(m.inner(a)).x + 1))` becomes
  `LcPythonEval(outer, Project(... x + 1 AS arg ..., LcPythonEval(inner, ...)))`.
  Argument expressions are computed by a `Project` below the operator. The operator itself only
  ever selects columns, and Comet can run that `Project` natively.
* **Redundant-call elimination.** Calls are keyed by `(spec incl. code hash, canonicalized args,
  canonicalized group keys)`. One evaluation serves every reference in the same SELECT, and also
  across a `Filter` and the `Project` above it (the WHERE + SELECT example evaluates once; the
  test asserts one operator in the plan). `semanticEquals` is not usable here because it requires
  determinism, which we deliberately do not claim for Python code.
* **N-to-1** (`mode: grouped`) turns its projection into an aggregation, `LcPythonAggregate(keys,
  args) → keys ++ results`. The rest of the SELECT list may only reference the `<group_by>`
  expressions (matched semantically, so `lower(region)` in the SELECT matches the key
  `lower(region)`) and the call results. Row calls inside the grouped call's arguments run below
  it; row calls over its results run above it. Several N-to-1 calls in one SELECT share one
  operator and one worker only if their keys are identical.

Why analysis time: an N-to-1 call changes cardinality. Left as an expression inside a `Project`,
optimizer rules would treat it as row-preserving. Column pruning, for example, would drop an
unused call and silently turn "one row per group" into "one row per input row".

`PushFiltersBelowLcEval` (optimizer) moves deterministic conjuncts that touch only pass-through
columns below a 1-to-1 operator, so the sandbox never sees rows that get filtered out anyway.
This relies on the 1-to-1 contract being row-local (it can be disabled with
`spark.lcpy.optimizer.pushFilters=false`).

### 2.4 Clean analysis-time failures

| Construct | Behaviour |
| --- | --- |
| Column outside `<group_by>` next to an N-to-1 call (`SELECT id, lc(m.agg(v), region)`) | `AnalysisException` naming the columns |
| Two N-to-1 calls with different keys in one SELECT | `AnalysisException` ("identical `<group_by>`") |
| Generator (`explode`) in args or keys | `AnalysisException` |
| Aggregate in args or keys (`lc(m.f(v), sum(v))`) | `AnalysisException` |
| `<group_by>` on a row-preserving function; non-deterministic or non-orderable key | `AnalysisException` |
| N-to-1 call nested in another N-to-1 call in one SELECT | `AnalysisException` (use a subquery) |
| N-to-1 call in WHERE / ORDER BY / GROUP BY | `AnalysisException` |
| Call in a join condition, window, generate, … | `AnalysisException` (compute it in a subquery) |
| Call over aggregated values in the SELECT of a GROUP BY | `AnalysisException` (aggregate in a subquery) |
| Unknown module/function, bad first argument, argument type not representable in Arrow | `AnalysisException` |

Supported 1-to-1 contexts: SELECT list, WHERE, GROUP BY keys, aggregate-function inputs,
ORDER BY, views and DataFrame `expr()`.

## 3. Execution

### 3.1 Operators

* `LcPythonEvalExec` keeps the child's partitioning and ordering and emits
  `child.output ++ result`.
* `LcPythonAggregateExec` requires `ClusteredDistribution(keys)` (`AllTuples` without keys) and
  ascending ordering on the keys. `EnsureRequirements` then inserts the exchange and sort (Comet
  makes both native when it can). Each group arrives **contiguously**, so the worker holds one
  group at a time. Memory is bounded by the largest group, not by how many groups a partition
  contains. Spark's and Comet's sorts spill; the JVM holds only in-flight group keys.

Both operators report `supportsColumnar = supportsRowBased = true`. They consume whichever form
the child produces and emit whichever form the parent wants.

### 3.2 Spark / Comet interoperability

| Upstream → us | What happens | Copies |
| --- | --- | --- |
| Comet native operator (CometVector, shaded Arrow) | Batch consumed directly, no `InternalRow`. Buffers are copied by address from the shaded vectors (reflection, since Comet relocates Arrow to `org.apache.comet.shaded.arrow`). Dictionary-encoded or layout-mismatched columns use a per-column typed-getter copy | 1 memcpy per buffer |
| Spark columnar (Arrow / on-heap vectors) | Same: raw copy for Arrow vectors, column-wise getters otherwise | 1 |
| Spark row operator (fallback, or Comet disabled) | Rows written straight into Arrow (Spark's `ArrowWriter`); no `RowToColumnarExec` in between | 1 |

| Us → downstream | What happens |
| --- | --- |
| Comet native operator | The planner already emits `CometSparkToColumnarExec(ourExec)`. Comet's own rule turns that into a native input, so the parent stays native (`CometProject`, `CometHashAggregate`, `CometExchange`, `CometBroadcastHashJoin` observed in CI). One columnar copy into Comet's Arrow; no row transition |
| Spark row operator, or Comet off, or a direct `execute()` caller | We emit `UnsafeRow`s ourselves; the useless wrapper and any `ColumnarToRow` on us are removed by `LcColumnarRule` |
| Spark columnar consumer | `ColumnarBatch` of `ArrowColumnVector` |

The upstream copy is not avoidable, and that is deliberate. Comet's native operators reuse
buffers across batches, the batch has to outlive the upstream `next()`, and the bytes have to
cross a process boundary anyway. See 3.3.

### 3.3 Memory ownership model

* **Borrowed:** an upstream batch is valid only until the upstream iterator advances. The
  writer thread copies everything it needs (arguments for the pipe; pass-through columns for
  the output) **before** it touches the upstream iterator again.
* **Owned:** copies live in a per-task child allocator of a JVM-wide `RootAllocator`. A
  `ResourceTracker` registers every allocation until a consumer-visible batch takes it over.
  Teardown frees whatever is left after the writer thread has been joined. The allocator is
  reference-counted between the writer thread and the task thread, and whichever finishes last
  closes it and checks it is empty. A non-empty allocator counts as a leak; tests run with
  `spark.lcpy.debug.failOnLeak=true`.
* **Emitted:** an output batch is valid until the next `hasNext()`, which closes it. This is the
  same contract Spark's vectorized readers and Comet use.
* **Result vectors** are transferred (ownership move, no copy) out of the IPC reader's root.

### 3.4 Wire protocol and streaming

Communication runs over inherited pipes only, with no sockets: `stdin` carries a length-prefixed
JSON header followed by **one** Arrow IPC stream, and `stdout` carries **one** Arrow IPC stream
of results (`r0..rN`, one struct per call). The worker moves the real stdout to a private
descriptor first, so `print()` in user code lands in stderr instead of corrupting the stream.

* **1-to-1:** one output batch per input batch, same row count and order (verified; violations
  fail the task). Pass-through copies wait in a FIFO, capped at `spark.lcpy.batch.maxInflight`.
* **N-to-1:** the writer tags rows with a running group id (input is sorted). The worker
  assembles one group, calls the function with a `pyarrow.Table` and answers one row per group.
  The JVM pairs answers with the keys it queued.

**Flushing without timers:** each side flushes only when it is about to block, never on a
timer. The JVM uses a 1 MiB pipe buffer, flushed at end of input or right before waiting on
back-pressure. The worker flushes its output only when its input has no data ready (`select`
before each read). Neither side can wait for bytes that sit in the other side's buffer, so small
partitions cannot deadlock, and large ones stream with full buffers. Tests run this with 3-row
batches and `maxInflight=2`, and with 5000 groups and `maxInflightGroups=2`.

### 3.5 Lifecycle and cleanup

| Event | Mechanism |
| --- | --- |
| Task success / failure / `LIMIT` early stop | Task-completion listener → `close()`. The worker is killed first (unblocking any reader), then the writer is joined, buffers freed, the cgroup killed (`cgroup.kill`), waited empty and removed |
| Task kill / job cancel | A monitor thread watches `TaskContext.isInterrupted` and kills the worker (a blocked pipe read is not interruptible). Teardown clears and restores the thread's interrupt flag, because Spark runs the listener on the interrupted thread |
| Executor stop | `LcPythonPlugin` executor plugin `shutdown()` (runs inside `Executor.stop()`, before `SparkEnv` teardown) |
| JVM shutdown (SIGTERM) | Spark `ShutdownHookManager` hook at priority 60, ahead of SparkContext (50) and temp-dir deletion (25) |
| JVM SIGKILL | The kernel: `bwrap --die-with-parent` (PDEATHSIG) plus PID-namespace teardown kill every worker process. The next JVM's plugin `init` (and the entrypoint) remove the empty cgroups |
| Worker's own children | PID namespace: when the worker exits, bwrap's init exits and the kernel kills the namespace; `memory.oom.group=1` kills the whole tree on OOM |

There are no host temp directories to leak. The worker's `/tmp` is a tmpfs inside the sandbox,
charged to its cgroup. The only host file is the worker script, extracted once per JVM.

## 4. The sandbox (Linux mechanics)

### 4.1 Process tree

```
PID 1  entrypoint.sh (root) ── cgroups + egress ── exec setpriv → java (sparkuser, cgroup /lcpy/jvm)
                                                      └─ lcpy-launch (sparkuser)
                                                           mkdir /lcpy/sandbox/w-<id>; set limits; join it
                                                           exec bwrap  ── new user/pid/ipc/uts/cgroup ns
                                                                └─ bwrap init (pid 1 in ns)
                                                                     └─ python3 -I -B -u /lcpy/worker.py
                                                                        (uid 65534, no caps, seccomp)
```

### 4.2 cgroup v2 delegation without systemd

In the pod the entrypoint is PID 1, alone in the root of its cgroup namespace. cgroup v2's
"no internal processes" rule forbids enabling controllers for children of a cgroup that still
contains processes. The entrypoint, running as root, therefore:

1. moves every process into `lcpy-init`, then enables `+cpu +memory +pids` on the root;
2. creates `lcpy/{jvm,sandbox}` and enables the controllers on `lcpy` and `lcpy/sandbox`;
3. **delegates**: `chown`s `lcpy/sandbox` plus its `cgroup.procs` / `cgroup.subtree_control` /
   `cgroup.threads`, `lcpy/jvm/cgroup.procs`, and `lcpy/cgroup.procs`. The last one matters
   because moving a process between cgroups requires write access to `cgroup.procs` of their
   **common ancestor**: the launcher moves from `lcpy/jvm` to `lcpy/sandbox/w-x`, whose common
   ancestor is `lcpy`;
4. joins `lcpy/jvm`, then `exec`s the JVM as `sparkuser` with an empty inheritable capability set.

From then on the unprivileged JVM creates, limits and removes `w-*` cgroups itself. Nothing above
`lcpy` is writable by it, and the sandboxed worker cannot see cgroupfs at all (no `/sys`, own
cgroup namespace).

### 4.3 Hard limits (per worker, `lcpy-launch`)

`memory.max` = `spark.lcpy.worker.memory` · `memory.swap.max = 0` (the cap cannot be dodged by
swapping) · `memory.oom.group = 1` (an OOM kills the whole worker tree at once) · `cpu.max` from
`spark.lcpy.worker.cpus` · `pids.max` = `spark.lcpy.worker.pids`. The launcher joins the cgroup
**before** `exec bwrap`, so every byte and every process of the sandbox is accounted from the
first instruction.

### 4.4 Filesystem, user, environment

`/usr` read-only (with `/bin`, `/lib`, `/lib64` symlinks), a handful of public `/etc` files, a
synthetic `/etc/passwd`, fresh `/proc` and minimal `/dev`, size-capped tmpfs `/tmp`, and the
registry plus the worker script read-only under `/lcpy`. Nothing else exists. That rules out
`/opt/spark`, Spark scratch dirs, the K8s service-account token, `/sys` and host `/tmp`. The
worker runs as uid 65534 in its own user namespace with `--cap-drop ALL`, `--disable-userns`
(no nested user namespaces), `--new-session` and a cleared environment (`--clearenv`, plus the
JVM strips its own environment before spawning).

### 4.5 seccomp (`gen-seccomp.py`, libseccomp)

The policy is default-allow with a deny-list sized for "Python + pyarrow":

* `socket()` is refused for every family except AF_INET/AF_INET6. In particular **AF_UNIX is
  blocked**: abstract UNIX sockets are scoped per network namespace, which the worker shares
  with the pod. `socketpair()` stays allowed.
* namespace, mount, module, kexec, keyring, bpf, perf, ptrace and io_uring calls are refused;
* `clone` with any `CLONE_NEW*` flag is refused, and `clone3` returns `ENOSYS` so libc falls back
  to `clone`.

### 4.6 Network: why the network namespace is shared

The JVM needs full pod networking; the worker needs an allowlist. A private network namespace
would give the worker nothing (plus a loopback of its own), and granting an allowlist from there
needs a veth/NAT or a userspace proxy managed with `CAP_NET_ADMIN`. Instead the worker **shares**
the pod network namespace, and the root entrypoint installs iptables rules that match on the
**cgroup a socket was created in** (`-m cgroup --path lcpy/sandbox`, cgroup v2, resolved inside
the cgroup namespace and matching all descendants):

```
OUTPUT -m cgroup --path lcpy/sandbox -j LCPY_SANDBOX
LCPY_SANDBOX: ACCEPT allowlisted CIDR[:port] (LCPY_EGRESS_ALLOW) [+ DNS to resolv.conf servers]
              REJECT tcp-reset / icmp-admin-prohibited for everything else
```

This rejects pod-local loopback services, the cloud metadata endpoint, cluster IPs and the
internet, IPv4 and IPv6 alike; startup fails closed if IPv6 is up but cannot be filtered.
Replies on accepted connections pass. The JVM (`lcpy/jvm`) is never matched. Neither the JVM nor
the worker has `CAP_NET_ADMIN`, so neither can change the rules. The JVM↔worker channel is pipes,
so blocking loopback costs nothing.

### 4.7 Observability under failure

The worker runs unbuffered (`-u`), and user `print()` goes to stderr. A pump thread drains stderr
continuously into a 64 KiB tail (and the executor log). On any failure, the task error is built
**after** the pump hits EOF, so nothing written before a crash is lost. It contains the exit
status with the decoded signal, whether the cgroup recorded `oom_kill` events (read from
`memory.events` before the cgroup is removed), the configured cap, and the stderr tail (Python
tracebacks included). A real one from CI:

```
lc_python_runner worker w-driver-87-1-0-67 [sandbox_probe.mem_hog] failed: worker failed before producing any result
worker status: killed by signal 9 (SIGKILL), exit 137
The kernel OOM-killed the worker cgroup (4 oom_kill event(s)); memory.max=209715200 bytes (spark.lcpy.worker.memory), swap disabled.
--- worker stderr (tail) ---
before-oom-marker: allocating until the cgroup limit
--- end of worker stderr ---
```

## 5. Deployment notes

Pod: `securityContext.privileged: true` (needed for the root entrypoint to write cgroupfs and
iptables; the JVM itself drops to `sparkuser`). The container's command must go through
`/opt/lcpy/bin/entrypoint.sh`. `sandbox/Dockerfile` is the reference image (Ubuntu 24.04, JRE
17, PySpark 3.5 jars, pyarrow, bubblewrap 0.9, iptables, python3-seccomp).

Node requirements: cgroup v2 with cpu/memory/pids; kernel ≥ 5.14 (`cgroup.kill`); unprivileged
user namespaces usable by bwrap. On Ubuntu ≥ 23.10 nodes that means
`kernel.apparmor_restrict_unprivileged_userns=0`, or an AppArmor profile allowing userns for
`/usr/bin/bwrap`; the entrypoint warns if the restriction is on.

| Key | Default | |
| --- | --- | --- |
| `spark.lcpy.registry.dir` | - | registry root (driver and executors) |
| `spark.lcpy.sandbox.mode` | `bwrap` | `none` only with `spark.lcpy.sandbox.allowUnsandboxed=true` (tests) |
| `spark.lcpy.worker.memory` / `.cpus` / `.pids` / `.tmpfsSize` | `1g` / `1.0` / `64` / `256m` | per worker |
| `spark.lcpy.batch.rows` / `.maxInflight` | `8192` / `4` | row input batching, back-pressure |
| `spark.lcpy.groups.maxInflight` | `4096` | N-to-1 back-pressure |
| `spark.lcpy.comet.nativeOutput` | `true` | wrap for native Comet consumers |
| `LCPY_EGRESS_ALLOW`, `LCPY_EGRESS_DNS` (env, entrypoint) | empty, `0` | worker egress allowlist |

## 6. Verification and test plan

`LcTestMain` is a self-contained runner, so it works the same locally and in CI. It runs one
suite three ways: plain Spark, Spark + Comet, and inside the runtime container with the real
sandbox. That last container is **privileged** and started with `--cgroupns=private`; root runs
the entrypoint as PID 1, the JVM runs as `sparkuser`, and there is no systemd, which matches the
pod. CI reports are uploaded as artifacts.

| Requirement | Test(s) |
| --- | --- |
| Arbitrary SQL argument expressions | `1-to-1 with arbitrary SQL argument expressions` (compared with the same computation in Spark SQL) |
| Implicit whole-relation input | `zero-arg call receives the whole child relation` |
| Subfields and operators on top, WHERE | `subfields + operators on top, WHERE on a subfield, evaluated once` |
| Nesting to arbitrary depth | `nested calls to arbitrary depth` (3 levels, shared inner call) |
| Redundant-call elimination | `redundant call elimination…`, the WHERE+SELECT test, the GROUP BY test (operator counts in the plan) |
| 1-to-1 vs N-to-1, computed keys, global, empty input | the N-to-1 tests (vs `GROUP BY` results) |
| Analysis-time failures | `…fails at analysis`, `invalid constructs fail at analysis` |
| Comet → us without `InternalRow` | `[comet] Comet-native upstream feeds us columnar…` (no transition below; raw-copy counter moves) |
| Spark → us | `row-based and columnar inputs…`, `[comet] Spark-fallback upstream…` (Comet project disabled → real `ProjectExec`) |
| Us → Comet | `[comet] downstream Comet operators consume our output natively` |
| Us → Spark rows / pure Spark | `[spark] no transitions directly below…`, every non-Comet test |
| Buffer reuse / off-heap safety | `[comet] small Comet batches (buffer reuse)…` (64-row batches vs Spark results); `failOnLeak=true` everywhere; leak counters; Arrow debug allocator in e2e; `ingest:` unit test |
| Streaming and deadlock freedom | tiny-batch and tiny-in-flight tests; `LIMIT stops early…` |
| Task / context teardown | `LIMIT…`, `task cancellation tears down…`, `kill9.sh` (SIGKILL JVM with live workers) |
| FS / PID / user / env isolation | `[sandbox] filesystem, process, user and environment isolation` |
| seccomp | `[sandbox] seccomp + no-new-userns…` |
| Network allowlist | `[sandbox] egress…` (allowlisted 127.0.0.2 connects; listening 127.0.0.1, 169.254.169.254, 10.96.0.1, 1.1.1.1 rejected; the JVM still reaches 127.0.0.1) |
| cgroup limits / fork bomb / OOM / CPU | `[sandbox] cgroup limits…`, `fork bomb…`, `memory cap…`, `CPU quota…` |
| Orphans, private /tmp | `[sandbox] no process outlives…`, `/tmp is private…` |
| Failure context | `Python exception: traceback and earlier prints…`, `memory cap…` (pre-crash output and OOM) |

Run locally (no sandbox) with Maven and Python with pyarrow:

```bash
mvn package && mvn dependency:build-classpath -Dmdep.includeScope=test -Dmdep.outputFile=cp.txt
java <add-opens…> -cp "target/classes:target/test-classes:$(cat cp.txt)" \
  org.apache.spark.sql.lcpy.LcTestMain --registry registry --python python3
```

## 7. Facts vs. hypotheses, and known limits

**Verified in CI (GitHub-hosted Ubuntu 24.04 runners; Spark 3.5.10, Comet 1.2.0, bwrap 0.9.0):**
everything in the table above. The Comet plans in the logs show `CometNativeScan → CometFilter →
CometProject → LcPythonEval → CometSparkColumnarToColumnar → CometProject/CometHashAggregate`,
with Comet reporting our operator as the only non-native one. Observed inside the sandbox:
uid 65534, PID 2, hostname `lcpy-sandbox`, cleared env (no injected secret); `/etc/shadow`, the
K8s token, host paths and `/sys` absent; writes to `/usr` and the registry fail with EROFS;
AF_UNIX / netlink / raw sockets, `unshare`, `mount` and `setuid(0)` refused; fork bomb stopped
at 28 forks with `pids.max=32`; a 200 MiB cap produced SIGKILL with `oom_kill` events reported;
`cpu.max 50000 100000` gave a CPU/wall ratio of 0.50.

**Not verified / reasoning only:**

* **Performance.** No throughput benchmark was run. The claim that the upstream copy costs
  about one memcpy per buffer and is cheaper than the pipe write is an expectation, not a
  measurement. Worker start-up (bwrap + Python + pyarrow import) is paid **per task**, which is
  the safer default (no state leaks between tasks or users) but a real cost for short tasks. A
  measured comparison against Spark's `ArrowEvalPythonExec` would be the next step before
  claiming "no degradation".
* **Kubernetes itself.** CI uses a privileged Docker container with a private cgroup namespace
  as the pod stand-in. Pod-specific details (containerd's cgroup layout, node AppArmor,
  NetworkPolicy interplay with in-pod iptables) were not exercised on a real cluster.
* **Comet-specific cases** beyond those tested (decimal and timestamp raw-copy layouts against
  every Comet scan mode, AQE re-planning with the wrapper under complex stage boundaries).
  Mismatches fall back to the getter copy by design, but only the paths in the test table were
  observed.
* Our off-heap buffers are not registered with Spark's `TaskMemoryManager`. The bound is
  `maxInflight` batches (or `maxInflightGroups` keys) per task, not the unified memory pool.

**Known limitations, by design:** star arguments (`m.f(*)`) are not supported; use the zero-arg
form. `input_file_name()` is not propagated to the writer thread. 1-to-1 functions must be
row-local, because filters may be pushed below them. Calls over aggregated values in a GROUP BY
SELECT must go through a subquery. Self-joins of a DataFrame that already contains an
`lc_python_runner` result were not tested.
