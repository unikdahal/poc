package org.apache.spark.sql.lcpy

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf

/** All configuration keys. SQL-session keys are read from SQLConf; runtime keys from SparkConf. */
object LcConf {
  val PREFIX = "spark.lcpy."

  /** Module registry root (must exist at the same path on the driver and on executors). */
  val REGISTRY_DIR = "spark.lcpy.registry.dir"

  /** `bwrap` (default, production) or `none` (plain subprocess; tests/dev only). */
  val SANDBOX_MODE = "spark.lcpy.sandbox.mode"
  /** `none` is refused unless this is also true, so a typo cannot silently disable isolation. */
  val ALLOW_UNSANDBOXED = "spark.lcpy.sandbox.allowUnsandboxed"
  /** Launcher that creates the per-worker cgroup and execs bwrap. */
  val LAUNCHER = "spark.lcpy.sandbox.launcher"
  /** Delegated cgroup v2 directory under which per-worker cgroups are created. */
  val CGROUP_ROOT = "spark.lcpy.sandbox.cgroupRoot"
  /** Python interpreter, as seen inside the sandbox (or on the host for mode=none). */
  val PYTHON = "spark.lcpy.python"

  val WORKER_MEMORY = "spark.lcpy.worker.memory"
  val WORKER_CPUS = "spark.lcpy.worker.cpus"
  val WORKER_PIDS = "spark.lcpy.worker.pids"
  val WORKER_TMPFS = "spark.lcpy.worker.tmpfsSize"

  /** Rows per Arrow batch sent to Python when the input is row-based. */
  val BATCH_ROWS = "spark.lcpy.batch.rows"
  /** Max input batches in flight (sent but not yet answered) per worker. */
  val MAX_INFLIGHT = "spark.lcpy.batch.maxInflight"
  /** Max group keys in flight per grouped worker. */
  val MAX_INFLIGHT_GROUPS = "spark.lcpy.groups.maxInflight"
  /** JVM -> worker pipe buffer. Flushed only at end of input or before blocking. */
  val PIPE_BUFFER = "spark.lcpy.pipe.bufferBytes"
  /** Bytes of worker stderr kept for failure reports. */
  val STDERR_TAIL = "spark.lcpy.stderr.tailBytes"
  /** Grace period between closing a worker's stdin and killing its cgroup. */
  val SHUTDOWN_GRACE_MS = "spark.lcpy.worker.shutdownGraceMs"

  /** Wrap the operator for native Comet consumption when Comet is active. */
  val COMET_NATIVE_OUTPUT = "spark.lcpy.comet.nativeOutput"
  /** Throw (instead of logging) if a task ends with Arrow memory still allocated. */
  val FAIL_ON_LEAK = "spark.lcpy.debug.failOnLeak"
  /** Push deterministic filters on pass-through columns below the Python operator. */
  val PUSH_FILTERS = "spark.lcpy.optimizer.pushFilters"

  def sqlConf(key: String, default: String): String =
    SQLConf.get.getConfString(key, default)
}

/** Executor-side settings captured on the driver at planning time and shipped with the plan. */
case class RunnerConf(
    sandboxMode: String,
    allowUnsandboxed: Boolean,
    launcher: String,
    cgroupRoot: String,
    python: String,
    registryDir: String,
    workerMemoryBytes: Long,
    workerCpus: Double,
    workerPids: Int,
    tmpfsBytes: Long,
    batchRows: Int,
    maxInflight: Int,
    maxInflightGroups: Int,
    pipeBufferBytes: Int,
    stderrTailBytes: Int,
    shutdownGraceMs: Long,
    failOnLeak: Boolean,
    sessionTimeZone: String)

object RunnerConf {
  def fromSession(conf: SQLConf, sc: SparkConf): RunnerConf = {
    def get(k: String, d: String): String = conf.getConfString(k, sc.get(k, d))
    RunnerConf(
      sandboxMode = get(LcConf.SANDBOX_MODE, "bwrap"),
      allowUnsandboxed = get(LcConf.ALLOW_UNSANDBOXED, "false").toBoolean,
      launcher = get(LcConf.LAUNCHER, "/opt/lcpy/bin/lcpy-launch"),
      cgroupRoot = get(LcConf.CGROUP_ROOT, "/sys/fs/cgroup/lcpy/sandbox"),
      python = get(LcConf.PYTHON, "/usr/bin/python3"),
      registryDir = get(LcConf.REGISTRY_DIR, ""),
      workerMemoryBytes = parseBytes(get(LcConf.WORKER_MEMORY, "1g")),
      workerCpus = get(LcConf.WORKER_CPUS, "1.0").toDouble,
      workerPids = get(LcConf.WORKER_PIDS, "64").toInt,
      tmpfsBytes = parseBytes(get(LcConf.WORKER_TMPFS, "256m")),
      batchRows = get(LcConf.BATCH_ROWS, "8192").toInt,
      maxInflight = math.max(2, get(LcConf.MAX_INFLIGHT, "4").toInt),
      maxInflightGroups = math.max(2, get(LcConf.MAX_INFLIGHT_GROUPS, "4096").toInt),
      pipeBufferBytes = parseBytes(get(LcConf.PIPE_BUFFER, "1m")).toInt,
      stderrTailBytes = parseBytes(get(LcConf.STDERR_TAIL, "64k")).toInt,
      shutdownGraceMs = get(LcConf.SHUTDOWN_GRACE_MS, "2000").toLong,
      failOnLeak = get(LcConf.FAIL_ON_LEAK, "false").toBoolean,
      sessionTimeZone = conf.sessionLocalTimeZone)
  }

  def parseBytes(s: String): Long = {
    val t = s.trim.toLowerCase
    val (num, mul) = t.last match {
      case 'k' => (t.init, 1L << 10)
      case 'm' => (t.init, 1L << 20)
      case 'g' => (t.init, 1L << 30)
      case 'b' => (t.init, 1L)
      case _ => (t, 1L)
    }
    (num.trim.toDouble * mul).toLong
  }
}
