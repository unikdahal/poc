package org.apache.spark.sql.lcpy

import java.util.{Map => JMap}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.util.ShutdownHookManager

/**
 * Entry point: `spark.sql.extensions=org.apache.spark.sql.lcpy.LcPythonExtensions`.
 *
 * Can be listed before or after Comet's extension: the only ordering-sensitive piece (wrapping the
 * operator for Comet) happens in the planner strategy, which always runs before any columnar rule.
 */
class LcPythonExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(ext: SparkSessionExtensions): Unit = {
    ext.injectParser((_, delegate) => new LcParser(delegate))
    ext.injectResolutionRule(session => ResolveLcCalls(session))
    ext.injectPostHocResolutionRule(_ => ExtractLcCalls)
    ext.injectOptimizerRule(_ => PushFiltersBelowLcEval)
    ext.injectPlannerStrategy(session => LcStrategy(session))
    ext.injectColumnar(session => LcColumnarRule(session))
    LcLifecycle.installShutdownHook()
  }
}

/**
 * Executor lifecycle: `spark.plugins=org.apache.spark.sql.lcpy.LcPythonPlugin`.
 *
 * - init: remove worker cgroups left by a previous JVM in this container (abrupt termination).
 * - shutdown: called by Executor.stop() before SparkEnv is torn down; kills live workers.
 * Task-level cleanup does not depend on this plugin (task completion listeners do it); the plugin
 * and the JVM shutdown hook cover executor stop and SIGTERM. SIGKILL of the JVM is covered by the
 * kernel: bwrap --die-with-parent + PID namespaces kill the workers, and the next JVM's init
 * removes the empty cgroups.
 */
class LcPythonPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = null
  override def executorPlugin(): ExecutorPlugin = new ExecutorPlugin with Logging {
    override def init(ctx: PluginContext, extraConf: JMap[String, String]): Unit = {
      LcLifecycle.installShutdownHook()
      val root = ctx.conf().get(LcConf.CGROUP_ROOT, "/sys/fs/cgroup/lcpy/sandbox")
      if (ctx.conf().get(LcConf.SANDBOX_MODE, "bwrap") == "bwrap") {
        try WorkerProcess.cleanupStaleCgroups(root) catch {
          case NonFatal(e) => logWarning(s"lcpy: stale cgroup cleanup failed under $root", e)
        }
      }
    }
    override def shutdown(): Unit = WorkerProcess.killAll("executor shutdown")
  }
}

object LcLifecycle extends Logging {
  @volatile private var installed = false
  /**
   * Higher priority runs first. SparkContext stop is 50, temp-dir deletion 25: running at 60
   * kills workers while SparkEnv (block manager, scratch dirs) is still intact.
   */
  val ShutdownPriority = 60
  def installShutdownHook(): Unit = synchronized {
    if (!installed) {
      installed = true
      ShutdownHookManager.addShutdownHook(ShutdownPriority) { () =>
        WorkerProcess.killAll("JVM shutdown")
      }
    }
  }
}
