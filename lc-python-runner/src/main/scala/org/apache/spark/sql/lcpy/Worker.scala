package org.apache.spark.sql.lcpy

import java.io._
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.security.MessageDigest
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.{SparkEnv, SparkException, TaskContext}
import org.apache.spark.internal.Logging

/** Raised for any sandboxed-worker failure; the message carries exit status, OOM and stderr. */
class LcPythonWorkerException(msg: String, cause: Throwable = null)
  extends SparkException(msg, cause)

/**
 * Bounded tail of a stream: keeps the last `capacity` bytes and counts what was dropped. Written
 * by the pump thread, read by the failing thread after the pump has drained (EOF), so a crash
 * never loses output that the worker managed to write to its stderr pipe before dying.
 */
final class TailBuffer(capacity: Int) {
  private val buf = new Array[Byte](capacity)
  private var start = 0
  private var size = 0
  private var dropped = 0L
  def append(b: Array[Byte], off: Int, len: Int): Unit = synchronized {
    var i = 0
    while (i < len) {
      if (size < capacity) { buf((start + size) % capacity) = b(off + i); size += 1 }
      else { buf(start) = b(off + i); start = (start + 1) % capacity; dropped += 1 }
      i += 1
    }
  }
  def text: String = synchronized {
    val out = new Array[Byte](size)
    var i = 0
    while (i < size) { out(i) = buf((start + i) % capacity); i += 1 }
    val s = new String(out, StandardCharsets.UTF_8)
    if (dropped > 0) s"[... $dropped earlier bytes dropped ...]\n$s" else s
  }
}

/**
 * One sandboxed Python process, bound to one task.
 *
 * Isolation (mode=bwrap) is delegated to the launcher, which creates `cgroupDir` with the memory,
 * swap, CPU and pid limits, moves itself into it and execs bubblewrap. The JVM keeps the pipes,
 * the cgroup path (to kill the whole tree and read OOM events) and the stderr tail.
 */
final class WorkerProcess(conf: RunnerConf, val label: String) extends Logging {
  import WorkerProcess._

  val id: String = {
    val tc = TaskContext.get()
    val exec = Option(SparkEnv.get).map(_.executorId).getOrElse("x").replaceAll("[^A-Za-z0-9]", "")
    val task = if (tc != null) s"${tc.stageId()}-${tc.partitionId()}-${tc.attemptNumber()}" else "0"
    s"w-$exec-$task-${counter.incrementAndGet()}"
  }

  private val sandboxed = conf.sandboxMode match {
    case "bwrap" => true
    case "none" if conf.allowUnsandboxed => false
    case "none" => throw new LcPythonWorkerException(
      s"${LcConf.SANDBOX_MODE}=none requires ${LcConf.ALLOW_UNSANDBOXED}=true (never in production)")
    case other => throw new LcPythonWorkerException(s"unknown ${LcConf.SANDBOX_MODE}=$other")
  }

  val cgroupDir: Option[Path] = if (sandboxed) Some(Paths.get(conf.cgroupRoot, id)) else None
  private val tail = new TailBuffer(conf.stderrTailBytes)
  @volatile private var destroyed = false
  @volatile var spawnNanos: Long = 0L

  private val process: Process = {
    val script = workerScript().toString
    val cmd: Seq[String] = if (sandboxed) {
      Seq(conf.launcher,
        "--cgroup", cgroupDir.get.toString,
        "--memory", conf.workerMemoryBytes.toString,
        "--cpus", conf.workerCpus.toString,
        "--pids", conf.workerPids.toString,
        "--tmpfs-size", conf.tmpfsBytes.toString,
        "--registry", conf.registryDir,
        "--worker", script,
        "--python", conf.python)
    } else {
      Seq(conf.python, "-I", "-B", "-u", script)
    }
    val pb = new ProcessBuilder(cmd.asJava)
    val env = pb.environment()
    // Never leak executor environment (credentials, tokens, Spark secrets) to the worker. The
    // sandbox also runs bwrap --clearenv; this covers mode=none and the launcher itself.
    val keep = Seq("PATH", "LANG", "LC_ALL")
    env.keySet().asScala.toList.filterNot(keep.contains).foreach(env.remove)
    if (!sandboxed) env.put("LCPY_REGISTRY", conf.registryDir)
    env.put("PYTHONUNBUFFERED", "1")
    env.put("PYTHONDONTWRITEBYTECODE", "1")
    val t0 = System.nanoTime()
    val p = pb.start()
    spawnNanos = System.nanoTime() - t0
    live.put(id, this)
    p
  }

  val stdin: OutputStream = new BufferedOutputStream(process.getOutputStream, conf.pipeBufferBytes)
  val stdout: InputStream = new BufferedInputStream(process.getInputStream, 1 << 16)

  private val stderrPump: Thread = {
    val t = new Thread(s"lcpy-stderr-$id") {
      override def run(): Unit = {
        val in = process.getErrorStream
        val b = new Array[Byte](8192)
        val line = new ByteArrayOutputStream()
        try {
          var n = in.read(b)
          while (n >= 0) {
            tail.append(b, 0, n)
            var i = 0
            while (i < n) {
              if (b(i) == '\n') {
                logInfo(s"[$label $id] ${line.toString("UTF-8")}"); line.reset()
              } else line.write(b(i))
              i += 1
            }
            n = in.read(b)
          }
          if (line.size() > 0) logInfo(s"[$label $id] ${line.toString("UTF-8")}")
        } catch { case NonFatal(_) => }
      }
    }
    t.setDaemon(true)
    t.start()
    t
  }

  def isAlive: Boolean = process.isAlive

  /** Wait (bounded) for natural exit; returns the exit code if it exited. */
  def waitExit(ms: Long): Option[Int] =
    if (process.waitFor(ms, TimeUnit.MILLISECONDS)) Some(process.exitValue()) else None

  def oomKills: Long = cgroupDir.flatMap(d => readKey(d.resolve("memory.events"), "oom_kill"))
    .getOrElse(0L)

  /** Kill every process of the worker: the whole cgroup when sandboxed, else the process. */
  def kill(): Unit = {
    destroyed = true
    cgroupDir.foreach { d =>
      val k = d.resolve("cgroup.kill")
      if (Files.exists(k)) try Files.write(k, "1".getBytes) catch { case NonFatal(_) => }
    }
    process.destroyForcibly()
  }

  /**
   * Human-readable failure context: exit status (decoded signal), whether the kernel OOM-killed
   * the cgroup, and the stderr tail. Waits for the stderr pump to reach EOF first so nothing the
   * worker wrote before dying is lost.
   */
  def describeFailure(what: String): String = {
    val exit = waitExit(5000)
    stderrPump.join(5000)
    val oom = oomKills
    val status = exit match {
      case Some(c) if c > 128 => s"killed by signal ${c - 128} (${signalName(c - 128)}), exit $c"
      case Some(c) => s"exit code $c"
      case None => "still running (killed)"
    }
    val oomText = if (oom > 0) {
      s"\nThe kernel OOM-killed the worker cgroup ($oom oom_kill event(s)); memory.max=" +
        s"${conf.workerMemoryBytes} bytes (${LcConf.WORKER_MEMORY}), swap disabled."
    } else ""
    val killedByUs = if (destroyed && oom == 0) " (terminated by the executor)" else ""
    s"lc_python_runner worker $id [$label] failed: $what\nworker status: $status$killedByUs" +
      s"$oomText\n--- worker stderr (tail) ---\n${tail.text}\n--- end of worker stderr ---"
  }

  def stderrTail: String = tail.text

  /**
   * Idempotent teardown: close pipes, give the worker `grace` to exit, then kill the cgroup, wait
   * until it is empty and remove it.
   */
  def close(graceMs: Long): Unit = {
    try stdin.close() catch { case NonFatal(_) => }
    if (waitExit(graceMs).isEmpty) kill()
    waitExit(10000)
    try stdout.close() catch { case NonFatal(_) => }
    stderrPump.join(2000)
    cgroupDir.foreach(removeCgroup)
    live.remove(id)
  }
}

object WorkerProcess extends Logging {
  private val counter = new AtomicLong()
  /** All workers alive in this JVM, for executor-shutdown cleanup. */
  val live = new ConcurrentHashMap[String, WorkerProcess]()

  def killAll(reason: String): Unit = {
    val ws = live.values().asScala.toList
    if (ws.nonEmpty) logWarning(s"lcpy: killing ${ws.size} live worker(s): $reason")
    ws.foreach(w => try w.close(0) catch { case NonFatal(_) => })
  }

  private def readKey(p: Path, key: String): Option[Long] =
    try {
      Files.readAllLines(p).asScala.map(_.trim.split("\\s+")).collectFirst {
        case Array(k, v) if k == key => v.toLong
      }
    } catch { case NonFatal(_) => None }

  /** Kill (cgroup.kill), wait for `populated 0`, rmdir. */
  def removeCgroup(d: Path): Unit = {
    if (!Files.isDirectory(d)) return
    val kill = d.resolve("cgroup.kill")
    if (Files.exists(kill)) try Files.write(kill, "1".getBytes) catch { case NonFatal(_) => }
    val deadline = System.currentTimeMillis() + 10000
    var done = false
    while (!done && System.currentTimeMillis() < deadline) {
      if (readKey(d.resolve("cgroup.events"), "populated").contains(0L)) {
        try { Files.delete(d); done = true } catch { case NonFatal(_) => Thread.sleep(20) }
      } else Thread.sleep(20)
    }
    if (!done) logWarning(s"lcpy: could not remove cgroup $d")
  }

  /** Remove cgroups left behind by a previous JVM in this container (e.g. after SIGKILL). */
  def cleanupStaleCgroups(root: String): Unit = {
    val r = Paths.get(root)
    if (!Files.isDirectory(r)) return
    val s = Files.list(r)
    try {
      s.iterator().asScala.filter(p => Files.isDirectory(p) && p.getFileName.toString.startsWith("w-"))
        .foreach { d => logWarning(s"lcpy: removing stale worker cgroup $d"); removeCgroup(d) }
    } finally s.close()
  }

  private def signalName(s: Int): String = s match {
    case 9 => "SIGKILL"; case 15 => "SIGTERM"; case 11 => "SIGSEGV"; case 6 => "SIGABRT"
    case 7 => "SIGBUS"; case 31 => "SIGSYS"; case 2 => "SIGINT"; case _ => s"signal $s"
  }

  @volatile private var extracted: Path = _

  /** worker.py, extracted from the jar once per JVM into a private, content-addressed file. */
  def workerScript(): Path = synchronized {
    if (extracted != null && Files.exists(extracted)) return extracted
    val in = getClass.getResourceAsStream("/lcpy/worker.py")
    if (in == null) throw new LcPythonWorkerException("worker.py missing from the jar")
    val bytes = try in.readAllBytes() finally in.close()
    val sha = MessageDigest.getInstance("SHA-256").digest(bytes).take(8).map("%02x".format(_)).mkString
    val base = Option(System.getProperty("java.io.tmpdir")).getOrElse("/tmp")
    val dir = Paths.get(base, s"lcpy-${ProcessHandle.current().pid()}")
    Files.createDirectories(dir)
    val target = dir.resolve(s"worker-$sha.py")
    if (!Files.exists(target)) {
      val tmp = Files.createTempFile(dir, "worker", ".py")
      Files.write(tmp, bytes)
      Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE)
    }
    target.toFile.setReadable(true, false)
    dir.toFile.deleteOnExit()
    target.toFile.deleteOnExit()
    extracted = target
    target
  }
}
