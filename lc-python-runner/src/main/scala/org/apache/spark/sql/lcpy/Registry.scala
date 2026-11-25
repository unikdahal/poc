package org.apache.spark.sql.lcpy

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.types.{DataType, StructType}

/** Cardinality contract of a registered function. */
sealed trait Mode { def wire: String }
object Mode {
  /** 1-to-1: one output struct row per input row, aligned with the input. */
  case object Row extends Mode { val wire = "row" }
  /** N-to-1: one output struct row per `<group_by>` key (or per dataset). */
  case object Grouped extends Mode { val wire = "grouped" }
  def parse(s: String): Mode = s.toLowerCase match {
    case "row" | "row_preserving" | "1-to-1" => Row
    case "grouped" | "grouped_aggregation" | "n-to-1" => Grouped
    case other => throw new IllegalArgumentException(s"unknown mode '$other'")
  }
}

/**
 * Everything the planner needs to know about one registered function, resolved on the driver at
 * analysis time. `moduleSha256` pins the code the plan was analysed against; the worker refuses to
 * run if the module it finds in the sandbox hashes differently.
 */
case class FunctionSpec(
    module: String,
    function: String,
    mode: Mode,
    returnType: StructType,
    moduleSha256: String) {
  def qualifiedName: String = s"$module.$function"
}

/**
 * File-system module registry.
 *
 * Layout (one directory per module):
 * {{{
 *   <registry>/<module>/manifest.json   {"functions": {"score": {"mode": "row",
 *                                                               "returns": "label string, ..."}}}
 *   <registry>/<module>/<module>.py     the code (other files in the directory are allowed)
 * }}}
 * The driver reads manifests for analysis. Executors bind the same directory read-only into the
 * sandbox. How the directory is distributed (baked into the image, a synced volume, an init
 * container pulling from object storage) is deployment-specific and outside this module.
 */
object Registry {
  private val mapper = new ObjectMapper()
  private val NameRe = "^[A-Za-z_][A-Za-z0-9_]*$".r

  private case class Cached(stamp: String, specs: Map[String, FunctionSpec])
  private val cache = new ConcurrentHashMap[String, Cached]()

  def lookup(registryDir: String, module: String, function: String): FunctionSpec = {
    if (registryDir == null || registryDir.isEmpty) {
      throw new AnalysisException(
        s"lc_python_runner: ${LcConf.REGISTRY_DIR} is not set; cannot resolve $module.$function")
    }
    if (NameRe.findFirstIn(module).isEmpty || NameRe.findFirstIn(function).isEmpty) {
      throw new AnalysisException(
        s"lc_python_runner: invalid module/function name '$module.$function'")
    }
    val dir = new File(registryDir, module)
    val manifest = new File(dir, "manifest.json")
    if (!manifest.isFile) {
      throw new AnalysisException(
        s"lc_python_runner: module '$module' is not registered (no $manifest)")
    }
    val stamp = moduleStamp(dir.toPath)
    val key = dir.getCanonicalPath
    val cached = cache.compute(key, (_, old) =>
      if (old != null && old.stamp == stamp) old else Cached(stamp, load(module, dir, manifest)))
    cached.specs.getOrElse(function, throw new AnalysisException(
      s"lc_python_runner: function '$function' is not registered in module '$module' " +
        s"(registered: ${cached.specs.keys.toSeq.sorted.mkString(", ")})"))
  }

  private def load(module: String, dir: File, manifest: File): Map[String, FunctionSpec] = {
    val root: JsonNode = mapper.readTree(manifest)
    val fns = root.get("functions")
    if (fns == null || !fns.isObject) {
      throw new AnalysisException(s"lc_python_runner: $manifest has no 'functions' object")
    }
    val sha = moduleSha256(dir.toPath)
    fns.fields().asScala.map { e =>
      val name = e.getKey
      val node = e.getValue
      val mode = Mode.parse(Option(node.get("mode")).map(_.asText).getOrElse("row"))
      val returns = Option(node.get("returns")).map(_.asText).getOrElse(
        throw new AnalysisException(s"lc_python_runner: $module.$name has no 'returns' schema"))
      val dt = try DataType.fromDDL(returns) catch {
        case ex: Exception => throw new AnalysisException(
          s"lc_python_runner: $module.$name has an invalid 'returns' schema: ${ex.getMessage}")
      }
      val st = dt match {
        case s: StructType if s.nonEmpty => s
        case other => throw new AnalysisException(
          s"lc_python_runner: $module.$name must return a non-empty struct, got ${other.sql}")
      }
      name -> FunctionSpec(module, name, mode, st.asNullable, sha)
    }.toMap
  }

  /** Cheap change detector: names, sizes and mtimes of the module's files. */
  private def moduleStamp(dir: Path): String = {
    val sb = new StringBuilder
    files(dir).foreach { p =>
      sb.append(dir.relativize(p)).append(':').append(Files.size(p)).append(':')
        .append(Files.getLastModifiedTime(p).toMillis).append(';')
    }
    sb.toString
  }

  /**
   * Content hash of a module directory: sha256 over (relative path, NUL, bytes, NUL) of every
   * regular file, sorted by path, skipping `__pycache__`. `worker.py` computes the same thing.
   */
  def moduleSha256(dir: Path): String = {
    val md = MessageDigest.getInstance("SHA-256")
    files(dir).foreach { p =>
      md.update(dir.relativize(p).toString.replace(File.separatorChar, '/')
        .getBytes(StandardCharsets.UTF_8))
      md.update(0.toByte)
      md.update(Files.readAllBytes(p))
      md.update(0.toByte)
    }
    md.digest().map("%02x".format(_)).mkString
  }

  private def files(dir: Path): Seq[Path] = {
    val s = Files.walk(dir)
    try {
      s.iterator().asScala
        .filter(p => Files.isRegularFile(p))
        .filterNot(p => dir.relativize(p).iterator().asScala.exists(_.toString == "__pycache__"))
        .toSeq
        .sortBy(p => dir.relativize(p).toString.replace(File.separatorChar, '/'))
    } finally s.close()
  }
}
