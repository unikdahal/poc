package org.apache.spark.sql.lcpy

import java.nio.file.Files

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.{ColumnarToRowTransition, RowToColumnarTransition, SparkPlan}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.arrow.LcArrowIngest

/**
 * Self-contained test runner (no test framework dependency, so it runs the same way locally and
 * under Maven in CI). Usage:
 *   LcTestMain --registry DIR --python PY [--comet] [--sandbox bwrap|none] [--only REGEX]
 * Exit code = number of failed tests.
 */
object LcTestMain extends AdaptiveSparkPlanHelper {

  final case class Result(name: String, ok: Boolean, millis: Long, detail: String)
  private val results = mutable.ArrayBuffer.empty[Result]
  private var only: Option[String] = None

  def test(name: String)(body: => Unit): Unit = {
    if (only.exists(r => !name.matches(r))) return
    val t0 = System.currentTimeMillis()
    val r = try { body; Result(name, ok = true, System.currentTimeMillis() - t0, "") } catch {
      case NonFatal(e) =>
        val sw = new java.io.StringWriter
        e.printStackTrace(new java.io.PrintWriter(sw))
        Result(name, ok = false, System.currentTimeMillis() - t0, sw.toString.take(6000))
    }
    results += r
    println(f"[${if (r.ok) "PASS" else "FAIL"}] ${r.millis}%6d ms  $name")
    if (!r.ok) println(r.detail)
  }

  def check(cond: Boolean, msg: => String): Unit = if (!cond) throw new AssertionError(msg)
  def eq[T](got: T, exp: T, what: String = ""): Unit =
    check(got == exp, s"$what\n  expected: $exp\n  got:      $got")
  def rows(df: DataFrame): Seq[Row] = df.collect().toSeq
  def sorted(df: DataFrame): Seq[String] = df.collect().map(_.toString).sorted.toSeq

  def analysisError(sql: => DataFrame, fragment: String): Unit = {
    val e = try { sql.queryExecution.assertAnalyzed(); None } catch {
      case e: AnalysisException => Some(e)
    }
    check(e.isDefined, s"expected an AnalysisException containing '$fragment'")
    check(e.get.getMessage.contains(fragment),
      s"expected message to contain '$fragment', got: ${e.get.getMessage}")
  }

  def execError(df: => DataFrame, fragments: String*): String = {
    val msg = try { df.collect(); "" } catch { case NonFatal(e) => fullMessage(e) }
    check(msg.nonEmpty, s"expected the query to fail")
    fragments.foreach(f => check(msg.contains(f), s"expected failure to contain '$f', got:\n$msg"))
    msg
  }

  def fullMessage(t: Throwable): String = {
    val sb = new StringBuilder
    var c = t
    while (c != null) { sb.append(c.toString).append('\n'); c = c.getCause }
    sb.toString
  }

  def plan(df: DataFrame): SparkPlan = {
    df.collect() // materialise AQE
    df.queryExecution.executedPlan
  }

  def nodes(df: DataFrame): Seq[SparkPlan] = collectWithSubqueries(plan(df)) { case p => p }

  def lcNodeCount(df: DataFrame): Int =
    df.queryExecution.optimizedPlan.collect {
      case _: LcPythonEval | _: LcPythonAggregate => 1
    }.sum

  def main(args: Array[String]): Unit = {
    val opts = args.sliding(2, 2).collect { case Array(k, v) => k -> v }.toMap
    val flags = args.toSet
    only = opts.get("--only")
    val registry = opts("--registry")
    val python = opts.getOrElse("--python", "python3")
    val comet = flags.contains("--comet")
    val sandbox = opts.getOrElse("--sandbox", "none")

    val b = SparkSession.builder()
      .master(opts.getOrElse("--master", "local[2]"))
      .appName("lcpy-tests")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.plugins", "org.apache.spark.sql.lcpy.LcPythonPlugin" +
        (if (comet) ",org.apache.spark.CometPlugin" else ""))
      .config(LcConf.REGISTRY_DIR, registry)
      .config(LcConf.PYTHON, python)
      .config(LcConf.SANDBOX_MODE, sandbox)
      .config(LcConf.ALLOW_UNSANDBOXED, "true")
      .config(LcConf.FAIL_ON_LEAK, "true")
    if (comet) {
      b.config("spark.sql.extensions",
          "org.apache.comet.CometSparkSessionExtensions,org.apache.spark.sql.lcpy.LcPythonExtensions")
        .config("spark.comet.enabled", "true")
        .config("spark.comet.exec.enabled", "true")
        .config("spark.comet.exec.shuffle.enabled", "true")
        .config("spark.shuffle.manager",
          "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "2g")
        .config("spark.comet.explainFallback.enabled", "true")
    } else {
      b.config("spark.sql.extensions", "org.apache.spark.sql.lcpy.LcPythonExtensions")
    }
    opts.get("--conf").foreach(_.split(";").filter(_.contains("=")).foreach { kv =>
      val Array(k, v) = kv.split("=", 2); b.config(k, v)
    })
    val spark = b.getOrCreate()
    try {
      new Suites(spark, comet).run()
      if (sandbox == "bwrap") new SandboxSuites(spark).run()
    } finally {
      spark.stop()
    }
    val failed = results.count(!_.ok)
    println(s"\n==== ${results.size - failed} passed, $failed failed ====")
    results.filterNot(_.ok).foreach(r => println(s"FAILED: ${r.name}"))
    if (opts.contains("--report")) {
      val json = results.map(r =>
        s"""{"name":${quote(r.name)},"ok":${r.ok},"ms":${r.millis},"detail":${quote(r.detail)}}""")
      Files.write(java.nio.file.Paths.get(opts("--report")),
        json.mkString("[\n", ",\n", "\n]\n").getBytes("UTF-8"))
    }
    System.exit(math.min(failed, 100))
  }

  private def quote(s: String): String =
    "\"" + s.flatMap {
      case '"' => "\\\""; case '\\' => "\\\\"; case '\n' => "\\n"; case '\t' => "\\t"
      case c if c < ' ' => f"\\u${c.toInt}%04x"; case c => c.toString
    } + "\""
}

/** SQL semantics + execution paths. Run with and without Comet; results must be identical. */
class Suites(spark: SparkSession, comet: Boolean) {
  import LcTestMain._

  private val tmp = Files.createTempDirectory("lcpy-data").toFile

  private def setup(): Unit = {
    import spark.implicits._
    val n = 20000
    val df = spark.range(n).selectExpr(
      "id",
      "cast(id % 37 as int) as col1",
      "cast(id % 11 as double) as col2",
      "case when id % 5 = 0 then null else concat('s', cast(id % 7 as string)) end as col3",
      "array(id, id + 1) as arr",
      "case id % 3 when 0 then 'North' when 1 then 'south' else 'EaSt' end as region",
      "cast(id % 13 as double) as v",
      "timestamp_seconds(1700000000 + id * 3600) as ts")
    df.write.mode("overwrite").parquet(s"$tmp/t")
    spark.read.parquet(s"$tmp/t").createOrReplaceTempView("t")
    spark.read.parquet(s"$tmp/t").limit(0).createOrReplaceTempView("empty_t")
    spark.range(500).selectExpr("id", "cast(id % 37 as int) as col1",
      "cast(id % 11 as double) as col2", "concat('r', cast(id as string)) as col3",
      "case id % 3 when 0 then 'North' when 1 then 'south' else 'EaSt' end as region",
      "cast(id % 13 as double) as v")
      .createOrReplaceTempView("rows_t") // Range -> row-based input
    Seq((1, "a"), (2, "b")).toDF("k", "name").createOrReplaceTempView("dim")
  }

  private val S = "lc_python_runner(my_mod.score(col1, col2 * 1.18, coalesce(col3, 'default')))"

  def run(): Unit = {
    setup()
    val tag = if (comet) "[comet] " else ""

    test(s"${tag}1-to-1 with arbitrary SQL argument expressions") {
      val got = spark.sql(s"SELECT id, $S AS r FROM t").selectExpr("id", "r.*")
      val exp = spark.sql("""SELECT id, upper(coalesce(col3, 'default')) AS label,
        cast(col1 as double) + col2 * 1.18 AS score,
        cast(col1 as double) + col2 * 1.18 > 10 AS valid FROM t""")
      eq(got.count(), 20000L, "row count")
      check(got.exceptAll(exp).isEmpty && exp.exceptAll(got).isEmpty,
        s"mismatch: ${got.exceptAll(exp).take(5).mkString(", ")}")
    }

    test(s"${tag}subfields + operators on top, WHERE on a subfield, evaluated once") {
      val df = spark.sql(s"""SELECT id, upper($S.label) AS l, $S.score + 10 AS s10
        FROM t WHERE $S.valid = true""")
      val exp = spark.sql("""SELECT id, upper(coalesce(col3, 'default')) AS l,
        cast(col1 as double) + col2 * 1.18 + 10 AS s10 FROM t
        WHERE cast(col1 as double) + col2 * 1.18 > 10""")
      check(df.exceptAll(exp).isEmpty && exp.exceptAll(df).isEmpty, "mismatch")
      eq(lcNodeCount(df), 1, "WHERE + SELECT share one evaluation")
    }

    test(s"${tag}redundant call elimination in one SELECT list") {
      val df = spark.sql(s"SELECT $S.score, $S.label, $S.valid FROM t")
      eq(lcNodeCount(df), 1, "one Python evaluation for three subfield references")
      eq(df.count(), 20000L)
    }

    test(s"${tag}nested calls to arbitrary depth") {
      val df = spark.sql("""SELECT id,
        lc_python_runner(my_mod.outer(lc_python_runner(my_mod.inner(col1)).x + 1)).z AS z,
        lc_python_runner(my_mod.outer(lc_python_runner(my_mod.outer(
          lc_python_runner(my_mod.inner(col1)).x)).z)).z AS zz FROM t""")
      val chk = df.join(spark.table("t").select("id", "col1"), "id")
        .where("z != col1 * 2 + 1 + 100 OR zz != col1 * 2 + 200")
      eq(chk.count(), 0L, "nested results")
      eq(lcNodeCount(df), 4, "inner shared by both nests: inner, outer, outer, outer")
    }

    test(s"${tag}zero-arg call receives the whole child relation") {
      val r = spark.sql("SELECT lc_python_runner(my_mod.clean_dataset()) AS r FROM rows_t")
        .selectExpr("r.ncols", "r.cols").distinct().collect()
      eq(r.length, 1)
      eq(r.head.getInt(0), 6)
      eq(r.head.getString(1), "id,col1,col2,col3,region,v")
    }

    test(s"${tag}argument column names (attributes keep names, expressions are argN)") {
      val r = spark.sql(
        "SELECT lc_python_runner(my_mod.arg_names(col1, col2 * 2, col1)).names FROM rows_t LIMIT 1")
        .collect().head.getString(0)
      eq(r, "col1,arg1,col1_1")
    }

    test(s"${tag}N-to-1 with computed group keys, keys usable in SELECT") {
      val df = spark.sql("""SELECT lower(region) AS rk,
          lc_python_runner(my_mod.summarize(v, lower(region)), lower(region)) AS s FROM t""")
        .selectExpr("rk", "s.n", "s.total", "s.first_key")
      val exp = spark.sql("""SELECT lower(region) AS rk, count(*) AS n, sum(v) AS total,
        lower(region) AS first_key FROM t GROUP BY lower(region)""")
      eq(sorted(df), sorted(exp), "grouped results")
    }

    test(s"${tag}N-to-1 with two keys incl. date_trunc, plus outer row call on the result") {
      val df = spark.sql("""SELECT date_trunc('day', ts) AS d, upper(region) AS r,
          lc_python_runner(my_mod.count_rows(id), date_trunc('day', ts), upper(region)).n AS n,
          lc_python_runner(my_mod.outer(
            lc_python_runner(my_mod.count_rows(id), date_trunc('day', ts), upper(region)).n)).z AS z
        FROM t""")
      val exp = spark.sql("""SELECT date_trunc('day', ts) AS d, upper(region) AS r, count(*) AS n,
        count(*) + 100 AS z FROM t GROUP BY 1, 2""")
      eq(sorted(df), sorted(exp))
      eq(lcNodeCount(df), 2, "one aggregate (deduplicated) + one outer row eval")
    }

    test(s"${tag}N-to-1 without group_by: whole dataset, one row (also on empty input)") {
      val r = spark.sql("SELECT lc_python_runner(my_mod.summarize(v)) AS s FROM t").collect()
      eq(r.length, 1)
      eq(r.head.getStruct(0).getLong(0), 20000L)
      val e = spark.sql("SELECT lc_python_runner(my_mod.summarize(v)) AS s FROM empty_t").collect()
      eq(e.length, 1, "global aggregate over empty input still yields one row")
      eq(e.head.getStruct(0).getLong(0), 0L)
      val g = spark.sql("SELECT lc_python_runner(my_mod.count_rows(id), region) FROM empty_t")
      eq(g.count(), 0L, "grouped over empty input yields no rows")
    }

    test(s"${tag}N-to-1 with many groups and tiny in-flight limits (back-pressure)") {
      spark.conf.set(LcConf.MAX_INFLIGHT_GROUPS, "2")
      spark.conf.set(LcConf.BATCH_ROWS, "7")
      try {
        val df = spark.sql(
          "SELECT id % 5000 AS k, lc_python_runner(my_mod.count_rows(id), id % 5000).n AS n FROM t")
        val r = df.collect()
        eq(r.length, 5000)
        check(r.forall(_.getLong(1) == 4L), "every group has 4 rows")
      } finally {
        spark.conf.unset(LcConf.MAX_INFLIGHT_GROUPS); spark.conf.unset(LcConf.BATCH_ROWS)
      }
    }

    test(s"${tag}1-to-1 with tiny batches and maxInflight=2 (flush-before-block)") {
      spark.conf.set(LcConf.MAX_INFLIGHT, "2")
      spark.conf.set(LcConf.BATCH_ROWS, "3")
      try {
        val n = spark.sql("SELECT lc_python_runner(my_mod.inner(col1)).x AS x FROM rows_t")
          .where("x IS NOT NULL").count()
        eq(n, 500L)
      } finally { spark.conf.unset(LcConf.MAX_INFLIGHT); spark.conf.unset(LcConf.BATCH_ROWS) }
    }

    test(s"${tag}row-based and columnar inputs give identical results") {
      val q = s"SELECT id, $S.score AS s FROM %s"
      val rowsRes = sorted(spark.sql(q.format("rows_t")))
      spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
      val parquetRows = try sorted(spark.sql(q.format("t") + " WHERE id < 500"))
        finally spark.conf.unset("spark.sql.parquet.enableVectorizedReader")
      val parquetCols = sorted(spark.sql(q.format("t") + " WHERE id < 500"))
      eq(parquetRows, parquetCols, "parquet rows vs columnar")
      eq(rowsRes.size, 500)
    }

    if (!comet) test("ingest: raw buffer copy == getter copy for every flat type (nulls, offsets)") {
      IngestCheck.run()
    }

    test(s"${tag}calls in GROUP BY and inside aggregate functions, and in ORDER BY") {
      val df = spark.sql(s"""SELECT $S.label AS l, count(*) AS c, sum($S.score) AS ss FROM t
        GROUP BY $S.label""")
      val exp = spark.sql("""SELECT upper(coalesce(col3, 'default')) AS l, count(*) AS c,
        sum(cast(col1 as double) + col2 * 1.18) AS ss FROM t GROUP BY 1""")
      eq(sorted(df.selectExpr("l", "c", "round(ss, 6)")), sorted(exp.selectExpr("l", "c", "round(ss, 6)")))
      eq(lcNodeCount(df), 1, "GROUP BY key and aggregate input share one evaluation")
      val top = spark.sql(s"SELECT id FROM rows_t ORDER BY $S.score DESC, id LIMIT 3").collect()
        .map(_.getLong(0)).toSeq
      val expTop = spark.sql("SELECT id FROM rows_t ORDER BY col1 + col2 * 1.18 DESC, id LIMIT 3")
        .collect().map(_.getLong(0)).toSeq
      eq(top, expTop)
    }

    test(s"${tag}DataFrame API (selectExpr / expr) and views") {
      val df = spark.table("rows_t").selectExpr("id", s"$S.score AS s")
      eq(df.count(), 500L)
      spark.sql(s"CREATE OR REPLACE TEMP VIEW scored AS SELECT id, $S AS r FROM rows_t")
      eq(spark.sql("SELECT count(*) FROM scored WHERE r.valid").collect().head.getLong(0),
        spark.sql("SELECT count(*) FROM rows_t WHERE col1 + col2 * 1.18 > 10").collect().head.getLong(0))
    }

    test(s"${tag}LIMIT stops early without leaks or orphan workers") {
      spark.sql("SELECT lc_python_runner(my_mod.slow_echo(id)).x FROM t LIMIT 5").collect()
      Thread.sleep(500)
      eq(WorkerProcess.live.size(), 0, "no live workers after the query")
    }

    // ---- analysis-time failures
    test(s"${tag}N-to-1: ungrouped column alongside the call fails at analysis") {
      analysisError(spark.sql(
        "SELECT id, lc_python_runner(my_mod.count_rows(v), region) FROM t"), "`id`")
      analysisError(spark.sql(
        "SELECT region, lc_python_runner(my_mod.count_rows(v), lower(region)) FROM t"), "`region`")
    }

    test(s"${tag}N-to-1: incompatible grouping keys in one projection fail at analysis") {
      analysisError(spark.sql("""SELECT lc_python_runner(my_mod.count_rows(v), region),
        lc_python_runner(my_mod.count_rows(v), col1) FROM t"""), "identical <group_by>")
    }

    test(s"${tag}invalid constructs fail at analysis") {
      analysisError(spark.sql("SELECT lc_python_runner(my_mod.count_rows(v), sum(v)) FROM t"),
        "aggregate function")
      analysisError(spark.sql("SELECT lc_python_runner(my_mod.inner(explode(arr))) FROM t"),
        "explode")
      analysisError(spark.sql("SELECT lc_python_runner(my_mod.count_rows(v), explode(arr)) FROM t"),
        "explode")
      analysisError(spark.sql("SELECT lc_python_runner(my_mod.inner(col1), region) FROM t"),
        "row-preserving")
      analysisError(spark.sql("SELECT lc_python_runner(my_mod.nope(col1)) FROM t"),
        "not registered")
      analysisError(spark.sql("SELECT lc_python_runner(nomod.f(col1)) FROM t"), "not registered")
      analysisError(spark.sql("SELECT lc_python_runner(upper(col3)) FROM t"), "<module>.<function>")
      analysisError(spark.sql("SELECT * FROM t WHERE lc_python_runner(my_mod.count_rows(v)).n > 0"),
        "not allowed in WHERE")
      analysisError(spark.sql("""SELECT * FROM t JOIN dim
        ON lc_python_runner(my_mod.inner(t.col1)).x = dim.k"""), "not supported inside Join")
      analysisError(spark.sql("""SELECT lc_python_runner(my_mod.count_rows(
        lc_python_runner(my_mod.count_rows(v), region).n)) FROM t"""), "N-to-1")
    }

    // ---- runtime failures surface clearly
    test(s"${tag}Python exception: traceback and earlier prints reach the task error") {
      execError(spark.sql("SELECT lc_python_runner(my_mod.boom(col1)) FROM rows_t"),
        "boom marker from user code", "boom-stdout-marker", "boom-stderr-marker", "exit code 3")
    }

    test(s"${tag}1-to-1 contract violation is detected") {
      execError(spark.sql("SELECT lc_python_runner(my_mod.drop_rows(col1)) FROM rows_t"),
        "expected")
    }

    test(s"${tag}no Arrow memory leaked, no workers left") {
      eq(LcMemory.leaks.get(), 0L, "leaked allocators")
      eq(WorkerProcess.live.size(), 0, "live workers")
    }

    if (comet) cometPaths()
    else sparkPaths()
  }

  private def isTransition(p: SparkPlan): Boolean =
    p.isInstanceOf[ColumnarToRowTransition] || p.isInstanceOf[RowToColumnarTransition] ||
      p.getClass.getSimpleName.contains("ColumnarToRow") ||
      p.getClass.getSimpleName.contains("RowToColumnar")

  private def sparkPaths(): Unit = {
    test("[spark] no transitions directly below our operator; row parent gets rows natively") {
      val df = spark.sql(s"SELECT id, $S.score + 1 AS s FROM t")
      val lcs = nodes(df).collect { case l: LcExecBase => l }
      check(lcs.nonEmpty, "operator missing")
      lcs.foreach(l => check(!isTransition(l.child), s"transition below: ${l.child.nodeName}"))
      println(plan(df).treeString)
    }
  }

  private def cometPaths(): Unit = {
    test("[comet] Comet-native upstream feeds us columnar (no row transition), raw-copy path") {
      val before = LcArrowIngest.rawCopies.get()
      val df = spark.sql(s"SELECT id, $S.score AS s FROM t WHERE col1 > 3")
      val p = plan(df)
      println(p.treeString)
      val lcs = nodes(df).collect { case l: LcExecBase => l }
      check(lcs.nonEmpty, "operator missing")
      lcs.foreach { l =>
        check(!isTransition(l.child), s"transition below: ${l.child.nodeName}")
        check(l.child.getClass.getName.contains("comet"),
          s"expected a Comet operator below, got ${l.child.getClass.getName}")
      }
      check(LcArrowIngest.rawCopies.get() > before, "expected raw copies from Comet vectors")
    }

    test("[comet] downstream Comet operators consume our output natively") {
      val df = spark.sql(s"""SELECT r.label, sum(r.score) AS s FROM
        (SELECT $S AS r FROM t) GROUP BY r.label""")
      val p = plan(df)
      println(p.treeString)
      val all = nodes(df)
      val wrapper = all.find(n => CometBridge.isWrapper(n) &&
        n.children.headOption.exists(_.isInstanceOf[LcExecBase]))
      check(wrapper.isDefined, "expected CometSparkToColumnarExec over our operator")
      val parentNative = all.exists(n => n.getClass.getName.contains("comet") &&
        !CometBridge.isWrapper(n) && n.children.exists(_ eq wrapper.get))
      println(s"native parent consuming the wrapper directly: $parentNative")
    }

    test("[comet] small Comet batches (buffer reuse) give the same results as Spark") {
      spark.conf.set("spark.comet.batchSize", "64")
      try {
        val got = sorted(spark.sql(s"SELECT id, $S AS r FROM t WHERE id < 3000"))
        spark.conf.set("spark.comet.enabled", "false")
        val exp = try sorted(spark.sql(s"SELECT id, $S AS r FROM t WHERE id < 3000"))
          finally spark.conf.set("spark.comet.enabled", "true")
        eq(got, exp, "comet vs spark")
      } finally spark.conf.unset("spark.comet.batchSize")
    }

    test("[comet] Spark-fallback upstream operator (rows) still works under Comet") {
      // Disabling Comet's project operator forces a genuine Spark ProjectExec (row output) right
      // below us, inside an otherwise native plan.
      spark.conf.set("spark.comet.exec.project.enabled", "false")
      try {
        val df = spark.sql(s"""SELECT id, lc_python_runner(my_mod.inner(col1 + 1)).x AS x
          FROM t WHERE id < 100""")
        val p = plan(df)
        println(p.treeString)
        val lcs = nodes(df).collect { case l: LcExecBase => l }
        check(lcs.nonEmpty, "operator missing")
        lcs.foreach { l =>
          check(!isTransition(l.child), s"transition below: ${l.child.nodeName}")
          check(!l.child.getClass.getName.contains("comet") && !l.child.supportsColumnar,
            s"expected a row-based Spark operator below, got ${l.child.getClass.getName}")
        }
        val bad = df.join(spark.table("t"), "id").where("x != (col1 + 1) * 2")
        eq(bad.count(), 0L)
      } finally spark.conf.unset("spark.comet.exec.project.enabled")
    }
  }
}

/** Unit check of LcArrowIngest against hand-built Arrow vectors. */
object IngestCheck {
  import org.apache.arrow.vector._
  import org.apache.spark.sql.types._
  import org.apache.spark.sql.vectorized.ArrowColumnVector
  import LcTestMain._

  def run(): Unit = {
    val alloc = LcMemory.root.newChildAllocator("ingest-test", 0, Long.MaxValue)
    try {
      val n = 1000
      def nul(i: Int) = i % 7 == 3
      val ints = new IntVector("i", alloc); ints.allocateNew(n)
      val longs = new BigIntVector("l", alloc); longs.allocateNew(n)
      val dbl = new Float8Vector("d", alloc); dbl.allocateNew(n)
      val bits = new BitVector("b", alloc); bits.allocateNew(n)
      val str = new VarCharVector("s", alloc); str.allocateNew()
      val bin = new VarBinaryVector("y", alloc); bin.allocateNew()
      for (i <- 0 until n) {
        if (nul(i)) { ints.setNull(i); longs.setNull(i); dbl.setNull(i); bits.setNull(i)
          str.setNull(i); bin.setNull(i) }
        else {
          ints.set(i, i * 3); longs.set(i, i * 1000000007L); dbl.set(i, i / 3.0)
          bits.set(i, i % 2)
          str.setSafe(i, s"v$i-\u00e9".getBytes("UTF-8")); bin.setSafe(i, Array[Byte](i.toByte, 1))
        }
      }
      Seq(ints, longs, dbl, bits, str, bin).foreach(_.setValueCount(n))
      val cases = Seq[(ValueVector, DataType)](ints -> IntegerType, longs -> LongType,
        dbl -> DoubleType, bits -> BooleanType, str -> StringType, bin -> BinaryType)
      cases.foreach { case (v, dt) =>
        val src = new ArrowColumnVector(v)
        val before = org.apache.spark.sql.execution.arrow.LcArrowIngest.rawCopies.get()
        // copy only the tail half too: exercises non-zero start offsets for var-width
        val raw = org.apache.spark.sql.execution.arrow.LcArrowIngest.copyColumn(
          src, n, "x", dt, "UTC", alloc)
        check(org.apache.spark.sql.execution.arrow.LcArrowIngest.rawCopies.get() == before + 1,
          s"$dt did not take the raw path")
        val viaGetters = org.apache.spark.sql.execution.arrow.LcArrowIngest.newVector(
          "g", dt, "UTC", alloc)
        org.apache.spark.sql.execution.arrow.LcArrowIngest.getterCopy(src, n, viaGetters)
        val a = new ArrowColumnVector(raw); val b = new ArrowColumnVector(viaGetters)
        for (i <- 0 until n) {
          check(a.isNullAt(i) == b.isNullAt(i) && a.isNullAt(i) == nul(i), s"$dt null @ $i")
          if (!nul(i)) {
            val same = dt match {
              case IntegerType => a.getInt(i) == b.getInt(i) && a.getInt(i) == i * 3
              case LongType => a.getLong(i) == b.getLong(i)
              case DoubleType => a.getDouble(i) == b.getDouble(i)
              case BooleanType => a.getBoolean(i) == b.getBoolean(i) && a.getBoolean(i) == (i % 2 == 1)
              case StringType => a.getUTF8String(i) == b.getUTF8String(i) &&
                a.getUTF8String(i).toString == s"v$i-\u00e9"
              case BinaryType => a.getBinary(i).sameElements(b.getBinary(i))
            }
            check(same, s"$dt value @ $i")
          }
        }
        a.close(); b.close()
      }
      // a slice with a non-zero first offset: offsets must be rebased
      val tp = str.getTransferPair(alloc)
      tp.splitAndTransfer(10, 50)
      val sliced = tp.getTo.asInstanceOf[VarCharVector]
      val copy = org.apache.spark.sql.execution.arrow.LcArrowIngest.copyColumn(
        new ArrowColumnVector(sliced), 50, "z", StringType, "UTC", alloc)
      val cv = new ArrowColumnVector(copy)
      for (i <- 0 until 50) {
        val j = i + 10
        check(cv.isNullAt(i) == nul(j), s"slice null @ $i")
        if (!nul(j)) eq(cv.getUTF8String(i).toString, s"v$j-\u00e9", s"slice value @ $i")
      }
      cv.close(); sliced.close()
      Seq(ints, longs, dbl, bits, str, bin).foreach(_.close())
    } finally {
      eq(alloc.getAllocatedMemory, 0L, "ingest test leaked")
      alloc.close()
    }
  }
}

/**
 * Sandbox enforcement, run inside the runtime container (entrypoint as root -> JVM as sparkuser,
 * cgroup v2 without systemd, iptables egress policy). Expects listeners on 127.0.0.1:18080 (not
 * allowlisted) and 127.0.0.2:18081 (allowlisted via LCPY_EGRESS_ALLOW=127.0.0.2/32), see e2e/.
 */
class SandboxSuites(spark: SparkSession) {
  import LcTestMain._
  import com.fasterxml.jackson.databind.ObjectMapper
  import java.nio.file.{Files, Paths}
  import scala.collection.JavaConverters._

  private val mapper = new ObjectMapper()
  private val cgRoot = Paths.get(spark.conf.get(LcConf.CGROUP_ROOT, "/sys/fs/cgroup/lcpy/sandbox"))

  private def workerCgroups(): Seq[java.nio.file.Path] = {
    val s = Files.list(cgRoot)
    try s.iterator().asScala.filter(p => p.getFileName.toString.startsWith("w-")).toList
    finally s.close()
  }
  private def pgrep(pattern: String): Seq[String] = {
    val p = new ProcessBuilder("pgrep", "-af", pattern).redirectErrorStream(true).start()
    val out = scala.io.Source.fromInputStream(p.getInputStream).getLines().toList
    p.waitFor()
    out.filterNot(_.contains("pgrep"))
  }
  private def json(sql: String): com.fasterxml.jackson.databind.JsonNode =
    mapper.readTree(spark.sql(sql).collect().head.getString(0))
  private def denied(n: com.fasterxml.jackson.databind.JsonNode, k: String): Unit =
    check(n.get(k).asText.startsWith("denied"), s"$k should be denied, got ${n.get(k)}")
  private def waitFor(what: String, secs: Int)(cond: => Boolean): Unit = {
    val end = System.currentTimeMillis() + secs * 1000L
    while (!cond && System.currentTimeMillis() < end) Thread.sleep(100)
    check(cond, s"timed out waiting for: $what")
  }

  def run(): Unit = {
    test("[sandbox] filesystem, process, user and environment isolation") {
      val hostPath = WorkerProcess.workerScript().getParent.toString
      val r = json(s"SELECT lc_python_runner(sandbox_probe.fs('$hostPath')).report FROM range(1)")
      println(r.toPrettyString)
      eq(r.get("uid").asInt, 65534, "uid inside the sandbox")
      check(r.get("pid").asInt < 10, s"worker should see a fresh PID namespace, pid=${r.get("pid")}")
      eq(r.get("hostname").asText, "lcpy-sandbox")
      val root = r.get("root_listing").elements().asScala.map(_.asText).toSet
      check(root.subsetOf(Set("bin", "dev", "etc", "lcpy", "lib", "lib64", "proc", "sbin", "tmp", "usr")),
        s"unexpected entries in /: $root")
      val envText = r.get("env").toString
      check(!envText.contains("lcpy-topsecret"), s"executor secret leaked into the sandbox: $envText")
      check(!envText.contains("SPARK_"), s"Spark env leaked: $envText")
      Seq("etc_shadow", "k8s_token", "host_path", "sys", "write_usr", "write_registry")
        .foreach(denied(r, _))
      check(r.get("write_tmp").asText.startsWith("ok"), "private /tmp must be writable")
      val pids = r.get("proc_pids").elements().asScala.map(_.asInt).toSeq
      check(pids.forall(_ < 20), s"host processes visible in /proc: $pids")
      val etc = r.get("etc_listing").elements().asScala.map(_.asText).toSet
      check(!etc.contains("shadow") && !etc.contains("spark"), s"/etc exposes: $etc")
    }

    test("[sandbox] seccomp + no-new-userns: unix/netlink/raw sockets, unshare, mount, setuid") {
      val r = json("SELECT lc_python_runner(sandbox_probe.syscalls(id)).report FROM range(1)")
      println(r.toPrettyString)
      Seq("af_unix", "abstract_connect", "netlink", "unshare_user", "mount", "raw_socket", "setuid0")
        .foreach(denied(r, _))
      check(r.get("socketpair").asText.startsWith("ok"), "socketpair stays available")
    }

    test("[sandbox] egress: allowlist only; loopback, metadata, cluster and internet blocked") {
      val res = spark.sql("""SELECT host, port, lc_python_runner(sandbox_probe.connect(host, port)).result AS r
        FROM VALUES ('127.0.0.2', 18081), ('127.0.0.1', 18080), ('169.254.169.254', 80),
                    ('10.96.0.1', 443), ('1.1.1.1', 443) AS v(host, port)""").collect()
        .map(r => s"${r.getString(0)}:${r.getInt(1)}" -> r.getString(2)).toMap
      res.foreach(println)
      check(res("127.0.0.2:18081").startsWith("connected"), s"allowlisted: ${res("127.0.0.2:18081")}")
      Seq("127.0.0.1:18080", "169.254.169.254:80", "10.96.0.1:443", "1.1.1.1:443").foreach { k =>
        check(!res(k).startsWith("connected"), s"$k must be blocked, got ${res(k)}")
      }
      check(res("127.0.0.1:18080").contains("ECONNREFUSED"),
        s"loopback service (listening) must be actively rejected, got ${res("127.0.0.1:18080")}")
      // ...while the executor JVM itself keeps full network access
      val s = new java.net.Socket()
      s.connect(new java.net.InetSocketAddress("127.0.0.1", 18080), 3000)
      s.close()
    }

    test("[sandbox] cgroup limits are applied per worker and removed afterwards") {
      spark.conf.set(LcConf.WORKER_MEMORY, "300m")
      spark.conf.set(LcConf.WORKER_PIDS, "40")
      spark.conf.set(LcConf.WORKER_CPUS, "0.5")
      val seen = new java.util.concurrent.ConcurrentHashMap[String, String]()
      val t = new Thread(() => try {
        spark.sql("SELECT lc_python_runner(sandbox_probe.sleepy(id)).x FROM range(1)").collect()
      } catch { case NonFatal(_) => })
      t.start()
      try {
        waitFor("a worker cgroup", 30)(workerCgroups().nonEmpty)
        val d = workerCgroups().head
        Seq("memory.max", "memory.swap.max", "pids.max", "cpu.max", "memory.oom.group").foreach { f =>
          val p = d.resolve(f)
          if (Files.exists(p)) seen.put(f, new String(Files.readAllBytes(p)).trim)
        }
        println(s"$d: $seen")
        eq(seen.get("memory.max"), (300L << 20).toString)
        eq(seen.get("pids.max"), "40")
        eq(seen.get("cpu.max"), "50000 100000")
        eq(seen.get("memory.oom.group"), "1")
        if (seen.containsKey("memory.swap.max")) eq(seen.get("memory.swap.max"), "0")
        t.join(60000)
        waitFor("worker cgroups removed", 15)(workerCgroups().isEmpty)
      } finally {
        Seq(LcConf.WORKER_MEMORY, LcConf.WORKER_PIDS, LcConf.WORKER_CPUS).foreach(spark.conf.unset)
      }
    }

    test("[sandbox] fork bomb is capped by pids.max") {
      spark.conf.set(LcConf.WORKER_PIDS, "32")
      try {
        val n = spark.sql("SELECT lc_python_runner(sandbox_probe.fork_bomb(id)).forks FROM range(1)")
          .collect().head.getInt(0)
        println(s"forks before EAGAIN: $n")
        check(n < 32, s"pids.max=32 but $n forks succeeded")
        waitFor("fork-bomb children gone", 15)(pgrep("sandbox_probe").isEmpty && workerCgroups().isEmpty)
      } finally spark.conf.unset(LcConf.WORKER_PIDS)
    }

    test("[sandbox] memory cap: kernel OOM kill is reported with the pre-crash output") {
      spark.conf.set(LcConf.WORKER_MEMORY, "200m")
      try {
        val msg = execError(spark.sql("SELECT lc_python_runner(sandbox_probe.mem_hog(id)) FROM range(1)"),
          "before-oom-marker", "OOM-killed", "SIGKILL")
        println(msg.take(1500))
      } finally spark.conf.unset(LcConf.WORKER_MEMORY)
    }

    test("[sandbox] CPU quota (0.5 CPU) throttles a busy loop") {
      spark.conf.set(LcConf.WORKER_CPUS, "0.5")
      try {
        val r = spark.sql("SELECT lc_python_runner(sandbox_probe.cpu_burn(id)) AS r FROM range(1)")
          .selectExpr("r.cpu", "r.wall").collect().head
        val ratio = r.getDouble(0) / r.getDouble(1)
        println(f"cpu=${r.getDouble(0)}%.2fs wall=${r.getDouble(1)}%.2fs ratio=$ratio%.2f")
        check(ratio < 0.7, f"0.5 CPU quota not enforced: ratio $ratio%.2f")
      } finally spark.conf.unset(LcConf.WORKER_CPUS)
    }

    test("[sandbox] no process outlives its worker (detached children, double-forked daemons)") {
      spark.sql("SELECT lc_python_runner(sandbox_probe.orphans(id)).pids FROM range(2)").collect()
      waitFor("orphans killed", 15)(pgrep("sleep 30[12]").isEmpty)
      eq(workerCgroups(), Nil, "worker cgroups")
    }

    test("[sandbox] /tmp is private and ephemeral per worker") {
      val q = "SELECT lc_python_runner(sandbox_probe.tmp_marker(id)).seen_previous FROM range(1)"
      spark.sql(q).collect()
      eq(spark.sql(q).collect().head.getBoolean(0), false, "a later worker saw an earlier worker's /tmp")
    }

    if (sys.env.get("LCPY_KILL9").contains("1")) test("[kill9] long-running sandboxed query") {
      spark.conf.set(LcConf.BATCH_ROWS, "1")
      spark.sql("SELECT lc_python_runner(sandbox_probe.sleepy(id)).x FROM range(200)").collect()
    }

    test("[sandbox] task cancellation tears down the worker and its cgroup") {
      val sc = spark.sparkContext
      val t = new Thread(() => try {
        sc.setJobGroup("lcpy-cancel", "cancel me", interruptOnCancel = true)
        spark.sql("SELECT lc_python_runner(sandbox_probe.sleepy(id)).x FROM range(4)").collect()
      } catch { case NonFatal(_) => })
      t.start()
      waitFor("workers started", 30)(workerCgroups().nonEmpty)
      waitFor("python running in the sandbox", 30)(pgrep("^/usr/bin/python3 .*lcpy/worker.py").nonEmpty)
      println(s"before cancel: ${pgrep("lcpy/worker.py").mkString(" | ")}")
      sc.cancelJobGroup("lcpy-cancel")
      t.join(30000)
      try waitFor("workers and cgroups gone after cancel", 15)(
        workerCgroups().isEmpty && pgrep("lcpy/worker.py").isEmpty && WorkerProcess.live.isEmpty)
      finally println(s"after cancel: cgroups=${workerCgroups()} procs=${pgrep("lcpy/worker.py")} " +
        s"live=${WorkerProcess.live.keySet()}")
      eq(LcMemory.leaks.get(), 0L, "leaked allocators after cancellation")
    }

    test("[sandbox] no Arrow memory leaked, no workers left") {
      eq(LcMemory.leaks.get(), 0L, "leaked allocators")
      eq(WorkerProcess.live.size(), 0, "live workers")
    }
  }
}
