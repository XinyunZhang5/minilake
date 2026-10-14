package io.minilake.spark.bench

import io.minilake.MiniLakeTable
import io.minilake.spark.{MiniLakeFileIndex, Optimize}
import io.minilake.spark.bench.BenchSupport._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.jdk.CollectionConverters._

/**
 * How many files does a selective query read under each layout?
 *
 * 4M rows, x and y uniform and independent in [0, 1e6), 64 files per layout:
 *   random  - rows shuffled into files, so every file spans the full range of both columns
 *   linear  - range-partitioned and sorted by x only
 *   z-order - the random table after OPTIMIZE ZORDER BY (x, y)
 */
class SkippingBenchmark extends AnyFunSuite with BeforeAndAfterAll {
  private lazy val spark: SparkSession = session("minilake-skipping-bench", "spark.sql.shuffle.partitions" -> "64")
  override def afterAll(): Unit = spark.stop()

  private val Rows = 4000000L
  private val Files = 64
  private val Range = 1000000L

  private def read(path: String): DataFrame = spark.read.format("minilake").load(path)

  test("data skipping: random vs linear sort vs Z-order", Benchmark) {
    val dir = tempDir()
    val data = spark.range(Rows).select(
      col("id"),
      (rand(1) * Range).cast("long").as("x"),
      (rand(2) * Range).cast("long").as("y"),
      sha2(col("id").cast("string"), 256).as("payload"))

    val random = dir.resolve("random").toString
    val linear = dir.resolve("linear").toString
    val zorder = dir.resolve("zorder").toString
    data.repartition(Files).write.format("minilake").save(random)
    data.repartitionByRange(Files, col("x")).sortWithinPartitions("x").write.format("minilake").save(linear)
    data.repartition(Files).write.format("minilake").save(zorder)
    val bytes = MiniLakeTable.forPath(zorder).latest().allFiles().asScala.map(_.size()).sum
    val t0 = System.nanoTime()
    val m = Optimize.run(spark, zorder, zOrderBy = Seq("x", "y"), targetFileBytes = bytes / Files)
    val optimizeSecs = (System.nanoTime() - t0) / 1e9

    val queries: Seq[(String, Column)] = Seq(
      "x < 10k (1%)" -> (col("x") < 10000),
      "y < 10k (1%)" -> (col("y") < 10000),
      "x, y in [450k, 550k) (1%)" -> (col("x").between(450000, 549999) && col("y").between(450000, 549999)),
      "x = 123456" -> (col("x") === 123456),
      "x < 100k or y < 100k (19%)" -> (col("x") < 100000 || col("y") < 100000))

    val layouts = Seq("random" -> random, "linear (x)" -> linear, "z-order (x, y)" -> zorder)
    val rows = for ((label, q) <- queries) yield {
      val expected = data.filter(q).count()
      val cells = layouts.map { case (_, path) =>
        // A fresh DataFrame per run: re-collecting one would reuse its finished AQE shuffle stages.
        val ms = medianMillis(5)(read(path).filter(q).agg(sum("y")).collect())
        val scan = MiniLakeFileIndex.lastScan.get()
        assert(read(path).filter(q).count() == expected)
        f"${scan.scannedFiles}/${scan.totalFiles} files, $ms%.0f ms"
      }
      label +: cells
    }
    report("skipping.md",
      s"### Data skipping: files read per query\n\n" +
        s"$Rows rows, $Files files per layout, Spark local[8]. Cells: files read / total, median of 5 runs.\n\n" +
        table("query" +: layouts.map(_._1), rows) +
        f"\n\nOPTIMIZE ZORDER BY (x, y): ${m.filesRemoved} -> ${m.filesAdded} files in $optimizeSecs%.1f s.\n")
  }

  test("compaction: 200 small appends vs one OPTIMIZE", Benchmark) {
    val path = tempDir().resolve("t").toString
    for (i <- 0 until 200) {
      spark.range(i * 20000L, (i + 1) * 20000L).select(col("id"), (col("id") % 97).as("k"))
        .coalesce(1).write.format("minilake").mode("append").save(path)
    }
    val q = () => read(path).groupBy("k").count().collect()
    val snapshotMs = () => medianMillis(5)(MiniLakeTable.forPath(path).latest())
    val beforeFiles = MiniLakeTable.forPath(path).latest().numFiles()
    val beforeQuery = medianMillis(5)(q())
    val beforeSnap = snapshotMs()
    val m = Optimize.run(spark, path)
    val afterFiles = MiniLakeTable.forPath(path).latest().numFiles()
    val afterQuery = medianMillis(5)(q())
    val afterSnap = snapshotMs()
    report("compaction.md",
      "### Compaction: 200 appends of 20k rows, then OPTIMIZE\n\n" +
        table(Seq("", "files", "group-by query (median ms)", "load latest snapshot (median ms)"), Seq(
          Seq("before", beforeFiles, f"$beforeQuery%.0f", f"$beforeSnap%.1f"),
          Seq("after", afterFiles, f"$afterQuery%.0f", f"$afterSnap%.1f"))) +
        s"\n\nOPTIMIZE removed ${m.filesRemoved} files and added ${m.filesAdded}.\n")
  }
}
