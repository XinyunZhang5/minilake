package io.minilake.spark

import io.minilake.MiniLakeTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

class OptimizeSuite extends AnyFunSuite with BeforeAndAfterAll {

  private lazy val spark: SparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("minilake-optimize-test")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  private def tempTable(): String = Files.createTempDirectory("minilake").resolve("t").toString

  private def read(path: String, opts: (String, String)*): DataFrame =
    spark.read.format("minilake").options(opts.toMap).load(path)

  private def rows(df: DataFrame): Seq[String] = df.collect().map(_.toString).sorted.toSeq

  private def numFiles(path: String): Int = MiniLakeTable.forPath(path).latest().numFiles()

  /** x and y uniform in [0, 1000), rows shuffled across `files` files so every file spans both ranges. */
  private def points(n: Int, files: Int): DataFrame =
    spark.range(n).select(
      col("id"),
      (rand(1) * 1000).cast("long").as("x"),
      (rand(2) * 1000).cast("long").as("y"))
      .repartition(files)

  test("bin-packing merges small appends into one file without changing contents or history") {
    val path = tempTable()
    for (i <- 0 until 12) spark.range(i * 10, i * 10 + 10).write.format("minilake").mode("append").save(path)
    val before = rows(read(path))
    assert(numFiles(path) >= 12)
    val versionBefore = MiniLakeTable.forPath(path).latestVersion()

    val m = Optimize.run(spark, path)
    assert(m.version.contains(versionBefore + 1))
    assert(numFiles(path) == 1 && m.filesAdded == 1 && m.filesRemoved >= 12, m)
    assert(rows(read(path)) == before)
    assert(rows(read(path, "versionAsOf" -> versionBefore.toString)) == before, "time travel still sees old files")

    val commit = Files.readString(Path.of(path, "_log", f"${versionBefore + 1}%020d.json"))
    assert(commit.contains("\"dataChange\":false") && !commit.contains("\"dataChange\":true"), commit)
    assert(Optimize.run(spark, path).version.isEmpty, "nothing left to compact is a no-op")
  }

  test("bin-packing works per partition and leaves files at the target size alone") {
    val path = tempTable()
    val df = spark.range(0, 400).withColumn("p", (col("id") % 2).cast("string"))
    for (i <- 0 until 4) df.filter(col("id") % 4 === i).write.format("minilake").partitionBy("p").mode("append").save(path)
    val before = rows(read(path))
    assert(numFiles(path) >= 8)

    val m = Optimize.run(spark, path)
    assert(numFiles(path) == 2, m)
    assert(rows(read(path)) == before)
    assert(read(path).filter(col("p") === "1").count() == 200)

    // Every file is now >= a 1-byte target, so nothing qualifies as small.
    for (i <- 0 until 3) spark.range(1000 + i, 1001 + i).withColumn("p", lit("0")).write.format("minilake").mode("append").save(path)
    assert(Optimize.run(spark, path, targetFileBytes = 1).version.isEmpty)
  }

  test("Z-order lets min/max skipping work on both columns") {
    val random = tempTable()
    val zordered = tempTable()
    val data = points(40000, 16).cache()
    data.write.format("minilake").save(random)
    data.write.format("minilake").save(zordered)
    val totalBytes = MiniLakeTable.forPath(zordered).latest().allFiles().asScala.map(_.size()).sum
    val m = Optimize.run(spark, zordered, zOrderBy = Seq("x", "y"), targetFileBytes = totalBytes / 16)
    assert(m.filesAdded >= 12, m)

    def scanned(path: String, f: org.apache.spark.sql.Column): Int = {
      val got = rows(read(path).filter(f))
      assert(got == rows(data.filter(f)), s"results differ for $f")
      MiniLakeFileIndex.lastScan.get().scannedFiles
    }
    for (f <- Seq(col("x") < 50, col("y") < 50, col("x").between(400, 450) && col("y").between(400, 450))) {
      val r = scanned(random, f)
      val z = scanned(zordered, f)
      withClue(s"$f: random read $r files, Z-ordered read $z of ${m.filesAdded}") {
        assert(r == 16)
        assert(z <= m.filesAdded / 2)
      }
    }
  }

  test("Z-order keeps partitions apart") {
    val path = tempTable()
    points(4000, 8).withColumn("p", (col("id") % 2).cast("string")).write.format("minilake").partitionBy("p").save(path)
    val before = rows(read(path))
    Optimize.run(spark, path, zOrderBy = Seq("x", "y"), targetFileBytes = 1024)
    assert(rows(read(path)) == before)
    for (f <- MiniLakeTable.forPath(path).latest().allFiles().asScala) {
      assert(f.path().contains(s"p=${f.partitionValues().get("p")}/"), f.path())
    }
  }

  test("Z-order rejects partition, unknown and non-numeric columns") {
    val path = tempTable()
    points(100, 2).withColumn("s", lit("a")).withColumn("p", lit("0"))
      .write.format("minilake").partitionBy("p").save(path)
    for (bad <- Seq(Seq("p"), Seq("nope"), Seq("s"), Seq("x", "x"))) {
      intercept[IllegalArgumentException](Optimize.run(spark, path, zOrderBy = bad))
    }
  }
}
