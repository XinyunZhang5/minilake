package io.minilake.spark

import io.minilake.MiniLakeTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}
import java.sql.Date
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class MiniLakeSparkSuite extends AnyFunSuite with BeforeAndAfterAll {

  private lazy val spark: SparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("minilake-test")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  private def tempTable(): String = Files.createTempDirectory("minilake").resolve("t").toString

  private def read(path: String, opts: (String, String)*): DataFrame =
    spark.read.format("minilake").options(opts.toMap).load(path)

  private def people(from: Int, until: Int): DataFrame = {
    import spark.implicits._
    (from until until).map(i => (i.toLong, s"user$i", if (i % 2 == 0) "even" else "odd")).toDF("id", "name", "kind")
  }

  private def sameRows(a: DataFrame, b: DataFrame): Unit =
    assert(a.collect().map(_.toString).sorted.toSeq == b.collect().map(_.toString).sorted.toSeq)

  test("write then read round-trips rows and creates version 0") {
    val path = tempTable()
    people(0, 100).write.format("minilake").save(path)
    sameRows(read(path), people(0, 100))
    assert(MiniLakeTable.forPath(path).latestVersion() == 0)
  }

  test("append adds a version; overwrite replaces; time travel sees the past") {
    val path = tempTable()
    people(0, 10).write.format("minilake").save(path)
    people(10, 20).write.format("minilake").mode("append").save(path)
    assert(read(path).count() == 20)

    people(100, 105).write.format("minilake").mode("overwrite").save(path)
    sameRows(read(path), people(100, 105))

    sameRows(read(path, "versionAsOf" -> "0"), people(0, 10))
    sameRows(read(path, "versionAsOf" -> "1"), people(0, 20))
    val ts1 = MiniLakeTable.forPath(path).commitTimestamp(1)
    sameRows(read(path, "timestampAsOf" -> ts1.toString), people(0, 20))
  }

  test("save modes: ErrorIfExists and Ignore") {
    val path = tempTable()
    people(0, 5).write.format("minilake").save(path)
    intercept[IllegalStateException](people(0, 5).write.format("minilake").mode(SaveMode.ErrorIfExists).save(path))
    people(50, 60).write.format("minilake").mode(SaveMode.Ignore).save(path)
    assert(read(path).count() == 5)
  }

  test("schema mismatch on append is rejected") {
    val path = tempTable()
    people(0, 5).write.format("minilake").save(path)
    val e = intercept[IllegalArgumentException](
      people(0, 5).withColumn("extra", lit(1)).write.format("minilake").mode("append").save(path))
    assert(e.getMessage.contains("schema mismatch"))
  }

  test("partitionBy: values come back typed and partition filters prune files") {
    val path = tempTable()
    people(0, 100).write.format("minilake").partitionBy("kind").save(path)
    val df = read(path)
    assert(df.schema.fieldNames.toSeq == Seq("id", "name", "kind"))
    val evens = df.filter(col("kind") === "even")
    assert(evens.count() == 50)
    val scan = MiniLakeFileIndex.lastScan.get()
    assert(scan.scannedFiles < scan.totalFiles, scan)
    sameRows(evens, people(0, 100).filter(col("kind") === "even").select("id", "name", "kind"))
  }

  test("null partition values survive the round trip") {
    import spark.implicits._
    val path = tempTable()
    Seq((1L, Some("a")), (2L, None)).toDF("id", "p").write.format("minilake").partitionBy("p").save(path)
    assert(read(path).filter(col("p").isNull).collect().toSeq == Seq(Row(2L, null)))
    assert(MiniLakeFileIndex.lastScan.get().scannedFiles == 1)
  }

  test("min/max stats skip files on non-partition columns") {
    val path = tempTable()
    people(0, 1000).repartitionByRange(10, col("id")).write.format("minilake").save(path)
    val q = read(path).filter(col("id") < 50)
    assert(q.count() == 50)
    val scan = MiniLakeFileIndex.lastScan.get()
    assert(scan.totalFiles == 10 && scan.scannedFiles == 1, scan)

    // Reversed operands, IN, and string ranges go through the same path.
    assert(read(path).filter(lit(990) < col("id")).count() == 9)
    assert(MiniLakeFileIndex.lastScan.get().scannedFiles == 1)
    assert(read(path).filter(col("id").isin(5, 995)).count() == 2)
    assert(MiniLakeFileIndex.lastScan.get().scannedFiles == 2)
  }

  test("skipping never changes query results, including NOT over partially convertible filters") {
    val path = tempTable()
    val data = people(0, 1000).repartitionByRange(10, col("id"))
    data.write.format("minilake").save(path)
    val expected = data.cache()
    val filters = Seq(
      not(col("id") < 500 && col("name").contains("7")), // NOT(AND(convertible, not convertible))
      not(col("id") >= 100) || col("name") === "user999",
      col("id").between(95, 105),
      !(col("id") =!= 3),
      col("id") < 100 && col("name").endsWith("9"))
    for (f <- filters) withClue(f.toString) { sameRows(read(path).filter(f), expected.filter(f)) }
  }

  test("date columns: stats and predicates") {
    import spark.implicits._
    val path = tempTable()
    Seq((1L, Date.valueOf("2026-01-01")), (2L, Date.valueOf("2026-06-01")))
      .toDF("id", "d").repartition(2, col("id")).write.format("minilake").save(path)
    assert(read(path).filter(col("d") > Date.valueOf("2026-03-01")).collect().toSeq ==
      Seq(Row(2L, Date.valueOf("2026-06-01"))))
    assert(MiniLakeFileIndex.lastScan.get().scannedFiles == 1)
  }

  test("files not in the log are invisible") {
    val path = tempTable()
    people(0, 10).write.format("minilake").save(path)
    // Simulates a writer that crashed after writing data but before committing.
    people(10, 20).write.parquet(Path.of(path, "_data", "orphan").toString)
    assert(read(path).count() == 10)
  }

  test("concurrent appends from separate threads all commit") {
    val path = tempTable()
    people(0, 1).write.format("minilake").save(path)
    implicit val ec: ExecutionContext = ExecutionContext.global
    val writes = (1 to 6).map(i => Future(people(i * 100, i * 100 + 10).write.format("minilake").mode("append").save(path)))
    Await.result(Future.sequence(writes), 2.minutes)
    assert(read(path).count() == 61)
    assert(MiniLakeTable.forPath(path).latestVersion() == 6)
  }
}
