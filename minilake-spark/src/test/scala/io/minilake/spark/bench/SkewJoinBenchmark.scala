package io.minilake.spark.bench

import io.minilake.spark.bench.BenchSupport._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * A join where half the fact rows share one key, run three ways:
 *   baseline - AQE off: the hot key lands in one reduce task
 *   AQE      - adaptive skew join splits the oversized partition at runtime
 *   salting  - AQE off; fact rows get a random salt and the dimension is replicated per salt
 *
 * Broadcast joins are disabled so every variant is a shuffle sort-merge join. The skew threshold is
 * lowered from its 256 MB default because a laptop-sized shuffle never reaches it.
 */
class SkewJoinBenchmark extends AnyFunSuite with BeforeAndAfterAll {
  private lazy val spark: SparkSession = session("minilake-skew-bench",
    "spark.sql.shuffle.partitions" -> "64",
    "spark.sql.autoBroadcastJoinThreshold" -> "-1")
  override def afterAll(): Unit = spark.stop()

  private val FactRows = 8000000L
  private val Keys = 1000000L
  private val HotShare = 0.5
  private val Salts = 32

  test("skewed join: baseline vs AQE skew join vs salting", Benchmark) {
    val dir = tempDir()
    val factPath = dir.resolve("fact").toString
    val dimPath = dir.resolve("dim").toString
    spark.range(FactRows)
      .select(when(rand(7) < HotShare, lit(0L)).otherwise((rand(8) * Keys).cast("long")).as("key"),
        (rand(9) * 100).cast("long").as("amount"))
      .write.format("minilake").save(factPath)
    spark.range(Keys).select(col("id").as("key"), (col("id") % 13).as("region"))
      .write.format("minilake").save(dimPath)
    def fact = spark.read.format("minilake").load(factPath)
    def dim = spark.read.format("minilake").load(dimPath)

    def plain(): DataFrame = fact.join(dim, "key").groupBy("region").agg(sum("amount"))
    def salted(): DataFrame = {
      val f = fact.withColumn("salt", (rand(10) * Salts).cast("int"))
      val d = dim.withColumn("salt", explode(sequence(lit(0), lit(Salts - 1))))
      f.join(d, Seq("key", "salt")).groupBy("region").agg(sum("amount"))
    }

    val aqeOff = Seq("spark.sql.adaptive.enabled" -> "false")
    val aqeSkew = Seq(
      "spark.sql.adaptive.enabled" -> "true",
      "spark.sql.adaptive.skewJoin.enabled" -> "true",
      "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes" -> "4MB",
      "spark.sql.adaptive.advisoryPartitionSizeInBytes" -> "4MB")
    val variants = Seq(
      ("baseline (AQE off)", aqeOff, () => plain()),
      ("AQE skew join", aqeSkew, () => plain()),
      (s"salting x$Salts (AQE off)", aqeOff, () => salted()))

    var expected: Option[Set[String]] = None
    val rows = for ((label, conf, query) <- variants) yield {
      conf.foreach { case (k, v) => spark.conf.set(k, v) }
      val result = query().collect().map(_.toString).toSet
      expected match {
        case None => expected = Some(result)
        case Some(e) => assert(result == e, s"$label returned different results")
      }
      val ms = medianMillis(3)(query().collect())
      val (_, tasks) = withTasks(spark)(query().collect())
      // The join stage is the one whose tasks read the most shuffle records.
      val join = tasks.groupBy(_.stageId).values.maxBy(_.map(_.shuffleRecordsRead).sum)
      val recs = join.map(_.shuffleRecordsRead)
      val times = join.map(_.millis)
      Seq(label, f"$ms%.0f", join.size,
        f"${recs.max}%,d / ${middle(recs)}%,d",
        s"${times.max} / ${middle(times)}",
        f"${times.max.toDouble / math.max(1, middle(times))}%.1fx")
    }
    report("skew.md",
      s"### Skewed join: ${FactRows / 1000000}M fact rows, ${(HotShare * 100).toInt}% on one key, ${Keys / 1000}k dimension keys\n\n" +
        "Spark local[8], 64 shuffle partitions, broadcast disabled. Wall time is the median of 3 runs; " +
        "task stats are from the join stage of one more run.\n\n" +
        table(Seq("variant", "wall ms", "join tasks", "records read max / median", "task ms max / median", "straggler"), rows) + "\n")
  }
}
