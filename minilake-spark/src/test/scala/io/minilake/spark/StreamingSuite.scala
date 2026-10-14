package io.minilake.spark

import io.minilake.MiniLakeTable
import org.apache.kafka.clients.admin.{Admin, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.testcontainers.DockerClientFactory
import org.testcontainers.kafka.KafkaContainer

import java.nio.file.{Files, Path}
import java.util.Properties
import scala.jdk.CollectionConverters._

class StreamingSuite extends AnyFunSuite with BeforeAndAfterAll {

  private lazy val spark: SparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("minilake-streaming-test")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  private def dirs(): (String, Path) = {
    val root = Files.createTempDirectory("minilake-stream")
    (root.resolve("t").toString, root.resolve("checkpoint"))
  }

  private def read(path: String): DataFrame = spark.read.format("minilake").load(path)

  /**
   * Simulates a driver crash after the sink committed batch `n` but before Spark wrote its own
   * commit marker: on restart Spark re-runs batch `n` with the same data.
   */
  private def forgetCommit(checkpoint: Path, n: Long): Unit = {
    Files.delete(checkpoint.resolve("commits").resolve(n.toString))
    Files.deleteIfExists(checkpoint.resolve("commits").resolve(s".$n.crc"))
  }

  private def lastCommittedBatch(checkpoint: Path): Long =
    Files.list(checkpoint.resolve("commits")).iterator().asScala
      .map(_.getFileName.toString).filter(_.forall(_.isDigit)).map(_.toLong).max

  test("a replayed micro-batch is skipped, so every row lands exactly once") {
    val (path, checkpoint) = dirs()
    val input = MemoryStream[Long](Encoders.scalaLong, spark)
    def start(): StreamingQuery = input.toDF()
      .select(col("value").as("id"), (col("value") % 3).as("k"))
      .writeStream.format("minilake").option("checkpointLocation", checkpoint.toString).start(path)

    val q1 = start()
    input.addData(0L until 10L)
    q1.processAllAvailable()
    input.addData(10L until 20L)
    q1.processAllAvailable()
    q1.stop()
    val versionBefore = MiniLakeTable.forPath(path).latestVersion()

    forgetCommit(checkpoint, lastCommittedBatch(checkpoint))
    val skipped = MiniLakeSink.skippedBatches.get()
    val q2 = start()
    q2.processAllAvailable()
    assert(MiniLakeSink.skippedBatches.get() == skipped + 1, "Spark did replay the batch")
    assert(MiniLakeTable.forPath(path).latestVersion() == versionBefore, "and the replay committed nothing")

    input.addData(20L until 30L)
    q2.processAllAvailable()
    q2.stop()
    val ids = read(path).select("id").collect().map(_.getLong(0)).sorted.toSeq
    assert(ids == (0L until 30L))
    val queryId = q2.id.toString
    assert(MiniLakeTable.forPath(path).latest().txnVersion(queryId).get() == lastCommittedBatch(checkpoint))
  }

  test("complete mode replaces the table with each batch's full result") {
    val (path, checkpoint) = dirs()
    val input = MemoryStream[Long](Encoders.scalaLong, spark)
    val q = input.toDF().groupBy((col("value") % 2).as("parity")).count()
      .writeStream.format("minilake").outputMode("complete")
      .option("checkpointLocation", checkpoint.toString).start(path)
    input.addData(0L until 5L)
    q.processAllAvailable()
    input.addData(5L until 12L)
    q.processAllAvailable()
    q.stop()
    assert(read(path).orderBy("parity").collect().map(r => (r.getLong(0), r.getLong(1))).toSeq == Seq((0L, 6L), (1L, 6L)))
  }

  test("update output mode is rejected") {
    val (path, checkpoint) = dirs()
    val input = MemoryStream[Long](Encoders.scalaLong, spark)
    intercept[IllegalArgumentException] {
      input.toDF().groupBy("value").count().writeStream.format("minilake").outputMode("update")
        .option("checkpointLocation", checkpoint.toString).start(path)
    }
  }

  test("Kafka to MiniLake: exactly once across a crash-replay and a restart") {
    assume(DockerClientFactory.instance().isDockerAvailable, "needs Docker for the Kafka container")
    val kafka = new KafkaContainer("apache/kafka:3.9.1")
    kafka.start()
    try {
      val topic = "events"
      val props = new Properties()
      props.put("bootstrap.servers", kafka.getBootstrapServers)
      val admin = Admin.create(props)
      admin.createTopics(java.util.List.of(new NewTopic(topic, 4, 1.toShort))).all().get()
      admin.close()
      props.put("key.serializer", classOf[StringSerializer].getName)
      props.put("value.serializer", classOf[StringSerializer].getName)
      props.put("enable.idempotence", "true")
      val producer = new KafkaProducer[String, String](props)
      def produce(range: Range): Unit = {
        range.foreach(i => producer.send(new ProducerRecord(topic, i.toString, i.toString)))
        producer.flush()
      }

      val (path, checkpoint) = dirs()
      def start(): StreamingQuery = spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka.getBootstrapServers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "100")
        .load()
        .select(col("value").cast("string").cast("long").as("id"), col("partition"), col("offset"))
        .writeStream.format("minilake").option("checkpointLocation", checkpoint.toString).start(path)

      produce(0 until 1000)
      val q1 = start()
      q1.processAllAvailable()
      q1.stop()
      val batches = lastCommittedBatch(checkpoint) + 1
      assert(batches >= 10, s"maxOffsetsPerTrigger split the backlog into $batches batches")

      forgetCommit(checkpoint, batches - 1)
      produce(1000 until 1500)
      val skipped = MiniLakeSink.skippedBatches.get()
      val q2 = start()
      q2.processAllAvailable()
      q2.stop()
      producer.close()

      assert(MiniLakeSink.skippedBatches.get() == skipped + 1)
      val rows = read(path)
      assert(rows.count() == 1500)
      assert(rows.select("id").distinct().count() == 1500)
      assert(rows.select("partition", "offset").distinct().count() == 1500)
      println(s"kafka -> minilake: 1500 records in ${lastCommittedBatch(checkpoint) + 1} batches, " +
        s"1 replayed batch skipped, 0 duplicates")
    } finally kafka.stop()
  }
}
