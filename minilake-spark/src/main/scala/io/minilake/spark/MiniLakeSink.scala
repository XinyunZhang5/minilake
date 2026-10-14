package io.minilake.spark

import io.minilake.MiniLakeTable
import io.minilake.actions.{Action, Metadata, SetTransaction}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

/**
 * Structured Streaming sink with exactly-once writes.
 *
 * Spark guarantees each micro-batch is delivered at least once: after a crash it re-runs the last
 * batch whose completion it didn't record, even if that batch's output was already committed.
 * So every batch commits its data files together with `txn(appId = query id, version = batchId)`
 * in one atomic log commit, and a batch whose id is not above the table's recorded version for
 * this query is skipped. The query id is stable across restarts from the same checkpoint.
 *
 * Reading the txn version also registers it with the transaction, so two runs of the same query
 * racing to commit the same batch conflict instead of both writing.
 */
class MiniLakeSink(
    spark: SparkSession,
    path: String,
    partitionBy: Seq[String],
    outputMode: OutputMode,
    appIdOption: Option[String]) extends Sink {

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val appId = appIdOption.orElse(Option(spark.sparkContext.getLocalProperty(MiniLakeSink.QueryIdKey)))
      .getOrElse(throw new IllegalStateException("no streaming query id; set the 'txnAppId' option"))
    val table = MiniLakeTable.forPath(path)
    val txn = table.startTransaction()
    val existing = txn.snapshot()
    if (txn.txnVersion(appId).map[Boolean](_ >= batchId).orElse(false)) {
      MiniLakeSink.skippedBatches.incrementAndGet()
      return
    }

    val schema = SchemaConverter.toMiniLake(data.schema)
    val partitionColumns =
      if (existing.exists()) {
        DefaultSource.checkCompatible(existing, data.schema, partitionBy)
        existing.metadata().partitionColumns().asScala.toSeq
      } else {
        txn.updateMetadata(new Metadata(UUID.randomUUID().toString, schema.toJson, partitionBy.asJava,
          java.util.Map.of(), System.currentTimeMillis()))
        partitionBy
      }

    val actions = ArrayBuffer[Action]()
    if (outputMode == OutputMode.Complete() && existing.exists()) {
      val now = System.currentTimeMillis()
      txn.scanAll().asScala.foreach(f => actions += f.remove(now, true))
    }
    // The batch DataFrame still carries the streaming flag: `.write` refuses it, and `.rdd`
    // re-analyzes it as a streaming query. Its already-planned incremental execution is fine, so
    // take those rows and wrap them as a plain DataFrame.
    val toRow = CatalystTypeConverters.createToScalaConverter(data.schema)
    val rows = data.queryExecution.toRdd.map(r => toRow(r).asInstanceOf[Row])
    val batch = spark.createDataFrame(rows, data.schema)
    actions ++= DefaultSource.writeFiles(spark, table, batch, partitionColumns, schema)
    actions += new SetTransaction(appId, batchId, System.currentTimeMillis())
    txn.commit(actions.asJava, "STREAMING UPDATE", Map(
      "queryId" -> appId, "batchId" -> batchId.toString, "outputMode" -> outputMode.toString).asJava)
  }

  override def toString: String = s"MiniLakeSink[$path]"
}

object MiniLakeSink {
  /** Local property Spark sets on every job of a streaming query (StreamExecution.QUERY_ID_KEY). */
  val QueryIdKey = "sql.streaming.queryId"

  /** Batches skipped because they were already committed; for tests. */
  val skippedBatches = new AtomicLong()
}
