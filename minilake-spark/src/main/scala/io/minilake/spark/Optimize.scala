package io.minilake.spark

import io.minilake.MiniLakeTable
import io.minilake.actions.{Action, AddFile}
import io.minilake.layout.ZOrder
import io.minilake.schema.{DataType => MLType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

/**
 * What one OPTIMIZE did. `version` is None when there was nothing worth rewriting.
 */
case class OptimizeMetrics(
    version: Option[Long],
    filesRemoved: Int,
    filesAdded: Int,
    bytesRemoved: Long,
    bytesAdded: Long,
    zOrderBy: Seq[String])

/**
 * OPTIMIZE rewrites files without changing the table's contents, so its commit marks every add and
 * remove `dataChange = false` and records only the exact input files as read. Together that means
 * concurrent appends never conflict with it, while a concurrent delete or overwrite of one of its
 * inputs does (otherwise the rewrite would resurrect deleted rows).
 *
 *  - Without Z-order it bin-packs: in each partition, files smaller than the target size are
 *    merged into as few target-sized files as possible. Large files are left alone.
 *  - With Z-order it rewrites every file of each partition that has more than one, range-partitioned
 *    and sorted by the interleaved bits of the chosen columns, so each output file covers a small
 *    box in all of them at once and min/max skipping works on every Z-order column, not just the first.
 */
object Optimize {
  val DefaultTargetFileBytes: Long = 128L * 1024 * 1024

  /** Bits per column for the rank buckets: 1024 buckets is far finer than any realistic file count. */
  private val MaxBitsPerColumn = 10

  def run(
      spark: SparkSession,
      path: String,
      zOrderBy: Seq[String] = Nil,
      targetFileBytes: Long = DefaultTargetFileBytes): OptimizeMetrics = {
    require(targetFileBytes > 0, "targetFileBytes must be positive")
    val table = MiniLakeTable.forPath(new java.io.File(path).getAbsolutePath)
    val txn = table.startTransaction()
    val snapshot = txn.snapshot()
    require(snapshot.exists(), s"no MiniLake table at ${table.path()}")
    val schema = snapshot.schema()
    val partitionColumns = snapshot.metadata().partitionColumns().asScala.toSeq
    validateZOrder(zOrderBy, schema, partitionColumns)

    val byPartition = snapshot.allFiles().asScala.toSeq.groupBy(_.partitionValues().asScala.toMap).values.toSeq
    val groups: Seq[Seq[AddFile]] =
      if (zOrderBy.nonEmpty) byPartition.filter(_.size > 1)
      else byPartition.map(_.filter(_.size() < targetFileBytes)).filter(_.size > 1)
    val inputs = groups.flatten
    if (inputs.isEmpty) return OptimizeMetrics(None, 0, 0, 0, 0, zOrderBy)

    txn.readFiles(inputs.asJava)
    val adds = ArrayBuffer[AddFile]()
    if (zOrderBy.nonEmpty) {
      val data = read(spark, table, snapshot, inputs)
      val numFiles = math.max(groups.size, filesFor(inputs, targetFileBytes))
      val laidOut = zOrdered(data, zOrderBy, partitionColumns, numFiles)
      // AQE would otherwise merge small range partitions after the shuffle and blur the layout.
      adds ++= withConf(spark, "spark.sql.adaptive.coalescePartitions.enabled" -> "false") {
        DefaultSource.writeFiles(spark, table, laidOut, partitionColumns, schema, dataChange = false)
      }
    } else {
      // One partition at a time: a scan of small files is already packed into few Spark
      // partitions, and coalesce caps the count without a shuffle.
      for (group <- groups) {
        val data = read(spark, table, snapshot, group).coalesce(filesFor(group, targetFileBytes))
        adds ++= DefaultSource.writeFiles(spark, table, data, partitionColumns, schema, dataChange = false)
      }
    }

    val now = System.currentTimeMillis()
    val actions: Seq[Action] = inputs.map(_.remove(now, false)) ++ adds
    val version = txn.commit(actions.asJava, "OPTIMIZE", Map(
      "zOrderBy" -> zOrderBy.mkString(","),
      "targetFileBytes" -> targetFileBytes.toString).asJava)
    OptimizeMetrics(Some(version), inputs.size, adds.size, inputs.map(_.size()).sum, adds.map(_.size()).sum, zOrderBy)
  }

  private def validateZOrder(cols: Seq[String], schema: io.minilake.schema.Schema, partitionColumns: Seq[String]): Unit = {
    require(cols.distinct.size == cols.size, s"duplicate Z-order columns: $cols")
    require(cols.size <= 6, "at most 6 Z-order columns")
    for (c <- cols) {
      require(schema.fields().asScala.exists(_.name == c), s"unknown column $c")
      require(!partitionColumns.contains(c), s"$c is a partition column; Z-ordering by it does nothing")
      schema.typeOf(c) match {
        case MLType.INT | MLType.LONG | MLType.DOUBLE | MLType.DATE =>
        case t => throw new IllegalArgumentException(s"cannot Z-order by $c of type $t (numeric and date only)")
      }
    }
  }

  private def filesFor(files: Seq[AddFile], target: Long): Int =
    math.max(1, math.ceil(files.map(_.size()).sum.toDouble / target).toInt)

  private def read(spark: SparkSession, table: MiniLakeTable, snapshot: io.minilake.Snapshot, files: Seq[AddFile]): DataFrame =
    spark.baseRelationToDataFrame(DefaultSource.relation(spark, table, snapshot, Some(files.map(_.path()).toSet)))

  /**
   * Values become rank buckets via approximate quantiles, so a skewed or wide-ranged column gets
   * the same resolution as a uniform one; then bucket bits are interleaved into one key.
   */
  private def zOrdered(data: DataFrame, cols: Seq[String], partitionColumns: Seq[String], numFiles: Int): DataFrame = {
    val bits = math.min(MaxBitsPerColumn, ZOrder.maxBitsPerDim(cols.size))
    val buckets = 1 << bits
    val numeric = cols.map(c => if (data.schema(c).dataType == org.apache.spark.sql.types.DateType) unix_date(col(c)) else col(c))
    val asDouble = data.select(cols.zip(numeric).map { case (c, e) => e.cast("double").as(c) }: _*)
    val probs = (1 until buckets).map(_.toDouble / buckets).toArray
    val bounds: Array[Array[Double]] = asDouble.stat.approxQuantile(cols.toArray, probs, 0.001)

    val bucketCols: Seq[Column] = cols.indices.map { i =>
      val b = bounds(i)
      val toBucket = udf { (v: java.lang.Double) =>
        if (v == null) 0
        else if (v.isNaN) buckets - 1
        else {
          // Number of bounds strictly below v; ties go to the lower bucket.
          var lo = 0
          var hi = b.length
          while (lo < hi) { val mid = (lo + hi) >>> 1; if (b(mid) < v) lo = mid + 1 else hi = mid }
          lo
        }
      }
      toBucket(numeric(i).cast("double"))
    }
    val interleave = udf((coords: Seq[Int]) => ZOrder.interleave(coords.toArray, bits))
    val key = "__minilake_z"
    data.withColumn(key, interleave(array(bucketCols: _*)))
      .repartitionByRange(numFiles, (partitionColumns.map(col) :+ col(key)): _*)
      .sortWithinPartitions((partitionColumns.map(col) :+ col(key)): _*)
      .drop(key)
  }

  private def withConf[T](spark: SparkSession, kv: (String, String))(body: => T): T = {
    val (k, v) = kv
    val old = spark.conf.getOption(k)
    spark.conf.set(k, v)
    try body
    finally old match {
      case Some(o) => spark.conf.set(k, o)
      case None => spark.conf.unset(k)
    }
  }
}
