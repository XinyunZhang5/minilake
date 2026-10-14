package io.minilake.spark

import io.minilake.actions.{Action, AddFile, Metadata}
import io.minilake.{MiniLakeTable, Snapshot}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, HadoopFsRelation}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

import java.net.URI
import java.util.UUID
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

/**
 * `spark.read.format("minilake")` / `df.write.format("minilake")`.
 *
 * Reads return a plain HadoopFsRelation over Parquet whose file list comes from a MiniLake
 * snapshot, so Spark's own Parquet reader, vectorization, column pruning and partition pruning
 * all keep working; MiniLake only decides which files exist.
 */
class DefaultSource extends RelationProvider with CreatableRelationProvider with DataSourceRegister {

  override def shortName(): String = "minilake"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val table = MiniLakeTable.forPath(DefaultSource.tablePath(parameters))
    val snapshot = DefaultSource.selectSnapshot(table, parameters)
    if (!snapshot.exists()) throw new IllegalArgumentException(s"no MiniLake table at ${table.path()}")
    DefaultSource.relation(sqlContext.sparkSession, table, snapshot)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val spark = sqlContext.sparkSession
    val table = MiniLakeTable.forPath(DefaultSource.tablePath(parameters))
    val txn = table.startTransaction()
    val existing = txn.snapshot()
    val partitionBy = parameters.get(DataSourceUtils.PARTITIONING_COLUMNS_KEY)
      .map(DataSourceUtils.decodePartitioningColumns).getOrElse(Seq.empty)

    mode match {
      case SaveMode.ErrorIfExists if existing.exists() =>
        throw new IllegalStateException(s"table ${table.path()} already exists")
      case SaveMode.Ignore if existing.exists() =>
        return DefaultSource.relation(spark, table, existing)
      case _ =>
    }

    val schema = SchemaConverter.toMiniLake(data.schema)
    if (!existing.exists()) {
      txn.updateMetadata(new Metadata(UUID.randomUUID().toString, schema.toJson, partitionBy.asJava,
        java.util.Map.of(), System.currentTimeMillis()))
    } else {
      DefaultSource.checkCompatible(existing, data.schema, partitionBy)
    }
    val partitionColumns = if (existing.exists()) existing.metadata().partitionColumns().asScala.toSeq else partitionBy

    val adds = DefaultSource.writeFiles(spark, table, data, partitionColumns, schema)
    val actions = ArrayBuffer[Action]()
    if (mode == SaveMode.Overwrite && existing.exists()) {
      val now = System.currentTimeMillis()
      txn.scanAll().asScala.foreach(f => actions += f.remove(now, true))
    }
    actions ++= adds
    val op = if (mode == SaveMode.Overwrite) "OVERWRITE" else "WRITE"
    val version = txn.commit(actions.asJava, op,
      Map("mode" -> mode.name(), "partitionBy" -> partitionColumns.mkString(",")).asJava)
    DefaultSource.relation(spark, table, table.snapshotAt(version))
  }
}

object DefaultSource {
  val VersionAsOf = "versionAsOf"
  val TimestampAsOf = "timestampAsOf"

  private def tablePath(parameters: Map[String, String]): String = {
    val raw = parameters.getOrElse("path", throw new IllegalArgumentException("'path' is required"))
    val uri = new URI(raw)
    if (uri.getScheme == null || uri.getScheme == "file") new java.io.File(uri.getPath).getAbsolutePath
    else throw new IllegalArgumentException(s"only local paths are supported for now, got $raw")
  }

  private def selectSnapshot(table: MiniLakeTable, parameters: Map[String, String]): Snapshot =
    (parameters.get(VersionAsOf), parameters.get(TimestampAsOf)) match {
      case (Some(_), Some(_)) => throw new IllegalArgumentException(s"set only one of $VersionAsOf / $TimestampAsOf")
      case (Some(v), None) => table.snapshotAt(v.toLong)
      case (None, Some(ts)) => table.snapshotAsOf(parseTimestamp(ts))
      case (None, None) => table.latest()
    }

  /** Epoch millis, or anything java.sql.Timestamp accepts ("2026-10-13 12:00:00"). */
  private def parseTimestamp(ts: String): Long =
    ts.toLongOption.getOrElse(java.sql.Timestamp.valueOf(ts).getTime)

  /** @param only restrict the relation to these file paths (OPTIMIZE reads just its inputs) */
  private[spark] def relation(
      spark: SparkSession,
      table: MiniLakeTable,
      snapshot: Snapshot,
      only: Option[Set[String]] = None): HadoopFsRelation = {
    val full = SchemaConverter.toSpark(snapshot.schema())
    val partitionCols = snapshot.metadata().partitionColumns().asScala.toSeq
    val partitionSchema = StructType(partitionCols.map(c => full(c)))
    val dataSchema = StructType(full.filterNot(f => partitionCols.contains(f.name)))
    HadoopFsRelation(
      location = new MiniLakeFileIndex(new Path(table.path()), snapshot, partitionSchema, only),
      partitionSchema = partitionSchema,
      dataSchema = dataSchema,
      bucketSpec = None,
      fileFormat = new ParquetFileFormat(),
      options = Map.empty)(spark)
  }

  private def checkCompatible(existing: Snapshot, incoming: StructType, partitionBy: Seq[String]): Unit = {
    val current = SchemaConverter.toSpark(existing.schema())
    def shape(s: StructType) = s.fields.map(f => (f.name, f.dataType)).toSeq
    if (shape(current).sortBy(_._1) != shape(incoming).sortBy(_._1)) {
      throw new IllegalArgumentException(
        s"schema mismatch: table has ${current.simpleString}, data has ${incoming.simpleString}")
    }
    val tableParts = existing.metadata().partitionColumns().asScala.toSeq
    if (partitionBy.nonEmpty && partitionBy != tableParts) {
      throw new IllegalArgumentException(s"table is partitioned by $tableParts, write asked for $partitionBy")
    }
  }

  /**
   * Writes Parquet with Spark's own writer into a fresh directory under the table, then turns
   * every produced file into an AddFile. Nothing is visible until the commit: if the job or the
   * commit fails, the directory is just unreferenced garbage for a future VACUUM.
   */
  private[spark] def writeFiles(
      spark: SparkSession,
      table: MiniLakeTable,
      data: DataFrame,
      partitionColumns: Seq[String],
      schema: io.minilake.schema.Schema,
      dataChange: Boolean = true): Seq[AddFile] = {
    val batchDir = s"_data/${UUID.randomUUID()}"
    val out = new Path(table.path(), batchDir)
    data.write
      .partitionBy(partitionColumns: _*)
      .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
      .parquet(out.toString)

    val conf = spark.sessionState.newHadoopConf()
    val fs: FileSystem = out.getFileSystem(conf)
    val files = fs.listFiles(out, true)
    val adds = ArrayBuffer[AddFile]()
    while (files.hasNext) {
      val st = files.next()
      val name = st.getPath.getName
      if (name.endsWith(".parquet") && !name.startsWith(".") && !name.startsWith("_")) {
        val prefix = out.toUri.getPath + "/"
        val full = st.getPath.toUri.getPath
        require(full.startsWith(prefix), s"$full is not under $prefix")
        val rel = full.substring(prefix.length)
        adds += new AddFile(
          s"$batchDir/$rel",
          partitionValues(rel, partitionColumns).asJava,
          st.getLen,
          st.getModificationTime,
          dataChange,
          ParquetStats.collect(st.getPath, schema, conf))
      }
    }
    adds.toSeq
  }

  /** "a=1/b=x/part-0.parquet" -> {a: "1", b: "x"}; Hive's default-partition marker means null. */
  private def partitionValues(relPath: String, columns: Seq[String]): Map[String, String] = {
    val segs = relPath.split('/').dropRight(1).map { s =>
      val i = s.indexOf('=')
      val v = org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.unescapePathName(s.substring(i + 1))
      s.substring(0, i) -> (if (v == org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.DEFAULT_PARTITION_NAME) null else v)
    }.toMap
    columns.map(c => c -> segs(c)).toMap
  }
}
