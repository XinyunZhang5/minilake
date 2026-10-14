package io.minilake.spark

import io.minilake.Snapshot
import io.minilake.actions.AddFile
import io.minilake.schema.{DataType => MLType}
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow}
import org.apache.spark.sql.execution.datasources.{FileIndex, FileStatusWithMetadata, PartitionDirectory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import java.time.LocalDate
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters._

/**
 * The file list comes from the transaction log, never from listing the table directory: files of
 * uncommitted or removed writes sit right next to live ones and must stay invisible.
 *
 * Spark hands us partition filters and data filters separately; both go through the same
 * stats check because partition values are just exact stats (min = max).
 */
class MiniLakeFileIndex(
    tablePath: Path,
    val snapshot: Snapshot,
    override val partitionSchema: StructType,
    only: Option[Set[String]] = None)
    extends FileIndex {

  private val columns = snapshot.schema().fields().asScala.map(_.name).toSet
  private val partitionColumns = snapshot.metadata().partitionColumns().asScala.toSeq

  override def rootPaths: Seq[Path] = Seq(tablePath)

  override def listFiles(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val predicate = FilterConverter.convert(partitionFilters ++ dataFilters, columns)
    val selected = snapshot.files(predicate).asScala.toSeq.filter(visible)
    MiniLakeFileIndex.lastScan.set(ScanStats(allFiles.size, selected.size, predicate.toString))
    selected
      .groupBy(_.partitionValues().asScala.toMap)
      .toSeq
      .map { case (values, files) => PartitionDirectory(partitionRow(values), files.map(status)) }
  }

  private def visible(f: AddFile): Boolean = only.forall(_.contains(f.path()))

  private def allFiles: Seq[AddFile] = snapshot.allFiles().asScala.toSeq.filter(visible)

  override def inputFiles: Array[String] = allFiles.map(f => absolute(f).toString).toArray

  override def refresh(): Unit = ()

  override def sizeInBytes: Long = allFiles.map(_.size()).sum

  private def absolute(f: AddFile): Path = new Path(tablePath, f.path())

  private def status(f: AddFile): FileStatusWithMetadata =
    FileStatusWithMetadata(new FileStatus(f.size(), false, 0, 0, f.modificationTime(), absolute(f)))

  private def partitionRow(values: Map[String, String]): InternalRow = {
    val schema = snapshot.schema()
    new GenericInternalRow(partitionColumns.map { c =>
      val raw = values.getOrElse(c, null)
      if (raw == null) null
      else schema.typeOf(c) match {
        case MLType.INT => Integer.valueOf(raw.toInt)
        case MLType.LONG => java.lang.Long.valueOf(raw.toLong)
        case MLType.DOUBLE => java.lang.Double.valueOf(raw.toDouble)
        case MLType.STRING => UTF8String.fromString(raw)
        case MLType.BOOLEAN => java.lang.Boolean.valueOf(raw.toBoolean)
        case MLType.DATE => Integer.valueOf(LocalDate.parse(raw).toEpochDay.toInt)
      }
    }.toArray[Any])
  }
}

/** How many files the last scan had to read; used by tests and the skipping benchmark. */
case class ScanStats(totalFiles: Int, scannedFiles: Int, predicate: String) {
  def skippedFraction: Double = if (totalFiles == 0) 0 else 1.0 - scannedFiles.toDouble / totalFiles
}

object MiniLakeFileIndex {
  val lastScan = new AtomicReference[ScanStats]()
}
