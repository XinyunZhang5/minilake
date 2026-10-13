package io.minilake.spark

import io.minilake.expr.FileStats
import io.minilake.schema.{DataType => MLType, Schema}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.column.statistics.{BinaryStatistics, Statistics}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile

import java.time.LocalDate
import scala.jdk.CollectionConverters._
import scala.util.Using

/**
 * File-level stats from the Parquet footer: Parquet already keeps min/max/nullCount per row group,
 * so collecting them costs one footer read per file instead of a pass over the data.
 */
object ParquetStats {

  def collect(file: Path, schema: Schema, conf: Configuration): String = {
    val footer = Using.resource(ParquetFileReader.open(HadoopInputFile.fromPath(file, conf)))(_.getFooter)
    val blocks = footer.getBlocks.asScala
    val numRecords = blocks.map(_.getRowCount).sum

    val mins = new java.util.HashMap[String, AnyRef]()
    val maxs = new java.util.HashMap[String, AnyRef]()
    val nulls = new java.util.HashMap[String, java.lang.Long]()

    for (field <- schema.fields().asScala) {
      val chunks = blocks.flatMap(_.getColumns.asScala.find(c => c.getPath.toDotString == field.name))
      // A column absent from the file (e.g. a partition column) or any row group without stats
      // means we know nothing: leave it out rather than record a too-narrow range.
      if (chunks.size == blocks.size && blocks.nonEmpty && chunks.forall(c => c.getStatistics != null && !c.getStatistics.isEmpty)) {
        val stats = chunks.map(_.getStatistics)
        nulls.put(field.name, stats.map(_.getNumNulls).sum)
        val withValues = stats.filter(_.hasNonNullValue)
        if (withValues.nonEmpty) {
          val values = withValues.map(s => (value(field.`type`, s, min = true), value(field.`type`, s, min = false)))
          if (values.forall { case (lo, hi) => lo != null && hi != null }) {
            mins.put(field.name, values.map(_._1).min(ordering))
            maxs.put(field.name, values.map(_._2).max(ordering))
          }
        }
      }
    }
    FileStats.toJson(numRecords, mins, maxs, nulls)
  }

  private val ordering: Ordering[AnyRef] = (a: AnyRef, b: AnyRef) =>
    a.asInstanceOf[Comparable[AnyRef]].compareTo(b)

  private def value(t: MLType, s: Statistics[_], min: Boolean): AnyRef = {
    val v: Any = if (min) s.genericGetMin() else s.genericGetMax()
    t match {
      case MLType.INT | MLType.LONG => java.lang.Long.valueOf(v.asInstanceOf[Number].longValue())
      case MLType.DOUBLE =>
        val d = v.asInstanceOf[Number].doubleValue()
        if (d.isNaN) null else java.lang.Double.valueOf(d) // NaN breaks ordering; treat as unknown
      case MLType.BOOLEAN => v.asInstanceOf[java.lang.Boolean]
      case MLType.DATE => LocalDate.ofEpochDay(v.asInstanceOf[Number].longValue())
      case MLType.STRING =>
        // Parquet orders binary by unsigned bytes = code point order. Java String order differs
        // only for characters above U+FFFF vs. U+E000-U+FFFF; skip such values to stay safe.
        val str = s.asInstanceOf[BinaryStatistics]
        val bin = if (min) str.genericGetMin() else str.genericGetMax()
        val text = bin.toStringUsingUTF8
        if (text.codePoints().anyMatch(_ > 0xFFFF)) null else text
    }
  }
}
