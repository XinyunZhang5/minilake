package io.minilake.spark

import io.minilake.schema.{DataType => MLType, Field, Schema}
import org.apache.spark.sql.types._

import scala.jdk.CollectionConverters._

/** Spark <-> MiniLake schema. Only types whose Parquet stats we can interpret are supported. */
object SchemaConverter {

  def toMiniLake(struct: StructType): Schema =
    new Schema(struct.fields.toSeq.map(f => new Field(f.name, toMiniLake(f.name, f.dataType), f.nullable)).asJava)

  private def toMiniLake(name: String, dt: DataType): MLType = dt match {
    case IntegerType => MLType.INT
    case LongType => MLType.LONG
    case DoubleType => MLType.DOUBLE
    case _: StringType => MLType.STRING
    case BooleanType => MLType.BOOLEAN
    case DateType => MLType.DATE
    case other => throw new IllegalArgumentException(
      s"column `$name` has unsupported type ${other.simpleString}; supported: int, bigint, double, string, boolean, date")
  }

  def toSpark(schema: Schema): StructType =
    StructType(schema.fields().asScala.toSeq.map(f => StructField(f.name, toSpark(f.`type`), f.nullable)))

  def toSpark(t: MLType): DataType = t match {
    case MLType.INT => IntegerType
    case MLType.LONG => LongType
    case MLType.DOUBLE => DoubleType
    case MLType.STRING => StringType
    case MLType.BOOLEAN => BooleanType
    case MLType.DATE => DateType
  }
}
