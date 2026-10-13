package io.minilake.spark

import io.minilake.expr.Predicate
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{DataType, DateType}
import org.apache.spark.unsafe.types.UTF8String

import java.time.LocalDate

/**
 * Catalyst filters -> MiniLake predicates, used only to pick files. Anything we cannot translate
 * becomes "can't rule this file out", which is always safe because Spark still applies every
 * filter to the rows it reads.
 */
object FilterConverter {

  def convert(filters: Seq[Expression], columns: Set[String]): Predicate = {
    val byLower = columns.map(c => c.toLowerCase -> c).toMap
    val parts = filters.flatMap(e => new Converter(byLower).convert(e, strict = false))
    if (parts.isEmpty) Predicate.TRUE else Predicate.and(parts: _*)
  }

  private class Converter(columns: Map[String, String]) {
    private object Col {
      def unapply(e: Expression): Option[String] = e match {
        case a: AttributeReference => columns.get(a.name.toLowerCase)
        case _ => None
      }
    }

    private object Lit {
      def unapply(e: Expression): Option[AnyRef] = e match {
        case Literal(v, dt) => Some(external(v, dt))
        case _ => None
      }
    }

    /**
     * @param strict every node must translate exactly. Needed under NOT: dropping a conjunct
     *               widens an AND, and NOT of something wider is narrower, i.e. unsafe.
     */
    def convert(e: Expression, strict: Boolean): Option[Predicate] = e match {
      case EqualTo(Col(c), Lit(v)) => Some(Predicate.eq(c, v))
      case EqualTo(Lit(v), Col(c)) => Some(Predicate.eq(c, v))
      case EqualNullSafe(Col(c), Lit(v)) => Some(if (v == null) Predicate.isNull(c) else Predicate.eq(c, v))
      case EqualNullSafe(Lit(v), Col(c)) => Some(if (v == null) Predicate.isNull(c) else Predicate.eq(c, v))
      case LessThan(Col(c), Lit(v)) => Some(Predicate.lt(c, v))
      case LessThan(Lit(v), Col(c)) => Some(Predicate.gt(c, v))
      case LessThanOrEqual(Col(c), Lit(v)) => Some(Predicate.le(c, v))
      case LessThanOrEqual(Lit(v), Col(c)) => Some(Predicate.ge(c, v))
      case GreaterThan(Col(c), Lit(v)) => Some(Predicate.gt(c, v))
      case GreaterThan(Lit(v), Col(c)) => Some(Predicate.lt(c, v))
      case GreaterThanOrEqual(Col(c), Lit(v)) => Some(Predicate.ge(c, v))
      case GreaterThanOrEqual(Lit(v), Col(c)) => Some(Predicate.le(c, v))
      case In(Col(c), list) if list.nonEmpty && list.forall(_.isInstanceOf[Literal]) =>
        Some(Predicate.in(c, list.map { case Lit(v) => v }: _*))
      case InSet(child @ Col(c), set) if set.nonEmpty =>
        Some(Predicate.in(c, set.toSeq.map(v => external(v, child.dataType)): _*))
      case IsNull(Col(c)) => Some(Predicate.isNull(c))
      case IsNotNull(Col(c)) => Some(Predicate.isNotNull(c))
      case And(l, r) =>
        val (a, b) = (convert(l, strict), convert(r, strict))
        if (strict) for (x <- a; y <- b) yield Predicate.and(x, y)
        else (a ++ b).reduceOption((x, y) => Predicate.and(x, y))
      case Or(l, r) => for (x <- convert(l, strict); y <- convert(r, strict)) yield Predicate.or(x, y)
      case Not(child) => convert(child, strict = true).map(_.negate())
      case _ => None
    }

    /** Catalyst's internal value -> what DataType.normalize understands. */
    private def external(v: Any, dt: DataType): AnyRef = (v, dt) match {
      case (null, _) => null
      case (s: UTF8String, _) => s.toString
      case (d: Int, DateType) => LocalDate.ofEpochDay(d.toLong)
      case (other, _) => other.asInstanceOf[AnyRef]
    }
  }
}
