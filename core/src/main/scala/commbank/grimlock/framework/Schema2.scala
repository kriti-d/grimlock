// Copyright 2014,2015,2016,2017,2018,2019 Commonwealth Bank of Australia
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package commbank.grimlock.framework.encoding2

import java.text.{ ParsePosition, SimpleDateFormat }
import java.util.Date

import scala.util.{ Success, Try }
import scala.util.matching.Regex

trait Schema[T] {
  /** Custom convertors (i.e. other than identity) for converting `T` to another type. */
  def converters: Set[Schema.Converter[T, Any]]

  /** An optional date type class for this data type. */
  def date: Option[T => Date]

  /** An optional numeric type class for this data type. */
  def numeric: Option[Numeric[T]]

  /** An optional intergal type class for this data type. */
  def integral: Option[Integral[T]]

  /** An ordering for this data type. */
  def ordering: Ordering[T]

  /**
   * Box a (basic) data type in a `Value`.
   *
   * @param value The data to box.
   *
   * @return The value wrapped in a `Value`.
   * @throws IllegalArgumentException if the value does not conform to this schema.
   */
  protected def boxUnsafe(value: T): Value[T]

  /**
   * Safely box a (basic) data type in a `Value`
   *
   * @param value The data to box.
   *
   * @return `Some[Value[T]]` if the value conforms to this schema, `None` otherwise.
   */
  def box(value: T): Option[Value[T]] = Try(boxUnsafe(value)).toOption

  /**
   * Compare two values.
   *
   * @param x The first value to compare.
   * @param y The second value to compare.
   *
   * @return The returned value is < 0 iff x < y, 0 iff x = y, > 0 iff x > y.
   */
  def compare(x: T, y: T): Int = ordering.compare(x, y)

  /**
   * Decode a basic data type.
   *
   * @param str String to decode.
   *
   * @return `Some[T]` if the decode was successful, `None` otherwise.
   */
  def decode(str: String): Option[T] = for {
    t <- parse(str)
    if (validate(t))
  } yield t

  /**
   * Converts a value to a consise (terse) string.
   *
   * @param value The value to convert to string.
   *
   * @return Short string representation of the value.
   */
  def encode(value: T): String

  /** Return a consise (terse) string representation of a schema. */
  def toShortString: String = name + round(paramString)

  /**
   * Validates if a value confirms to this schema.
   *
   * @param value The value to validate.
   *
   * @return True is the value confirms to this schema, false otherwise.
   */
  def validate(value: T): Boolean

  protected def name: String

  protected def paramString: String = ""

  /** Subtypes should implement parsing logic in this method. */
  protected def parse(str: String): Option[T]

  private def round(str: String): String = if (str.isEmpty) str else "(" + str + ")"
}

object Schema {
  type Converter[T, X] = (T) => X
}

/** Trait for schemas for which may have a range of valid values. */
trait RangeSchema[T] extends Schema[T] {
  val min: Option[T]
  val max: Option[T]

  def validate(value: T): Boolean = validateRange(value)

  protected def validateRange(
    value: T
  ): Boolean = min.fold(true)(t => ordering.gteq(t, value)) && max.fold(true)(t => ordering.lteq(t, value))
}

trait ScaleSchema[T] extends RangeSchema[T] {
  val date: Option[T => Date] = None
  val integral: Option[Integral[T]] = None
  val min: Option[T]
  val max: Option[T]
  val numeric: Option[Numeric[T]] = Option(num)
  val precision: Option[Int]
  val scale: Option[Int]

  def ordering: Ordering[T] = num

  override def validate(value: T): Boolean = {
    val dec = toDecimal(value)

    validateRange(value) &&
      precision.fold(true)(p => dec.precision <= p) &&
      scale.fold(true)(s => dec.scale <= s)
  }

  protected implicit val num: Numeric[T]

  override protected def paramString: String = SchemaParameters.writeRange(min, max, this, extra)

  protected def toDecimal(value: T): BigDecimal = BigDecimal.double2bigDecimal(num.toDouble(value))

  private val extra = List(("precision", precision.map(_.toString)), ("scale", scale.map(_.toString)))
}

/** Schema for dealing with `BigDecimal`. */
case class DecimalSchema(
  min: Option[BigDecimal],
  max: Option[BigDecimal],
  decimalPrecision: Int,
  decimalScale: Int
) extends ScaleSchema[BigDecimal] {
  val converters: Set[Schema.Converter[BigDecimal, Any]] = decimalAsDoubleConvertor.toSet ++
    decimalAsFloatConvertor.toSet
  val precision: Option[Int] = Option(decimalPrecision)
  val scale: Option[Int] = Option(decimalScale)

  def boxUnsafe(value: BigDecimal): Value[BigDecimal] = DecimalValue(value, this)

  def encode(value: BigDecimal): String = value.toString

  protected implicit val num: Numeric[BigDecimal] = Numeric.BigDecimalIsFractional

  protected def name: String = "decimal"

  protected def parse(str: String): Option[BigDecimal] = Try(BigDecimal(str)).toOption

  override protected def toDecimal(value: BigDecimal): BigDecimal = value

  private def decimalAsDoubleConvertor: Option[Schema.Converter[BigDecimal, Double]] = {
    if ((decimalPrecision - decimalScale) >= 309)
      None
    else
      Option(DecimalSchema.BigDecimalAsDouble)
  }

  private def decimalAsFloatConvertor: Option[Schema.Converter[BigDecimal, Float]] = {
    if ((decimalPrecision - decimalScale) >= 39)
      None
    else
      Option(DecimalSchema.BigDecimalAsFloat)
  }
}

object DecimalSchema {
  private case object BigDecimalAsDouble extends Schema.Converter[BigDecimal, Double] {
    def apply(i: BigDecimal): Double = i.doubleValue()
  }

  private case object BigDecimalAsFloat extends Schema.Converter[BigDecimal, Float] {
    def apply(i: BigDecimal): Float = i.floatValue()
  }
}

/** Schema for dealing with `Double`. */
case class DoubleSchema(
  min: Option[Double],
  max: Option[Double],
  precision: Option[Int],
  scale: Option[Int]
) extends ScaleSchema[Double] {
  val converters: Set[Schema.Converter[Double, Any]] = Set.empty

  def boxUnsafe(value: Double): Value[Double] = DoubleValue(value, this)

  def encode(value: Double): String = value.toString

  protected implicit val num: Numeric[Double] = Numeric.DoubleIsFractional

  protected def name: String = "double"

  protected def parse(str: String): Option[Double] = Try(str.toDouble).toOption
}

/** Schema for dealing with `Float`. */
case class FloatSchema(
  min: Option[Float],
  max: Option[Float],
  precision: Option[Int],
  scale: Option[Int]
) extends ScaleSchema[Float] {
  val converters: Set[Schema.Converter[Float, Any]] = Set(FloatSchema.FloatAsDouble)

  def boxUnsafe(value: Float): Value[Float] = FloatValue(value, this)

  def encode(value: Float): String = value.toString

  protected implicit val num: Numeric[Float] = Numeric.FloatIsFractional

  protected def name: String = "double"

  protected def parse(str: String): Option[Float] = Try(str.toFloat).toOption
}

/** Companion object to `FloatSchema`. */
object FloatSchema {
  private case object FloatAsDouble extends Schema.Converter[Float, Double] {
    def apply(f: Float): Double = f.toDouble
  }
}

case class IntSchema(min: Option[Int], max: Option[Int]) extends RangeSchema[Int] {
  val converters: Set[Schema.Converter[Int, Any]] = Set(
    IntSchema.IntAsBigDecimal, IntSchema.IntAsDouble, IntSchema.IntAsFloat, IntSchema.IntAsLong
  )
  val date: Option[Int => Date] = None

  def boxUnsafe(value: Int): Value[Int] = IntValue(value, this)

  def encode(value: Int): String = value.toString

  def integral: Option[Integral[Int]] = Option(Numeric.IntIsIntegral)

  def numeric: Option[Numeric[Int]] = Option(Numeric.IntIsIntegral)

  def ordering: Ordering[Int] = Ordering.Int

  protected def name: String = "int"

  protected def parse(str: String): Option[Int] = Try(str.toInt).toOption
}

/** Companion object to `IntSchema`. */
object IntSchema {
  private case object IntAsBigDecimal extends Schema.Converter[Int, BigDecimal] {
    def apply(i: Int): BigDecimal = BigDecimal(i)
  }

  private case object IntAsDouble extends Schema.Converter[Int, Double] {
    def apply(i: Int): Double = i.toDouble
  }

  private case object IntAsFloat extends Schema.Converter[Int, Float] {
    def apply(i: Int): Float = i.toFloat
  }

  private case object IntAsLong extends Schema.Converter[Int, Long] {
    def apply(i: Int): Long = i.toLong
  }
}

/** Trait for schemas which mark values as valid if and only if they exist in a domain. */
trait DomainSchema[T] extends Schema[T] {
  /** Values the variable can take. */
  val domain: Either[Set[T], Regex]

  def validate(value: T): Boolean = domain
    .fold(set => set.isEmpty || set.contains(value), regex => regex.pattern.matcher(encode(value)).matches)
}

/** Trait for dealing with `Schema`s of `String` values. */
trait StringSchema extends Schema[String] {
  val converters: Set[Schema.Converter[String, Any]] = Set.empty

  def boxUnsafe(value: String): Value[String] = StringValue(value, this)

  def date: Option[String => Date] = None

  def encode(value: String): String = value

  def integral: Option[Integral[String]] = None

  def numeric: Option[Numeric[String]] = None

  protected def parse(str: String): Option[String] = Option(str)
}

/** Schema for dealing with `String` with specific domain. */
case class DomainStringSchema(
  domain: Either[Set[String], Regex],
  customOrdering: Option[Ordering[String]] = None
) extends DomainSchema[String] with StringSchema {
  def ordering: Ordering[String] = customOrdering.getOrElse(Ordering.String)

  protected def name: String = "domainString"
}

/** Schema for dealing with strings of bounded length. */
case class BoundedStringSchema(min: Int, max: Int) extends StringSchema {
  def ordering: Ordering[String] = Ordering.String

  def validate(value: String): Boolean = value.size >= min && value.size <= max

  protected def name: String = "boundedString"
}

/**
 * Schema for date variables.
 *
 * @param dates The values of the variable, a set or range.
 */
case class DateSchema(
  dates: Either[Set[Date], (Option[Date], Option[Date])] = Left(Set.empty[Date]),
  format: String = "yyyy-MM-dd"
) extends Schema[Date] {
  val converters: Set[Schema.Converter[Date, Any]] = Set(DateSchema.DateAsLong)

  def boxUnsafe(value: Date): Value[Date] = DateValue(value, this)

  def date: Option[Date => Date] = Option(identity)

  def encode(value: Date): String = df.format(value)

  def integral: Option[Integral[Date]] = None

  def numeric: Option[Numeric[Date]] = None

  def ordering: Ordering[Date] = new Ordering[Date] { def compare(x: Date, y: Date): Int = x.compareTo(y) }

  def parse(value: String): Option[Date] = {
    val fmt = df
    val pos = new ParsePosition(0)

    fmt.setLenient(false)

    Try(fmt.parse(value, pos)) match {
      case Success(d) if (pos.getIndex == value.length) => Option(d)
      case _ => None
    }
  }

  def validate(value: Date): Boolean = dates.fold(
    domain => domain.isEmpty || domain.contains(value),
    range => range._1.fold(true)(t => compare(value, t) >= 0) && range._2.fold(true)(t => compare(value, t) <= 0)
  )

  protected def name: String = "date"

  private def df: SimpleDateFormat = new SimpleDateFormat(format)
}

object DateSchema {
  private case object DateAsLong extends Schema.Converter[Date, Long] {
    def apply(d: Date): Long = d.getTime
  }
}

/** Functions for dealing with schema parameters. */
private object SchemaParameters {
  def writeRange[T](
    min: Option[T],
    max: Option[T],
    schema: Schema[T],
    extra: List[(String, Option[String])]
  ): String = {
    val list = List(
      min.map(t => s"min=${schema.encode(t)}"),
      max.map(t => s"max=${schema.encode(t)}")
    ) ++ extra.map { case (name, value) => value.map(v => s"${name}=${v}") }

    list.flatten.mkString(",")
  }
}

