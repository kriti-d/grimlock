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

/** Trait for defining legal values of a schema. */
trait Validator[T] {
  /**
   * Check if a value is valid.
   *
   * @param value The value to validate.
   *
   * @return True is the value is valid, false otherwise.
   */
  def validate(value: T): Boolean
}

case class RangeValidator[T](min: Option[T], max: Option[T])(implicit ordering: Ordering[T]) extends Validator[T] {
  def validate(
    value: T
  ): Boolean = min.fold(true)(t => ordering.gteq(t, value)) && max.fold(true)(t => ordering.lteq(t, value))
}

trait ScaleValidator[T] extends Validator[T] {
  val precision: Option[Int]
  val scale: Option[Int]

  def validate(value: T): Boolean = {
    val dec = toDecimal(value)

    precision.fold(true)(p => dec.precision <= p) && scale.fold(true)(s => dec.scale <= s)
  }

  protected def toDecimal(value: T): BigDecimal
}

case class BigDecimalScaleValidator(precision: Option[Int], scale: Option[Int]) extends ScaleValidator[BigDecimal] {
  protected def toDecimal(value: BigDecimal): BigDecimal = value
}

case class NumericScaleValidator[
  T
](
  precision: Option[Int],
  scale: Option[Int]
)(implicit
  numeric: Numeric[T]
) extends ScaleValidator[T] {
  protected def toDecimal(value: T): BigDecimal = BigDecimal.double2bigDecimal(numeric.toDouble(value))
}

case class DomainValidator[T](domain: Set[T]) extends Validator[T] {
  def validate(value: T): Boolean = domain.isEmpty || domain.contains(value)
}

case class RegexValidator[T](regex: Regex) extends Validator[T] {
  def validate(value: T): Boolean = regex.pattern.matcher(value.toString).matches
}

case class StringLengthValidator(min: Int, max: Int) extends Validator[String] {
  def validate(value: String): Boolean = min <= value.length && value.length <= max
}

/** Schema defines a set of legal values, parsing methods and conversions for a variable. */
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

  /** Set of validators for this schema. */
  def validators: Set[Validator[T]]

  /**
   * Box a (basic) data type in a `Value`.
   *
   * @param value The data to box.
   *
   * @return The value wrapped in a `Value`.
   * @throws IllegalArgumentException if the value does not conform to this schema.
   */
  def boxUnsafe(value: T): Value[T]

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
    v <- parse(str)
    if (validate(v))
  } yield v

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
   * Validates if a value conforms to this schema.
   *
   * @param value The value to validate.
   *
   * @return True is the value conforms to this schema, false otherwise.
   */
  def validate(value: T): Boolean = validators.forall(_.validate(value))

  protected def name: String

  protected def paramString: String = ""

  /** Subtypes should implement parsing logic in this method. */
  protected def parse(str: String): Option[T]

  private def round(str: String): String = if (str.isEmpty) str else "(" + str + ")"
}

object Schema {
  type Converter[T, X] = (T) => X
}

/** Schema for dealing with `BigDecimal`. */
case class DecimalSchema(
  precision: Int,
  scale: Int,
  additionalValidators: Set[Validator[BigDecimal]] = Set()
) extends Schema[BigDecimal] {
  val converters: Set[Schema.Converter[BigDecimal, Any]] = decimalAsDoubleConvertor.toSet ++
    decimalAsFloatConvertor.toSet

  val date: Option[BigDecimal => Date] = None

  def boxUnsafe(value: BigDecimal): Value[BigDecimal] = DecimalValue(value, this)

  def encode(value: BigDecimal): String = value.toString

  def integral: Option[Integral[BigDecimal]] = None

  def numeric: Option[Numeric[BigDecimal]] = Option(Numeric.BigDecimalIsFractional)

  def ordering: Ordering[BigDecimal] = Ordering.BigDecimal

  def validators: Set[Validator[BigDecimal]] = additionalValidators +
    BigDecimalScaleValidator(Option(precision), Option(scale))

  protected def name: String = "decimal"

  protected def parse(str: String): Option[BigDecimal] = Try(BigDecimal(str)).toOption

  private def decimalAsDoubleConvertor: Option[Schema.Converter[BigDecimal, Double]] = {
    if ((precision - scale) >= 309)
      None
    else
      Option(DecimalSchema.BigDecimalAsDouble)
  }

  private def decimalAsFloatConvertor: Option[Schema.Converter[BigDecimal, Float]] = {
    if ((precision - scale) >= 39)
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
case class DoubleSchema(validators: Set[Validator[Double]]) extends Schema[Double] {
  val converters: Set[Schema.Converter[Double, Any]] = Set.empty

  def date: Option[Double => Date] = None

  def boxUnsafe(value: Double): Value[Double] = DoubleValue(value, this)

  def encode(value: Double): String = value.toString

  def integral: Option[Integral[Double]] = None

  def numeric: Option[Numeric[Double]] = Option(Numeric.DoubleIsFractional)

  def ordering: Ordering[Double] = Ordering.Double

  protected def name: String = "double"

  protected def parse(str: String): Option[Double] = Try(str.toDouble).toOption
}

/** Schema for dealing with `Float`. */
case class FloatSchema(validators: Set[Validator[Float]]) extends Schema[Float] {
  val converters: Set[Schema.Converter[Float, Any]] = Set(FloatSchema.FloatAsDouble)

  def date: Option[Float => Date] = None

  def boxUnsafe(value: Float): Value[Float] = FloatValue(value, this)

  def encode(value: Float): String = value.toString

  def integral: Option[Integral[Float]] = None

  def numeric: Option[Numeric[Float]] = Option(Numeric.FloatIsFractional)

  def ordering: Ordering[Float] = Ordering.Float

  protected def name: String = "double"

  protected def parse(str: String): Option[Float] = Try(str.toFloat).toOption
}

/** Companion object to `FloatSchema`. */
object FloatSchema {
  private case object FloatAsDouble extends Schema.Converter[Float, Double] {
    def apply(f: Float): Double = f.toDouble
  }
}

case class IntSchema(validators: Set[Validator[Int]]) extends Schema[Int] {
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

/** Trait for dealing with `Schema`s of `String` values. */
case class StringSchema(
  validators: Set[Validator[String]],
  ordering: Ordering[String] = Ordering.String
) extends Schema[String] {
  val converters: Set[Schema.Converter[String, Any]] = Set.empty

  def boxUnsafe(value: String): Value[String] = StringValue(value, this)

  def date: Option[String => Date] = None

  def encode(value: String): String = value

  def integral: Option[Integral[String]] = None

  def numeric: Option[Numeric[String]] = None

  protected def name = "string"

  protected def parse(str: String): Option[String] = Option(str)
}

/**
 * Schema for date variables.
 *
 * @param dates The values of the variable, a set or range.
 */
case class DateSchema(
  validators: Set[Validator[Date]],
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

  protected def name: String = "date"

  private def df: SimpleDateFormat = new SimpleDateFormat(format)
}

object DateSchema {
  private case object DateAsLong extends Schema.Converter[Date, Long] {
    def apply(d: Date): Long = d.getTime
  }
}

