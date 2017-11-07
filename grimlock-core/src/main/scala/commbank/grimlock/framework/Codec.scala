// Copyright 2014,2015,2016,2017 Commonwealth Bank of Australia
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

package commbank.grimlock.framework.encoding

import commbank.grimlock.framework.metadata.Type

import java.math.BigDecimal
import java.text.SimpleDateFormat
import java.util.Date

import scala.util.Try

import shapeless.{ :+:, CNil, Coproduct }
import shapeless.ops.coproduct.Inject

/** Trait for encoding/decoding (basic) data types. */
trait Codec[T] { self =>
  /** Custom convertors (i.e. other than identity) for converting `T` to another type. */
  val converters: Set[Codec.Convert[T]]

  /** An optional date type class for this data type. */
  val date: Option[T => Date]

  /** An optional numeric type class for this data type. */
  val numeric: Option[Numeric[T]]

  /** An optional intergal type class for this data type. */
  val integral: Option[Integral[T]]

  /** An Ordering for this data type. */
  val ordering: Ordering[T] = new Ordering[T] { def compare(x: T, y: T): Int = self.compare(x, y) }

  /**
   * Box a (basic) data type in a `Value`.
   *
   * @param value The data to box.
   *
   * @return The value wrapped in a `Value`.
   */
  def box(value: T): Value[T]

  /**
   * Compare two values.
   *
   * @param x The first value to compare.
   * @param y The second value to compare.
   *
   * @return The returned value is < 0 iff x < y, 0 iff x = y, > 0 iff x > y.
   */
  def compare(x: T, y: T): Int

  /**
   * Decode a basic data type.
   *
   * @param str String to decode.
   *
   * @return `Some[T]` if the decode was successful, `None` otherwise.
   */
  def decode(str: String): Option[T]

  /**
   * Converts a value to a consise (terse) string.
   *
   * @param value The value to convert to string.
   *
   * @return Short string representation of the value.
   */
  def encode(value: T): String

  /** Return a consise (terse) string representation of a codec. */
  def toShortString: String
}

/** Companion object to the `Codec` trait. */
object Codec {
  /** Type for converting a T to any other type. */
  type Convert[T] = (T) => Any

  /** Type for a default Codec co-product when parsing Codecs from string. */
  type DefaultCodecs = BooleanCodec.type :+:
    DateCodec :+:
    DoubleCodec.type :+:
    IntCodec.type :+:
    LongCodec.type :+:
    StringCodec.type :+:
    TypeCodec.type :+:
    CNil

  /** Type that captures all constraints for parsing default Codecs from string. */
  trait TextParseConstraints[C <: Coproduct] extends java.io.Serializable {
    implicit val asBoolean: Inject[C, BooleanCodec.type]
    implicit val asDate: Inject[C, DateCodec]
    implicit val asDouble: Inject[C, DoubleCodec.type]
    implicit val asInt: Inject[C, IntCodec.type]
    implicit val asLong: Inject[C, LongCodec.type]
    implicit val asString: Inject[C, StringCodec.type]
    implicit val asType: Inject[C, TypeCodec.type]
  }

  /** Implicit meeting text parsing constraints for the default Codecs. */
  implicit def codecTextParseConstraints[
    C <: Coproduct
  ](implicit
    ev1: Inject[C, BooleanCodec.type],
    ev2: Inject[C, DateCodec],
    ev3: Inject[C, DoubleCodec.type],
    ev4: Inject[C, IntCodec.type],
    ev5: Inject[C, LongCodec.type],
    ev6: Inject[C, StringCodec.type],
    ev7: Inject[C, TypeCodec.type]
  ): TextParseConstraints[C] = new TextParseConstraints[C] {
    implicit val asBoolean = ev1
    implicit val asDate = ev2
    implicit val asDouble = ev3
    implicit val asInt = ev4
    implicit val asLong = ev5
    implicit val asString = ev6
    implicit val asType = ev7
  }

  /**
   * Parse a codec from a string.
   *
   * @param str String from which to parse the codec.
   *
   * @return A `Some[C]` in case of success, `None` otherwise.
   */
  def fromShortString[C <: Coproduct](str: String)(implicit ev: TextParseConstraints[C]): Option[C] = {
    import ev._

    str match {
      case BooleanCodec.Pattern() => BooleanCodec.fromShortString(str).map(Coproduct(_))
      case DateCodec.Pattern(_) => DateCodec.fromShortString(str).map(Coproduct(_))
      case DoubleCodec.Pattern() => DoubleCodec.fromShortString(str).map(Coproduct(_))
      case IntCodec.Pattern() => IntCodec.fromShortString(str).map(Coproduct(_))
      case LongCodec.Pattern() => LongCodec.fromShortString(str).map(Coproduct(_))
      case StringCodec.Pattern() => StringCodec.fromShortString(str).map(Coproduct(_))
      case TypeCodec.Pattern() => TypeCodec.fromShortString(str).map(Coproduct(_))
      case _ => None
    }
  }
}

/** Codec for dealing with `Boolean`. */
case object BooleanCodec extends Codec[Boolean] {
  val converters: Set[Codec.Convert[Boolean]] = Set(BooleanAsDouble, BooleanAsInt, BooleanAsLong)
  val date: Option[Boolean => Date] = None
  val integral: Option[Integral[Boolean]] = None
  val numeric: Option[Numeric[Boolean]] = None

  /** Pattern for parsing `BooleanCodec` from string. */
  val Pattern = "boolean".r

  def box(value: Boolean): Value[Boolean] = BooleanValue(value, this)

  def compare(x: Boolean, y: Boolean): Int = x.compare(y)

  def decode(str: String): Option[Boolean] = Try(str.toBoolean).toOption

  def encode(value: Boolean): String = value.toString

  /**
   * Parse a BooleanCodec from a string.
   *
   * @param str String from which to parse the codec.
   *
   * @return A `Some[BooleanCodec]` in case of success, `None` otherwise.
   */
  def fromShortString(str: String): Option[BooleanCodec.type] = str match {
    case Pattern() => Option(this)
    case _ => None
  }

  def toShortString = Pattern.toString

  private case object BooleanAsDouble extends (Boolean => Double) {
    def apply(b: Boolean): Double = if (b) 1 else 0
  }

  private case object BooleanAsInt extends (Boolean => Int) {
    def apply(b: Boolean): Int = if (b) 1 else 0
  }

  private case object BooleanAsLong extends (Boolean => Long) {
    def apply(b: Boolean): Long = if (b) 1 else 0
  }
}

/** Codec for dealing with `java.util.Date`. */
case class DateCodec(format: String = "yyyy-MM-dd") extends Codec[Date] {
  val converters: Set[Codec.Convert[Date]] = Set.empty
  val date: Option[Date => Date] = Option(identity)
  val integral: Option[Integral[Date]] = None
  val numeric: Option[Numeric[Date]] = None

  def box(value: Date): Value[Date] = DateValue(value, this)

  def compare(x: Date, y: Date): Int = x.getTime.compare(y.getTime)

  def decode(value: String): Option[Date] = Try(df.parse(value)).toOption

  def encode(value: Date): String = df.format(value)

  def toShortString = s"date(${format})"

  private def df: SimpleDateFormat = new SimpleDateFormat(format)
}

/** Companion object to DateCodec. */
object DateCodec {
  /** Pattern for parsing `DateCodec` from string. */
  val Pattern = """date\((.*)\)""".r

  /**
   * Parse a DateCodec from a string.
   *
   * @param str String from which to parse the codec.
   *
   * @return A `Some[DateCodec]` in case of success, `None` otherwise.
   */
  def fromShortString(str: String): Option[DateCodec] = str match {
    case Pattern(format) => Option(DateCodec(format))
    case _ => None
  }
}

/** Codec for dealing with `Double`. */
case object DoubleCodec extends Codec[Double] {
  val converters: Set[Codec.Convert[Double]] = Set.empty
  val date: Option[Double => Date] = None
  val integral: Option[Integral[Double]] = None
  val numeric: Option[Numeric[Double]] = Option(Numeric.DoubleIsFractional)

  /** Pattern for parsing `DoubleCodec` from string. */
  val Pattern = "double".r

  def box(value: Double): Value[Double] = DoubleValue(value, this)

  def compare(x: Double, y: Double): Int = x.compare(y)

  def decode(str: String): Option[Double] = Try(str.toDouble).toOption

  def encode(value: Double): String = value.toString

  /**
   * Parse a DoubleCodec from a string.
   *
   * @param str String from which to parse the codec.
   *
   * @return A `Some[DoubleCodec]` in case of success, `None` otherwise.
   */
  def fromShortString(str: String): Option[DoubleCodec.type] = str match {
    case Pattern() => Option(this)
    case _ => None
  }

  def toShortString = Pattern.toString
}

/** Codec for dealing with `Int`. */
case object IntCodec extends Codec[Int] {
  val converters: Set[Codec.Convert[Int]] = Set(IntAsDouble, IntAsLong)
  val date: Option[Int => Date] = None
  val integral: Option[Integral[Int]] = Option(Numeric.IntIsIntegral)
  val numeric: Option[Numeric[Int]] = Option(Numeric.IntIsIntegral)

  /** Pattern for parsing `IntCodec` from string. */
  val Pattern = "int".r

  def box(value: Int): Value[Int] = IntValue(value, this)

  def compare(x: Int, y: Int): Int = x.compare(y)

  def decode(str: String): Option[Int] = Try(new BigDecimal(str.trim).intValueExact).toOption

  def encode(value: Int): String = value.toString

  /**
   * Parse a IntCodec from a string.
   *
   * @param str String from which to parse the codec.
   *
   * @return A `Some[IntCodec]` in case of success, `None` otherwise.
   */
  def fromShortString(str: String): Option[IntCodec.type] = str match {
    case Pattern() => Option(this)
    case _ => None
  }

  def toShortString = Pattern.toString

  private case object IntAsDouble extends (Int => Double) {
    def apply(l: Int): Double = l.toDouble
  }

  private case object IntAsLong extends (Int => Long) {
    def apply(l: Int): Long = l.toLong
  }
}

/** Codec for dealing with `Long`. */
case object LongCodec extends Codec[Long] {
  val converters: Set[Codec.Convert[Long]] = Set(LongAsDouble)
  val date: Option[Long => Date] = None
  val integral: Option[Integral[Long]] = Option(Numeric.LongIsIntegral)
  val numeric: Option[Numeric[Long]] = Option(Numeric.LongIsIntegral)

  /** Pattern for parsing `LongCodec` from string. */
  val Pattern = "long".r

  def box(value: Long): Value[Long] = LongValue(value, this)

  def compare(x: Long, y: Long): Int = x.compare(y)

  def decode(str: String): Option[Long] = Try(new BigDecimal(str.trim).longValueExact).toOption

  def encode(value: Long): String = value.toString

  /**
   * Parse a LongCodec from a string.
   *
   * @param str String from which to parse the codec.
   *
   * @return A `Some[LongCodec]` in case of success, `None` otherwise.
   */
  def fromShortString(str: String): Option[LongCodec.type] = str match {
    case Pattern() => Option(this)
    case _ => None
  }

  def toShortString = Pattern.toString

  private case object LongAsDouble extends (Long => Double) {
    def apply(l: Long): Double = l.toDouble
  }
}

/** Codec for dealing with `String`. */
case object StringCodec extends Codec[String] {
  val converters: Set[Codec.Convert[String]] = Set.empty
  val date: Option[String => Date] = None
  val integral: Option[Integral[String]] = None
  val numeric: Option[Numeric[String]] = None

  /** Pattern for parsing `StringCodec` from string. */
  val Pattern = "string".r

  def box(value: String): Value[String] = StringValue(value, this)

  def compare(x: String, y: String): Int = x.compare(y)

  def decode(str: String): Option[String] = Option(str)

  def encode(value: String): String = value

  /**
   * Parse a StringCodec from a string.
   *
   * @param str String from which to parse the codec.
   *
   * @return A `Some[StringCodec]` in case of success, `None` otherwise.
   */
  def fromShortString(str: String): Option[StringCodec.type] = str match {
    case Pattern() => Option(this)
    case _ => None
  }

  def toShortString = Pattern.toString
}

/** Codec for dealing with `Type`. */
case object TypeCodec extends Codec[Type] {
  val converters: Set[Codec.Convert[Type]] = Set.empty
  val date: Option[Type => Date] = None
  val integral: Option[Integral[Type]] = None
  val numeric: Option[Numeric[Type]] = None

  /** Pattern for parsing `TypeCodec` from string. */
  val Pattern = "type".r

  def box(value: Type): Value[Type] = TypeValue(value, this)

  def compare(x: Type, y: Type): Int = x.toString.compare(y.toString)

  def decode(str: String): Option[Type] = Type.fromShortString[Type.DefaultTypes](str).map(_.unify)

  def encode(value: Type): String = value.toShortString

  /**
   * Parse a TypeCodec from a string.
   *
   * @param str String from which to parse the codec.
   *
   * @return A `Some[TypeCodec]` in case of success, `None` otherwise.
   */
  def fromShortString(str: String): Option[TypeCodec.type] = str match {
    case Pattern() => Option(this)
    case _ => None
  }

  def toShortString = Pattern.toString
}

