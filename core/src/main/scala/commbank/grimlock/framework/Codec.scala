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

import scala.reflect.{ classTag, ClassTag }
import scala.util.Try

/** Trait for encoding/decoding (basic) data types. */
trait Codec { self =>
  /** The data type. */
  type D

  /** The value (boxed data) type. */
  type V <: Value { type D = self.D }

  /**
   * Decode a basic data type into a value.
   *
   * @param str String to decode into a value.
   *
   * @return `Some[Value]` if the decode was successful, `None` otherwise.
   */
  def decode(str: String): Option[V]

  /**
   * Converts a value to a consise (terse) string.
   *
   * @param value The value to convert to string.
   *
   * @return Short string representation of the value.
   */
  def encode(value: D): String

  /**
   * Compare two value objects.
   *
   * @param x The first value to compare.
   * @param y The second value to compare.
   *
   * @return `Some[Int]` if `x` and `y` can be compared, `None` otherwise. If successful, then the returned value
   *         is < 0 iff x < y, 0 iff x = y, > 0 iff x > y.
   */
  def compare(x: Value, y: Value): Option[Int]

  /** Return a consise (terse) string representation of a codec. */
  def toShortString(): String

  /** Return an optional ClassTag for this data type. */
  def tag(): Option[ClassTag[D]] = None

  /** Return an optional Ordering for this data type. */
  def ordering(): Option[Ordering[D]] = None

  /** Return an optional date type class for this data type. */
  def date(): Option[D => Date] = None

  /** Return an optional numeric type class for this data type. */
  def numeric(): Option[Numeric[D]] = None

  /** Return an optional intergal type class for this data type. */
  def integral(): Option[Integral[D]] = None
}

/** Companion object to the `Codec` trait. */
object Codec {
  /**
   * Parse a codec from a string.
   *
   * @param str String from which to parse the codec.
   *
   * @return A `Some[Codec]` in case of success, `None` otherwise.
   */
  def fromShortString(str: String): Option[Codec] = str match {
    case DateCodec.Pattern(_) => DateCodec.fromShortString(str)
    case StringCodec.Pattern() => StringCodec.fromShortString(str)
    case DoubleCodec.Pattern() => DoubleCodec.fromShortString(str)
    case LongCodec.Pattern() => LongCodec.fromShortString(str)
    case BooleanCodec.Pattern() => BooleanCodec.fromShortString(str)
    case TypeCodec.Pattern() => TypeCodec.fromShortString(str)
    case _ => None
  }
}

/** Codec for dealing with `java.util.Date`. */
case class DateCodec(format: String = "yyyy-MM-dd") extends Codec {
  type D = Date
  type V = DateValue

  def decode(value: String): Option[V] = Try(DateValue(df.parse(value), this)).toOption
  def encode(value: D): String = df.format(value)

  def compare(x: Value, y: Value): Option[Int] = (x.asDate, y.asDate) match {
    case (Some(l), Some(r)) => Option(cmp(l, r))
    case _ => None
  }

  def toShortString() = s"date(${format})"

  override def tag(): Option[ClassTag[D]] = Option(classTag[D])
  override def ordering(): Option[Ordering[D]] = Option(new Ordering[D] { def compare(x: D, y: D): Int = cmp(x, y) })
  override def date(): Option[D => Date] = Option(c => c)

  private def cmp(x: D, y: D): Int = x.getTime().compare(y.getTime())
  private def df(): SimpleDateFormat = new SimpleDateFormat(format)
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

/** Codec for dealing with `String`. */
case object StringCodec extends Codec {
  type D = String
  type V = StringValue

  /** Pattern for parsing `StringCodec` from string. */
  val Pattern = "string".r

  def decode(str: String): Option[V] = Option(StringValue(str, this))
  def encode(value: D): String = value

  def compare(x: Value, y: Value): Option[Int] = (x.asString, y.asString) match {
    case (Some(l), Some(r)) => Option(l.compare(r))
    case _ => None
  }

  def toShortString() = "string"

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

  override def tag(): Option[ClassTag[D]] = Option(classTag[D])
  override def ordering(): Option[Ordering[D]] = Option(Ordering.String)
}

/** Codec for dealing with `Double`. */
case object DoubleCodec extends Codec {
  type D = Double
  type V = DoubleValue

  /** Pattern for parsing `DoubleCodec` from string. */
  val Pattern = "double".r

  def decode(str: String): Option[V] = Try(DoubleValue(str.toDouble, this)).toOption
  def encode(value: D): String = value.toString

  def compare(x: Value, y: Value): Option[Int] = (x.asDouble, y.asDouble) match {
    case (Some(l), Some(r)) => Option(l.compare(r))
    case _ => None
  }

  def toShortString() = "double"

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

  override def tag(): Option[ClassTag[D]] = Option(classTag[D])
  override def ordering(): Option[Ordering[D]] = Option(Ordering.Double)
  override def numeric(): Option[Numeric[D]] = Option(Numeric.DoubleIsFractional)
}

/** Codec for dealing with `Long`. */
case object LongCodec extends Codec {
  type D = Long
  type V = LongValue

  /** Pattern for parsing `LongCodec` from string. */
  val Pattern = "long|int|short".r

  def decode(str: String): Option[V] = Try(LongValue(new BigDecimal(str.trim).longValueExact, this)).toOption
  def encode(value: D): String = value.toString

  def compare(x: Value, y: Value): Option[Int] = (x.asLong, y.asLong) match {
    case (Some(l), Some(r)) => Option(l.compare(r))
    case _ => DoubleCodec.compare(x, y)
  }

  def toShortString() = "long"

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

  override def tag(): Option[ClassTag[D]] = Option(classTag[D])
  override def ordering(): Option[Ordering[D]] = Option(Ordering.Long)
  override def numeric(): Option[Numeric[D]] = Option(Numeric.LongIsIntegral)
  override def integral(): Option[Integral[D]] = Option(Numeric.LongIsIntegral)
}

/** Codec for dealing with `Boolean`. */
case object BooleanCodec extends Codec {
  type D = Boolean
  type V = BooleanValue

  /** Pattern for parsing `BooleanCodec` from string. */
  val Pattern = "boolean".r

  def decode(str: String): Option[V] = Try(BooleanValue(str.toBoolean, this)).toOption
  def encode(value: D): String = value.toString

  def compare(x: Value, y: Value): Option[Int] = (x.asBoolean, y.asBoolean) match {
    case (Some(l), Some(r)) => Option(l.compare(r))
    case _ => None
  }

  def toShortString() = "boolean"

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

  override def tag(): Option[ClassTag[D]] = Option(classTag[D])
  override def ordering(): Option[Ordering[D]] = Option(Ordering.Boolean)
}

/** Codec for dealing with `Type`. */
case object TypeCodec extends Codec {
  type D = Type
  type V = TypeValue

  /** Pattern for parsing `TypeCodec` from string. */
  val Pattern = "type".r

  def decode(str: String): Option[V] = Type.fromShortString(str).map(TypeValue(_, this))
  def encode(value: D): String = value.toShortString

  def compare(x: Value, y: Value): Option[Int] = (x.asType, y.asType) match {
    case (Some(l), Some(r)) => Option(l.toString.compare(r.toString))
    case _ => None
  }

  def toShortString() = "type"

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

  override def tag(): Option[ClassTag[D]] = Option(classTag[D])
}

/** Trait for dealing with structured data. */
trait StructuredCodec extends Codec {
  type T <: Structured
}

