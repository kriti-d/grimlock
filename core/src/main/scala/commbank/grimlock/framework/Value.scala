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

import java.util.Date

import scala.util.matching.Regex

/** Trait for representing strutured data. */
trait Structured

/** Trait for variable values. */
trait Value { self =>
  /** Type of the value. */
  type D

  /** The encapsulated value. */
  val value: D

  /** The codec used to encode/decode this value. */
  val codec: Codec { type D = self.D }

  /**
   * Check for equality with `that`.
   *
   * @param that Value to compare against.
   */
  def equ(that: Value): Boolean = evaluate(that, Equal)

  /**
   * Check for in-equality with `that`.
   *
   * @param that Value to compare against.
   */
  def neq(that: Value): Boolean = !(this equ that)

  /**
   * Check for for match with `that` regular expression.
   *
   * @param that Regular expression to match against.
   *
   * @note This always applies `toShortString` method before matching.
   */
  def like(that: Regex): Boolean = that.pattern.matcher(this.toShortString).matches

  // Note: These next 4 methods implement comparison in a non-standard way when comparing two objects that can't be
  //       compared. In such cases the result is always false. This is the desired behaviour for the 'which' method
  //       (i.e. a filter).

  /**
   * Check if `this` is less than `that`.
   *
   * @param that Value to compare against.
   *
   * @note If `that` is of a different type than `this`, then the result is always `false`.
   */
  def lss(that: Value): Boolean = evaluate(that, Less)

  /**
   * Check if `this` is less or equal to `that`.
   *
   * @param that Value to compare against.
   *
   * @note If `that` is of a different type than `this`, then the result is always `false`.
   */
  def leq(that: Value): Boolean = evaluate(that, LessEqual)

  /**
   * Check if `this` is greater than `that`.
   *
   * @param that Value to compare against.
   *
   * @note If `that` is of a different type than `this`, then the result is always `false`.
   */
  def gtr(that: Value): Boolean = evaluate(that, Greater)

  /**
   * Check if `this` is greater or equal to `that`.
   *
   * @param that Value to compare against.
   *
   * @note If `that` is of a different type than `this`, then the result is always `false`.
   */
  def geq(that: Value): Boolean = evaluate(that, GreaterEqual)

  /** Return value as `java.util.Date`. */
  def asDate: Option[Date] = None

  /** Return value as `String`. */
  def asString: Option[String] = None

  /** Return value as `Double`. */
  def asDouble: Option[Double] = None

  /** Return value as `Long`. */
  def asLong: Option[Long] = None

  /** Return value as `Boolean`. */
  def asBoolean: Option[Boolean] = None

  /** Return value as `Type`. */
  def asType: Option[Type] = None

  /** Return value as event. */
  def asStructured: Option[Structured] = None

  /** Return a consise (terse) string representation of a value. */
  def toShortString(): String = codec.encode(value)

  private def evaluate(that: Value, op: CompareResult): Boolean = codec.compare(this, that) match {
    case Some(0) => (op == Equal) || (op == GreaterEqual) || (op == LessEqual)
    case Some(x) if (x > 0) => (op == Greater) || (op == GreaterEqual)
    case Some(x) if (x < 0) => (op == Less) || (op == LessEqual)
    case _ => false
  }
}

/** Compantion object to the `Value` trait. */
object Value {
  /**
   * Concatenates the string representation of two values.
   *
   * @param separator Separator to use between the strings.
   *
   * @return A function that concatenates values as a string.
   */
  def concatenate(separator: String): (Value, Value) => Value = (left: Value, right: Value) =>
    left.toShortString + separator + right.toShortString

  /**
   * Parse a value from string.
   *
   * @param str The string to parse.
   * @param cdc The codec to decode with.
   *
   * @return A `Some[Value]` if successful, `None` otherwise.
   */
  def fromShortString(str: String, cdc: Codec): Option[cdc.V] = cdc.decode(str)

  /** Define an ordering between 2 values. Only use with values of the same type. */
  val ordering: Ordering[Value] = new Ordering[Value] {
    def compare(x: Value, y: Value): Int = x
      .codec
      .compare(x, y)
      .getOrElse(throw new Exception("unable to compare different values."))
  }

  /** Converts a `String` to a `Value`. */
  implicit def stringToValue(t: String): StringValue = StringValue(t)

  /** Converts a `Double` to a `Value`. */
  implicit def doubleToValue(t: Double): DoubleValue = DoubleValue(t)

  /** Converts a `Long` to a `Value`. */
  implicit def longToValue(t: Long): LongValue = LongValue(t)

  /** Converts a `Int` to a `Value`. */
  implicit def intToValue(t: Int): LongValue = LongValue(t)

  /** Converts a `Boolean` to a `Value`. */
  implicit def booleanToValue(t: Boolean): BooleanValue = BooleanValue(t)

  /** Converts a `Type` to a `Value`. */
  implicit def typeToValue[T <: Type](t: T): TypeValue = TypeValue(t)
}

/**
 * Value for when the data is of type `java.util.Date`
 *
 * @param value A `java.util.Date`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class DateValue(value: Date, codec: Codec { type D = Date } = DateCodec()) extends Value {
  type D = Date

  override def asDate: Option[Date] = Option(value)
}

/** Companion object to `DateValue` case class. */
object DateValue {
  /** Convenience constructure that returns a `DateValue` from a date and format string. */
  def apply(value: Date, format: String): DateValue = DateValue(value, DateCodec(format))
}

/**
 * Value for when the data is of type `String`.
 *
 * @param value A `String`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class StringValue(value: String, codec: Codec { type D = String } = StringCodec) extends Value {
  type D = String

  override def asString: Option[String] = Option(value)
}

/**
 * Value for when the data is of type `Double`.
 *
 * @param value A `Double`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class DoubleValue(value: Double, codec: Codec { type D = Double } = DoubleCodec) extends Value {
  type D = Double

  override def asDouble: Option[Double] = Option(value)
}

/**
 * Value for when the data is of type `Long`.
 *
 * @param value A `Long`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class LongValue(value: Long, codec: Codec { type D = Long } = LongCodec) extends Value {
  type D = Long

  override def asDouble: Option[Double] = Option(value)
  override def asLong: Option[Long] = Option(value)
}

/**
 * Value for when the data is of type `Boolean`.
 *
 * @param value A `Boolean`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class BooleanValue(value: Boolean, codec: Codec { type D = Boolean } = BooleanCodec) extends Value {
  type D = Boolean

  override def asDouble: Option[Double] = Option(if (value) 1 else 0)
  override def asLong: Option[Long] = Option(if (value) 1 else 0)
  override def asBoolean: Option[Boolean] = Option(value)
}

/**
 * Value for when the data is of type `Type`.
 *
 * @param value A `Type`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class TypeValue(value: Type, codec: Codec { type D = Type } = TypeCodec) extends Value {
  type D = Type

  override def asType: Option[Type] = Option(value)
}

/**
 * Value for when the data is an event type.
 *
 * @param value An event.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class StructuredValue[U <: Structured, C <: StructuredCodec { type D = U }](value: U, codec: C) extends Value {
  type D = U

  override def asStructured: Option[Structured] = Option(value)
}

/** Hetrogeneous comparison results. */
sealed private trait CompareResult
private case object GreaterEqual extends CompareResult
private case object Greater extends CompareResult
private case object Equal extends CompareResult
private case object Less extends CompareResult
private case object LessEqual extends CompareResult

