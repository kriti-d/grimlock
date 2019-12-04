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

import java.util.Date

import scala.reflect.ClassTag
import scala.util.matching.Regex

/** Trait for variable values. */
trait Value[T] {
  /** The schema used to validate and encode/decode this value. */
  val schema: Schema[T]

  /** The encapsulated value. */
  val value: T

  if (!schema.validate(value))
    throw new IllegalArgumentException(s"${value} does not conform to schema ${schema.toShortString}")

  /** Return value as `X` (if an appropriate converter exists), or `None` if the conversion is not supported. */
  def as[X : ClassTag]: Option[X] = {
    val ct = implicitly[ClassTag[X]]

    def cast(a: Any): Option[X] = a match {
      case ct(x) => Option(x)
      case _ => None
    }

    schema
      .converters
      .flatMap { case convert => cast(convert(value)) }
      .headOption
      .orElse(cast(value))
  }

  /**
   * Compare this value with a `T`.
   *
   * @param that The value to compare against.
   *
   * @return The returned value is < 0 iff this < that, 0 iff this = that, > 0 iff this > that.
   */
  def cmp(that: T): Int = schema.compare(value, that)

  /**
   * Compare this value with another.
   *
   * @param that The `Value` to compare against.
   *
   * @return If that can be compared to this, then an `Option` where the value is < 0 iff this < that,
   *         0 iff this = that, > 0 iff this > that. A `None` in all other case.
   */
  def cmp(that: Value[_]): Option[Int]

  /**
   * Check for equality with `that`.
   *
   * @param that Value to compare against.
   */
  def equ(that: Value[_]): Boolean = evaluate(that, Equal)

  /**
   * Check if `this` is greater or equal to `that`.
   *
   * @param that Value to compare against.
   *
   * @note If `that` is of a type that can't be compares to `this`, then the result is always `false`. This
   *       is the desired behaviour for the `Matrix.which` method (i.e. a filter).
   */
  def geq(that: Value[_]): Boolean = evaluate(that, GreaterEqual)

  /**
   * Check if `this` is greater than `that`.
   *
   * @param that Value to compare against.
   *
   * @note If `that` is of a type that can't be compares to `this`, then the result is always `false`. This
   *       is the desired behaviour for the `Matrix.which` method (i.e. a filter).
   */
  def gtr(that: Value[_]): Boolean = evaluate(that, Greater)

  /**
   * Check if `this` is less or equal to `that`.
   *
   * @param that Value to compare against.
   *
   * @note If `that` is of a type that can't be compares to `this`, then the result is always `false`. This
   *       is the desired behaviour for the `Matrix.which` method (i.e. a filter).
   */
  def leq(that: Value[_]): Boolean = evaluate(that, LessEqual)

  /**
   * Check for for match with `that` regular expression.
   *
   * @param that Regular expression to match against.
   *
   * @note This always applies `toShortString` method before matching.
   */
  def like(that: Regex): Boolean = that.pattern.matcher(this.toShortString).matches

  /**
   * Check if `this` is less than `that`.
   *
   * @param that Value to compare against.
   *
   * @note If `that` is of a type that can't be compares to `this`, then the result is always `false`. This
   *       is the desired behaviour for the `Matrix.which` method (i.e. a filter).
   */
  def lss(that: Value[_]): Boolean = evaluate(that, Less)

  /**
   * Check for in-equality with `that`.
   *
   * @param that Value to compare against.
   */
  def neq(that: Value[_]): Boolean = !(this equ that)

  /** Return a consise (terse) string representation of a value. */
  def toShortString: String = schema.encode(value)

  private def evaluate(that: Value[_], op: CompareResult): Boolean = cmp(that) match {
    case Some(0) => (op == Equal) || (op == GreaterEqual) || (op == LessEqual)
    case Some(x) if (x > 0) => (op == Greater) || (op == GreaterEqual)
    case Some(x) if (x < 0) => (op == Less) || (op == LessEqual)
    case _ => false
  }
}

/**
 * Value for when the data is of type `java.util.Date`
 *
 * @param value A `java.util.Date`.
 * @param schema The schema used for encoding/decoding `value`.
 */
case class DateValue(value: Date, schema: Schema[Date]) extends Value[Date] {
  def cmp(that: Value[_]): Option[Int] = that.as[Date].map(d => cmp(d))
}

/**
 * Value for when the data is of type `BigDecimal`.
 *
 * @param value A `BigDecimal`.
 * @param schema The schema used for encoding/decoding `value`.
 */
case class DecimalValue(value: BigDecimal, schema: Schema[BigDecimal]) extends Value[BigDecimal] {
  def cmp(that: Value[_]): Option[Int] = that.as[BigDecimal].map(bd => cmp(bd))
}

/**
 * Value for when the data is of type `Double`.
 *
 * @param value A `Double`.
 * @param schema The schema used for encoding/decoding `value`.
 */
case class DoubleValue(value: Double, schema: Schema[Double]) extends Value[Double] {
  def cmp(that: Value[_]): Option[Int] = that.as[Double].map(d => cmp(d))
}

/**
 * Value for when the data is of type `Float`.
 *
 * @param value A `Float`.
 * @param schema The schema used for encoding/decoding `value`.
 */
case class FloatValue(value: Float, schema: Schema[Float]) extends Value[Float] {
  def cmp(that: Value[_]): Option[Int] = that.as[Float].map(d => cmp(d))
}

/**
 * Value for when the data is of type `Int`.
 *
 * @param value A `Int`.
 * @param schema The schema used for encoding/decoding `value`.
 */
case class IntValue(value: Int, schema: Schema[Int]) extends Value[Int] {
  def cmp(that: Value[_]): Option[Int] = that
    .as[Int]
    .map(i => super.cmp(i))
    .orElse(that.as[Long].map(l => super.cmp(l.toInt)))
    .orElse(that.as[Double].map(d => super.cmp(if (d > value) math.ceil(d).toInt else math.floor(d).toInt)))
}

/**
 * Value for when the data is of type `String`.
 *
 * @param value A `String`.
 * @param codec The codec used for encoding/decoding `value`.
 */
case class StringValue(value: String, schema: Schema[String]) extends Value[String] {
  def cmp(that: Value[_]): Option[Int] = that.as[String].map(s => cmp(s))
}

/** Hetrogeneous comparison results. */
sealed private trait CompareResult
private case object GreaterEqual extends CompareResult
private case object Greater extends CompareResult
private case object Equal extends CompareResult
private case object Less extends CompareResult
private case object LessEqual extends CompareResult

