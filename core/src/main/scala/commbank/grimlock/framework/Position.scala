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

package commbank.grimlock.framework.position

import commbank.grimlock.framework.Persist
import commbank.grimlock.framework.encoding.{ Codec, Value }
import commbank.grimlock.framework.environment.tuner.Tuner

import play.api.libs.json.{ JsArray, JsError, Json, JsResult, JsString, JsSuccess, JsValue, Reads, Writes }

import scala.util.matching.Regex
import scala.collection.immutable.ListSet

import shapeless.{ ::, =:!=, HNil, IsDistinctConstraint, Nat, Sized, Succ }
import shapeless.nat.{ _0, _1, _2, _3, _4, _5, _6, _7, _8, _9 }
import shapeless.ops.nat.{ GT, LTEq, Pred, ToInt }
import shapeless.syntax.sized._

/** Trait for dealing with positions. */
sealed trait Position[P <: Nat] {
  /** List of coordinates of the position. */
  val coordinates: List[Value]

  /**
   * Return the coordinate at dimension (index) `dim`.
   *
   * @param dim Dimension of the coordinate to get.
   */
  def apply[D <: Nat : ToInt](dim: D)(implicit ev: LTEq[D, P]): Value = coordinates(toIndex[D])

  /**
   * Update the coordinate at `dim` with `value`.
   *
   * @param dim   The dimension to set.
   * @param value The coordinate to set.
   *
   * @return A position of the same size as `this` but with `value` set at index `dim`.
   */
  def update[
    D <: Nat : ToInt
  ](
    dim: D,
    value: Value
  )(implicit
    ev: LTEq[D, P]
  ): Position[P] = PositionImpl(coordinates.updated(toIndex[D], value))

  /**
   * Prepend a coordinate to the position.
   *
   * @param value The coordinate to prepend.
   *
   * @return A new position with the coordinate `value` prepended.
   */
  def prepend(value: Value): Position[Succ[P]] = PositionImpl(value +: coordinates)

  /**
   * Insert a coordinate into the position.
   *
   * @param dim   The dimension to insert at.
   * @param value The coordinate to prepend.
   *
   * @return A new position with the coordinate `value` prepended.
   */
  def insert[D <: Nat : ToInt](dim: D, value: Value)(implicit ev: LTEq[D, P]): Position[Succ[P]] = {
    val (h, t) = coordinates.splitAt(toIndex[D])

    PositionImpl(h ++ (value +: t))
  }

  /**
   * Append a coordinate to the position.
   *
   * @param value The coordinate to append.
   *
   * @return A new position with the coordinate `value` appended.
   */
  def append(value: Value): Position[Succ[P]] = PositionImpl(coordinates :+ value)

  /**
   * Converts the position to a consise (terse) string.
   *
   * @param separator The separator to use between the coordinates.
   *
   * @return Short string representation.
   */
  def toShortString(separator: String): String = coordinates.map(_.toShortString).mkString(separator)

  override def toString = "Position(" + coordinates.map(_.toString).mkString(",") + ")"

  /**
   * Converts the position to a JSON string.
   *
   * @param pretty Indicator if the resulting JSON string to be indented.
   */
  def toJSON(pretty: Boolean = false): String = {
    val json = Json.toJson(this)

    if (pretty) Json.prettyPrint(json) else Json.stringify(json)
  }

  /** Return this position as an option. */
  def toOption(): Option[this.type] = Option(this)

  /**
   * Compare this object with another position.
   *
   * @param that Position to compare against.
   *
   * @return x < 0 iff this < that, x = 0 iff this = that, x > 0 iff this > that.
   *
   * @note If the comparison is between two positions with different dimensions, then a comparison on the number of
   *       dimensions is performed.
   */
  def compare(that: Position[_]): Int =
    if (coordinates.length == that.coordinates.length && coordinates.length > 0)
      coordinates
        .zip(that.coordinates)
        .map { case (m, t) => Value.ordering.compare(m, t) }
        .collectFirst { case cmp if (cmp != 0) => cmp }
        .getOrElse(0)
    else
      coordinates.length.compare(that.coordinates.length)

  protected def toIndex[D <: Nat : ToInt](implicit ev: LTEq[D, P]): Int = {
    val index = Nat.toInt[D]

    if (index == 0) coordinates.length - 1 else index - 1
  }
}

/** Companion object with constructor, implicits, etc. */
object Position {
  /** Constructor for 0 dimensional position. */
  def apply(): Position[_0] = PositionImpl(List())

  /** Constructor for 1 dimensional position. */
  def apply(first: Value): Position[_1] = PositionImpl(List(first))

  /** Constructor for 2 dimensional position. */
  def apply(first: Value, second: Value): Position[_2] = PositionImpl(List(first, second))

  /** Constructor for 3 dimensional position. */
  def apply(first: Value, second: Value, third: Value): Position[_3] = PositionImpl(List(first, second, third))

  /** Constructor for 4 dimensional position. */
  def apply(
    first: Value,
    second: Value,
    third: Value,
    fourth: Value
  ): Position[_4] = PositionImpl(List(first, second, third, fourth))

  /** Constructor for 5 dimensional position. */
  def apply(
    first: Value,
    second: Value,
    third: Value,
    fourth: Value,
    fifth: Value
  ): Position[_5] = PositionImpl(List(first, second, third, fourth, fifth))

  /** Constructor for 6 dimensional position. */
  def apply(
    first: Value,
    second: Value,
    third: Value,
    fourth: Value,
    fifth: Value,
    sixth: Value
  ): Position[_6] = PositionImpl(List(first, second, third, fourth, fifth, sixth))

  /** Constructor for 7 dimensional position. */
  def apply(
    first: Value,
    second: Value,
    third: Value,
    fourth: Value,
    fifth: Value,
    sixth: Value,
    seventh: Value
  ): Position[_7] = PositionImpl(List(first, second, third, fourth, fifth, sixth, seventh))

  /** Constructor for 8 dimensional position. */
  def apply(
    first: Value,
    second: Value,
    third: Value,
    fourth: Value,
    fifth: Value,
    sixth: Value,
    seventh: Value,
    eighth: Value
  ): Position[_8] = PositionImpl(List(first, second, third, fourth, fifth, sixth, seventh, eighth))

  /** Constructor for 9 dimensional position. */
  def apply(
    first: Value,
    second: Value,
    third: Value,
    fourth: Value,
    fifth: Value,
    sixth: Value,
    seventh: Value,
    eighth: Value,
    nineth: Value
  ): Position[_9] = PositionImpl(List(first, second, third, fourth, fifth, sixth, seventh, eighth, nineth))

  /** Constructor for P dimensional position. */
  def apply[P <: Nat](coordinates: Sized[List[Value], P]): Position[P] = PositionImpl(coordinates.unsized)

  /** Standard `unapplySeq` method for pattern matching. */
  def unapplySeq[P <: Nat](pos: Position[P]): Option[Seq[Value]] = Option(pos.coordinates)

  /** Define an ordering between 2 position. Only use with position of the same type coordinates. */
  def ordering[P <: Nat](ascending: Boolean = true): Ordering[Position[P]] = new Ordering[Position[P]] {
    def compare(x: Position[P], y: Position[P]): Int = x.compare(y) * (if (ascending) 1 else -1)
  }

  /**
   * Return function that returns a string representation of a position.
   *
   * @param verbose   Indicator if verbose string is required or not.
   * @param separator The separator to use between various fields (only used if verbose is `false`).
   */
  def toString[
    P <: Nat
  ](
    verbose: Boolean = false,
    separator: String = "|"
  ): (Position[P]) => TraversableOnce[String] = (p: Position[P]) =>
    List(if (verbose) p.toString else p.toShortString(separator))

  /**
   * Return function that returns a JSON representation of a position.
   *
   * @param pretty Indicator if the resulting JSON string to be indented.
   */
  def toJSON[P <: Nat ](pretty: Boolean = false): (Position[P]) => TraversableOnce[String] = (p: Position[P]) =>
    List(p.toJSON(pretty))

  /**
   * Return a `Reads` for parsing a JSON position.
   *
   * @param codecs The codecs used to parse the JSON position data.
   */
  def reads[P <: Nat : ToInt](codecs: Sized[List[Codec], P]): Reads[Position[P]] = new Reads[Position[P]] {
    def reads(json: JsValue): JsResult[Position[P]] = {
      val fields = json.as[JsArray].value.toList

      if (fields.size == codecs.size)
        codecs
          .zip(fields)
          .flatMap { case (c, v) => c.decode(v.as[String]) }
          .sized[P]
          .map(c => JsSuccess(Position(c)))
          .getOrElse(JsError("Unable to parse coordinates"))
      else
        JsError("Incorrect number of coordinates")
    }
  }

  /** Implicit writes for use in Position.toJSON. */
  implicit def writes[P <: Nat]: Writes[Position[P]] = new Writes[Position[P]] {
    def writes(p: Position[P]): JsValue = JsArray(p.coordinates.map(v => JsString(v.toShortString)))
  }

  /** Converts a `Value` to a `Position[P]` */
  implicit def valueToPosition[V <% Value](v: V): Position[_1] = Position(v)

  /** Converts a `Value` to a `List[Position[P]]` */
  implicit def valueToListPosition[V <% Value](v: V): List[Position[_1]] = List(Position(v))

  /** Converts a `Value` to a `List[Position[P]]` */
  implicit def listValueToListPosition[V <% Value](l: List[V]): List[Position[_1]] = l.map(v => Position(v))

  /** Converts a `Position[P]` to a `List[Position[P]]` */
  implicit def positionToListPosition[P <: Nat](p: Position[P]): List[Position[P]] = List(p)

  /** Implicit conversion from Position[P] to PermutablePosition[P]. */
  implicit def positionToPermutable[
    P <: Nat
  ](
    pos: Position[P]
  )(implicit
    ev: GT[P, _1]
  ): PermutablePosition[P] = PermutablePositionImpl(pos.coordinates)

  /** Implicit conversion from Position[P] to ReduciblePosition[L, P]. */
  implicit def positionToReducible[
    P <: Nat,
    L <: Nat
  ](
    pos: Position[P]
  )(implicit
    ev1: Pred.Aux[P, L]
  ): ReduciblePosition[L, P] = ReduciblePositionImpl(pos.coordinates)
}

private case class PositionImpl[P <: Nat](coordinates: List[Value]) extends Position[P]

/** Trait for operations that modify a position (but keep the number of dimensions the same). */
trait PermutablePosition[P <: Nat] extends Position[P] {
  /**
   * Permute the order of coordinates.
   *
   * @param order The new ordering of the coordinates.
   *
   * @return A position of the same size as `this` but with the coordinates ordered according to `order`.
   */
  def permute(order: Sized[ListSet[Int], P])(implicit ev: ToInt[P]): Position[P] = PositionImpl(
    coordinates.zip(order.unsized.ensureSized[P].toList.reverse).sortBy(_._2).map(_._1)
  )
}

private case class PermutablePositionImpl[P <: Nat](coordinates: List[Value]) extends PermutablePosition[P]

/** Trait for operations that reduce a position by one dimension. */
trait ReduciblePosition[L <: Nat, P <: Nat] extends Position[P] {
  /**
   * Remove the coordinate at dimension `dim`.
   *
   * @param dim The dimension to remove.
   *
   * @return A new position with dimension `dim` removed.
   */
  def remove[D <: Nat : ToInt](dim: D)(implicit ev: LTEq[D, P]): Position[L] = {
    val (h, t) = coordinates.splitAt(toIndex[D])

    PositionImpl(h ++ t.tail)
  }

  /**
   * Melt dimension `dim` into `into`.
   *
   * @param dim   The dimension to remove.
   * @param into  The dimension into which to melt.
   * @param merge The function to use for merging coordinates
   *
   * @return A new position with dimension `dim` removed. The coordinate at `unto` will be a string value consisting of
   *         the string representations of the coordinates `dim` and `unto` separated by `separator`.
   *
   * @note `dim` and `into` must not be the same.
   */
  def melt[
    D <: Nat : ToInt,
    I <: Nat : ToInt
  ](
    dim: D,
    into: I,
    merge: (Value, Value) => Value
  )(implicit
    ev1: LTEq[D, P],
    ev2: LTEq[I, P],
    ev3: IsDistinctConstraint[D :: I :: HNil] // shapeless.=:!= doesn't serialise
  ): Position[L] = {
    val iidx = toIndex[I]
    val didx = toIndex[D]

    PositionImpl(
      coordinates
        .updated(iidx, merge(coordinates(iidx), coordinates(didx)))
        .zipWithIndex.collect { case (c, i) if (i != didx) => c }
    )
  }
}

private case class ReduciblePositionImpl[L <: Nat, P <: Nat](coordinates: List[Value]) extends ReduciblePosition[L, P]

/** Trait that represents the positions of a matrix. */
trait Positions[P <: Nat, U[_]] extends Persist[Position[P], U] {
  /**
   * Returns the distinct position(s) (or names) for a given `slice`.
   *
   * @param slice Encapsulates the dimension(s) for which the names are to be returned.
   * @param tuner The tuner for the job.
   *
   * @return A `U[Position[slice.S]]` of the distinct position(s).
   */
  def names[
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T
  )(implicit
    ev1: slice.S =:!= _0,
    ev2: Positions.NamesTuner[U, T]
  ): U[Position[slice.S]]

  /**
   * Persist to disk.
   *
   * @param file   Name of the output file.
   * @param writer Writer that converts `Position[N]` to string.
   * @param tuner  The tuner for the job.
   *
   * @return A `U[Position[P]]` which is this object's data.
   */
  def saveAsText[
    T <: Tuner
  ](
    file: String,
    writer: Persist.TextWriter[Position[P]] = Position.toString(),
    tuner: T
  )(implicit
    ev: Persist.SaveAsTextTuner[U, T]
  ): U[Position[P]]

  /**
   * Slice the positions using a regular expression applied to a dimension.
   *
   * @param keep  Indicator if the matched positions should be kept or removed.
   * @param dim   Dimension to slice on.
   * @param regex The regular expression to match on.
   *
   * @return A `U[Position[P]]` with only the positions of interest.
   *
   * @note The matching is done by converting the coordinate to its short string reprensentation and then applying the
   *       regular expression.
   */
  def slice[
    D <: Nat : ToInt
  ](
    keep: Boolean,
    dim: D,
    regex: Regex
  )(implicit
    ev: LTEq[D, P]
  ): U[Position[P]] = slice(keep, p => regex.pattern.matcher(p(dim).toShortString).matches)

  /**
   * Slice the positions using a regular expression.
   *
   * @param keep  Indicator if the matched positions should be kept or removed.
   * @param regex The regular expression to match on.
   *
   * @return A `U[Position[P]]` with only the positions of interest.
   *
   * @note The matching is done by converting each coordinate to its short string reprensentation and then applying the
   *       regular expression.
   */
  def slice(keep: Boolean, regex: Regex): U[Position[P]] = slice(
    keep,
    p => p.coordinates.map(c => regex.pattern.matcher(c.toShortString).matches).reduce(_ && _)
  )

  /**
   * Slice the positions using one or more positions.
   *
   * @param keep      Indicator if the matched positions should be kept or removed.
   * @param positions The positions to slice on.
   *
   * @return A `U[Position[P]]` with only the positions of interest.
   */
  def slice(
    keep: Boolean,
    positions: List[Position[P]]
  ): U[Position[P]] = slice(keep, p => positions.contains(p))

  protected def slice(keep: Boolean, f: Position[P] => Boolean): U[Position[P]]
}

/** Companion object to `Positions` with types, implicits, etc. */
object Positions {
  /** Trait for tuners permitted on a call to `names`. */
  trait NamesTuner[U[_], T <: Tuner]
}

