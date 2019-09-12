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

package commbank.grimlock.framework.position

import commbank.grimlock.framework.Persist
import commbank.grimlock.framework.encoding.{
  BinaryCodec,
  BooleanCodec,
  BoundedStringCodec,
  Codec,
  DateCodec,
  DecimalCodec,
  DoubleCodec,
  FloatCodec,
  IntCodec,
  LongCodec,
  StringCodec,
  TimestampCodec,
  TypeCodec,
  Value
}
import commbank.grimlock.framework.environment.Context
import commbank.grimlock.framework.environment.tuner.Tuner
import commbank.grimlock.framework.error.{ IncorrectNumberOfFields, UnableToDecodePosition }
import commbank.grimlock.framework.utility.JSON

import java.util.regex.Pattern

import play.api.libs.json.{ JsArray, JsError, JsResult, JsString, JsSuccess, JsValue, Reads, Writes }

import scala.reflect.ClassTag
import scala.util.{ Failure, Success, Try }
import scala.util.matching.Regex

import shapeless.{
  ::,
  =:!=,
  HList,
  HNil,
  Lazy,
  LUBConstraint,
  Poly1,
  Poly2,
  Nat
}
import shapeless.ops.hlist.{
  At,
  ConstMapper,
  FlatMapper,
  IsHCons,
  LeftFolder,
  Length,
  Mapper,
  Prepend,
  ReplaceAt,
  Split,
  Zip,
  ZipWithIndex
}
import shapeless.ops.nat.{ GT, GTEq, ToInt }
import shapeless.nat._1

/**
 * Case class for dealing with positions.
 *
 * @param coordinates List of coordinates of the position.
 */
case class Position[P <: HList](coordinates: P)(implicit v: Position.ValueConstraints[P]) {
  /**
   * Append a coordinate to the position.
   *
   * @param value The coordinate to append.
   *
   * @return A new position with the coordinate `value` appended.
   */
  def append[
    T <% Value[T]
  ](
    value: T
  )(implicit
    ev: Position.AppendConstraints[P, Value[T]]
  ): Position[ev.Q] = append(implicitly[Value[T]](value))

  /**
   * Append a coordinate to the position.
   *
   * @param value The coordinate to append.
   *
   * @return A new position with the coordinate `value` appended.
   */
  def append[
    V <: Value[_]
  ](
    value: V
  )(implicit
    ev: Position.AppendConstraints[P, V]
  ): Position[ev.Q] = ev.append(coordinates, value)

  /**
   * Return the coordinate(s) at the provided indice(s). Dimension can either be of type
   * `shapeless.Nat` or a `shapeless.HList` containing only `shapeless.Nat`s. The former case
   * returns a single result type while the latter case returns a `shapeless.HList`.
   *
   * @param dimension Dimension(s) of the coordinate(s) to get.
   */
  def apply[
    I
  ](
    dimension: I
  )(implicit
    ev: Position.IndexConstraints[P, I]
  ): ev.V = ev.index(coordinates, dimension)

  /** Converts this position to a list of `Value`. */
  def asList: List[Value[_]] = coordinates
    .runtimeList // can't use HListOps.toList as shapeless.ops.hlist.ToTraversable does not serialise
    .asInstanceOf[List[Value[_]]]

  /**
   * Compare this object with another position.
   *
   * @param that Position to compare against.
   *
   * @return x < 0 iff this < that, x = 0 iff this = that, x > 0 iff this > that.
   */
  def compare(that: Position[P]): Int = asList
    .zip(that.asList)
    .map { case (x, y) => Value.ordering().compare(x, y) }
    .collectFirst { case cmp if (cmp != 0) => cmp }
    .getOrElse(0)

  /**
   * Insert a coordinate into the position.
   *
   * @param dimension The dimension to insert at.
   * @param value     The coordinate to insert.
   *
   * @return A new position with the coordinate `value` prepended.
   */
  def insert[
    D <: Nat,
    T <% Value[T]
  ](
    dimension: D,
    value: T
  )(implicit
    ev: Position.InsertConstraints[P, D, Value[T]]
  ): Position[ev.Q] = insert(dimension, implicitly[Value[T]](value))

  /**
   * Insert a coordinate into the position.
   *
   * @param dimension The dimension to insert at.
   * @param value     The coordinate to insert.
   *
   * @return A new position with the coordinate `value` prepended.
   */
  def insert[
    D <: Nat,
    V <: Value[_]
  ](
    dimension: D,
    value: V
  )(implicit
    ev: Position.InsertConstraints[P, D, V]
  ): Position[ev.Q] = ev.insert(coordinates, dimension, value)

  /**
   * Melt dimension `dim` into `into`.
   *
   * @param dimension The dimension to remove.
   * @param into      The dimension into which to melt.
   * @param merge     The function to use for merging coordinates
   *
   * @return A new position with `dimension` removed. The coordinate at `into` will be the output of `merge`.
   */
  def melt[
    D <: Nat,
    I <: Nat,
    VI <: Value[_],
    VD <: Value[_],
    T <% Value[T]
  ](
    dim: D,
    into: I,
    merge: (VI, VD) => T
  )(implicit
    ev: Position.MeltConstraints[P, D, I, VI, VD, T]
  ): Position[ev.Q] = ev.melt(coordinates, dim, into, (x, y) => merge(x, y))

  /**
   * Prepend a coordinate to the position.
   *
   * @param value The coordinate to prepend.
   *
   * @return A new position with the coordinate `value` prepended.
   */
  def prepend[
    T <% Value[T]
  ](
    value: T
  )(implicit
    ev: Position.PrependConstraints[P, Value[T]]
  ): Position[ev.Q] = prepend(implicitly[Value[T]](value))

  /**
   * Prepend a coordinate to the position.
   *
   * @param value The coordinate to prepend.
   *
   * @return A new position with the coordinate `value` prepended.
   */
  def prepend[
    V <: Value[_]
  ](
    value: V
  )(implicit
    ev: Position.PrependConstraints[P, V]
  ): Position[ev.Q] = ev.prepend(coordinates, value)

  /**
   * Remove the coordinate(s) at dimension(s) `dimension`(s). Dimension can either be of type
   * `shapeless.Nat` or a `shapeless.HList` containing only `shapeless.Nat`s.
   *
   * @param dimension The dimension(s) to remove.
   *
   * @return A new position with `dimension`(s) removed.
   */
  def remove[
    I
  ](
    dimension: I
  )(implicit
    ev: Position.RemoveConstraints[P, I]
  ): Position[ev.Q] = ev.remove(coordinates, dimension)

  /**
   * Converts the position to a JSON string.
   *
   * @param pretty Indicator if the resulting JSON string to be indented.
   */
  def toJSON(pretty: Boolean = false): String = JSON.to(this, Position.writes[P], pretty)

  /** Return this position as an option. */
  def toOption: Option[Position[P]] = Option(this)

  /**
   * Converts the position to a consise (terse) string.
   *
   * @param separator The separator to use between the coordinates.
   *
   * @return Short string representation.
   */
  def toShortString(separator: String): String = asList.map(_.toShortString).mkString(separator)

  /**
   * Update the coordinate at `dim` with `value`.
   *
   * @param dim   The dimension to set.
   * @param value The coordinate to set.
   *
   * @return A position of the same size as `this` but with `value` set at index `dim`.
   */
  def update[
    D <: Nat,
    T <% Value[T]
  ](
    dimension: D,
    value: T
  )(implicit
    ev: Position.UpdateConstraints[P, D, Value[T]]
  ): Position[ev.Q] = update(dimension, implicitly[Value[T]](value))

  /**
   * Update the coordinate at `dim` with `value`.
   *
   * @param dim   The dimension to set.
   * @param value The coordinate to set.
   *
   * @return A position of the same size as `this` but with `value` set at index `dim`.
   */
  def update[
    D <: Nat,
    V <: Value[_]
  ](
    dimension: D,
    value: V
  )(implicit
    ev: Position.UpdateConstraints[P, D, V]
  ): Position[ev.Q] = ev.update(coordinates, dimension, value)
}

/** Companion object with constructor, implicits, etc. */
object Position {
  /** Type that captures all constraints for appending coordinates to a position. */
  trait AppendConstraints[P <: HList, V <: Value[_]] extends java.io.Serializable {
    type Q <: HList

    def append(coordinates: P, value: V): Position[Q]
  }

  /** Companion object with convenience types and constructors. */
  object AppendConstraints {
    /** Auxilary type that exposes the type of the appended value. */
    type Aux[P <: HList, V <: Value[_], OUT <: HList] = AppendConstraints[P, V] { type Q = OUT }

    /** Implicit with all constraints for appending coordinates to a position. */
    implicit def appendConstraints[
      P <: HList,
      V <: Value[_],
      OUT <: HList
    ](implicit
      ev1: Prepend.Aux[P, V :: HNil, OUT],
      ev2: LUBConstraint[OUT, Value[_]]
    ): Aux[P, V, OUT] = new AppendConstraints[P, V] {
      type Q = OUT

      def append(coordinates: P, value: V): Position[Q] = Position(coordinates :+ value)
    }

    /** Constructore that returns an auxillary append constraints. */
    def apply[P <: HList, V <: Value[_]](implicit ev: AppendConstraints[P, V]): Aux[P, V, ev.Q] = ev
  }

  /** Type that captures that `Q` should have the same number of coordinates as `P`. */
  trait EqualConstraints[Q <: HList, P <: HList] extends java.io.Serializable { }

  /** Companion object with convenience types and constructors. */
  object EqualConstraints {
    /** Implicit that ensures that `Q` has at same number of coordinates as `P`. */
    implicit def equalConstraints[
      P <: HList,
      Q <: HList,
      L <: Nat,
      M <: Nat
    ](implicit
      ev1: Length.Aux[P, L],
      ev2: Length.Aux[Q, M],
      ev3: L =:= M
    ): EqualConstraints[Q, P] = new EqualConstraints[Q, P] { }
  }

  /** Type that captures that `Q` should have the same or more coordinates than `P`. */
  trait GreaterEqualConstraints[Q <: HList, P <: HList] extends java.io.Serializable { }

  /** Companion object with convenience types and constructors. */
  object GreaterEqualConstraints {
    /** Implicit that ensures that `Q` has at least as many coordinates as `P`. */
    implicit def greaterEqualConstraints[
      P <: HList,
      Q <: HList,
      L <: Nat,
      M <: Nat
    ](implicit
      ev1: Length.Aux[P, L],
      ev2: Length.Aux[Q, M],
      ev3: GTEq[M, L]
    ): GreaterEqualConstraints[Q, P] = new GreaterEqualConstraints[Q, P] { }
  }

  /** Type that captures that `Q` should have more coordinates than `P`. */
  trait GreaterThanConstraints[Q <: HList, P <: HList] extends java.io.Serializable { }

  /** Companion object with convenience types and constructors. */
  object GreaterThanConstraints {
    /** Implicit that ensures that `Q` has at more coordinates than `P`. */
    implicit def greaterThanConstraints[
      P <: HList,
      Q <: HList,
      L <: Nat,
      M <: Nat
    ](implicit
      ev1: Length.Aux[P, L],
      ev2: Length.Aux[Q, M],
      ev3: GT[M, L]
    ): GreaterThanConstraints[Q, P] = new GreaterThanConstraints[Q, P] { }
  }

  /**
   * Type that captures all constraints for indexing a position at one or more indices. The implicits on the companion
   * object ensure that the type parameter `P` always satisfies `ValueConstraints[P]`.
   */
  trait IndexConstraints[P <: HList, I] extends java.io.Serializable {
    /** Output type after selecting coordinates at the given dimension. */
    type V

    implicit val vTag: ClassTag[V]

    def index(coordinates: P, dimension: I): V
  }

  /** Companion object with convenience types and constructors. */
  object IndexConstraints {
    /** Auxilary type that exposes the type of the value(s) at the indice(s). */
    type Aux[P <: HList, I, OUT] = IndexConstraints[P, I] { type V = OUT }

    /** Implicit with all constraints for indexing a position at a singleton HList of indices. */
    implicit def indexSingleton[
      P <: HList,
      D <: Nat,
      OUT <: Value[_]
    ](implicit
      ev: Aux[P, D, OUT] // use the instance for the single Nat (`indexNat`)
    ): Aux[P, D :: HNil, OUT :: HNil] = new IndexConstraints[P, D :: HNil] {
      type V = OUT :: HNil

      implicit val vTag: ClassTag[V] = implicitly[ClassTag[V]]

      def index(coordinates: P, dimension: D :: HNil): V = ev.index(coordinates, dimension.head) :: HNil
    }

    /** Implicit with all constraints for indexing a position at a HList of indices of length 2 or higher. */
    implicit def index2OrMore[
      P <: HList,
      D <: Nat,
      E <: Nat,
      OH <: Value[_],
      OHT <: Value[_],
      W <: HList,
      I <: HList
    ](implicit
      ev1: Aux[P, D, OH], // use the instance for the single Nat (`indexNat`)
      ev2: Lazy[Aux[P, E :: I, OHT :: W]],
      ev3: GT[E, D] // indices must be in ascending order
    ): Aux[P, D :: E :: I, OH :: OHT :: W] = new IndexConstraints[P, D :: E :: I] {
      type V = OH :: OHT :: W

      implicit val vTag: ClassTag[V] = implicitly[ClassTag[V]]

      def index(
        coordinates: P,
        dimension: D :: E :: I
      ): V = ev1.index(coordinates, dimension.head) :: ev2.value.index(coordinates, dimension.tail)
    }

    /** Implicit with all constraints for indexing a position at a single `shapeless.Nat`. */
    implicit def indexNat[
      P <: HList,
      D <: Nat,
      OUT <: Value[_]
    ](implicit
      ev1: At.Aux[P, D, OUT],
      ev2: ClassTag[OUT],
      ev3: ValueConstraints[P]
    ): Aux[P, D, OUT] = new IndexConstraints[P, D] {
      type V = OUT

      implicit val vTag: ClassTag[V] = ev2

      def index(coordinates: P, dimension: D): V = coordinates.at[D]
    }

    def apply[P <: HList, I](implicit ev: IndexConstraints[P, I]): Aux[P, I, ev.V] = ev
  }

  /** Type that captures all constraints for inserting coordinates into a position. */
  trait InsertConstraints[P <: HList, D <: Nat, V <: Value[_]] extends java.io.Serializable {
    type Q <: HList

    def insert(coordinates: P, dimension: D, value: V): Position[Q]
  }

  /** Companion object with convenience types and constructors. */
  object InsertConstraints {
    /** Auxilary type that exposes the type with the inserted value. */
    type Aux[P <: HList, D <: Nat, V <: Value[_], OUT <: HList] = InsertConstraints[P, D, V] { type Q = OUT }

    /** Implicit with all constraints for inserting coordinates in a position. */
    implicit def insertConstraints[
      P <: HList,
      D <: Nat,
      V <: Value[_],
      OUT <: HList,
      PRE <: HList,
      SUF <: HList
    ](implicit
      ev1: Split.Aux[P, D, PRE, SUF],
      ev2: Prepend.Aux[PRE, V :: SUF, OUT],
      ev3: LUBConstraint[OUT, Value[_]]
    ): Aux[P, D, V, OUT] = new InsertConstraints[P, D, V] {
      type Q = OUT

      def insert(coordinates: P, dimension: D, value: V): Position[Q] = {
        val (prefix, suffix) = coordinates.split[D]

        Position(prefix ::: value :: suffix)
      }
    }

    /** Constructore that returns an auxillary insert constraints. */
    def apply[P <: HList, D <: Nat, V <: Value[_]](implicit ev: InsertConstraints[P, D, V]): Aux[P, D, V, ev.Q] = ev
  }

  /** Type that captures that `P` has more than 1 coordinates. */
  trait IsMultiDimensionalConstraints[P <: HList] extends java.io.Serializable { }

  /** Companion object with convenience types and constructors. */
  object IsMultiDimensionalConstraints {
    /** Implicit that ensures that `P` has more than 1 coordinates. */
    implicit def isMultiDimensionalConstraints[
      P <: HList,
      L <: Nat
    ](implicit
      ev1: Length.Aux[P, L],
      ev2: GT[L, _1]
    ): IsMultiDimensionalConstraints[P] = new IsMultiDimensionalConstraints[P] { }
  }

  /** Type that captures that `P` has only 1 coordinate. */
  trait IsVectorConstraints[P <: HList] extends java.io.Serializable { }

  /** Companion object with convenience types and constructors. */
  object IsVectorConstraints {
    /** Implicit that ensures that `P` has only 1 coordinate. */
    implicit def isVectorConstraints[
      P <: HList,
      L <: Nat
    ](implicit
      ev1: Length.Aux[P, L],
      ev2: L =:= _1
    ): IsVectorConstraints[P] = new IsVectorConstraints[P] { }
  }

  /** Type that captures all constraints for melting coordinates of a position. */
  trait MeltConstraints[
    P <: HList,
    D <: Nat,
    I <: Nat,
    VI,
    VD,
    T
  ] extends java.io.Serializable {
    type Q <: HList

    def melt(coordinates: P, dim: D, into: I, merge: (VI, VD) => Value[T]): Position[Q]
  }

  /** Companion object with convenience types and constructors. */
  object MeltConstraints {
    /** Auxilary type that exposes the type of the melted position. */
    type Aux[
      P <: HList,
      D <: Nat,
      I <: Nat,
      VI,
      VD,
      T,
      OUT <: HList
    ] = MeltConstraints[P, D, I, VI, VD, T] { type Q = OUT }

    /** Implicit with all constraints for melting coordinates of a position. */
    implicit def meltConstraints[
      P <: HList,
      D <: Nat,
      I <: Nat,
      VI,
      VD,
      T,
      OUT <: HList,
      REP <: HList,
      PRE <: HList,
      SUF <: HList,
      TAI <: HList
    ](implicit
      ev1: At.Aux[P, I, VI],
      ev2: At.Aux[P, D, VD],
      ev3: ReplaceAt.Aux[P, I, Value[T], (VI, REP)],
      ev4: Split.Aux[REP, D, PRE, SUF],
      ev5: IsHCons.Aux[SUF, _, TAI],
      ev6: Prepend.Aux[PRE, TAI, OUT],
      ev7: D =:!= I,
      ev8: LUBConstraint[OUT, Value[_]]
    ): Aux[P, D, I, VI, VD, T, OUT] =  new MeltConstraints[P, D, I, VI, VD, T] {
      type Q = OUT

      def melt(coordinates: P, dim: D, into: I, merge: (VI, VD) => Value[T]): Position[Q] = {
        val value = merge(coordinates.at[I], coordinates.at[D])
        val (prefix, suffix) = coordinates.updatedAt[I](value).split[D]

        Position(prefix ++ suffix.tail)
      }
    }

    /** Constructore that returns an auxillary melt constraints. */
    def apply[
      P <: HList,
      D <: Nat,
      I <: Nat,
      VI,
      VD,
      T
    ](implicit
      ev: MeltConstraints[P, D, I, VI, VD, T]
    ): Aux[P, D, I, VI, VD, T, ev.Q] = ev
  }

  /** Type that captures that the position needs at least one coordinate. */
  trait NonEmptyConstraints[P <: HList] extends java.io.Serializable { }

  /** Companion object with convenience types and constructors. */
  object NonEmptyConstraints {
    /** Implicit for ensuring the position has coordinates. */
    implicit def nonEmptyConstrains[
      P <: HList
    ](implicit
      ev: P =:!= HNil
    ): NonEmptyConstraints[P] = new NonEmptyConstraints[P] { }
  }

  /** Type that captures all constraints for prepending coordinates to a position. */
  trait PrependConstraints[P <: HList, V <: Value[_]] extends java.io.Serializable {
    type Q <: HList

    def prepend(coordinates: P, value: V): Position[Q]
  }

  /** Companion object with convenience types and constructors. */
  object PrependConstraints {
    /** Auxilary type that exposes the type of the prepended value. */
    type Aux[P <: HList, V <: Value[_], OUT <: HList] = PrependConstraints[P, V] { type Q = OUT }

    /** Implicit with all constraints for appending coordinates to a position. */
    implicit def prependConstraints[
      P <: HList,
      V <: Value[_]
    ](implicit
      ev: LUBConstraint[V :: P, Value[_]]
    ): Aux[P, V, V :: P] = new PrependConstraints[P, V] {
      type Q = V :: P

      def prepend(coordinates: P, value: V): Position[Q] = Position(value :: coordinates)
    }

    /** Constructore that returns an auxillary append constraints. */
    def apply[P <: HList, V <: Value[_]](implicit ev: PrependConstraints[P, V]): Aux[P, V, ev.Q] = ev
  }

  /** Type that captures all constraints for removing coordinate(s) from a position. */
  trait RemoveConstraints[P <: HList, I] extends java.io.Serializable {
    /** Coordinate type after removing values at the given indice(s). */
    type Q <: HList

    def remove(coordinates: P, dimension: I): Position[Q]
  }

  /** Companion object with convenience types and constructors. */
  object RemoveConstraints {
    /** Auxilary type that exposes the type with the removed value(s). */
    type Aux[P <: HList, I, OUT <: HList] = RemoveConstraints[P, I] { type Q = OUT }

    /** Implicit with all constraints for removing a singleton HList of coordinates. */
    implicit def removeSingleton[
      P <: HList,
      D <: Nat,
      OUT <: HList
    ](implicit
      ev: Aux[P, D, OUT] // use the instance for the single Nat case (`removeNat`)
    ): Aux[P, D :: HNil, OUT] = new RemoveConstraints[P, D :: HNil] {
      type Q = OUT

      def remove(coordinates: P, dimension: D :: HNil): Position[Q] = ev.remove(coordinates, dimension.head)
    }

    /** Implicit with all constraints for removing a HList of coordinates with 2 or more elements. */
    implicit def remove2OrMore[
      P <: HList,
      D <: Nat,
      E <: Nat,
      R <: HList,
      OUT <: HList,
      I <: HList
    ](implicit
      ev1: Lazy[Aux[P, E :: I, R]], // remove the indices in the tail first
      ev2: Aux[R, D, OUT], // use the instance for the single Nat case (`removeNat`)
      ev3: GT[E, D] // indices should be in ascending order
    ): Aux[P, D :: E :: I, OUT] = new RemoveConstraints[P, D :: E :: I] {
      type Q = OUT

      def remove(
        coordinates: P,
        dimension: D :: E :: I
      ): Position[Q] = ev2.remove(ev1.value.remove(coordinates, dimension.tail).coordinates, dimension.head)
    }

    /** Implicit with all constraints for removing a single coordinate of a position. */
    implicit def removeNat[
      P <: HList,
      D <: Nat,
      OUT <: HList,
      PRE <: HList,
      SUF <: HList,
      TAI <: HList
    ](implicit
      ev1: Split.Aux[P, D, PRE, SUF],
      ev2: IsHCons.Aux[SUF, _, TAI],
      ev3: Prepend.Aux[PRE, TAI, OUT],
      ev4: LUBConstraint[OUT, Value[_]]
    ): Aux[P, D, OUT] = new RemoveConstraints[P, D] {
      type Q = OUT

      def remove(coordinates: P, dimension: D): Position[Q] = {
        val (prefix, suffix) = coordinates.split[D]

        Position(prefix ++ suffix.tail)
      }
    }

    def apply[P <: HList, I](implicit ev: RemoveConstraints[P, I]): Aux[P, I, ev.Q] = ev
  }

  /** Type that captures all constraints for parsing positions from string. */
  trait TextParseConstraints[L <: HList] extends java.io.Serializable {
    type Q <: HList

    def parse(coordinates: List[String], codecs: L): Try[Position[Q]]
  }

  /** Companion object with convenience types and constructors. */
  object TextParseConstraints {
    /** Auxilary type that exposes the type of the parsed text. */
    type Aux[L <: HList, OUT <: HList] = TextParseConstraints[L] { type Q = OUT }

    /** Implicit with all constraints for parsing a position from string. */
    implicit def textParseConstraints[
      L <: HList,
      OUT <: HList,
      TUP <: HList,
      ZWI <: HList,
      IND <: HList,
      ZIP <: HList,
      DEC <: HList
    ](implicit
      ev1: LUBConstraint[L, Codec[_]],
      ev2: ConstMapper.Aux[List[String], L, TUP],
      ev3: ZipWithIndex.Aux[TUP, ZWI],
      ev4: Mapper.Aux[GetAtIndex.type, ZWI, IND],
      ev5: Zip.Aux[L :: IND :: HNil, ZIP],
      ev6: Mapper.Aux[DecodeString.type, ZIP, DEC],
      ev7: LeftFolder.Aux[DEC, Boolean, IsEmpty.type, Boolean],
      ev8: FlatMapper.Aux[RemoveOption.type, DEC, OUT],
      ev9: LUBConstraint[OUT, Value[_]]
    ): Aux[L, OUT] = new TextParseConstraints[L] {
      type Q = OUT

      def parse(coordinates: List[String], codecs: L): Try[Position[Q]] = {
        if (codecs.runtimeLength == coordinates.length) {
          val parsed = codecs
            .zip(codecs.mapConst(coordinates).zipWithIndex.map(GetAtIndex))
            .map(DecodeString)

          if (!parsed.foldLeft(false)(IsEmpty))
            Success(Position(parsed.flatMap(RemoveOption)))
          else
            Failure(UnableToDecodePosition(coordinates.mkString("(", ",", ")")))
        } else
          Failure(IncorrectNumberOfFields(coordinates.mkString("(", ",", ")")))
      }
    }

    /** Constructore that returns an auxillary parse constraints. */
    def apply[L <: HList](implicit ev: TextParseConstraints[L]): Aux[L, ev.Q] = ev
  }

  /** Type that captures all constraints for updating coordinates in a position. */
  trait UpdateConstraints[P <: HList, D <: Nat, V <: Value[_]] extends java.io.Serializable {
    type Q <: HList

    def update(coordinates: P, dimension: D, value: V): Position[Q]
  }

  /** Companion object with convenience types and constructors. */
  object UpdateConstraints {
    /** Auxilary type that exposes the type with the updated position. */
    type Aux[P <: HList, D <: Nat, V <: Value[_], OUT <: HList] = UpdateConstraints[P, D, V] { type Q = OUT }

    /** Implicit with all constraints for updating coordinates of a position. */
    implicit def updateConstraints[
      P <: HList,
      D <: Nat,
      V <: Value[_],
      OUT <: HList,
      UPD <: Value[_]
    ](implicit
      ev1: ReplaceAt.Aux[P, D, V, (UPD, OUT)],
      ev2: LUBConstraint[OUT, Value[_]]
    ): Aux[P, D, V, OUT] = new UpdateConstraints[P, D, V] {
      type Q = OUT

      def update(coordinates: P, dimension: D, value: V): Position[Q] = Position(coordinates.updatedAt[D](value))
    }

    /** Constructore that returns an auxillary update constraints. */
    def apply[P <: HList, D <: Nat, V <: Value[_]](implicit ev: UpdateConstraints[P, D, V]): Aux[P, D, V, ev.Q] = ev
  }

  /** Type that captures all constraints for the type of the coordinates in a position. */
  trait ValueConstraints[P <: HList] extends java.io.Serializable { }

  /**
   * Trait containing methods to resolve instancs of `ValueConstraints[P]` that should only be used by the compiler if
   * it cannot resolve `ValueConstraints[P]` using the methods on the `ValueConstraints` companion object.
   */
  trait ValueConstraintsLowPriority extends java.io.Serializable {
    /**
     * Given `IndexConstraints.Aux[P, I, S]` (which implies `ValueConstraints[P]`) we know that any indexing of `P` will
     * also satisfy `ValueConstraints`.
     */
    implicit def valueConstraintsIndex[
      P <: HList,
      I <: HList,
      S <: HList
    ](implicit
      ev: IndexConstraints.Aux[P, I, S]
    ): ValueConstraints[S] = new ValueConstraints[S] { }
  }

  /** Companion object with convenience types and constructors. */
  object ValueConstraints extends ValueConstraintsLowPriority {
    /** Any `HList` composed only of `Value[_]`s satisfies `ValueConstraints`. */
    implicit def valueConstraints[
      P <: HList
    ](implicit
      ev: LUBConstraint[P, Value[_]]
    ): ValueConstraints[P] = new ValueConstraints[P] { }
  }

  /** Converts a `List[T]` to a `List[Position[Coordinates1[T]]]` */
  implicit def listTToListPosition[T <% Value[T]](l: List[T]): List[Position[Coordinates1[T]]] = l.map(t => Position(t))

  /** Converts a `List[V]` to a `List[Position[V :: HNil]]` */
  implicit def listValueToListPosition[V <: Value[_]](l: List[V]): List[Position[V :: HNil]] = l.map(v => Position(v))

  /** Converts a `Position[P]` to a `List[Position[P]]` */
  implicit def positionToListPosition[P <: HList](p: Position[P]): List[Position[P]] = List(p)

  /** Converts a `T` to a `List[Position[Coordinates1[T]]]` */
  implicit def tToListPosition[T <% Value[T]]( t: T): List[Position[Coordinates1[T]]] = List(Position(t))

  /** Converts a `T` to a `Position[Coordinates1[T]]` */
  implicit def tToPosition[T <% Value[T]](t: T): Position[Coordinates1[T]] = Position(t)

  /** Converts a `V` to a `List[Position[V :: HNil]]` */
  implicit def valueToListPosition[V <: Value[_]](v: V): List[Position[V :: HNil]] = List(Position(v))

  /** Converts a `V` to a `Position[V :: HNil]` */
  implicit def valueToPosition[V <: Value[_]](v: V): Position[V :: HNil] = Position(v)

  /** Constructor for 0 dimensional position. */
  def apply(): Position[HNil] = Position(HNil)

  /** Constructor for 1 dimensional position using types `Ti`. */
  def apply[T1 <% Value[T1]](first: T1): Position[Coordinates1[T1]] = Position(implicitly[Value[T1]](first))

  /** Constructor for 1 dimensional position using values `Vi`. */
  def apply[V1 <: Value[_]](first: V1): Position[V1 :: HNil] = Position(first :: HNil)

  /** Constructor for 2 dimensional position using types `Ti`. */
  def apply[
    T1 <% Value[T1],
    T2 <% Value[T2]
  ](
    first: T1,
    second: T2
  ): Position[Coordinates2[T1, T2]] = Position(implicitly[Value[T1]](first), implicitly[Value[T2]](second))

  /** Constructor for 2 dimensional position using values `Vi`. */
  def apply[
    V1 <: Value[_],
    V2 <: Value[_]
  ](
    first: V1,
    second: V2
  ): Position[V1 :: V2 :: HNil] = Position(first :: second :: HNil)

  /** Constructor for 3 dimensional position using types `Ti`. */
  def apply[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3]
  ](
    first: T1,
    second: T2,
    third: T3
  ): Position[Coordinates3[T1, T2, T3]] = Position(
    implicitly[Value[T1]](first),
    implicitly[Value[T2]](second),
    implicitly[Value[T3]](third)
  )

  /** Constructor for 3 dimensional position using values `Vi`. */
  def apply[
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_]
  ](
    first: V1,
    second: V2,
    third: V3
  ): Position[V1 :: V2 :: V3 :: HNil] = Position(first :: second :: third :: HNil)

  /** Constructor for 4 dimensional position using types `Ti`. */
  def apply[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3],
    T4 <% Value[T4]
  ](
    first: T1,
    second: T2,
    third: T3,
    fourth: T4
  ): Position[Coordinates4[T1, T2, T3, T4]] = Position(
    implicitly[Value[T1]](first),
    implicitly[Value[T2]](second),
    implicitly[Value[T3]](third),
    implicitly[Value[T4]](fourth)
  )

  /** Constructor for 4 dimensional position using values `Vi`. */
  def apply[
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_],
    V4 <: Value[_]
  ](
    first: V1,
    second: V2,
    third: V3,
    fourth: V4
  ): Position[V1 :: V2 :: V3 :: V4 :: HNil] = Position(first :: second :: third :: fourth :: HNil)

  /** Constructor for 5 dimensional position using types `Ti`. */
  def apply[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3],
    T4 <% Value[T4],
    T5 <% Value[T5]
  ](
    first: T1,
    second: T2,
    third: T3,
    fourth: T4,
    fifth: T5
  ): Position[Coordinates5[T1, T2, T3, T4, T5]] = Position(
    implicitly[Value[T1]](first),
    implicitly[Value[T2]](second),
    implicitly[Value[T3]](third),
    implicitly[Value[T4]](fourth),
    implicitly[Value[T5]](fifth)
  )

  /** Constructor for 5 dimensional position using values `Vi`. */
  def apply[
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_],
    V4 <: Value[_],
    V5 <: Value[_]
  ](
    first: V1,
    second: V2,
    third: V3,
    fourth: V4,
    fifth: V5
  ): Position[V1 :: V2 :: V3 :: V4 :: V5 :: HNil] = Position(first :: second :: third :: fourth :: fifth :: HNil)

  /** Constructor for 6 dimensional position using values `Ti`. */
  def apply[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3],
    T4 <% Value[T4],
    T5 <% Value[T5],
    T6 <% Value[T6]
  ](
    first: T1,
    second: T2,
    third: T3,
    fourth: T4,
    fifth: T5,
    sixth: T6
  ): Position[Coordinates6[T1, T2, T3, T4, T5, T6]] = Position(
    implicitly[Value[T1]](first),
    implicitly[Value[T2]](second),
    implicitly[Value[T3]](third),
    implicitly[Value[T4]](fourth),
    implicitly[Value[T5]](fifth),
    implicitly[Value[T6]](sixth)
  )

  /** Constructor for 6 dimensional position using values `Vi`. */
  def apply[
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_],
    V4 <: Value[_],
    V5 <: Value[_],
    V6 <: Value[_]
  ](
    first: V1,
    second: V2,
    third: V3,
    fourth: V4,
    fifth: V5,
    sixth: V6
  ): Position[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil] = Position(
    first :: second :: third :: fourth :: fifth :: sixth :: HNil
  )

  /** Constructor for 7 dimensional position using types `Ti`. */
  def apply[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3],
    T4 <% Value[T4],
    T5 <% Value[T5],
    T6 <% Value[T6],
    T7 <% Value[T7]
  ](
    first: T1,
    second: T2,
    third: T3,
    fourth: T4,
    fifth: T5,
    sixth: T6,
    seventh: T7
  ): Position[Coordinates7[T1, T2, T3, T4, T5, T6, T7]] = Position(
    implicitly[Value[T1]](first),
    implicitly[Value[T2]](second),
    implicitly[Value[T3]](third),
    implicitly[Value[T4]](fourth),
    implicitly[Value[T5]](fifth),
    implicitly[Value[T6]](sixth),
    implicitly[Value[T7]](seventh)
  )

  /** Constructor for 7 dimensional position using values `Vi`. */
  def apply[
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_],
    V4 <: Value[_],
    V5 <: Value[_],
    V6 <: Value[_],
    V7 <: Value[_]
  ](
    first: V1,
    second: V2,
    third: V3,
    fourth: V4,
    fifth: V5,
    sixth: V6,
    seventh: V7
  ): Position[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil] = Position(
    first :: second :: third :: fourth :: fifth :: sixth :: seventh :: HNil
  )

  /** Constructor for 8 dimensional position using types `Ti`. */
  def apply[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3],
    T4 <% Value[T4],
    T5 <% Value[T5],
    T6 <% Value[T6],
    T7 <% Value[T7],
    T8 <% Value[T8]
  ](
    first: T1,
    second: T2,
    third: T3,
    fourth: T4,
    fifth: T5,
    sixth: T6,
    seventh: T7,
    eighth: T8
  ): Position[Coordinates8[T1, T2, T3, T4, T5, T6, T7, T8]] = Position(
    implicitly[Value[T1]](first),
    implicitly[Value[T2]](second),
    implicitly[Value[T3]](third),
    implicitly[Value[T4]](fourth),
    implicitly[Value[T5]](fifth),
    implicitly[Value[T6]](sixth),
    implicitly[Value[T7]](seventh),
    implicitly[Value[T8]](eighth)
  )

  /** Constructor for 8 dimensional position using values `Vi`. */
  def apply[
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_],
    V4 <: Value[_],
    V5 <: Value[_],
    V6 <: Value[_],
    V7 <: Value[_],
    V8 <: Value[_]
  ](
    first: V1,
    second: V2,
    third: V3,
    fourth: V4,
    fifth: V5,
    sixth: V6,
    seventh: V7,
    eighth: V8
  ): Position[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil] = Position(
    first :: second :: third :: fourth :: fifth :: sixth :: seventh :: eighth :: HNil
  )

  /** Constructor for 9 dimensional position using type `Ti`. */
  def apply[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3],
    T4 <% Value[T4],
    T5 <% Value[T5],
    T6 <% Value[T6],
    T7 <% Value[T7],
    T8 <% Value[T8],
    T9 <% Value[T9]
  ](
    first: T1,
    second: T2,
    third: T3,
    fourth: T4,
    fifth: T5,
    sixth: T6,
    seventh: T7,
    eighth: T8,
    nineth: T9
  ): Position[Coordinates9[T1, T2, T3, T4, T5, T6, T7, T8, T9]] = Position(
    implicitly[Value[T1]](first),
    implicitly[Value[T2]](second),
    implicitly[Value[T3]](third),
    implicitly[Value[T4]](fourth),
    implicitly[Value[T5]](fifth),
    implicitly[Value[T6]](sixth),
    implicitly[Value[T7]](seventh),
    implicitly[Value[T8]](eighth),
    implicitly[Value[T9]](nineth)
  )

  /** Constructor for 9 dimensional position using values `Vi`. */
  def apply[
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_],
    V4 <: Value[_],
    V5 <: Value[_],
    V6 <: Value[_],
    V7 <: Value[_],
    V8 <: Value[_],
    V9 <: Value[_]
  ](
    first: V1,
    second: V2,
    third: V3,
    fourth: V4,
    fifth: V5,
    sixth: V6,
    seventh: V7,
    eighth: V8,
    nineth: V9
  ): Position[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil] = Position(
    first :: second :: third :: fourth :: fifth :: sixth :: seventh :: eighth :: nineth :: HNil
  )

  /**
   * Parse a position from components.
   *
   * @param coordinates The coordinate strings to parse.
   * @param codecs      The codecs to parse with.
   *
   * @return A `Some[Position[Q]]` if successful, `None` otherwise.
   */
  def fromComponents[
    L <: HList
  ](
    coordinates: List[String],
    codecs: L
  )(implicit
    ev: TextParseConstraints[L]
  ): Option[Position[ev.Q]] = parse(coordinates, codecs).toOption

  /**
   * Parse a position from a JSON string.
   *
   * @param str    The JSON string to parse.
   * @param codecs The codecs to parse with.
   *
   * @return A `Some[Position[Q]]` if successful, `None` otherwise.
   */
  def fromJSON[
    L <: HList
  ](
    str: String,
    codecs: L
  )(implicit
    ev: TextParseConstraints[L]
  ): Option[Position[ev.Q]] = JSON.from(str, reads(codecs)).toOption

  /**
   * Parse a position from a short string.
   *
   * @param str       The short string to parse.
   * @param codecs    The codecs to parse with.
   * @param separator The separator between coordinate fields.
   *
   * @return A `Some[Position[Q]]` if successful, `None` otherwise.
   */
  def fromShortString[
    L <: HList
  ](
    str: String,
    codecs: L,
    separator: String
  )(implicit
    ev: TextParseConstraints[L]
  ): Option[Position[ev.Q]] = parse(str, codecs, separator).toOption

  /**
   * Return position parser for JSON strings.
   *
   * @param codecs The codecs to parse with.
   *
   * @return A position parser.
   */
  def jsonParser[
    L <: HList
  ](
    codecs: L
  )(implicit
    ev: TextParseConstraints[L]
  ): Persist.TextParser[Position[ev.Q]] = (str) => List(JSON.from(str, reads(codecs)))

  /**
   * Define an ordering between 2 positions.
   *
   * @param ascending Indicator if ordering should be ascending or descending.
   */
  def ordering[P <: HList](ascending: Boolean = true): Ordering[Position[P]] = new Ordering[Position[P]] {
    def compare(x: Position[P], y: Position[P]): Int = (if (ascending) 1 else -1) * x.compare(y)
  }

  /**
   * Return a `Reads` for parsing a JSON position.
   *
   * @param codecs The codecs used to parse the JSON position data.
   */
  def reads[
    L <: HList
  ](
    codecs: L
  )(implicit
    ev: TextParseConstraints[L]
  ): Reads[Position[ev.Q]] = new Reads[Position[ev.Q]] {
    def reads(
      json: JsValue
    ): JsResult[Position[ev.Q]] = parse(json.as[JsArray].value.toList.map(_.as[String]), codecs) match {
      case Success(pos) => JsSuccess(pos)
      case Failure(err) => JsError(err.toString)
    }
  }

  /**
   * Return position parser for short strings.
   *
   * @param codecs    The codecs to parse with.
   * @param separator The separator between coordinate fields.
   *
   * @return A position parser.
   */
  def shortStringParser[
    L <: HList
  ](
    codecs: L,
    separator: String
  )(implicit
    ev: TextParseConstraints[L]
  ): Persist.TextParser[Position[ev.Q]] = (str) => List(parse(str, codecs, separator))

  /**
   * Return function that returns a JSON representation of a position.
   *
   * @param pretty Indicator if the resulting JSON string to be indented.
   */
  def toJSON[P <: HList](pretty: Boolean = false): Persist.TextWriter[Position[P]] = (pos) => List(pos.toJSON(pretty))

  /**
   * Return function that returns a string representation of a position.
   *
   * @param separator The separator to use between the coordinates.
   */
  def toShortString[
    P <: HList
  ](
    separator: String
  ): Persist.TextWriter[Position[P]] = (pos) => List(pos.toShortString(separator))

  /** `Writes` for converting a position to JSON. */
  def writes[P <: HList]: Writes[Position[P]] = new Writes[Position[P]] {
    def writes(pos: Position[P]): JsValue = JsArray(pos.asList.map(v => JsString(v.toShortString)))
  }

  private def parse[
    L <: HList
  ](
    coordinates: List[String],
    codecs: L
  )(implicit
    ev: TextParseConstraints[L]
  ): Try[Position[ev.Q]] = ev.parse(coordinates, codecs)

  private def parse[
    L <: HList
  ](
    str: String,
    codecs: L,
    separator: String
  )(implicit
    ev: TextParseConstraints[L]
  ): Try[Position[ev.Q]] = parse(str.split(Pattern.quote(separator), -1).toList, codecs)
}

/** Object with implicits needed to parse coordinates from string. */
object DecodeString extends Poly1 {
  /** Parse coordinate from string using a Codec. */
  implicit def go[T] = at[(Codec[T], String)] { case (c, s) => Value.fromShortString(s, c) }

  /** Convenience implicit to parse byte array coordinate from string without having to cast it to a Codec. */
  implicit def goBinary = at[(BinaryCodec.type, String)] { case (c, s) => Value.fromShortString(s, c) }

  /** Convenience implicit to parse boolean coordinate from string without having to cast it to a Codec. */
  implicit def goBoolean = at[(BooleanCodec.type, String)] { case (c, s) => Value.fromShortString(s, c) }

  /** Convenience implicit to parse bounded string coordinate from string without having to cast it to a Codec. */
  implicit def goBounded = at[(BoundedStringCodec, String)] { case (c, s) => Value.fromShortString(s, c) }

  /** Convenience implicit to parse date coordinate from string without having to cast it to a Codec. */
  implicit def goDate = at[(DateCodec, String)] { case (c, s) => Value.fromShortString(s, c) }

  /** Convenience implicit to parse decimal coordinate from string without having to cast it to a Codec. */
  implicit def goDecimal = at[(DecimalCodec, String)] { case (c, s) => Value.fromShortString(s, c) }

  /** Convenience implicit to parse double coordinate from string without having to cast it to a Codec. */
  implicit def goDouble = at[(DoubleCodec.type, String)] { case (c, s) => Value.fromShortString(s, c) }

  /** Convenience implicit to parse float coordinate from string without having to cast it to a Codec. */
  implicit def goFloat = at[(FloatCodec.type, String)] { case (c, s) => Value.fromShortString(s, c) }

  /** Convenience implicit to parse int coordinate from string without having to cast it to a Codec. */
  implicit def goInt = at[(IntCodec.type, String)] { case (c, s) => Value.fromShortString(s, c) }

  /** Convenience implicit to parse long coordinate from string without having to cast it to a Codec. */
  implicit def goLong = at[(LongCodec.type, String)] { case (c, s) => Value.fromShortString(s, c) }

  /** Convenience implicit to parse string coordinate from string without having to cast it to a Codec. */
  implicit def goString = at[(StringCodec.type, String)] { case (c, s) => Value.fromShortString(s, c) }

  /** Convenience implicit to parse timestamp coordinate from string without having to cast it to a Codec. */
  implicit def goTimestamp = at[(TimestampCodec.type, String)] { case (c, s) => Value.fromShortString(s, c) }

  /** Convenience implicit to parse type coordinate from string without having to cast it to a Codec. */
  implicit def goType = at[(TypeCodec.type, String)] { case (c, s) => Value.fromShortString(s, c) }
}

/** Object with implicits needed to match strings and coordinates; used form parsing coordinates from string. */
object GetAtIndex extends Poly1 {
  /** Return the string (from a list), at the codec's index. */
  implicit def go[D <: Nat : ToInt] = at[(List[String], D)] { case (list, nat) => list(Nat.toInt[D]) }
}

/** Object with implicits needed to check if option is empty. */
object IsEmpty extends Poly2 {
  /** Check if either the boolean is true or if the option is empty. */
  implicit def go[V <: Value[_]] = at { (c: Boolean, e: Option[V]) => c || e.isEmpty }
}

/** Object with implicits needed to flatten options; used form parsing coordinates from string. */
object RemoveOption extends Poly1 {
  /** Remove option; use only if there are no None in the HList. */
  implicit def go[T] = at[Option[T]](_.get :: HNil)
}

/** Trait that represents the positions of a matrix. */
trait Positions[P <: HList, C <: Context[C]] extends Persist[Position[P], C] {
  /**
   * Returns the distinct position(s) (or names) for a given `slice`.
   *
   * @param slice Encapsulates the dimension(s) for which the names are to be returned.
   * @param tuner The tuner for the job.
   *
   * @return A `C#U[Position[S]]` of the distinct position(s).
   */
  def names[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T
  )(implicit
    ev1: Position.NonEmptyConstraints[S],
    ev2: Positions.NamesTuner[C#U, T]
  ): C#U[Position[S]]

  /**
   * Persist to disk.
   *
   * @param context The operating context.
   * @param file    Name of the output file.
   * @param writer  Writer that converts `Position[N]` to string.
   * @param tuner   The tuner for the job.
   *
   * @return A `C#U[Position[P]]` which is this object's data.
   */
  def saveAsText[
    T <: Tuner
  ](
    context: C,
    file: String,
    writer: Persist.TextWriter[Position[P]],
    tuner: T
  )(implicit
    ev: Persist.SaveAsTextTuner[C#U, T]
  ): C#U[Position[P]]

  /**
   * Select the positions using a regular expression applied to a dimension.
   *
   * @param keep  Indicator if the matched positions should be kept or removed.
   * @param dim   Dimension to select on.
   * @param regex The regular expression to match on.
   *
   * @return A `C#U[Position[P]]` with only the positions of interest.
   *
   * @note The matching is done by converting the coordinate to its short string reprensentation and then applying the
   *       regular expression.
   */
  def select[
    D <: Nat
  ](
    keep: Boolean,
    dim: D,
    regex: Regex
  )(implicit
    ev: Position.IndexConstraints[P, D] { type V <: Value[_] }
  ): C#U[Position[P]] = select(keep, p => regex.pattern.matcher(p(dim).toShortString).matches)

  /**
   * Select the positions using a regular expression.
   *
   * @param keep  Indicator if the matched positions should be kept or removed.
   * @param regex The regular expression to match on.
   *
   * @return A `C#U[Position[P]]` with only the positions of interest.
   *
   * @note The matching is done by converting each coordinate to its short string reprensentation and then applying the
   *       regular expression.
   */
  def select(
    keep: Boolean,
    regex: Regex
  ): C#U[Position[P]] = select(keep, _.asList.map(c => regex.pattern.matcher(c.toShortString).matches).reduce(_ && _))

  /**
   * Select the positions using one or more positions.
   *
   * @param keep      Indicator if the matched positions should be kept or removed.
   * @param positions The positions to select on.
   *
   * @return A `C#U[Position[P]]` with only the positions of interest.
   */
  def select(
    keep: Boolean,
    positions: List[Position[P]]
  ): C#U[Position[P]] = select(keep, p => positions.contains(p))

  protected def select(keep: Boolean, f: Position[P] => Boolean): C#U[Position[P]]
}

/** Companion object to `Positions` with types, implicits, etc. */
object Positions {
  /** Trait for tuners permitted on a call to `names`. */
  trait NamesTuner[U[_], T <: Tuner] extends java.io.Serializable
}

