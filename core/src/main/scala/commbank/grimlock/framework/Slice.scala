// Copyright 2014,2015,2016 Commonwealth Bank of Australia
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

import shapeless.{ Nat, Witness }
import shapeless.nat._1
import shapeless.ops.nat.{ Diff, LTEq, ToInt }

/** Base trait that encapsulates dimension on which to operate. */
sealed trait Slice[L <: Nat, P <: Nat] {
  /**
   * Return type of the `selected` method; a position of dimension less than `P`.
   *
   * @note `S` and `R` together make `P`.
   */
  type S <: Nat

  /**
   * Return type of the `remainder` method; a position of dimension less than `P`.
   *
   * @note `S` and `R` together make `P`.
   */
  type R <: Nat

  /** Type of the dimension. */
  type D <: Nat

  /** The dimension of this slice. */
  val dimension: D

  /** Returns the selected coordinate(s) for the given `pos`. */
  def selected(pos: ReduciblePosition[L, P]): Position[S]

  /** Returns the remaining coordinate(s) for the given `pos`. */
  def remainder(pos: ReduciblePosition[L, P]): Position[R]

  protected def remove(pos: ReduciblePosition[L, P])(implicit ev1: ToInt[D], ev2: LTEq[D, P]): Position[L] = pos
    .remove(dimension)
  protected def single(pos: Position[P])(implicit ev1: ToInt[D], ev2: LTEq[D, P]): Position[_1] =
    Position(pos(dimension))
}

/**
 * Indicates that the selected coordinate is indexed by `dimension`. In other words, when a groupBy is performed, it is
 * performed using a `Position[_1]` consisting of the coordinate at index `dimension`.
 */
sealed trait Over[
  L <: Nat,
  P <: Nat,
  E <: Nat
] extends Slice[L, P] {
  type S = _1
  type R = L
  type D = E
}

/** Companion object to `Over` trait. */
object Over {
  /**
   * Indicates that the selected coordinate is indexed by `dimension`. In other words, when a groupBy is performed,
   * it is performed using a `Position[_1]` consisting of the coordinate at index `dimension`.
   *
   * @param dimension Dimension of the selected coordinate.
   */
  def apply[
    L <: Nat,
    P <: Nat
  ](
    dimension: Nat
  )(implicit
    ev1: Diff.Aux[P, L, _1],
    ev2: LTEq[dimension.N, P],
    ev3: Witness.Aux[dimension.N],
    ev4: ToInt[dimension.N]
  ): Over[L, P, dimension.N] = OverImpl(ev3.value)

  /** Standard `unapply` method for pattern matching. */
  def unapply[D <: Nat](over: Over[_, _, D]): Option[D] = Option(over.dimension)
}

private case class OverImpl[
  L <: Nat,
  P <: Nat,
  D <: Nat : ToInt
](
  dimension: D
)(implicit
  ev1: Diff.Aux[P, L, _1],
  ev2: LTEq[D, P]
) extends Over[L, P, D] {
  def selected(pos: ReduciblePosition[L, P]): Position[S] = single(pos)
  def remainder(pos: ReduciblePosition[L, P]): Position[R] = remove(pos)
}

/**
 * Indicates that the selected coordinates are all except the one indexed by `dimension`. In other words, when a
 * groupBy is performed, it is performed using a `Position[L]` consisting of all coordinates except that at index
 * `dimension`.
 */
sealed trait Along[
  L <: Nat,
  P <: Nat,
  E <: Nat
] extends Slice[L, P] {
  type S = L
  type R = _1
  type D = E
}

/** Companion object to `Along` trait. */
object Along {
  /**
   * Indicates that the selected coordinates are all except the one indexed by `dimension`. In other words, when a
   * groupBy is performed, it is performed using a `Position[L]` consisting of all coordinates except that at index
   * `dimension`.
   *
   * @param dimension Dimension of the coordinate to exclude.
   */
  def apply[
    L <: Nat,
    P <: Nat
  ](
    dimension: Nat
  )(implicit
    ev1: Diff.Aux[P, L, _1],
    ev2: LTEq[dimension.N, P],
    ev3: Witness.Aux[dimension.N],
    ev4: ToInt[dimension.N]
  ): Along[L, P, dimension.N] = AlongImpl(ev3.value)

  /** Standard `unapply` method for pattern matching. */
  def unapply[D <: Nat](along: Along[_, _, D]): Option[D] = Option(along.dimension)
}

private case class AlongImpl[
  L <: Nat,
  P <: Nat,
  D <: Nat : ToInt
](
  dimension: D
)(implicit
  ev1: Diff.Aux[P, L, _1],
  ev2: LTEq[D, P]
) extends Along[L, P, D] {
  def selected(pos: ReduciblePosition[L, P]): Position[S] = remove(pos)
  def remainder(pos: ReduciblePosition[L, P]): Position[R] = single(pos)
}

