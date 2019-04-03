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

import commbank.grimlock.framework.encoding.Value

import shapeless.{ ::, HList, HNil, Nat, Witness }

/** Trait that encapsulates dimension on which to operate. */
trait Slice[P <: HList, S <: HList, R <: HList] {
  /** Returns the selected coordinate(s) for the given `pos`. */
  def selected(pos: Position[P]): Position[S]

  /** Returns the remaining coordinate(s) for the given `pos`. */
  def remainder(pos: Position[P]): Position[R]
}

/**
 * Indicates that the selected coordinate is indexed by `dimension`. In other words, when a groupBy is performed,
 * it is performed using a `Position` consisting of the coordinate at index `dimension`.
 *
 * @param dimension Dimension of the selected coordinate.
 */
case class Over[
  P <: HList,
  D <: Nat,
  V <: Value[_],
  Q <: HList
](
  dimension: D
)(implicit
  ev1: Position.IndexConstraints.Aux[P, D, V],
  ev2: Position.RemoveConstraints.Aux[P, D, Q]
) extends Slice[P, V :: HNil, Q] {
  def selected(pos: Position[P]): Position[V :: HNil] = Position(pos(dimension))
  def remainder(pos: Position[P]): Position[Q] = pos.remove(dimension)
}

/** Companion object to `Over` case class. */
object Over {
  /** Construct an `Over` using types. */
  def apply[
    P <: HList,
    D <: Nat
  ](implicit
    ev1: Position.IndexConstraints[P, D],
    ev2: Position.RemoveConstraints[P, D],
    ev3: Witness.Aux[D]
  ): Over[P, D, ev1.V, ev2.Q] = Over(ev3.value)(ev1, ev2)
}

/**
 * Indicates that the selected coordinates are all except the one indexed by `dimension`. In other words, when a
 * groupBy is performed, it is performed using a `Position` consisting of all coordinates except that at index
 * `dimension`.
 *
 * @param dimension Dimension of the coordinate to exclude.
 */
case class Along[
  P <: HList,
  D <: Nat,
  V <: Value[_],
  Q <: HList
](
  dimension: D
)(implicit
  ev1: Position.IndexConstraints.Aux[P, D, V],
  ev2: Position.RemoveConstraints.Aux[P, D, Q]
) extends Slice[P, Q, V :: HNil] {
  def selected(pos: Position[P]): Position[Q] = pos.remove(dimension)
  def remainder(pos: Position[P]): Position[V :: HNil] = Position(pos(dimension))
}

/** Companion object to `Along` case class. */
object Along {
  /** Construct an `Along` using types. */
  def apply[
    P <: HList,
    D <: Nat
  ](implicit
    ev1: Position.IndexConstraints[P, D],
    ev2: Position.RemoveConstraints[P, D],
    ev3: Witness.Aux[D]
  ): Along[P, D, ev1.V, ev2.Q] = Along(ev3.value)(ev1, ev2)
}

