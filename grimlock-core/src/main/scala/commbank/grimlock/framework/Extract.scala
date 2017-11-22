// Copyright 2015,2016,2017 Commonwealth Bank of Australia
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

package commbank.grimlock.framework.extract

import commbank.grimlock.framework.Cell
import commbank.grimlock.framework.encoding.Value
import commbank.grimlock.framework.position.{ Coordinates1, Position, Slice }

import shapeless.{ ::, HList, HNil, Nat, Witness }

/** Trait for extracting data from a user provided value given a cell. */
trait Extract[P <: HList, W, T] extends java.io.Serializable { self =>
  /**
   * Extract value for the given cell.
   *
   * @param cell The cell for which to extract a value.
   * @param ext  The user provided data from which to extract.
   *
   * @return Optional value (if present) or `None` otherwise.
   */
  def extract(cell: Cell[P], ext: W): Option[T]

  /**
   * Operator for transforming the returned value.
   *
   * @param presenter The function to apply and transform the returned value.
   *
   * @return An extract that runs `this` and then transfors the returned value.
   */
  def andThenPresent[X](presenter: (T) => Option[X]): Extract[P, W, X] = new Extract[P, W, X] {
    def extract(cell: Cell[P], ext: W) = self.extract(cell, ext).flatMap(r => presenter(r))
  }
}

/** Companion object for the `Extract` trait. */
object Extract {
  /** Converts a `(Cell[P], W) => Option[T]` to a `Extract[P, W, T]`. */
  implicit def funcToExtract[P <: HList, W, T](e: (Cell[P], W) => Option[T]): Extract[P, W, T] = new Extract[P, W, T] {
    def extract(cell: Cell[P], ext: W): Option[T] = e(cell, ext)
  }
}

/**
 * Extract from a `Map[Position[V :: HNil], T]` using a dimension from the cell.
 *
 * @param dimension Dimension used for extracting from the map.
 */
case class ExtractWithDimension[
  P <: HList,
  D <: Nat,
  V <: Value[_],
  T
](
  dimension: D
)(implicit
  ev: Position.IndexConstraints.Aux[P, D, V]
) extends Extract[P, Map[Position[V :: HNil], T], T] {
  def extract(cell: Cell[P], ext: Map[Position[V :: HNil], T]): Option[T] = ext
    .get(Position(cell.position(dimension)))
}

/** Companion object with convenience constructors. */
object ExtractWithDimension {
  /** Extract from a `Map[Position[V :: HNil], T]` using a dimension from the cell. */
  def apply[
    P <: HList,
    D <: Nat,
    T
  ](implicit
    ev1: Position.IndexConstraints[P, D],
    ev2: Witness.Aux[D]
  ): ExtractWithDimension[P, D, ev1.V, T] = ExtractWithDimension(ev2.value)(ev1)
}

/**
 * Extract from a `Map[Position[V :: HNil], Map[Position[Coordinates1[K]], T]]` using a
 * dimension from the cell and the provided key.
 *
 * @param dimension Dimension used for extracting from the outer map.
 * @param key       The key used for extracting from the inner map.
 */
case class ExtractWithDimensionAndKey[
  P <: HList,
  D <: Nat,
  V <: Value[_],
  K <% Value[K],
  T
](
  dimension: D,
  key: K
)(implicit
  ev: Position.IndexConstraints.Aux[P, D, V]
) extends Extract[P, Map[Position[V :: HNil], Map[Position[Coordinates1[K]], T]], T] {
  def extract(
    cell: Cell[P],
    ext: Map[Position[V :: HNil], Map[Position[Coordinates1[K]], T]]
  ): Option[T] = ext
    .get(Position(cell.position(dimension)))
    .flatMap(_.get(Position(key)))
}

/** Companion object with convenience constructors. */
object ExtractWithDimensionAndKey {
  /**
   * Extract from a `Map[Position[V :: HNil], Map[Position[Coordinates1[K]], T]]` using a
   * dimension from the cell and the provided key.
   *
   * @param key The key used for extracting from the inner map.
   */
  def apply[
    P <: HList,
    D <: Nat,
    K <% Value[K],
    T
  ](
    key: K
  )(implicit
    ev1: Position.IndexConstraints[P, D],
    ev2: Witness.Aux[D]
  ): ExtractWithDimensionAndKey[P, D, ev1.V, K, T] = {
    implicit val aux = Position.IndexConstraints[P, D]

    ExtractWithDimensionAndKey(ev2.value, key)
  }
}

/**
 * Extract from a `Map[Position[Coordinates1[K]], T]` using the provided key.
 *
 * @param The key used for extracting from the map.
 */
case class ExtractWithKey[
  P <: HList,
  K <% Value[K],
  T
](
  key: K
) extends Extract[P, Map[Position[Coordinates1[K]], T], T] {
  def extract(cell: Cell[P], ext: Map[Position[Coordinates1[K]], T]): Option[T] = ext.get(Position(key))
}

/** Extract from a `Map[Position[P], T]` using the position of the cell. */
case class ExtractWithPosition[P <: HList, T]() extends Extract[P, Map[Position[P], T], T] {
  def extract(cell: Cell[P], ext: Map[Position[P], T]): Option[T] = ext.get(cell.position)
}

/**
 * Extract from a `Map[Position[P], Map[Position[Coordinates1[K]], T]]` using the position of the cell and
 * the provided key.
 *
 * @param key The key used for extracting from the inner map.
 */
case class ExtractWithPositionAndKey[
  P <: HList,
  K <% Value[K],
  T
](
  key: K
) extends Extract[P, Map[Position[P], Map[Position[Coordinates1[K]], T]], T] {
  def extract(cell: Cell[P], ext: Map[Position[P], Map[Position[Coordinates1[K]], T]]): Option[T] = ext
    .get(cell.position)
    .flatMap(_.get(Position(key)))
}

/**
 * Extract from a `Map[Position[S], T]` using the selected position(s) of the cell.
 *
 * @param slice The slice used to extract the selected position(s) from the cell which are used as the key
 *              into the map.
 */
case class ExtractWithSelected[
  P <: HList,
  S <: HList,
  R <: HList,
  T
](
  slice: Slice[P, S, R]
) extends Extract[P, Map[Position[S], T], T] {
  def extract(cell: Cell[P], ext: Map[Position[S], T]): Option[T] = ext.get(slice.selected(cell.position))
}

/**
 * Extract from a `Map[Position[S], Map[Position[Coordinates1[K]], T]]` using the selected position(s) of
 * the cell and the provided key.
 *
 * @param slice The slice used to extract the selected position(s) from the cell which are used as the key
 *              into the map.
 * @param key   The key used for extracting from the inner map.
 */
case class ExtractWithSelectedAndKey[
  P <: HList,
  S <: HList,
  R <: HList,
  K <% Value[K],
  T
](
  slice: Slice[P, S, R],
  key: K
) extends Extract[P, Map[Position[S], Map[Position[Coordinates1[K]], T]], T] {
  def extract(cell: Cell[P], ext: Map[Position[S], Map[Position[Coordinates1[K]], T]]): Option[T] = ext
    .get(slice.selected(cell.position))
    .flatMap(_.get(Position(key)))
}

/**
 * Extract from a `Map[Position[S], Map[Position[R], T]]` using the selected and remainder position(s)
 * of the cell.
 *
 * @param slice The slice used to extract the selected and remainder position(s) from the cell which are used
 *              as the keys into the outer and inner maps.
 */
case class ExtractWithSlice[
  P <: HList,
  S <: HList,
  R <: HList,
  T
](
  slice: Slice[P, S, R]
) extends Extract[P, Map[Position[S], Map[Position[R], T]], T] {
  def extract(cell: Cell[P], ext: Map[Position[S], Map[Position[R], T]]): Option[T] = ext
    .get(slice.selected(cell.position))
    .flatMap(_.get(slice.remainder(cell.position)))
}

