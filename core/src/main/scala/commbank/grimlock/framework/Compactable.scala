// Copyright 2016 Commonwealth Bank of Australia
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

package commbank.grimlock.framework

import commbank.grimlock.framework.content._
import commbank.grimlock.framework.position._

import shapeless.Nat
import shapeless.nat.{ _0, _1, _2, _3, _4, _5, _6, _7, _8, _9 }
import shapeless.ops.nat.{ Diff, GT }

/** Trait for compacting a cell to a `Map`. */
trait Compactable[L <: Nat, P <: Nat] extends java.io.Serializable {
  /** Type of the compacted position and content. */
  type C[R <: Nat]

  /**
   * Convert a single cell to a `Map`.
   *
   * @param slice Encapsulates the dimension(s) to compact.
   * @param cell  The cell to compact.
   *
   * @return A `Map` with the compacted cell.
   */
  def toMap(
    slice: Slice[L, P]
  )(
    cell: Cell[P]
  )(implicit
    ev: Diff.Aux[P, _1, L]
  ): Map[Position[slice.S], C[slice.R]] = Map(
    slice.selected(cell.position) -> compact(slice)(slice.remainder(cell.position), cell.content)
  )

  /**
   * Combine two compacted cells.
   *
   * @param x The left map to combine.
   * @param y The right map to combine.
   *
   * @return The combined map.
   */
  def combineMaps(
    slice: Slice[L, P]
  )(
    x: Map[Position[slice.S], C[slice.R]],
    y: Map[Position[slice.S], C[slice.R]]
  ): Map[Position[slice.S], C[slice.R]] = x ++ y.map { case (k, v) => k -> combine(slice)(x.get(k), v) }

  protected def compact(slice: Slice[L, P])(rem: Position[slice.R], con: Content): C[slice.R]
  protected def combine(slice: Slice[L, P])(x: Option[C[slice.R]], y: C[slice.R]): C[slice.R]
}

/** Companion object to the `Compactable` trait. */
object Compactable {
  /** A `Compactable[_0, _1]`. */
  implicit val compactable1D = new Compactable[_0, _1] {
    type C[R <: Nat] = Content

    protected def compact(slice: Slice[_0, _1])(rem: Position[slice.R], con: Content): C[slice.R] = con
    protected def combine(slice: Slice[_0, _1])(x: Option[C[slice.R]], y: C[slice.R]): C[slice.R] = y
  }

  /** A `Compactable[_1, _2]`. */
  implicit val compactable2D = CompactableXD[_1, _2]()

  /** A `Compactable[_2, _3]`. */
  implicit val compactable3D = CompactableXD[_2, _3]()

  /** A `Compactable[_3, _4]`. */
  implicit val compactable4D = CompactableXD[_3, _4]()

  /** A `Compactable[_4, _5]`. */
  implicit val compactable5D = CompactableXD[_4, _5]()

  /** A `Compactable[_5, _6]`. */
  implicit val compactable6D = CompactableXD[_5, _6]()

  /** A `Compactable[_6, _7]`. */
  implicit val compactable7D = CompactableXD[_6, _7]()

  /** A `Compactable[_7, _8]`. */
  implicit val compactable8D = CompactableXD[_7, _8]()

  /** A `Compactable[_8, _9]`. */
  implicit val compactable9D = CompactableXD[_8, _9]()
}

/** Implementation of the `Compactable` trait for positions with 2 or more dimensions. */
case class CompactableXD[
  L <: Nat,
  P <: Nat
](
)(implicit
  ev1: GT[P, _1],
  ev2: Diff.Aux[P, _1, L]
) extends Compactable[L, P] {
  type C[R <: Nat] = Map[Position[R], Content]

  protected def compact(slice: Slice[L, P])(rem: Position[slice.R], con: Content): C[slice.R] = Map(rem -> con)
  protected def combine(slice: Slice[L, P])(x: Option[C[slice.R]], y: C[slice.R]): C[slice.R] = x
    .map(_ ++ y)
    .getOrElse(y)
}

