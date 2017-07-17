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

import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.position.{ Position, Slice }

import shapeless.Nat
import shapeless.nat.{ _0, _1 }
import shapeless.ops.nat.{ Diff, GT }

/** Trait for compacting a cell to a `Map`. */
trait Compactable[L <: Nat, P <: Nat, V[_ <: Nat]] extends java.io.Serializable {
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
  ): Map[Position[slice.S], V[slice.R]] = Map(
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
    x: Map[Position[slice.S], V[slice.R]],
    y: Map[Position[slice.S], V[slice.R]]
  ): Map[Position[slice.S], V[slice.R]] = x ++ y.map { case (k, v) => k -> combine(slice)(x.get(k), v) }

  protected def compact(slice: Slice[L, P])(rem: Position[slice.R], con: Content): V[slice.R]
  protected def combine(slice: Slice[L, P])(x: Option[V[slice.R]], y: V[slice.R]): V[slice.R]
}

/** Companion object to the `Compactable` trait. */
object Compactable {
  /** Compacted content for `Position1D`. */
  type V1[R <: Nat] = Content

  /** Compacted content for `Position[P]` with `P` greater than `_1`. */
  type VX[R <: Nat] = Map[Position[R], Content]

  /** A `Compactable[_0, _1]` for `Position1D`. */
  implicit val compactable1D: Compactable[_0, _1, Compactable.V1] = new Compactable[_0, _1, Compactable.V1] {
    protected def compact(slice: Slice[_0, _1])(rem: Position[slice.R], con: Content): Content = con
    protected def combine(slice: Slice[_0, _1])(x: Option[Content], y: Content): Content = y
  }

  /** A `Compactable[L, P]` for positions `P` greater than `_1`. */
  implicit def compactableXD[
    L <: Nat,
    P <: Nat
  ](implicit
    ev1: GT[P, _1],
    ev2: Diff.Aux[P, _1, L]
  ): Compactable[L, P, Compactable.VX] = new Compactable[L, P, Compactable.VX] {
    protected def compact(
      slice: Slice[L, P]
    )(
      rem: Position[slice.R],
      con: Content
    ): Map[Position[slice.R], Content] = Map(rem -> con)
    protected def combine(
      slice: Slice[L, P]
    )(
      x: Option[Map[Position[slice.R], Content]],
      y: Map[Position[slice.R], Content]
    ): Map[Position[slice.R], Content] = x.map(_ ++ y).getOrElse(y)
  }
}

