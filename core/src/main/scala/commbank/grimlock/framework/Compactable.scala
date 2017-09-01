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
import shapeless.nat._1
import shapeless.ops.nat.GT

/** Trait for compacting a cell to a `Map`. */
trait Compactable[P <: Nat, V[_ <: Nat]] extends java.io.Serializable {
  /**
   * Convert a single cell to a `Map`.
   *
   * @param slice Encapsulates the dimension(s) to compact.
   * @param cell  The cell to compact.
   *
   * @return A `Map` with the compacted cell.
   */
  def toMap(slice: Slice[P], cell: Cell[P]): Map[Position[slice.S], V[slice.R]] = Map(
    slice.selected(cell.position) -> compact(slice.remainder(cell.position), cell.content)
  )

  /**
   * Combine two compacted cells.
   *
   * @param x The left map to combine.
   * @param y The right map to combine.
   *
   * @return The combined map.
   */
  def combineMaps[
    S <: Nat,
    R <: Nat
  ](
    x: Map[Position[S], V[R]],
    y: Map[Position[S], V[R]]
  ): Map[Position[S], V[R]] = x ++ y.map { case (k, v) => k -> combine(x.get(k), v) }

  protected def compact[R <: Nat](rem: Position[R], con: Content): V[R]
  protected def combine[R <: Nat](x: Option[V[R]], y: V[R]): V[R]
}

/** Companion object to the `Compactable` trait. */
object Compactable {
  /** Compacted content for `Position1D`. */
  type V1[R <: Nat] = Content

  /** Compacted content for `Position[P]` with `P` greater than `_1`. */
  type VX[R <: Nat] = Map[Position[R], Content]

  /** A `Compactable[_1, V1]` for `Position1D`. */
  implicit val compactable1D: Compactable[_1, V1] = new Compactable[_1, V1] {
    protected def compact[R <: Nat](rem: Position[R], con: V1[R]): V1[R] = con
    protected def combine[R <: Nat](x: Option[V1[R]], y: V1[R]): V1[R] = y
  }

  /** A `Compactable[P, VX]` for positions `P` greater than `_1`. */
  implicit def compactableXD[P <: Nat](implicit ev: GT[P, _1]): Compactable[P, VX] = new Compactable[P, VX] {
    protected def compact[R <: Nat](rem: Position[R], con: Content): VX[R] = Map(rem -> con)
    protected def combine[R <: Nat](x: Option[VX[R]], y: VX[R]): VX[R] = x.map(_ ++ y).getOrElse(y)
  }
}

