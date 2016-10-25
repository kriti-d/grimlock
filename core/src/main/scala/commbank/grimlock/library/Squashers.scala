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

package commbank.grimlock.library.squash

import commbank.grimlock.framework._
import commbank.grimlock.framework.content._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.squash._

import  scala.reflect.classTag

import shapeless.Nat
import shapeless.ops.nat.{ LTEq, ToInt }

private[squash] trait PreservingPosition[P <: Nat] extends Squasher[P] {
  type T = (Value, Content)

  val tTag = classTag[T]

  def prepare[D <: Nat : ToInt](cell: Cell[P], dim: D)(implicit ev: LTEq[D, P]): T = (cell.position(dim), cell.content)
  def present(t: T): Option[Content] = Option(t._2)
}

/** Reduce two cells preserving the cell with maximal value for the coordinate of the dimension being squashed. */
case class PreservingMaxPosition[P <: Nat]() extends PreservingPosition[P] {
  def reduce(lt: T, rt: T): T = if (Value.ordering.compare(lt._1, rt._1) > 0) lt else rt
}

/** Reduce two cells preserving the cell with minimal value for the coordinate of the dimension being squashed. */
case class PreservingMinPosition[P <: Nat]() extends PreservingPosition[P] {
  def reduce(lt: T, rt: T): T = if (Value.ordering.compare(lt._1, rt._1) < 0) lt else rt
}

/** Reduce two cells preserving the cell whose coordinate matches `keep`. */
case class KeepSlice[P <: Nat](keep: Value) extends Squasher[P] {
  type T = Option[Content]

  val tTag = classTag[T]

  def prepare[D <: Nat : ToInt](cell: Cell[P], dim: D)(implicit ev: LTEq[D, P]): T =
    if (cell.position(dim) equ keep) Option(cell.content) else None
  def reduce(lt: T, rt: T): T = if (lt.isEmpty) rt else lt
  def present(t: T): Option[Content] = t
}

