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

package commbank.grimlock.library.squash

import commbank.grimlock.framework.Cell
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.encoding.Value
import commbank.grimlock.framework.position.Position
import commbank.grimlock.framework.squash.Squasher

import  scala.reflect.classTag

import shapeless.{ HList, Nat }

private[squash] object PreservingPosition {
  type T = (Value[_], Content)

  val tTag = classTag[T]

  def prepare[
    P <: HList,
    D <: Nat
  ](
    cell: Cell[P],
    dim: D
  )(implicit
    ev: Position.IndexConstraints[P, D]
  ): Option[T] = Option((cell.position(dim), cell.content))

  def reduce(maximum: Boolean)(lt: T, rt: T): T = {
    val (min, max) = if (lt._1.gtr(rt._1)) (rt, lt) else (lt, rt)

    if (maximum) max else min
  }

  def present(t: T): Option[Content] = Option(t._2)
}

/** Reduce two cells preserving the cell with maximal value for the coordinate of the dimension being squashed. */
case class PreservingMaximumPosition[P <: HList]() extends Squasher[P] {
  type T = PreservingPosition.T

  val tTag = PreservingPosition.tTag

  def prepare[
    D <: Nat
  ](
    cell: Cell[P],
    dim: D
  )(implicit
    ev: Position.IndexConstraints[P, D]
  ): Option[T] = PreservingPosition.prepare(cell, dim)

  def reduce(lt: T, rt: T): T = PreservingPosition.reduce(true)(lt, rt)

  def present(t: T): Option[Content] = PreservingPosition.present(t)
}

/** Reduce two cells preserving the cell with minimal value for the coordinate of the dimension being squashed. */
case class PreservingMinimumPosition[P <: HList]() extends Squasher[P] {
  type T = PreservingPosition.T

  val tTag = PreservingPosition.tTag

  def prepare[
    D <: Nat
  ](
    cell: Cell[P],
    dim: D
  )(implicit
    ev: Position.IndexConstraints[P, D]
  ): Option[T] = PreservingPosition.prepare(cell, dim)

  def reduce(lt: T, rt: T): T = PreservingPosition.reduce(false)(lt, rt)

  def present(t: T): Option[Content] = PreservingPosition.present(t)
}

/** Reduce two cells preserving the cell whose coordinate matches `keep`. */
case class KeepSlice[P <: HList](keep: Value[_]) extends Squasher[P] { // TODO: How to ensure type is same as at D?
  type T = Content

  val tTag = classTag[T]

  def prepare[
    D <: Nat
  ](
    cell: Cell[P],
    dim: D
  )(implicit
    ev: Position.IndexConstraints[P, D]
  ): Option[T] = if (cell.position(dim) equ keep) Option(cell.content) else None

  def reduce(lt: T, rt: T): T = lt

  def present(t: T): Option[Content] = Option(t)
}

