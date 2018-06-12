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

package commbank.grimlock.library.pairwise

import commbank.grimlock.framework.{ Cell, Locate }
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.encoding.Value
import commbank.grimlock.framework.metadata.{ ContinuousSchema, NominalSchema }
import commbank.grimlock.framework.pairwise.Operator

import shapeless.HList

private[pairwise] object DoubleOperator {
  def compute[
    P <: HList,
    Q <: HList
  ](
    left: Cell[P],
    right: Cell[P],
    pos: Locate.FromPairwiseCells[P, Q],
    inverse: Boolean,
    compute: (Double, Double) => Double
  )(implicit
    ev: Value.Box[Double]
  ): TraversableOnce[Cell[Q]] = for {
    p <- pos(left, right)
    l <- left.content.value.as[Double]
    r <- right.content.value.as[Double]
  } yield Cell(p, Content(ContinuousSchema[Double](), if (inverse) compute(r, l) else compute(l, r)))
}

/**
 * Add two values.
 *
 * @param pos Function to extract result position.
 */
case class Plus[
  P <: HList,
  Q <: HList
](
  pos: Locate.FromPairwiseCells[P, Q]
)(implicit
  ev: Value.Box[Double]
) extends Operator[P, Q] {
  def compute(
    left: Cell[P],
    right: Cell[P]
  ): TraversableOnce[Cell[Q]] = DoubleOperator.compute(left, right, pos, false, (l, r) => l + r)
}

/**
 * Subtract two values.
 *
 * @param pos     Function to extract result position.
 * @param inverse Indicator if pairwise operator `f()` should be called as `f(l, r)` or as `f(r, l)`.
 */
case class Minus[
  P <: HList,
  Q <: HList
](
  pos: Locate.FromPairwiseCells[P, Q],
  inverse: Boolean = false
)(implicit
  ev: Value.Box[Double]
) extends Operator[P, Q] {
  def compute(
    left: Cell[P],
    right: Cell[P]
  ): TraversableOnce[Cell[Q]] = DoubleOperator.compute(left, right, pos, inverse, (l, r) => l - r)
}

/**
 * Multiply two values.
 *
 * @param pos     Function to extract result position.
 */
case class Times[
  P <: HList,
  Q <: HList
](
  pos: Locate.FromPairwiseCells[P, Q]
)(implicit
  ev: Value.Box[Double]
) extends Operator[P, Q] {
  def compute(
    left: Cell[P],
    right: Cell[P]
  ): TraversableOnce[Cell[Q]] = DoubleOperator.compute(left, right, pos, false, (l, r) => l * r)
}

/**
 * Divide two values.
 *
 * @param pos     Function to extract result position.
 * @param inverse Indicator if pairwise operator `f()` should be called as `f(l, r)` or as `f(r, l)`.
 */
case class Divide[
  P <: HList,
  Q <: HList
](
  pos: Locate.FromPairwiseCells[P, Q],
  inverse: Boolean = false
)(implicit
  ev: Value.Box[Double]
) extends Operator[P, Q] {
  def compute(
    left: Cell[P],
    right: Cell[P]
  ): TraversableOnce[Cell[Q]] = DoubleOperator.compute(left, right, pos, inverse, (l, r) => l / r)
}

/**
 * Concatenate two cells.
 *
 * @param pos   Function to extract result position.
 * @param value Pattern for the new (string) value of the pairwise contents. Use `%[12]$``s` for the string
 *              representations of the content.
 */
case class Concatenate[
  P <: HList,
  Q <: HList
](
  pos: Locate.FromPairwiseCells[P, Q],
  value: String = "%1$s,%2$s"
)(implicit
  ev: Value.Box[String]
) extends Operator[P, Q] {
  def compute(left: Cell[P], right: Cell[P]): TraversableOnce[Cell[Q]] = pos(left, right)
    .map { case p =>
      Cell(
        p,
        Content(
          NominalSchema[String](),
          value.format(left.content.value.toShortString, right.content.value.toShortString)
        )
      )
    }
}

