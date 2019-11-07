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

package commbank.grimlock.library.window

import commbank.grimlock.framework.{ Cell, Locate }
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.encoding.Value
import commbank.grimlock.framework.metadata.ContinuousSchema
import commbank.grimlock.framework.position.Position
import commbank.grimlock.framework.window.Window

import shapeless.HList

private[window] object MovingAverage {
  type I = Double
  type O[R <: HList] = (Position[R], Double)

  def prepare[P <: HList](cell: Cell[P]): I = cell.content.value.as[Double].getOrElse(Double.NaN)

  def present[
    S <: HList,
    R <: HList,
    Q <: HList
  ](
    position: Locate.FromSelectedAndRemainder[S, R, Q]
  )(
    pos: Position[S],
    out: O[R]
  )(implicit
    ev: Value.Box[Double]
  ): TraversableOnce[Cell[Q]] = position(pos, out._1).map(Cell(_, Content(ContinuousSchema[Double](), out._2)))
}

private[window] object BatchMovingAverage {
  type T[R <: HList] = List[(Position[R], Double)]

  def initialise[
    R <: HList
  ](
    all: Boolean
  )(
    rem: Position[R],
    in: MovingAverage.I
  ): (T[R], TraversableOnce[MovingAverage.O[R]]) = (List((rem, in)), if (all) List((rem, in)) else List())

  def update[
    R <: HList
  ](
    window: Int,
    all: Boolean,
    idx: Int,
    compute: (T[R]) => Double
  )(
    rem: Position[R],
    in: MovingAverage.I,
    t: T[R]
  ): (T[R], TraversableOnce[MovingAverage.O[R]]) = {
    val lst = (if (t.size == window) t.tail else t) :+ ((rem, in))
    val out = if (all || lst.size == window) List((lst(math.min(idx, lst.size - 1))._1, compute(lst))) else List()

    (lst, out)
  }
}

private[window] object OnlineMovingAverage {
  type T = (Double, Long)

  def initialise[
    R <: HList
  ](
    rem: Position[R],
    in: MovingAverage.I
  ): (T, TraversableOnce[MovingAverage.O[R]]) = ((in, 1), List((rem, in)))

  def update[
    R <: HList
  ](
    compute: (Double, T) => Double
  )(
    rem: Position[R],
    in: MovingAverage.I,
    t: T
  ): (T, TraversableOnce[MovingAverage.O[R]]) = {
    val curr = compute(in, t)

    ((curr, t._2 + 1), List((rem, curr)))
  }
}

/**
 * Compute simple moving average over last `window` values.
 *
 * @param window   Size of the window.
 * @param position Function to extract result position.
 * @param all      Indicates if averages should be output when a full window isn't available yet.
 */
case class SimpleMovingAverage[
  P <: HList,
  S <: HList,
  R <: HList,
  Q <: HList
](
  window: Int,
  position: Locate.FromSelectedAndRemainder[S, R, Q],
  all: Boolean = false
)(implicit
  ev: Value.Box[Double]
) extends Window[P, S, R, Q] {
  type I = MovingAverage.I
  type T = BatchMovingAverage.T[R]
  type O = MovingAverage.O[R]

  def prepare(cell: Cell[P]): I = MovingAverage.prepare(cell)

  def initialise(rem: Position[R], in: I): (T, TraversableOnce[O]) = BatchMovingAverage.initialise(all)(rem, in)

  def update(
    rem: Position[R],
    in: I,
    t: T
  ): (T, TraversableOnce[O]) = BatchMovingAverage.update(window, all, window - 1, compute)(rem, in, t)

  def present(pos: Position[S], out: O): TraversableOnce[Cell[Q]] = MovingAverage.present(position)(pos, out)

  private def compute(lst: T): Double = lst.foldLeft(0.0)((c, p) => p._2 + c) / lst.size
}

/**
 * Compute centered moving average over last `2 * width + 1` values.
 *
 * @param width    Width of bands around centered value.
 * @param position Function to extract result position.
 */
case class CenteredMovingAverage[
  P <: HList,
  S <: HList,
  R <: HList,
  Q <: HList
](
  width: Int,
  position: Locate.FromSelectedAndRemainder[S, R, Q]
)(implicit
  ev: Value.Box[Double]
) extends Window[P, S, R, Q] {
  type I = MovingAverage.I
  type T = BatchMovingAverage.T[R]
  type O = MovingAverage.O[R]

  def prepare(cell: Cell[P]): I = MovingAverage.prepare(cell)

  def initialise(rem: Position[R], in: I): (T, TraversableOnce[O]) = BatchMovingAverage.initialise(false)(rem, in)

  def update(
    rem: Position[R],
    in: I,
    t: T
  ): (T, TraversableOnce[O]) = BatchMovingAverage.update(2 * width + 1, false, width, compute)(rem, in, t)

  def present(pos: Position[S], out: O): TraversableOnce[Cell[Q]] = MovingAverage.present(position)(pos, out)

  private def compute(lst: T): Double = lst.foldLeft(0.0)((c, p) => p._2 + c) / lst.size
}

/**
 * Compute weighted moving average over last `window` values.
 *
 * @param window   Size of the window.
 * @param position Function to extract result position.
 * @param all      Indicates if averages should be output when a full window isn't available yet.
 */
case class WeightedMovingAverage[
  P <: HList,
  S <: HList,
  R <: HList,
  Q <: HList
](
  window: Int,
  position: Locate.FromSelectedAndRemainder[S, R, Q],
  all: Boolean = false
)(implicit
  ev: Value.Box[Double]
) extends Window[P, S, R, Q] {
  type I = MovingAverage.I
  type T = BatchMovingAverage.T[R]
  type O = MovingAverage.O[R]

  def prepare(cell: Cell[P]): I = MovingAverage.prepare(cell)

  def initialise(rem: Position[R], in: I): (T, TraversableOnce[O]) = BatchMovingAverage.initialise(all)(rem, in)

  def update(
    rem: Position[R],
    in: I,
    t: T
  ): (T, TraversableOnce[O]) = BatchMovingAverage.update(window, all, window - 1, compute)(rem, in, t)

  def present(pos: Position[S], out: O): TraversableOnce[Cell[Q]] = MovingAverage.present(position)(pos, out)

  private def compute(lst: T): Double = {
    val curr = lst.zipWithIndex.foldLeft((0.0, 0))((c, p) => ((p._2 + 1) * p._1._2 + c._1, c._2 + p._2 + 1))

    curr._1 / curr._2
  }
}

/**
 * Compute cumulatve moving average.
 *
 * @param position Function to extract result position.
 */
case class CumulativeMovingAverage[
  P <: HList,
  S <: HList,
  R <: HList,
  Q <: HList
](
  position: Locate.FromSelectedAndRemainder[S, R, Q]
)(implicit
  ev: Value.Box[Double]
) extends Window[P, S, R, Q] {
  type I = MovingAverage.I
  type T = OnlineMovingAverage.T
  type O = MovingAverage.O[R]

  def prepare(cell: Cell[P]): I = MovingAverage.prepare(cell)
  def initialise(rem: Position[R], in: I): (T, TraversableOnce[O]) = OnlineMovingAverage.initialise(rem, in)
  def update(rem: Position[R], in: I, t: T): (T, TraversableOnce[O]) = OnlineMovingAverage.update(compute)(rem, in, t)
  def present(pos: Position[S], out: O): TraversableOnce[Cell[Q]] = MovingAverage.present(position)(pos, out)

  private def compute(curr: Double, t: T): Double = (curr + t._2 * t._1) / (t._2 + 1)
}

/**
 * Compute exponential moving average.
 *
 * @param alpha    Degree of weighting.
 * @param position Function to extract result position.
 */
case class ExponentialMovingAverage[
  P <: HList,
  S <: HList,
  R <: HList,
  Q <: HList
](
  alpha: Double,
  position: Locate.FromSelectedAndRemainder[S, R, Q]
)(implicit
  ev: Value.Box[Double]
) extends Window[P, S, R, Q] {
  type I = MovingAverage.I
  type T = OnlineMovingAverage.T
  type O = MovingAverage.O[R]

  def prepare(cell: Cell[P]): I = MovingAverage.prepare(cell)
  def initialise(rem: Position[R], in: I): (T, TraversableOnce[O]) = OnlineMovingAverage.initialise(rem, in)
  def update(rem: Position[R], in: I, t: T): (T, TraversableOnce[O]) = OnlineMovingAverage.update(compute)(rem, in, t)
  def present(pos: Position[S], out: O): TraversableOnce[Cell[Q]] = MovingAverage.present(position)(pos, out)

  private def compute(curr: Double, t: T): Double = alpha * curr + (1 - alpha) * t._1
}

/**
 * Compute cumulative sum.
 *
 * @param position Function to extract result position.
 * @param strict   Indicates is non-numeric values should result in NaN.
 */
case class CumulativeSums[
  P <: HList,
  S <: HList,
  R <: HList,
  Q <: HList
](
  position: Locate.FromSelectedAndRemainder[S, R, Q],
  strict: Boolean = true
)(implicit
  ev: Value.Box[Double]
) extends Window[P, S, R, Q] {
  type I = Option[Double]
  type T = Option[Double]
  type O = (Position[R], Double)

  val schema = ContinuousSchema[Double]()

  def prepare(cell: Cell[P]): I = (strict, cell.content.value.as[Double]) match {
    case (true, None) => Option(Double.NaN)
    case (_, v) => v
  }

  def initialise(rem: Position[R], in: I): (T, TraversableOnce[O]) = (in, in.map(d => (rem, d)))

  def update(rem: Position[R], in: I, t: T): (T, TraversableOnce[O]) = (strict, t, in) match {
    case (true, _, None) => (Option(Double.NaN), List((rem, Double.NaN)))
    case (false, p, None) => (p, List())
    case (_, None, Some(d)) => (Option(d), List((rem, d)))
    case (_, Some(p), Some(d)) => (Option(p + d), List((rem, p + d)))
  }

  def present(pos: Position[S], out: O): TraversableOnce[Cell[Q]] = position(pos, out._1)
    .map(Cell(_, Content(schema, out._2)))
}

/**
 * Compute sliding binary operator on sequential numeric cells.
 *
 * @param binop    The binary operator to apply to two sequential numeric cells.
 * @param position Function to extract result position.
 * @param strict   Indicates is non-numeric values should result in NaN.
 */
case class BinaryOperator[
  P <: HList,
  S <: HList,
  R <: HList,
  Q <: HList
](
  binop: (Double, Double) => Double,
  position: Locate.FromSelectedAndPairwiseRemainder[S, R, Q],
  strict: Boolean = true
)(implicit
  ev: Value.Box[Double]
) extends Window[P, S, R, Q] {
  type I = Option[Double]
  type T = (Option[Double], Position[R])
  type O = (Double, Position[R], Position[R])

  def prepare(cell: Cell[P]): I = (strict, cell.content.value.as[Double]) match {
    case (true, None) => Option(Double.NaN)
    case (_, v) => v
  }

  def initialise(rem: Position[R], in: I): (T, TraversableOnce[O]) = ((in, rem), List())

  def update(rem: Position[R], in: I, t: T): (T, TraversableOnce[O]) = (strict, t, in) match {
    case (true, (_, c), None) => getResult(rem, Double.NaN, Double.NaN, c)
    case (false, p, None) => (p, List())
    case (_, (None, _), Some(d)) => ((Option(d), rem), List())
    case (_, (Some(p), c), Some(d)) => getResult(rem, if (p.isNaN) p else d, binop(p, d), c)
  }

  def present(pos: Position[S], out: O): TraversableOnce[Cell[Q]] = position(pos, out._3, out._2)
    .map(Cell(_, Content(ContinuousSchema[Double](), out._1)))

  private def getResult(
    rem: Position[R],
    value: Double,
    result: Double,
    prev: Position[R]
  ): (T, TraversableOnce[O]) = ((Option(value), rem), List((result, prev, rem)))
}

