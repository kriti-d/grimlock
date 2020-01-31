// Copyright 2019,2020 Commonwealth Bank of Australia
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

package commbank.grimlock.scala.distribution

import commbank.grimlock.framework.{ Cell, Locate, Matrix => FwMatrix }
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.distribution.{
  ApproximateDistribution => FwApproximateDistribution,
  CountMap,
  Quantiles,
  QuantileImpl,
  StreamingHistogram,
  TDigest
}
import commbank.grimlock.framework.encoding.Value
import commbank.grimlock.framework.environment.tuner.{ Default, Tuner }
import commbank.grimlock.framework.metadata.{ CategoricalType, DiscreteSchema, NumericType }
import commbank.grimlock.framework.position.{ Position, Slice }

import commbank.grimlock.scala.environment.Context
import commbank.grimlock.scala.environment.tuner.ScalaImplicits._
import commbank.grimlock.scala.Persist

import shapeless.HList

/** Trait for computing approximate distributions from a matrix. */
trait ApproximateDistribution[
  P <: HList
] extends FwApproximateDistribution[P, Context]
  with Persist[Cell[P]] { self: FwMatrix[P, Context] =>
  def histogram[
    S <: HList,
    R <: HList,
    Q <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(
    name: Locate.FromPositionWithValue[S, Content, Q],
    filter: Boolean
  )(implicit
    ev1: Value.Box[Long],
    ev2: Position.GreaterThanConstraints[Q, S],
    ev3: FwApproximateDistribution.HistogramTuner[Context.U, T]
  ): Context.U[Cell[Q]] = data
    .filter { case c => (!filter || c.content.classification.isOfType(CategoricalType)) }
    .flatMap { case c => name(slice.selected(c.position), c.content) }
    .tunedSize(tuner)
    .map { case (p, s) => Cell(p, Content(DiscreteSchema[Long](), s)) }

  def quantiles[
    S <: HList,
    R <: HList,
    Q <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(
    probs: List[Double],
    quantiser: Quantiles.Quantiser,
    name: Locate.FromPositionWithValue[S, Double, Q],
    filter: Boolean,
    nan: Boolean
  )(implicit
    ev1: Value.Box[Double],
    ev2: Position.NonEmptyConstraints[R],
    ev3: Position.GreaterThanConstraints[Q, S],
    ev4: FwApproximateDistribution.QuantilesTuner[Context.U, T]
  ): Context.U[Cell[Q]] = {
    val qnt = QuantileImpl[P, S, Q](probs, quantiser, name, nan)

    val prep = data
      .collect { case c if (!filter || c.content.classification.isOfType(NumericType)) =>
        (slice.selected(c.position), qnt.prepare(c))
      }

    prep
      .tunedJoin(tuner, prep.map { case (sel, _) => sel }.tunedSize(tuner))
      .map { case (s, (q, c)) => ((s, c), q) }
      .sorted
      .tunedStream(tuner, (key, itr) => QuantileImpl.stream(qnt, key, itr).toIterator)
      .map { case (_, c) => c }
  }

  def countMapQuantiles[
    S <: HList,
    R <: HList,
    Q <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(
    probs: List[Double],
    quantiser: Quantiles.Quantiser,
    name: Locate.FromPositionWithValue[S, Double, Q],
    filter: Boolean,
    nan: Boolean
  )(implicit
    ev1: Value.Box[Double],
    ev2: Position.NonEmptyConstraints[R],
    ev3: Position.GreaterThanConstraints[Q, S],
    ev4: FwApproximateDistribution.CountMapQuantilesTuner[Context.U, T]
  ): Context.U[Cell[Q]] = data
    .flatMap { case c =>
      if (!filter || c.content.classification.isOfType(NumericType))
        Option((slice.selected(c.position), CountMap.from(c.content.value.as[Double].getOrElse(Double.NaN))))
      else
        None
    }
    .tunedReduce(tuner, CountMap.reduce)
    .flatMap { case (pos, t) => CountMap.toCells(t, probs, pos, quantiser, name, nan) }

  def tDigestQuantiles[
    S <: HList,
    R <: HList,
    Q <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(
    probs: List[Double],
    compression: Double,
    name: Locate.FromPositionWithValue[S, Double, Q],
    filter: Boolean,
    nan: Boolean
  )(implicit
    ev1: Value.Box[Double],
    ev2: Position.NonEmptyConstraints[R],
    ev3: Position.GreaterThanConstraints[Q, S],
    ev4: FwApproximateDistribution.TDigestQuantilesTuner[Context.U, T]
  ): Context.U[Cell[Q]] = data
    .flatMap { case c =>
      if (!filter || c.content.classification.isOfType(NumericType))
        c.content.value.as[Double].flatMap { case d =>
          TDigest.from(d, compression).map { case td => (slice.selected(c.position), td) }
        }
      else
        None
    }
    .tunedReduce(tuner, TDigest.reduce)
    .flatMap { case (pos, t) => TDigest.toCells(t, probs, pos, name, nan) }

  def uniformQuantiles[
    S <: HList,
    R <: HList,
    Q <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(
    count: Long,
    name: Locate.FromPositionWithValue[S, Double, Q],
    filter: Boolean,
    nan: Boolean
  )(implicit
    ev1: Value.Box[Double],
    ev2: Position.NonEmptyConstraints[R],
    ev3: Position.GreaterThanConstraints[Q, S],
    ev4: FwApproximateDistribution.UniformQuantilesTuner[Context.U, T]
  ): Context.U[Cell[Q]] = data
    .flatMap { case c =>
      if (!filter || c.content.classification.isOfType(NumericType))
        c.content.value.as[Double].map { case d => (slice.selected(c.position), StreamingHistogram.from(d, count)) }
      else
        None
    }
    .tunedReduce(tuner, StreamingHistogram.reduce)
    .flatMap { case (pos, t) => StreamingHistogram.toCells(t, count, pos, name, nan) }
}

