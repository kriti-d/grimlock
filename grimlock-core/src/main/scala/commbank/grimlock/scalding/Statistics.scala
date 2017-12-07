// Copyright 2016,2017 Commonwealth Bank of Australia
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

package commbank.grimlock.scalding.statistics

import commbank.grimlock.framework.{ Cell, Matrix => FwMatrix }
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.encoding.Value
import commbank.grimlock.framework.environment.tuner.{ Default, Tuner }
import commbank.grimlock.framework.metadata.{ ContinuousSchema, DiscreteSchema, NumericType }
import commbank.grimlock.framework.position.Slice
import commbank.grimlock.framework.statistics.{ Statistics => FwStatistics }

import commbank.grimlock.scalding.environment.Context
import commbank.grimlock.scalding.environment.tuner.ScaldingImplicits._
import commbank.grimlock.scalding.Persist

import com.twitter.algebird.{ Aggregator, Moments, Monoid }

import shapeless.HList

/** Trait for computing common statistics from a matrix. */
trait Statistics[P <: HList] extends FwStatistics[P, Context] with Persist[Cell[P]] { self: FwMatrix[P, Context] =>
  def counts[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(implicit
    ev1: Value.Box[Long],
    ev2: FwStatistics.CountsTuner[Context.U, T]
  ): Context.U[Cell[S]] = data
    .map { case c => slice.selected(c.position) }
    .tunedSize(tuner)
    .map { case (p, c) => Cell(p, Content(DiscreteSchema[Long](), c)) }

  def distinctCounts[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(implicit
    ev1: Value.Box[Long],
    ev2: FwStatistics.DistinctCountsTuner[Context.U, T]
  ): Context.U[Cell[S]] = data
    .map { case c => (slice.selected(c.position), c.content.value.toShortString) }
    .tunedSize(tuner)
    .map { case ((p, v), c) => p }
    .tunedSize(tuner)
    .map { case (p, c) => Cell(p, Content(DiscreteSchema[Long](), c)) }

  def predicateCounts[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(
    predicate: (Content) => Boolean
  )(implicit
    ev1: Value.Box[Long],
    ev2: FwStatistics.PredicateCountsTuner[Context.U, T]
  ): Context.U[Cell[S]] = data
    .collect { case c if (predicate(c.content)) => slice.selected(c.position) }
    .tunedSize(tuner)
    .map { case (p, c) => Cell(p, Content(DiscreteSchema[Long](), c)) }

  def mean[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(implicit
    ev1: Value.Box[Double],
    ev2: FwStatistics.MeanTuner[Context.U, T]
  ): Context.U[Cell[S]] = moments(slice, tuner, m => FwStatistics.mean(m))

  def standardDeviation[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(
    biased: Boolean
  )(implicit
    ev1: Value.Box[Double],
    ev2: FwStatistics.StandardDeviationTuner[Context.U, T]
  ): Context.U[Cell[S]] = moments(slice, tuner, m => FwStatistics.sd(m, biased))

  def skewness[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(implicit
    ev1: Value.Box[Double],
    ev2: FwStatistics.SkewnessTuner[Context.U, T]
  ): Context.U[Cell[S]] = moments(slice, tuner, m => FwStatistics.skewness(m))

  def kurtosis[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(
    excess: Boolean
  )(implicit
    ev1: Value.Box[Double],
    ev2: FwStatistics.KurtosisTuner[Context.U, T]
  ): Context.U[Cell[S]] = moments(slice, tuner, m => FwStatistics.kurtosis(m, excess))

  def minimum[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(implicit
    ev1: Value.Box[Double],
    ev2: FwStatistics.MinimumTuner[Context.U, T]
  ): Context.U[Cell[S]] = range(slice, tuner, Aggregator.min)

  def maximum[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(implicit
    ev1: Value.Box[Double],
    ev2: FwStatistics.MaximumTuner[Context.U, T]
  ): Context.U[Cell[S]] = range(slice, tuner, Aggregator.max)

  def maximumAbsolute[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(implicit
    ev1: Value.Box[Double],
    ev2: FwStatistics.MaximumAbsoluteTuner[Context.U, T]
  ): Context.U[Cell[S]] = range(slice, tuner, Aggregator.max[Double].composePrepare(d => math.abs(d)))

  def sums[
    S <: HList,
    R <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(implicit
    ev1: Value.Box[Double],
    ev2: FwStatistics.SumsTuner[Context.U, T]
  ): Context.U[Cell[S]] = data
    .collect { case c if (c.content.classification.isOfType(NumericType)) =>
      (slice.selected(c.position), c.content.value)
    }
    .flatMap { case (p, c) => c.as[Double].map { case d => (p, d) } }
    .tunedReduce(tuner, _ + _)
    .map { case (p, s) => Cell(p, Content(ContinuousSchema[Double](), s)) }

  private def moments[
    S <: HList,
    R <: HList
  ](
    slice: Slice[P, S, R],
    tuner: Tuner,
    extract: (Moments) => Double
  )(implicit
    ev: Value.Box[Double]
  ): Context.U[Cell[S]] = data
    .collect { case c if (c.content.classification.isOfType(NumericType)) =>
      (slice.selected(c.position), c.content.value)
    }
    .flatMap { case (p, c) => c.as[Double].map { case d => (p, Moments(d)) } }
    .tunedReduce(tuner, (lt, rt) => Monoid.plus(lt, rt))
    .map { case (p, m) => Cell(p, Content(ContinuousSchema[Double](), extract(m))) }

  private def range[
    S <: HList,
    R <: HList
  ](
    slice: Slice[P, S, R],
    tuner: Tuner,
    agg: Aggregator[Double, Double, Double]
  )(implicit
    ev: Value.Box[Double]
  ): Context.U[Cell[S]] = data
    .collect { case c if (c.content.classification.isOfType(NumericType)) =>
      (slice.selected(c.position), c.content.value)
    }
    .flatMap { case (p, c) => c.as[Double].map { case d => (p, agg.prepare(d)) } }
    .tunedReduce(tuner, (lt, rt) => agg.reduce(lt, rt))
    .map { case (p, t) => Cell(p, Content(ContinuousSchema[Double](), agg.present(t))) }
}

