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

package commbank.grimlock.spark.statistics

import commbank.grimlock.framework.{ Cell, Matrix => FwMatrix }
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.environment.tuner.{ Default, Tuner }
import commbank.grimlock.framework.metadata.{ ContinuousSchema, DiscreteSchema, NumericType }
import commbank.grimlock.framework.position.Slice
import commbank.grimlock.framework.statistics.{ Statistics => FwStatistics }

import commbank.grimlock.spark.environment.Context
import commbank.grimlock.spark.environment.tuner.SparkImplicits._
import commbank.grimlock.spark.Persist

import com.twitter.algebird.{ Aggregator, Moments, Monoid }

import shapeless.Nat

/** Trait for computing common statistics from a matrix. */
trait Statistics[P <: Nat] extends FwStatistics[P, Context] with Persist[Cell[P]] { self: FwMatrix[P, Context] =>
  def counts[
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T = Default()
  )(implicit
    ev: FwStatistics.CountsTuner[Context.U, T]
  ): Context.U[Cell[slice.S]] = data
    .map { case c => slice.selected(c.position) }
    .tunedSize(tuner)
    .map { case (p, c) => Cell(p, Content(DiscreteSchema[Long](), c)) }

  def distinctCounts[
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T = Default()
  )(implicit
    ev: FwStatistics.DistinctCountsTuner[Context.U, T]
  ): Context.U[Cell[slice.S]] = data
    .map { case c => (slice.selected(c.position), c.content.value.toShortString) }
    .tunedSize(tuner)
    .map { case ((p, v), c) => p }
    .tunedSize(tuner)
    .map { case (p, c) => Cell(p, Content(DiscreteSchema[Long](), c)) }

  def predicateCounts[
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T = Default()
  )(
    predicate: (Content) => Boolean
  )(implicit
    ev: FwStatistics.PredicateCountsTuner[Context.U, T]
  ): Context.U[Cell[slice.S]] = data
    .collect { case c if (predicate(c.content)) => slice.selected(c.position) }
    .tunedSize(tuner)
    .map { case (p, c) => Cell(p, Content(DiscreteSchema[Long](), c)) }

  def mean[
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T = Default()
  )(implicit
    ev: FwStatistics.MeanTuner[Context.U, T]
  ): Context.U[Cell[slice.S]] = moments(slice, tuner, m => FwStatistics.mean(m))

  def standardDeviation[
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T = Default()
  )(
    biased: Boolean
  )(implicit
    ev: FwStatistics.StandardDeviationTuner[Context.U, T]
  ): Context.U[Cell[slice.S]] = moments(slice, tuner, m => FwStatistics.sd(m, biased))

  def skewness[
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T = Default()
  )(implicit
    ev: FwStatistics.SkewnessTuner[Context.U, T]
  ): Context.U[Cell[slice.S]] = moments(slice, tuner, m => FwStatistics.skewness(m))

  def kurtosis[
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T = Default()
  )(
    excess: Boolean
  )(implicit
    ev: FwStatistics.KurtosisTuner[Context.U, T]
  ): Context.U[Cell[slice.S]] = moments(slice, tuner, m => FwStatistics.kurtosis(m, excess))

  def minimum[
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T = Default()
  )(implicit
    ev: FwStatistics.MinimumTuner[Context.U, T]
  ): Context.U[Cell[slice.S]] = range(slice, tuner, Aggregator.min)

  def maximum[
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T = Default()
  )(implicit
    ev: FwStatistics.MaximumTuner[Context.U, T]
  ): Context.U[Cell[slice.S]] = range(slice, tuner, Aggregator.max)

  def maximumAbsolute[
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T = Default()
  )(implicit
    ev: FwStatistics.MaximumAbsoluteTuner[Context.U, T]
  ): Context.U[Cell[slice.S]] = range(slice, tuner, Aggregator.max[Double].composePrepare(d => math.abs(d)))

  def sums[
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T = Default()
  )(implicit
    ev: FwStatistics.SumsTuner[Context.U, T]
  ): Context.U[Cell[slice.S]] = data
    .collect { case c if (c.content.schema.classification.isOfType(NumericType)) =>
      (slice.selected(c.position), c.content.value)
    }
    .flatMap { case (p, c) => c.asDouble.map { case d => (p, d) } }
    .tunedReduce(tuner, _ + _)
    .map { case (p, s) => Cell(p, Content(ContinuousSchema[Double](), s)) }

  private def moments(
    slice: Slice[P],
    tuner: Tuner,
    extract: (Moments) => Double
  ): Context.U[Cell[slice.S]] = data
    .collect { case c if (c.content.schema.classification.isOfType(NumericType)) =>
      (slice.selected(c.position), c.content.value)
    }
    .flatMap { case (p, c) => c.asDouble.map { case d => (p, Moments(d)) } }
    .tunedReduce(tuner, (lt, rt) => Monoid.plus(lt, rt))
    .map { case (p, m) => Cell(p, Content(ContinuousSchema[Double](), extract(m))) }

  private def range(
    slice: Slice[P],
    tuner: Tuner,
    agg: Aggregator[Double, Double, Double]
  ): Context.U[Cell[slice.S]] = data
    .collect { case c if (c.content.schema.classification.isOfType(NumericType)) =>
      (slice.selected(c.position), c.content.value)
    }
    .flatMap { case (p, c) => c.asDouble.map { case d => (p, agg.prepare(d)) } }
    .tunedReduce(tuner, (lt, rt) => agg.reduce(lt, rt))
    .map { case (p, t) => Cell(p, Content(ContinuousSchema[Double](), agg.present(t))) }
}

