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

import commbank.grimlock.framework.{ Cell, Default, NumericType, Tuner }
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.content.metadata.{ ContinuousSchema, DiscreteSchema }
import commbank.grimlock.framework.DefaultTuners.TP1
import commbank.grimlock.framework.statistics.{ Statistics => FwStatistics }
import commbank.grimlock.framework.position.{ Position, Slice }

import commbank.grimlock.spark.Matrix
import commbank.grimlock.spark.SparkImplicits._

import com.twitter.algebird.{ Aggregator, Moments, Monoid }

import scala.reflect.ClassTag

import shapeless.Nat
import shapeless.nat._1
import shapeless.ops.nat.Diff

/** Trait for computing common statistics from a matrix. */
trait Statistics[L <: Nat, P <: Nat] extends FwStatistics[L, P] { self: Matrix[L, P] =>
  type CountsTuners[T] = TP1[T]
  def counts[
    T <: Tuner : CountsTuners
  ](
    slice: Slice[L, P],
    tuner: T = Default()
  )(implicit
    ev1: ClassTag[Position[slice.S]],
    ev2: Diff.Aux[P, _1, L]
  ): U[Cell[slice.S]] = data
    .map { case c => slice.selected(c.position) }
    .tunedSize(tuner)
    .map { case (p, c) => Cell(p, Content(DiscreteSchema[Long](), c)) }

  type DistinctCountsTuners[T] = TP1[T]
  def distinctCounts[
    T <: Tuner : DistinctCountsTuners
  ](
    slice: Slice[L, P],
    tuner: T = Default()
  )(implicit
    ev1: ClassTag[Position[slice.S]],
    ev2: Diff.Aux[P, _1, L]
  ): U[Cell[slice.S]] = data
    .map { case c => (slice.selected(c.position), c.content.value.toShortString) }
    .tunedSize(tuner)
    .map { case ((p, v), c) => p }
    .tunedSize(tuner)
    .map { case (p, c) => Cell(p, Content(DiscreteSchema[Long](), c)) }

  type PredicateCountsTuners[T] = TP1[T]
  def predicateCounts[
    T <: Tuner : PredicateCountsTuners
  ](
    slice: Slice[L, P],
    tuner: T
  )(
    predicate: (Content) => Boolean
  )(implicit
    ev1: ClassTag[Position[slice.S]],
    ev2: Diff.Aux[P, _1, L]
  ): U[Cell[slice.S]] = data
    .collect { case c if (predicate(c.content)) => slice.selected(c.position) }
    .tunedSize(tuner)
    .map { case (p, c) => Cell(p, Content(DiscreteSchema[Long](), c)) }

  type MeanTuners[T] = TP1[T]
  def mean[
    T <: Tuner : MeanTuners
  ](
    slice: Slice[L, P],
    tuner: T
  )(implicit
    ev1: ClassTag[Position[slice.S]],
    ev2: Diff.Aux[P, _1, L]
  ): U[Cell[slice.S]] = moments(slice, tuner, m => FwStatistics.mean(m))

  type StandardDeviationTuners[T] = TP1[T]
  def standardDeviation[
    T <: Tuner : StandardDeviationTuners
  ](
    slice: Slice[L, P],
    tuner: T
  )(
    biased: Boolean
  )(implicit
    ev1: ClassTag[Position[slice.S]],
    ev2: Diff.Aux[P, _1, L]
  ): U[Cell[slice.S]] = moments(slice, tuner, m => FwStatistics.sd(m, biased))

  type SkewnessTuners[T] = TP1[T]
  def skewness[
    T <: Tuner : SkewnessTuners
  ](
    slice: Slice[L, P],
    tuner: T
  )(implicit
    ev1: ClassTag[Position[slice.S]],
    ev2: Diff.Aux[P, _1, L]
  ): U[Cell[slice.S]] = moments(slice, tuner, m => FwStatistics.skewness(m))

  type KurtosisTuners[T] = TP1[T]
  def kurtosis[
    T <: Tuner : KurtosisTuners
  ](
    slice: Slice[L, P],
    tuner: T
  )(
    excess: Boolean
  )(implicit
    ev1: ClassTag[Position[slice.S]],
    ev2: Diff.Aux[P, _1, L]
  ): U[Cell[slice.S]] = moments(slice, tuner, m => FwStatistics.kurtosis(m, excess))

  type MinimumTuners[T] = TP1[T]
  def minimum[
    T <: Tuner : MinimumTuners
  ](
    slice: Slice[L, P],
    tuner: T
  )(implicit
    ev1: ClassTag[Position[slice.S]],
    ev2: Diff.Aux[P, _1, L]
  ): U[Cell[slice.S]] = range(slice, tuner, Aggregator.min)

  type MaximumTuners[T] = TP1[T]
  def maximum[
    T <: Tuner : MaximumTuners
  ](
    slice: Slice[L, P],
    tuner: T
  )(implicit
    ev1: ClassTag[Position[slice.S]],
    ev2: Diff.Aux[P, _1, L]
  ): U[Cell[slice.S]] = range(slice, tuner, Aggregator.max)

  type MaximumAbsoluteTuners[T] = TP1[T]
  def maximumAbsolute[
    T <: Tuner : MaximumAbsoluteTuners
  ](
    slice: Slice[L, P],
    tuner: T
  )(implicit
    ev1: ClassTag[Position[slice.S]],
    ev2: Diff.Aux[P, _1, L]
  ): U[Cell[slice.S]] = range(slice, tuner, Aggregator.max[Double].composePrepare(d => math.abs(d)))

  type SumsTuners[T] = TP1[T]
  def sums[
    T <: Tuner : SumsTuners
  ](
    slice: Slice[L, P],
    tuner: T
  )(implicit
    ev1: ClassTag[Position[slice.S]],
    ev2: Diff.Aux[P, _1, L]
  ): U[Cell[slice.S]] = data
    .collect { case c if (c.content.schema.classification.isOfType(NumericType)) =>
      (slice.selected(c.position), c.content.value)
    }
    .flatMap { case (p, c) => c.asDouble.map { case d => (p, d) } }
    .tunedReduce(tuner, _ + _)
    .map { case (p, s) => Cell(p, Content(ContinuousSchema[Double](), s)) }

  private def moments(
    slice: Slice[L, P],
    tuner: Tuner,
    extract: (Moments) => Double
  )(implicit
    ev: Diff.Aux[P, _1, L]
  ): U[Cell[slice.S]] = data
    .collect { case c if (c.content.schema.classification.isOfType(NumericType)) =>
      (slice.selected(c.position), c.content.value)
    }
    .flatMap { case (p, c) => c.asDouble.map { case d => (p, Moments(d)) } }
    .tunedReduce(tuner, (lt, rt) => Monoid.plus(lt, rt))
    .map { case (p, m) => Cell(p, Content(ContinuousSchema[Double](), extract(m))) }

  private def range(
    slice: Slice[L, P],
    tuner: Tuner,
    agg: Aggregator[Double, Double, Double]
  )(implicit
    ev: Diff.Aux[P, _1, L]
  ): U[Cell[slice.S]] = data
    .collect { case c if (c.content.schema.classification.isOfType(NumericType)) =>
      (slice.selected(c.position), c.content.value)
    }
    .flatMap { case (p, c) => c.asDouble.map { case d => (p, agg.prepare(d)) } }
    .tunedReduce(tuner, (lt, rt) => agg.reduce(lt, rt))
    .map { case (p, t) => Cell(p, Content(ContinuousSchema[Double](), agg.present(t))) }
}

