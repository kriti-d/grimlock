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

package commbank.grimlock.framework.statistics

import commbank.grimlock.framework.{ Cell, Matrix }
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.environment.Context
import commbank.grimlock.framework.environment.tuner.Tuner
import commbank.grimlock.framework.position.Slice

import com.twitter.algebird.Moments

import shapeless.Nat

/** Trait for computing common statistics from a matrix. */
trait Statistics[P <: Nat, C <: Context[C]] { self: Matrix[P, C] =>
  /**
   * Compute counts.
   *
   * @param slice Encapsulates the dimension(s) to compute counts for.
   * @param tuner The tuner for the job.
   *
   * @return A `C#U[Cell[slice.S]]` with the counts.
   */
  def counts[T <: Tuner](slice: Slice[P], tuner: T)(implicit ev: Statistics.CountsTuner[C#U, T]): C#U[Cell[slice.S]]

  /**
   * Compute distinct value counts.
   *
   * @param slice Encapsulates the dimension(s) to compute distinct counts for.
   * @param tuner The tuner for the job.
   *
   * @return A `C#U[Cell[slice.S]]` with the distinct counts.
   */
  def distinctCounts[
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T
  )(implicit
    ev: Statistics.DistinctCountsTuner[C#U, T]
  ): C#U[Cell[slice.S]]

  /**
   * Compute predicate counts.
   *
   * @param slice     Encapsulates the dimension(s) to compute predicate counts for.
   * @param tuner     The tuner for the job.
   * @param predicate The predicate to count.
   *
   * @return A `C#U[Cell[slice.S]]` with the predicate counts.
   */
  def predicateCounts[
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T
  )(
    predicate: (Content) => Boolean
  )(implicit
    ev: Statistics.PredicateCountsTuner[C#U, T]
  ): C#U[Cell[slice.S]]

  /**
   * Compute mean values.
   *
   * @param slice Encapsulates the dimension(s) to compute mean values for.
   * @param tuner The tuner for the job.
   *
   * @return A `C#U[Cell[slice.S]]` with the mean values.
   */
  def mean[T <: Tuner](slice: Slice[P], tuner: T)(implicit ev: Statistics.MeanTuner[C#U, T]): C#U[Cell[slice.S]]

  /**
   * Compute standard deviation.
   *
   * @param slice  Encapsulates the dimension(s) to compute standard deviations for.
   * @param tuner  The tuner for the job.
   * @param biased Indicator if biased standard deviation should be computed or not.
   *
   * @return A `C#U[Cell[slice.S]]` with the standard deviations.
   */
  def standardDeviation[
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T
  )(
    biased: Boolean
  )(implicit
    ev: Statistics.StandardDeviationTuner[C#U, T]
  ): C#U[Cell[slice.S]]

  /**
   * Compute skewness values.
   *
   * @param slice Encapsulates the dimension(s) to compute skewness values for.
   * @param tuner The tuner for the job.
   *
   * @return A `C#U[Cell[slice.S]]` with the skewness values.
   */
  def skewness[T <: Tuner](slice: Slice[P], tuner: T)(implicit ev: Statistics.SkewnessTuner[C#U, T]): C#U[Cell[slice.S]]

  /**
   * Compute kurtosis values.
   *
   * @param slice  Encapsulates the dimension(s) to compute kurtosis values for.
   * @param tuner  The tuner for the job.
   * @param excess Indicator if excess kurtosis should be computed or not.
   *
   * @return A `C#U[Cell[slice.S]]` with the kurtosis values.
   */
  def kurtosis[
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T
  )(
    excess: Boolean
  )(implicit
    ev: Statistics.KurtosisTuner[C#U, T]
  ): C#U[Cell[slice.S]]

  /**
   * Compute minimum values.
   *
   * @param slice Encapsulates the dimension(s) to compute minimum values for.
   * @param tuner The tuner for the job.
   *
   * @return A `C#U[Cell[slice.S]]` with the minimum values.
   */
  def minimum[T <: Tuner](slice: Slice[P], tuner: T)(implicit ev: Statistics.MinimumTuner[C#U, T]): C#U[Cell[slice.S]]

  /**
   * Compute maximum values.
   *
   * @param slice Encapsulates the dimension(s) to compute maximum values for.
   * @param tuner The tuner for the job.
   *
   * @return A `C#U[Cell[slice.S]]` with the maximum values.
   */
  def maximum[T <: Tuner](slice: Slice[P], tuner: T)(implicit ev: Statistics.MaximumTuner[C#U, T]): C#U[Cell[slice.S]]

  /**
   * Compute maximum absolute values.
   *
   * @param slice Encapsulates the dimension(s) to compute maximum absolute values for.
   * @param tuner The tuner for the job.
   *
   * @return A `C#U[Cell[slice.S]]` with the maximum absolute values.
   */
  def maximumAbsolute[
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T
  )(implicit
    ev: Statistics.MaximumAbsoluteTuner[C#U, T]
  ): C#U[Cell[slice.S]]

  /**
   * Compute sum values.
   *
   * @param slice Encapsulates the dimension(s) to compute sum values for.
   * @param tuner The tuner for the job.
   *
   * @return A `C#U[Cell[slice.S]]` with the sum values.
   */
  def sums[T <: Tuner](slice: Slice[P], tuner: T)(implicit ev: Statistics.SumsTuner[C#U, T]): C#U[Cell[slice.S]]
}

/** Companion object to `Statistics`. */
object Statistics {
  /** Trait for tuners permitted on a call to `counts`. */
  trait CountsTuner[U[_], T <: Tuner] extends java.io.Serializable

  /** Trait for tuners permitted on a call to `distinctCounts`. */
  trait DistinctCountsTuner[U[_], T <: Tuner] extends java.io.Serializable

  /** Trait for tuners permitted on a call to `predicateCount`. */
  trait PredicateCountsTuner[U[_], T <: Tuner] extends java.io.Serializable

  /** Trait for tuners permitted on a call to `mean`. */
  trait MeanTuner[U[_], T <: Tuner] extends java.io.Serializable

  /** Trait for tuners permitted on a call to `standardDeviation`. */
  trait StandardDeviationTuner[U[_], T <: Tuner] extends java.io.Serializable

  /** Trait for tuners permitted on a call to `skewness`. */
  trait SkewnessTuner[U[_], T <: Tuner] extends java.io.Serializable

  /** Trait for tuners permitted on a call to `kurtosis`. */
  trait KurtosisTuner[U[_], T <: Tuner] extends java.io.Serializable

  /** Trait for tuners permitted on a call to `minimum`. */
  trait MinimumTuner[U[_], T <: Tuner] extends java.io.Serializable

  /** Trait for tuners permitted on a call to `maximum`. */
  trait MaximumTuner[U[_], T <: Tuner] extends java.io.Serializable

  /** Trait for tuners permitted on a call to `maximumAbsolute`. */
  trait MaximumAbsoluteTuner[U[_], T <: Tuner] extends java.io.Serializable

  /** Trait for tuners permitted on a call to `sums`. */
  trait SumsTuner[U[_], T <: Tuner] extends java.io.Serializable

  /**
   * Return the mean.
   *
   * @param t Algebird moments object to get mean from.
   */
  def mean(t: Moments): Double = t.mean

  /**
   * Return the standard deviation.
   *
   * @param t      Algebird moments object to get standard deviation from.
   * @param biased Indicates if the biased estimate should be return or not.
   */
  def sd(t: Moments, biased: Boolean): Double =
    if (t.count > 1) { if (biased) t.stddev else t.stddev * math.sqrt(t.count / (t.count - 1.0)) } else Double.NaN

  /**
   * Return the skewness.
   *
   * @param t Algebird moments object to get skewness from.
   */
  def skewness(t: Moments): Double = t.skewness

  /**
   * Return the kurtosis.
   *
   * @param t      Algebird moments object to get kurtosis from.
   * @param excess Indicates if the excess kurtosis should be return or not.
   */
  def kurtosis(t: Moments, excess: Boolean): Double = if (excess) t.kurtosis else t.kurtosis + 3
}

