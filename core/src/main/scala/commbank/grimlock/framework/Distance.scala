// Copyright 2017 Commonwealth Bank of Australia
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

package commbank.grimlock.framework.distance

import commbank.grimlock.framework.{ Cell, Locate, MultiDimensionMatrix }
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.environment.tuner.Tuner
import commbank.grimlock.framework.metadata.{ CategoricalType, ContinuousSchema, NumericType }
import commbank.grimlock.framework.position.{ Position, Slice }

import shapeless.Nat

/** Trait for computing pairwise distances from a matrix. */
trait PairwiseDistance[P <: Nat, U[_], E[_]] { self: MultiDimensionMatrix[P, U, E] =>
  /**
   * Compute correlations.
   *
   * @param slice  Encapsulates the dimension for which to compute correlations.
   * @param tuner  The tuner for the job.
   * @param name   Function for extracting the position of the correlation value.
   * @param filter Indicator if categorical values shoud be filtered or not.
   * @param strict Indicates if strict data handling is required. If so then any non-numeric value fails the
   *               computation. If not then non-numeric values are silently ignored.
   *
   * @return A `U[Cell[Q]]` with all pairwise correlations.
   */
  def correlation[
    Q <: Nat,
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T
  )(
    name: Locate.FromPairwisePositions[slice.S, Q],
    filter: Boolean = true,
    strict: Boolean = true
  )(implicit
    ev: PairwiseDistance.CorrelationTuner[U, T]
  ): U[Cell[Q]]

  /**
   * Compute mutual information.
   *
   * @param slice  Encapsulates the dimension for which to compute mutual information.
   * @param tuner  The tuner for the job.
   * @param name   Function for extracting the position of the mutual information values.
   * @param filter Indicator if numerical values shoud be filtered or not.
   * @param log    The log function to use.
   *
   * @return A `U[Cell[Q]]` with all pairwise mutual information values.
   */
  def mutualInformation[
    Q <: Nat,
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T
  )(
    name: Locate.FromPairwisePositions[slice.S, Q],
    filter: Boolean = true,
    log: (Double) => Double = (x: Double) => math.log(x) / math.log(2)
  )(implicit
    ev: PairwiseDistance.MutualInformationTuner[U, T]
  ): U[Cell[Q]]
}

/** Companion object to `PairwiseDistance` with types, implicits, etc. */
object PairwiseDistance {
  /** Trait for tuners permitted on a call to `correlation`. */
  trait CorrelationTuner[U[_], T <: Tuner]

  /** Trait for tuners permitted on a call to `mutualInformation`. */
  trait MutualInformationTuner[U[_], T <: Tuner]

  private[grimlock] def prepareCorrelation[
    P <: Nat
  ](
    slice: Slice[P],
    cell: Cell[P],
    filter: Boolean,
    strict: Boolean
  ): Option[(Position[slice.S], Position[slice.R], Double)] = {
    if (!filter || cell.content.schema.classification.isOfType(NumericType)) {
      val value = cell.content.value.asDouble
      val double = if (strict) value.orElse(Option(Double.NaN)) else value

      double.map { case d => (slice.selected(cell.position), slice.remainder(cell.position), d) }
    } else
      None
  }

  private[grimlock] def presentCorrelation[
    S <: Nat,
    Q <: Nat
  ](
    lsel: Position[S],
    lval: Double,
    rsel: Position[S],
    rval: Double,
    name: Locate.FromPairwisePositions[S, Q]
  ): Option[Cell[Q]] = name(lsel, rsel)
    .map { case pos => Cell(pos, Content(ContinuousSchema[Double](), lval / rval)) }

  private[grimlock] def prepareMutualInformation[
    P <: Nat
  ](
    slice: Slice[P],
    cell: Cell[P],
    filter: Boolean
  ): Option[(Position[slice.S], Position[slice.R], String)] = {
    if (!filter || cell.content.schema.classification.isOfType(CategoricalType))
      Option((slice.selected(cell.position), slice.remainder(cell.position), cell.content.value.toShortString))
    else
      None
  }

  private[grimlock] def presentMutualInformation[
    S <: Nat,
    Q <: Nat
  ](
    lsel: Position[S],
    rsel: Position[S],
    mi: Double,
    name: Locate.FromPairwisePositions[S, Q]
  ): Option[Cell[Q]] = name(lsel, rsel).map { case pos => Cell(pos, Content(ContinuousSchema[Double](), mi)) }

  private[grimlock] def partialEntropy(count: Long, total: Long, log: (Double) => Double, negate: Boolean): Double = {
    val pe = (count.toDouble / total) * log(count.toDouble / total)

    if (negate) - pe else pe
  }

  private[grimlock] def upper[V, S <: Nat] = (l: (Position[S], V), r: (Position[S], V)) => l._1.compare(r._1) > 0
  private[grimlock] def keyedUpper[K, V, S <: Nat] = (k: K, l: (Position[S], V), r: (Position[S], V)) => upper(l, r)
}

