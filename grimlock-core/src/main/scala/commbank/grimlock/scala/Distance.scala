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

package commbank.grimlock.scala.distance

import commbank.grimlock.framework.{ Cell, Locate, MultiDimensionMatrix => FwMultiDimensionMatrix }
import commbank.grimlock.framework.distance.{ PairwiseDistance => FwPairwiseDistance }
import commbank.grimlock.framework.encoding.Value
import commbank.grimlock.framework.environment.tuner.{ Default, Tuner }
import commbank.grimlock.framework.position.Slice

import commbank.grimlock.scala.environment.Context
import commbank.grimlock.scala.environment.tuner.ScalaImplicits._
import commbank.grimlock.scala.Persist

import com.twitter.algebird.{ Moments, Monoid }

import shapeless.HList

/** Trait for computing pairwise distances from a matrix. */
trait PairwiseDistance[
  P <: HList
] extends FwPairwiseDistance[P, Context]
  with Persist[Cell[P]] { self: FwMultiDimensionMatrix[P, Context] =>
  def correlation[
    S <: HList,
    R <: HList,
    Q <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(
    name: Locate.FromPairwisePositions[S, Q],
    filter: Boolean,
    strict: Boolean
  )(implicit
    ev1: Value.Box[Double],
    ev2: FwPairwiseDistance.CorrelationTuner[Context.U, T]
  ): Context.U[Cell[Q]] = {
    val (at, st, rt, ct) = getTuners(tuner)

    val d = data
      .flatMap { case c => FwPairwiseDistance.prepareCorrelation(slice, c, filter, strict) }

    val mean = d
      .map { case (sel, rem, d) => (sel, Moments(d)) }
      .tunedReduce(st, (lt, rt) => Monoid.plus(lt, rt))
      .map { case (sel, m) => (sel, m.mean) }

    val centered = d
      .map { case (sel, rem, value) => (sel, (rem, value)) }
      .tunedJoin(st, mean)
      .map { case (sel, ((rem, value), mean)) => (sel, rem, value - mean) }

    val numerator = centered
      .map { case (sel, rem, value) => (rem, (sel, value)) }
      .tunedSelfJoin(rt, FwPairwiseDistance.keyedUpper)
      .map { case (_, ((lsel, lval), (rsel, rval))) => ((lsel, rsel), lval * rval) }
      .tunedReduce(at, _ + _)

    val denominator = centered
      .map { case (sel, rem, value) => (sel, value * value) }
      .tunedReduce(st, _ + _)
      .tunedSelfCross(ct, FwPairwiseDistance.upper)
      .map { case ((lsel, lval), (rsel, rval))  => ((lsel, rsel), math.sqrt(lval * rval)) }

    numerator
      .tunedJoin(at, denominator)
      .flatMap { case ((lsel, rsel), (lval, rval)) =>
        FwPairwiseDistance.presentCorrelation(lsel, lval, rsel, rval, name)
      }
  }

  def mutualInformation[
    S <: HList,
    R <: HList,
    Q <: HList,
    T <: Tuner
  ](
    slice: Slice[P, S, R],
    tuner: T = Default()
  )(
    name: Locate.FromPairwisePositions[S, Q],
    filter: Boolean,
    log: (Double) => Double
  )(implicit
    ev1: Value.Box[Double],
    ev2: FwPairwiseDistance.MutualInformationTuner[Context.U, T]
  ): Context.U[Cell[Q]] = {
    val (at, st, rt, ct) = getTuners(tuner)

    val d = data
      .flatMap { case c => FwPairwiseDistance.prepareMutualInformation(slice, c, filter) }

    val mcount = d
      .map { case (sel, _, _) => sel }
      .tunedSize(st)

    val marginal = d
      .map { case (sel, _, s) => (sel, s) }
      .tunedSize(st)
      .map { case ((sel, _), cnt) => (sel, cnt) }
      .tunedJoin(st, mcount)
      .map { case (sel, (cnt, tot)) => (sel, FwPairwiseDistance.partialEntropy(cnt, tot, log, true)) }
      .tunedReduce(st, _ + _)
      .tunedSelfCross(ct, FwPairwiseDistance.upper)
      .map { case ((lsel, lval), (rsel, rval)) => ((lsel, rsel), lval + rval) }

    val jpair = d
      .map { case (sel, rel, s) => (rel, (sel, s)) }
      .tunedSelfJoin(rt, FwPairwiseDistance.keyedUpper)
      .map { case (_, ((lsel, lval), (rsel, rval))) => ((lsel, rsel), (lval, rval)) }

    val jcount = jpair
      .map { case ((lsel, rsel), (lval, rval)) => (lsel, rsel) }
      .tunedSize(at)

    val joint = jpair
      .tunedSize(at)
      .map { case (((lsel, rsel), (lval, rval)), cnt) => ((lsel, rsel), cnt) }
      .tunedJoin(at, jcount)
      .map { case ((lsel, rsel), (cnt, tot)) =>
        ((lsel, rsel), FwPairwiseDistance.partialEntropy(cnt, tot, log, false))
      }
      .tunedReduce(at, _ + _)

    (marginal ++ joint)
      .tunedReduce(at, _ + _)
      .flatMap { case ((lsel, rsel), mi) => FwPairwiseDistance.presentMutualInformation(lsel, rsel, mi, name) }
  }

  private def getTuners(tuner: Tuner): (Tuner, Tuner, Tuner, Tuner) = tuner match {
    case _ => (Default(), Default(), Default(), Default())
  }
}

