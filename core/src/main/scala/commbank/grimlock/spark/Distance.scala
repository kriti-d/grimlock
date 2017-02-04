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

package commbank.grimlock.spark.distance

import commbank.grimlock.framework.{
  Cell,
  Default,
  InMemory,
  Locate,
  NoParameters,
  Reducers,
  Ternary,
  Tuner
}
import commbank.grimlock.framework.distance.{ PairwiseDistance => FwPairwiseDistance }
import commbank.grimlock.framework.position.{ Position, Slice }
import commbank.grimlock.framework.utility.UnionTypes.{ In, OneOf }

import commbank.grimlock.spark.MapMapSideJoin
import commbank.grimlock.spark.Matrix
import commbank.grimlock.spark.SparkImplicits._

import com.twitter.algebird.{ Moments, Monoid }

import scala.reflect.ClassTag

import shapeless.Nat
import shapeless.nat._1
import shapeless.ops.nat.Diff

trait PairwiseDistance[L <: Nat, P <: Nat] extends FwPairwiseDistance[L, P] { self: Matrix[L, P] =>
  /** Permissible tuners on pairwise distance functions. */
  type DistanceTuners[T] = T In OneOf[InMemory[NoParameters]]#   // Def[NoParam], InMemory[NoParam], Def[NoParam]
    Or[Default[NoParameters]]#                                   // Def[NoParam], Def[NoParam], Def[NoParam]
    Or[Ternary[Default[Reducers], InMemory[NoParameters], Default[NoParameters]]]#
    Or[Ternary[Default[Reducers], InMemory[NoParameters], Default[Reducers]]]#
    Or[Ternary[Default[Reducers], InMemory[Reducers], Default[Reducers]]]#
    Or[Ternary[Default[Reducers], Default[NoParameters], Default[NoParameters]]]#
    Or[Ternary[Default[Reducers], Default[NoParameters], Default[Reducers]]]#
    Or[Ternary[Default[Reducers], Default[Reducers], Default[Reducers]]]

  type CorrelationTuners[T] = DistanceTuners[T]
  def correlation[
    Q <: Nat,
    T <: Tuner : CorrelationTuners
  ](
    slice: Slice[L, P],
    tuner: T = Default()
  )(
    name: Locate.FromPairwisePositions[slice.S, Q],
    filter: Boolean,
    strict: Boolean = true,
    nan: Boolean = false
  )(implicit
    ev1: ClassTag[Position[slice.S]],
    ev2: Diff.Aux[P, _1, L]
  ): U[Cell[Q]] = {
    val msj = Option(MapMapSideJoin[Position[slice.S], (Position[slice.R], Double), Double]())

    val (at, st, rt, ct) = getTuners(tuner)

    val d = data
      .flatMap { case c => prepareCorrelation(slice, c, filter, strict) }

    val mean = d
      .map { case (sel, rem, d) => (sel, Moments(d)) }
      .tunedReduce(st, (lt, rt) => Monoid.plus(lt, rt))
      .map { case (sel, m) => (sel, m.mean) }

    val centered = d
      .map { case (sel, rem, value) => (sel, (rem, value)) }
      .tunedJoin(st, mean, msj)
      .map { case (sel, ((rem, value), mean)) => (sel, rem, value - mean) }

    val numerator = centered
      .map { case (sel, rem, value) => (rem, (sel, value)) }
      .tunedSelfJoin(rt, keyedUpper)
      .map { case (_, ((lsel, lval), (rsel, rval))) => ((lsel, rsel), lval * rval) }
      .tunedReduce(at, _ + _)

    val denominator = centered
      .map { case (sel, rem, value) => (sel, value * value) }
      .tunedReduce(st, _ + _)
      .tunedSelfCross(ct, upper)
      .map { case ((lsel, lval), (rsel, rval))  => ((lsel, rsel), math.sqrt(lval * rval)) }

    numerator
      .tunedJoin(at, denominator)
      .flatMap { case ((lsel, rsel), (lval, rval)) => presentCorrelation(lsel, lval, rsel, rval, name, nan) }
  }

  type MutualInformationTuners[T] = DistanceTuners[T]
  def mutualInformation[
    Q <: Nat,
    T <: Tuner : MutualInformationTuners
  ](
    slice: Slice[L, P],
    tuner: T = Default()
  )(
    name: Locate.FromPairwisePositions[slice.S, Q],
    filter: Boolean,
    log: (Double) => Double
  )(implicit
    ev1: ClassTag[Position[slice.S]],
    ev2: Diff.Aux[P, _1, L]
  ): U[Cell[Q]] = {
    val msj = Option(MapMapSideJoin[Position[slice.S], Long, Long]())

    val (at, st, rt, ct) = getTuners(tuner)

    val d = data
      .flatMap { case c => prepareMutualInformation(slice, c, filter) }

    val mcount = d
      .map { case (sel, _, _) => sel }
      .tunedSize(st)

    val marginal = d
      .map { case (sel, _, s) => (sel, s) }
      .tunedSize(st)
      .map { case ((sel, _), cnt) => (sel, cnt) }
      .tunedJoin(st, mcount, msj)
      .map { case (sel, (cnt, tot)) => (sel, partialEntropy(cnt, tot, log, true)) }
      .tunedReduce(st, _ + _)
      .tunedSelfCross(ct, upper)
      .map { case ((lsel, lval), (rsel, rval)) => ((lsel, rsel), lval + rval) }

    val jpair = d
      .map { case (sel, rel, s) => (rel, (sel, s)) }
      .tunedSelfJoin(rt, keyedUpper)
      .map { case (_, ((lsel, lval), (rsel, rval))) => ((lsel, rsel), (lval, rval)) }

    val jcount = jpair
      .map { case ((lsel, rsel), (lval, rval)) => (lsel, rsel) }
      .tunedSize(at)

    val joint = jpair
      .tunedSize(at)
      .map { case (((lsel, rsel), (lval, rval)), cnt) => ((lsel, rsel), cnt) }
      .tunedJoin(at, jcount)
      .map { case ((lsel, rsel), (cnt, tot)) => ((lsel, rsel), partialEntropy(cnt, tot, log, false)) }
      .tunedReduce(at, _ + _)

    (marginal ++ joint)
      .tunedReduce(at, _ + _)
      .flatMap { case ((lsel, rsel), mi) => presentMutualInformation(lsel, rsel, mi, name) }
  }

  private def getTuners(tuner: Tuner): (Tuner, Tuner, Tuner, Tuner) = tuner match {
    case InMemory(_) => (Default(), InMemory(), Default(), Default())
    case Ternary(a, s, r) => (a, s, r, s)
    case _ => (Default(), Default(), Default(), Default())
  }
}

