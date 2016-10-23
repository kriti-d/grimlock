// Copyright 2015,2016 Commonwealth Bank of Australia
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

package commbank.grimlock.scalding.distribution

import commbank.grimlock.framework.{
  Cell,
  Default,
  InMemory,
  Locate,
  NoParameters,
  Reducers,
  Tuner,
  Type
}
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.content.metadata.DiscreteSchema
import commbank.grimlock.framework.DefaultTuners.TP2
import commbank.grimlock.framework.distribution.{
  ApproximateDistribution => FwApproximateDistribution,
  CountMap,
  Quantile,
  QuantileImpl,
  StreamingHistogram,
  TDigest
}
import commbank.grimlock.framework.position.{ Position, Slice }
import commbank.grimlock.framework.utility.=:!=
import commbank.grimlock.framework.utility.UnionTypes.{ In, OneOf }

import commbank.grimlock.scalding.Matrix
import commbank.grimlock.scalding.ScaldingImplicits._

import scala.reflect.ClassTag

import shapeless.Nat
import shapeless.nat.{ _0, _1 }
import shapeless.ops.nat.{ Diff, GT }

trait ApproximateDistribution[L <: Nat, P <: Nat] extends FwApproximateDistribution[L, P] { self: Matrix[L, P] =>
  type HistogramTuners[T] = TP2[T]
  def histogram[
    Q <: Nat,
    T <: Tuner : HistogramTuners
  ](
    slice: Slice[L, P],
    tuner: T = Default()
  )(
    name: Locate.FromSelectedAndContent[slice.S, Q],
    filter: Boolean
  )(implicit
    ev1: ClassTag[Position[Q]],
    ev2: GT[Q, slice.S],
    ev3: Diff.Aux[P, _1, L]
  ): U[Cell[Q]] = data
    .filter(c => (!filter || c.content.schema.kind.isSpecialisationOf(Type.Categorical)))
    .flatMap(c => name(slice.selected(c.position), c.content))
    .asKeys
    .tuneReducers(tuner.parameters)
    .size
    .map { case (p, s) => Cell(p, Content(DiscreteSchema[Long](), s)) }

  type QuantileTuners[T] = T In OneOf[InMemory[NoParameters]]#
    Or[InMemory[Reducers]]#
    Or[Default[NoParameters]]#
    Or[Default[Reducers]]
  def quantile[
    Q <: Nat,
    T <: Tuner : QuantileTuners
  ](
    slice: Slice[L, P],
    tuner: T = Default()
  )(
    probs: List[Double],
    quantiser: Quantile.Quantiser,
    name: Locate.FromSelectedAndOutput[slice.S, Double, Q],
    filter: Boolean,
    nan: Boolean
  )(implicit
    ev1: slice.R =:!= _0,
    ev2: ClassTag[Position[slice.S]],
    ev3: GT[Q, slice.S],
    ev4: Diff.Aux[P, _1, L]
  ): U[Cell[Q]] = {
    val q = QuantileImpl[P, slice.S, Q](probs, quantiser, name, nan)

    val prep = data
      .collect { case c if (!filter || c.content.schema.kind.isSpecialisationOf(Type.Numerical)) =>
        (slice.selected(c.position), q.prepare(c))
      }
      .group

    val grouped = (tuner match {
      case InMemory(_) =>
        prep
          .flatMapWithValue(prep.size.map { case (k, v) => Map(k -> v) }.sum) { case ((s, d), c) =>
            c.map(m => ((s, m(s)), d))
          }
      case _ =>
        prep
          .tunedJoin(tuner, tuner.parameters, prep.size)
          .map { case (s, (d, c)) => ((s, c), d) }
      })
      .group

    val tuned = tuner.parameters match {
      case Reducers(reducers) => grouped.withReducers(reducers)
      case _ => grouped
    }

    tuned
      .sorted
      .mapGroup { case ((_, count), itr) =>
        val first = itr.next
        val (t, c, o) = q.initialise(first, count)

        o.toIterator ++ itr
          .scanLeft((t, List[q.O]())) { case ((t, _), i) => q.update(i, t, c) }
          .flatMap { case (_, o) => o }
      }
      .flatMap { case ((p, _), o) => q.present(p, o) }
  }

  type CountMapQuantilesTuners[T] = TP2[T]
  def countMapQuantiles[
    Q <: Nat,
    T <: Tuner : CountMapQuantilesTuners
  ](
    slice: Slice[L, P],
    tuner: T = Default()
  )(
    probs: List[Double],
    quantiser: Quantile.Quantiser,
    name: Locate.FromSelectedAndOutput[slice.S, Double, Q],
    filter: Boolean,
    nan: Boolean
  )(implicit
    ev1: slice.R =:!= _0,
    ev2: ClassTag[Position[slice.S]],
    ev3: GT[Q, slice.S],
    ev4: Diff.Aux[P, _1, L]
  ): U[Cell[Q]] = data
    .flatMap { case c =>
      if (!filter || c.content.schema.kind.isSpecialisationOf(Type.Numerical))
        Option((slice.selected(c.position), CountMap.from(c.content.value.asDouble.getOrElse(Double.NaN))))
      else
        None
    }
    .group
    .tuneReducers(tuner.parameters)
    .reduce(CountMap.reduce)
    .flatMap { case (pos, t) => CountMap.toCells(t, probs, pos, quantiser, name, nan) }

  type TDigestQuantilesTuners[T] = TP2[T]
  def tDigestQuantiles[
    Q <: Nat,
    T <: Tuner : TDigestQuantilesTuners
  ](
    slice: Slice[L, P],
    tuner: T = Default()
  )(
    probs: List[Double],
    compression: Double,
    name: Locate.FromSelectedAndOutput[slice.S, Double, Q],
    filter: Boolean,
    nan: Boolean
  )(implicit
    ev1: slice.R =:!= _0,
    ev2: ClassTag[Position[slice.S]],
    ev3: GT[Q, slice.S],
    ev4: Diff.Aux[P, _1, L]
  ): U[Cell[Q]] = data
    .flatMap { case c =>
      if (!filter || c.content.schema.kind.isSpecialisationOf(Type.Numerical))
        c.content.value.asDouble.flatMap(d => TDigest.from(d, compression).map(td => (slice.selected(c.position), td)))
      else
        None
    }
    .group
    .tuneReducers(tuner.parameters)
    .reduce(TDigest.reduce)
    .flatMap { case (pos, t) => TDigest.toCells(t, probs, pos, name, nan) }

  type UniformQuantilesTuners[T] = TP2[T]
  def uniformQuantiles[
    Q <: Nat,
    T <: Tuner : UniformQuantilesTuners
  ](
    slice: Slice[L, P],
    tuner: T = Default()
  )(
    count: Long,
    name: Locate.FromSelectedAndOutput[slice.S, Double, Q],
    filter: Boolean,
    nan: Boolean
  )(implicit
    ev1: slice.R =:!= _0,
    ev2: ClassTag[Position[slice.S]],
    ev3: GT[Q, slice.S],
    ev4: Diff.Aux[P, _1, L]
  ): U[Cell[Q]] = data
    .flatMap { case c =>
      if (!filter || c.content.schema.kind.isSpecialisationOf(Type.Numerical))
        c.content.value.asDouble.map(d =>(slice.selected(c.position), StreamingHistogram.from(d, count)))
      else
        None
    }
    .group
    .tuneReducers(tuner.parameters)
    .reduce(StreamingHistogram.reduce)
    .flatMap { case (pos, t) => StreamingHistogram.toCells(t, count, pos, name, nan) }
}

