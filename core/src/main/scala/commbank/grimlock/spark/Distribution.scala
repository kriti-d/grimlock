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

package commbank.grimlock.spark.distribution

import commbank.grimlock.framework.{
  Cell,
  Default,
  Locate,
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

import commbank.grimlock.spark.Matrix
import commbank.grimlock.spark.SparkImplicits._

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
    .filter { case c => (!filter || c.content.schema.kind.isSpecialisationOf(Type.Categorical)) }
    .flatMap { case c => name(slice.selected(c.position), c.content).map((_, 1L)) }
    .tunedReduce(tuner.parameters, _ + _)
    .map { case (p, s) => Cell(p, Content(DiscreteSchema[Long](), s)) }

  type QuantileTuners[T] = TP2[T]
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

    prep
      .tunedJoin(tuner.parameters, prep.map { case (s, _) => (s, 1L) }.tunedReduce(tuner.parameters, _ + _))
      .map { case (s, (d, c)) => ((s, c), d) }
      .groupByKey
      .flatMap { case ((p, count), itr) =>
        val lst = itr.toList.sorted
        val first = lst.head
        val (t, c, o) = q.initialise(first, count)

        o.flatMap(q.present(p, _)) ++ lst
          .tail
          .scanLeft((t, List[q.O]())) { case ((t, _), i) => q.update(i, t, c) }
          .flatMap { case (_, o) => o.flatMap(q.present(p, _)) }
      }
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
    .tunedReduce(tuner.parameters, CountMap.reduce)
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
        c.content.value.asDouble.flatMap { case d =>
          TDigest.from(d, compression).map(td => (slice.selected(c.position), td))
        }
      else
        None
    }
    .tunedReduce(tuner.parameters, TDigest.reduce)
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
    .tunedReduce(tuner.parameters, StreamingHistogram.reduce)
    .flatMap { case (pos, t) => StreamingHistogram.toCells(t, count, pos, name, nan) }
}

