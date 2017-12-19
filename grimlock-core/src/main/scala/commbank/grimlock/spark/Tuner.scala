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

package commbank.grimlock.spark.environment.tuner

import commbank.grimlock.framework.environment.tuner.{
  Default,
  InMemory,
  MapMapSideJoin => FwMapMapSideJoin,
  MapSideJoin,
  Redistribute,
  Reducers,
  SetMapSideJoin => FwSetMapSideJoin,
  Tuner
}
import commbank.grimlock.framework.position.Position

import commbank.grimlock.spark.environment.Context

import scala.reflect.ClassTag

import shapeless.HList

private[spark] case class MapMapSideJoin[K, V, W]() extends FwMapMapSideJoin[K, V, W, Context.U, Context.E] {
  def compact(
    smaller: Context.U[(K, W)]
  )(implicit
    ev1: ClassTag[K],
    ev2: ClassTag[W],
    ev3: Ordering[K]
  ): T = smaller.collectAsMap().toMap
}

private[spark] case class SetMapSideJoin[K, V]() extends FwSetMapSideJoin[K, V, Context.U, Context.E] {
  def compact(
    smaller: Context.U[(K, Unit)]
  )(implicit
    ev1: ClassTag[K],
    ev2: ClassTag[Unit],
    ev3: Ordering[K]
  ): T = smaller.map { case (k, _) => k }.collect().toSet
}

private[spark] object SparkImplicits {
  implicit def positionOrdering[P <: HList] = Position.ordering[P]()

  private[spark] implicit class PairRDDTuner[K, V](rdd: Context.U[(K, V)])(implicit kt: ClassTag[K], vt: ClassTag[V]) {
    def tunedJoin[W](
      tuner: Tuner,
      smaller: Context.U[(K, W)],
      msj: Option[MapSideJoin[K, V, W, Context.U, Context.E]] = None
    )(implicit
      ev1: Ordering[K],
      ev2: ClassTag[W]
    ): Context.U[(K, (V, W))] = (tuner, msj) match {
      case (InMemory(_), Some(m)) =>
        rdd.flatMapWithValue(m.compact(smaller)) { case ((k, v), t) =>
          m.join(k, v, t.getOrElse(m.empty)).map { case w => (k, (v, w)) }
        }
      case (Default(Reducers(reducers)), _) => rdd.join(smaller, reducers)
      case _ => rdd.join(smaller)
    }

    def tunedLeftJoin[W](
      tuner: Tuner,
      smaller: Context.U[(K, W)],
      msj: Option[MapSideJoin[K, V, W, Context.U, Context.E]] = None
    )(implicit
      ev1: Ordering[K],
      ev2: ClassTag[W]
    ): Context.U[(K, (V, Option[W]))] = (tuner, msj) match {
      case (InMemory(_), Some(m)) =>
        // TODO: broadcast value?
        val value = m.compact(smaller)

        rdd.map { case (k, v) => (k, (v, m.join(k, v, value))) }
      case (Default(Reducers(reducers)), _) => rdd.leftOuterJoin(smaller, reducers)
      case _ => rdd.leftOuterJoin(smaller)
    }

    def tunedOuterJoin[W](
      tuner: Tuner,
      smaller: Context.U[(K, W)]
    ): Context.U[(K, (Option[V], Option[W]))] = tuner.parameters match {
      case Reducers(reducers) => rdd.fullOuterJoin(smaller, reducers)
      case _ => rdd.fullOuterJoin(smaller)
    }

    def tunedReduce(tuner: Tuner, reduction: (V, V) => V): Context.U[(K, V)] = tuner.parameters match {
      case Reducers(reducers) => rdd.reduceByKey(reduction, reducers)
      case _ => rdd.reduceByKey(reduction)
    }

    def tunedSelfJoin(
      tuner: Tuner,
      filter: (K, V, V) => Boolean
    )(implicit
      ev: Ordering[K]
    ): Context.U[(K, (V, V))] = rdd.tunedJoin(tuner, rdd).filter { case (k, (v, w)) => filter(k, v, w) }

    def tunedStream[Q](tuner: Tuner, f: (K, Iterator[V]) => TraversableOnce[Q]): Context.U[(K, Q)] = {
      val grouped = tuner.parameters match {
        case Reducers(reducers) => rdd.groupByKey(reducers)
        case _ => rdd.groupByKey
      }

      grouped.flatMap { case (k, i) => f(k, i.toIterator).map { case q => (k, q) } }
    }
  }

  implicit class RDDTuner[T](rdd: Context.U[T]) {
    // TODO: broadcast in WithValue functions?
    def filterWithValue[V](
      value: V
    )(
      f: (T, Option[V]) => Boolean
    ): Context.U[T] = rdd.filter { case t => f(t, Option(value)) }

    def flatMapWithValue[V, Q](
      value: V
    )(
      f: (T, Option[V]) => TraversableOnce[Q]
    )(implicit
      ev: ClassTag[Q]
    ): Context.U[Q] = rdd.flatMap { case t => f(t, Option(value)) }

    def tunedCross[X](
      tuner: Tuner,
      filter: (T, X) => Boolean,
      smaller: Context.U[X]
    )(implicit
      ev1: ClassTag[T],
      ev2: ClassTag[X]
    ): Context.U[(T, X)] = rdd.cartesian(smaller).filter { case (l, r) => filter(l, r) }

    def tunedDistinct(tuner: Tuner)(implicit ev: Ordering[T]): Context.U[T] = tuner.parameters match {
      case Reducers(reducers) => rdd.distinct(reducers)(ev)
      case _ => rdd.distinct()
    }

    def tunedRedistribute(tuner: Tuner): Context.U[T] = tuner match {
      case Redistribute(reducers) => rdd.repartition(reducers)
      case _ => rdd
    }

    def tunedSaveAsText(ctx: Context, tuner: Tuner, file: String) = rdd
      .tunedRedistribute(tuner)
      .saveAsTextFile(file)

    def tunedSelfCross(
      tuner: Tuner,
      filter: (T, T) => Boolean
    )(implicit
      ev: ClassTag[T]
    ): Context.U[(T, T)] = tunedCross(tuner, filter, rdd)

    def tunedSize(tuner: Tuner)(implicit ev: ClassTag[T]): Context.U[(T, Long)] = rdd
      .map { case t => (t, 1L) }
      .tunedReduce(tuner, _ + _)
  }
}

