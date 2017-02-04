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

package commbank.grimlock.spark

import commbank.grimlock.framework.{
  Default,
  InMemory,
  MapMapSideJoin => FwMapMapSideJoin,
  MapSideJoin,
  Redistribute,
  Reducers,
  SetMapSideJoin => FwSetMapSideJoin,
  Tuner
}

import commbank.grimlock.spark.environment.Context

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

private[spark] case class MapMapSideJoin[K, V, W]() extends FwMapMapSideJoin[K, V, W, RDD, ({type E[X]=X})#E] {
  def compact(
    smaller: RDD[(K, W)]
  )(implicit
    ev1: ClassTag[K],
    ev2: ClassTag[W],
    ev3: Ordering[K]
  ): T = smaller.collectAsMap().toMap
}

private[spark] case class SetMapSideJoin[K, V]() extends FwSetMapSideJoin[K, V, RDD, ({type E[X]=X})#E] {
  def compact(
    smaller: RDD[(K, Unit)]
  )(implicit
    ev1: ClassTag[K],
    ev2: ClassTag[Unit],
    ev3: Ordering[K]
  ): T = smaller.map { case (k, _) => k }.collect().toSet
}

private[spark] object SparkImplicits {
  private[spark] implicit class PairRDDTuner[K, V](rdd: RDD[(K, V)])(implicit kt: ClassTag[K], vt: ClassTag[V]) {
    def tunedJoin[W](
      tuner: Tuner,
      smaller: RDD[(K, W)],
      msj: Option[MapSideJoin[K, V, W, RDD, ({type E[X]=X})#E]] = None
    )(implicit
      ev1: Ordering[K],
      ev2: ClassTag[W]
    ): RDD[(K, (V, W))] = (tuner, msj) match {
      case (InMemory(_), Some(m)) =>
        rdd.flatMapWithValue(m.compact(smaller)) { case ((k, v), t) =>
          m.join(k, v, t.getOrElse(m.empty)).map { case w => (k, (v, w)) }
        }
      case (Default(Reducers(reducers)), _) => rdd.join(smaller, reducers)
      case _ => rdd.join(smaller)
    }

    def tunedLeftJoin[W](
      tuner: Tuner,
      smaller: RDD[(K, W)],
      msj: Option[MapSideJoin[K, V, W, RDD, ({type E[X]=X})#E]] = None
    )(implicit
      ev1: Ordering[K],
      ev2: ClassTag[W]
    ): RDD[(K, (V, Option[W]))] = (tuner, msj) match {
      case (InMemory(_), Some(m)) =>
        // TODO: broadcast value?
        val value = m.compact(smaller)

        rdd.map { case (k, v) => (k, (v, m.join(k, v, value))) }
      case (Default(Reducers(reducers)), _) => rdd.leftOuterJoin(smaller, reducers)
      case _ => rdd.leftOuterJoin(smaller)
    }

    def tunedOuterJoin[W](
      tuner: Tuner,
      smaller: RDD[(K, W)]
    ): RDD[(K, (Option[V], Option[W]))] = tuner.parameters match {
      case Reducers(reducers) => rdd.fullOuterJoin(smaller, reducers)
      case _ => rdd.fullOuterJoin(smaller)
    }

    def tunedReduce(tuner: Tuner, reduction: (V, V) => V): RDD[(K, V)] = tuner.parameters match {
      case Reducers(reducers) => rdd.reduceByKey(reduction, reducers)
      case _ => rdd.reduceByKey(reduction)
    }

    def tunedSelfJoin(tuner: Tuner, filter: (K, V, V) => Boolean)(implicit ev: Ordering[K]): RDD[(K, (V, V))] = rdd
      .tunedJoin(tuner, rdd)
      .filter { case (k, (v, w)) => filter(k, v, w) }

    def tunedStream[Q](tuner: Tuner, f: (K, Iterator[V]) => TraversableOnce[Q]): RDD[(K, Q)] = {
      val grouped = tuner.parameters match {
        case Reducers(reducers) => rdd.groupByKey(reducers)
        case _ => rdd.groupByKey
      }

      grouped.flatMap { case (k, i) => f(k, i.toIterator).map { case q => (k, q) } }
    }
  }

  private[spark] implicit class RDDTuner[T](rdd: RDD[T]) {
    // TODO: broadcast in WithValue functions?
    def filterWithValue[V](
      value: V
    )(
      f: (T, Option[V]) => Boolean
    ): RDD[T] = rdd.filter { case t => f(t, Option(value)) }

    def flatMapWithValue[V, Q](
      value: V
    )(
      f: (T, Option[V]) => TraversableOnce[Q]
    )(implicit
      ev: ClassTag[Q]
    ): RDD[Q] = rdd.flatMap { case t => f(t, Option(value)) }

    def tunedCross[X](
      tuner: Tuner,
      filter: (T, X) => Boolean,
      smaller: RDD[X]
    )(implicit
      ev1: ClassTag[T],
      ev2: ClassTag[X]
    ): RDD[(T, X)] = rdd.cartesian(smaller).filter { case (l, r) => filter(l, r) }

    def tunedDistinct(tuner: Tuner)(implicit ev: Ordering[T]): RDD[T] = tuner.parameters match {
      case Reducers(reducers) => rdd.distinct(reducers)(ev)
      case _ => rdd.distinct()
    }

    def tunedRedistribute(tuner: Tuner): RDD[T] = tuner match {
      case Redistribute(reducers) => rdd.repartition(reducers)
      case _ => rdd
    }

    def tunedSaveAsText(ctx: Context, tuner: Tuner, file: String) = rdd.tunedRedistribute(tuner).saveAsTextFile(file)

    def tunedSelfCross(
      tuner: Tuner,
      filter: (T, T) => Boolean
    )(implicit
      ev: ClassTag[T]
    ): RDD[(T, T)] = tunedCross(tuner, filter, rdd)

    def tunedSize(tuner: Tuner)(implicit ev: ClassTag[T]): RDD[(T, Long)] = rdd
      .map { case t => (t, 1L) }
      .tunedReduce(tuner, _ + _)
  }
}

