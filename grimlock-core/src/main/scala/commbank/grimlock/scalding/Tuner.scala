// Copyright 2015,2016,2017 Commonwealth Bank of Australia
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

package commbank.grimlock.scalding.environment.tuner

import com.twitter.algebird.Semigroup

import com.twitter.scalding.TextLine
import com.twitter.scalding.typed.{ Grouped, SortedGrouped, TypedSink }

import commbank.grimlock.framework.environment.tuner.{
  Default,
  InMemory,
  MapMapSideJoin => FwMapMapSideJoin,
  MapSideJoin,
  Redistribute,
  Reducers,
  SetMapSideJoin => FwSetMapSideJoin,
  Tuner,
  Unbalanced
}
import commbank.grimlock.framework.position.Position

import commbank.grimlock.scalding.environment.Context

import scala.reflect.ClassTag
import scala.util.Random

import shapeless.HList

private[scalding] case class MapMapSideJoin[K, V, W]() extends FwMapMapSideJoin[K, V, W, Context.U, Context.E] {
  def compact(
    smaller: Context.U[(K, W)]
  )(implicit
    ev1: ClassTag[K],
    ev2: ClassTag[W],
    ev3: Ordering[K]
  ): Context.E[T] = smaller.map { case (k, w) => Map(k -> w) }.sum(semigroup)

  private val semigroup = new Semigroup[T] { def plus(l: T, r: T): T = l ++ r }
}

private[scalding] case class SetMapSideJoin[K, V]() extends FwSetMapSideJoin[K, V, Context.U, Context.E] {
  def compact(
    smaller: Context.U[(K, Unit)]
  )(implicit
    ev1: ClassTag[K],
    ev2: ClassTag[Unit],
    ev3: Ordering[K]
  ): Context.E[T] = smaller.map { case (k, w) => Set(k) }.sum(semigroup)

  private val semigroup = new Semigroup[T] { def plus(l: T, r: T): T = l ++ r }
}

private[scalding] object ScaldingImplicits {
  implicit def positionOrdering[P <: HList] = Position.ordering[P]()

  implicit def serialisePosition[P <: HList](key: Position[P]): Array[Byte] = key
    .toShortString("|")
    .toCharArray
    .map { case c => c.toByte }

  implicit def serialisePairwisePosition[
    P <: HList
  ](
    key: (Position[P], Position[P])
  ): Array[Byte] = (key._1.toShortString("|") + "|" + key._2.toShortString("|")).toCharArray.map { case c => c.toByte }

  implicit class GroupedTuner[K, V](grouped: Grouped[K, V]) {
    def tunedStream[Q](
      tuner: Tuner,
      f: (K, Iterator[V]) => TraversableOnce[Q]
    )(implicit
      ev: Ordering[K]
    ): Context.U[(K, Q)] = {
      val tuned = tuner.parameters match {
        case Reducers(reducers) => grouped.withReducers(reducers)
        case _ => grouped
      }

      tuned.mapGroup { case (key, itr) => f(key, itr).toIterator }
    }
  }

  implicit class SortedGroupedTuner[K, V](sorted: SortedGrouped[K, V]) {
    def tunedStream[Q](
      tuner: Tuner,
      f: (K, Iterator[V]) => TraversableOnce[Q]
    )(implicit
      ev: Ordering[K]
    ): Context.U[(K, Q)] = {
      val tuned = tuner.parameters match {
        case Reducers(reducers) => sorted.withReducers(reducers)
        case _ => sorted
      }

      tuned.mapGroup { case (key, itr) => f(key, itr).toIterator }
    }
  }

  private[scalding] implicit class PairPipeTuner[K, V](pipe: Context.U[(K, V)]) {
    def tunedJoin[W](
      tuner: Tuner,
      smaller: Context.U[(K, W)],
      msj: Option[MapSideJoin[K, V, W, Context.U, Context.E]] = None
    )(implicit
      ev1: Ordering[K],
      ev2: K => Array[Byte],
      ev3: ClassTag[K],
      ev4: ClassTag[W]
    ): Context.U[(K, (V, W))] = (tuner, msj) match {
      case (InMemory(_), Some(m)) => pipe.flatMapWithValue(m.compact(smaller)) { case ((k, v), t) =>
          m.join(k, v, t.getOrElse(m.empty)).map { case w => (k, (v, w)) }
        }
      case (Default(Reducers(reducers)), _) => pipe.group.withReducers(reducers).join(smaller)
      case (Unbalanced(Reducers(reducers)), _) => pipe.sketch(reducers).join(smaller)
      case _ => pipe.group.join(smaller)
    }

    def tunedLeftJoin[W](
      tuner: Tuner,
      smaller: Context.U[(K, W)],
      msj: Option[MapSideJoin[K, V, W, Context.U, Context.E]] = None
    )(implicit
      ev1: Ordering[K],
      ev2: K => Array[Byte],
      ev3: ClassTag[K],
      ev4: ClassTag[W]
    ): Context.U[(K, (V, Option[W]))] = (tuner, msj) match {
      case (InMemory(_), Some(m)) => pipe.mapWithValue(m.compact(smaller)) { case ((k, v), t) =>
        (k, (v, m.join(k, v, t.getOrElse(m.empty))))
      }
      case (Default(Reducers(reducers)), _) => pipe.group.withReducers(reducers).leftJoin(smaller)
      case (Unbalanced(Reducers(reducers)), _) => pipe.sketch(reducers).leftJoin(smaller)
      case _ => pipe.group.leftJoin(smaller)
    }

    def tunedOuterJoin[W](
      tuner: Tuner,
      smaller: Context.U[(K, W)]
    )(implicit
      ev1: Ordering[K],
      ev2: K => Array[Byte]
    ): Context.U[(K, (Option[V], Option[W]))] = tuner.parameters match {
      case Reducers(reducers) => pipe.group.withReducers(reducers).outerJoin(smaller)
      case _ => pipe.group.outerJoin(smaller)
    }

    def tunedReduce(tuner: Tuner, reduction: (V, V) => V)(implicit ev: Ordering[K]): Context.U[(K, V)] = pipe
      .tuneReducers(tuner)
      .reduce(reduction)

    def tuneReducers(tuner: Tuner)(implicit ev: Ordering[K]): Grouped[K, V] = tuner.parameters match {
      case Reducers(reducers) => pipe.group.withReducers(reducers)
      case _ => pipe.group
    }

    def tunedSelfJoin(
      tuner: Tuner,
      filter: (K, V, V) => Boolean
    )(implicit
      ev1: Ordering[K],
      ev2: K => Array[Byte]
    ): Context.U[(K, (V, V))] = tuner match {
      case InMemory(_) =>
        pipe.flatMapWithValue(pipe.group.toList.map { case (k, l) => Map(k -> l) }.sum) {
          case ((k, v), Some(m)) =>
            m.get(k).map { case l => l.collect { case w if filter(k, v, w) => (k, (v, w)) } }getOrElse(List())
          case _ => List()
        }
      case Default(Reducers(reducers)) => pipe.group.withReducers(reducers).cogroup(pipe) { case (k, v, w) =>
          v.flatMap { case x => w.collect { case y if filter(k, x, y) => (x, y) } }
        }
      case Unbalanced(Reducers(reducers)) => pipe.sketch(reducers).cogroup(pipe) { case (k, v, w) =>
          w.collect { case y if filter(k, v, y) => (v, y) }.toIterator
        }
      case _ => pipe.group.cogroup(pipe) { case (k, v, w) =>
          v.flatMap { case x => w.collect { case y if filter(k, x, y) => (x, y) } }
        }
    }
  }

  implicit class PipeTuner[P](pipe: Context.U[P]) {
    def tunedCross[X](
      tuner: Tuner,
      filter: (P, X) => Boolean,
      smaller: Context.U[X]
    ): Context.U[(P, X)] = tuner match {
      case InMemory(_) => pipe.flatMapWithValue(smaller.map { case p => List(p) }.sum) { case (v, w) =>
          w.map { case l => l.collect { case x if filter(v, x) => (v, x) } }.getOrElse(List())
        }
      case Default(Reducers(reducers)) => pipe
        .flatMap { case s => (0 until reducers).map { case k => (k, s) } }
        .group
        .withReducers(reducers)
        .cogroup(smaller.map { case p => (Random.nextInt(reducers), p) }.group) { case (_, v, w) =>
          v.flatMap { case x => w.collect { case y if filter(x, y) => (x, y) } }
        }
        .values
      case _ => pipe.map { case t => ((), t) }.cogroup(smaller.map { case x => ((), x) }) { case (_, v, w) =>
          v.flatMap { case x => w.collect { case y if filter(x, y) => (x, y) } }
        }
        .values
    }

    def tunedDistinct(tuner: Tuner)(implicit ev: Ordering[P]): Context.U[P] = tuner.parameters match {
      case Reducers(reducers) => pipe.asKeys.withReducers(reducers).sum.keys
      case _ => pipe.asKeys.sum.keys
    }

    def tunedRedistribute(tuner: Tuner): Context.U[P] = tuner match {
      case Redistribute(reducers) => pipe.shard(reducers)
      case _ => pipe
    }

    def tunedSaveAsText(context: Context, tuner: Tuner, file: String) = pipe
      .tunedRedistribute(tuner)
      .write(TypedSink(TextLine(file)))(context.flow, context.mode)

    def tunedSelfCross(
      tuner: Tuner,
      filter: (P, P) => Boolean
    ): Context.U[(P, P)] = tunedCross(tuner, filter, pipe)

    def tunedSize(tuner: Tuner)(implicit ev: Ordering[P]): Context.U[(P, Long)] = tuner.parameters match {
      case Reducers(reducers) => pipe.asKeys.withReducers(reducers).size
      case _ => pipe.asKeys.size
    }
  }
}

