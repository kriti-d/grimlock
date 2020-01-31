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

package commbank.grimlock.scala.environment.tuner

import commbank.grimlock.framework.environment.tuner.Tuner
import commbank.grimlock.framework.position.Position

import commbank.grimlock.scala.environment.Context

import java.io.PrintWriter

import shapeless.HList

private[scala] object ScalaImplicits {
  implicit def positionOrdering[P <: HList] = Position.ordering[P]()

  private[scala] implicit class PairListTuner[K, V](list: Context.U[(K, V)]) {
    def tunedJoin[W](tuner: Tuner, smaller: Context.U[(K, W)]): Context.U[(K, (V, W))] = {
      val m = smaller.groupBy(_._1).mapValues { case lt => lt.map(_._2) }

      list.flatMap { case (k, v) => m
        .get(k)
        .getOrElse(List())
        .map { case w => (k, (v, w)) }
      }
    }

    def tunedLeftJoin[W](tuner: Tuner, smaller: Context.U[(K, W)]): Context.U[(K, (V, Option[W]))] = {
      val m = smaller.groupBy(_._1).mapValues { case lt => lt.map(_._2) }

      list.flatMap { case (k, v) => m
        .get(k)
        .map(ws => ws.map(w => Option(w)))
        .getOrElse(List(None))
        .map { case w => (k, (v, w)) }
      }
    }

    def tunedOuterJoin[W](tuner: Tuner, smaller: Context.U[(K, W)]): Context.U[(K, (Option[V], Option[W]))] = {
      val l = list.groupBy(_._1).mapValues { case lt => lt.map(_._2) }
      val m = smaller.groupBy(_._1).mapValues { case lt => lt.map(_._2) }

      (l.keySet ++ m.keySet).toList.flatMap { case k =>
        for {
          v <- l.get(k).map(vs => vs.map(Option(_))).getOrElse(List(None))
          w <- m.get(k).map(ws => ws.map(Option(_))).getOrElse(List(None))
        } yield (k, (v, w))
      }
    }

    def tunedReduce(tuner: Tuner, reduction: (V, V) => V): Context.U[(K, V)] = list
      .groupBy(_._1)
      .map { case (k, l) => (k, l.map(_._2).reduce(reduction)) }
      .toList

    def tunedSelfJoin(tuner: Tuner, filter: (K, V, V) => Boolean): Context.U[(K, (V, V))] = list
      .tunedJoin(tuner, list)
      .filter { case (k, (v, w)) => filter(k, v, w) }

    def tunedStream[Q](tuner: Tuner, f: (K, Iterator[V]) => TraversableOnce[Q]): Context.U[(K, Q)] = list
      .groupBy(_._1)
      .toList
      .flatMap { case (k, l) => f(k, l.map(_._2).toIterator).map { case q => (k, q) } }
  }

  implicit class ListTuner[T](list: Context.U[T]) {
    def filterWithValue[V](value: V)(f: (T, Option[V]) => Boolean): Context.U[T] = list
      .filter { case t => f(t, Option(value)) }

    def flatMapWithValue[V, Q](value: V)(f: (T, Option[V]) => TraversableOnce[Q]): Context.U[Q] = list
      .flatMap { case t => f(t, Option(value)) }

    def tunedCross[X](
      tuner: Tuner,
      filter: (T, X) => Boolean,
      smaller: Context.U[X]
    ): Context.U[(T, X)] = (for { i <- list } yield { for { j <- smaller } yield (i, j) })
      .flatten
      .filter { case (l, r) => filter(l, r) }

    def tunedDistinct(tuner: Tuner)(implicit ev: Ordering[T]): Context.U[T] = list.distinct

    def tunedRedistribute(tuner: Tuner): Context.U[T] = list

    def tunedSaveAsText(ctx: Context, tuner: Tuner, file: String) = {
      val writer = new PrintWriter(file)

      writer.write(list.mkString("\n"))

      writer.close
    }

    def tunedSelfCross(tuner: Tuner, filter: (T, T) => Boolean): Context.U[(T, T)] = tunedCross(tuner, filter, list)

    def tunedSize(tuner: Tuner): Context.U[(T, Long)] = list
      .map { case t => (t, 1L) }
      .tunedReduce(tuner, _ + _)
  }
}

