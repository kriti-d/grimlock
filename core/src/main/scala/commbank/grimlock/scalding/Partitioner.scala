// Copyright 2014,2015,2016,2017 Commonwealth Bank of Australia
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

package commbank.grimlock.scalding.partition

import commbank.grimlock.framework.{ Cell, Default, Pair, Reducers, Tuner }
import commbank.grimlock.framework.DefaultTuners.TP1
import commbank.grimlock.framework.partition.{ Partitions => FwPartitions }
import commbank.grimlock.framework.utility.UnionTypes.{ In, OneOf }

import commbank.grimlock.scalding.{ Execution, Persist }
import commbank.grimlock.scalding.ScaldingImplicits._

import com.twitter.scalding.typed.TypedPipe

import scala.reflect.ClassTag

import shapeless.Nat

/**
 * Rich wrapper around a `TypedPipe[(I, Cell[P])]`.
 *
 * @param data The `TypedPipe[(I, Cell[P])]`.
 */
case class Partitions[
  P <: Nat,
  I : Ordering
](
  data: TypedPipe[(I, Cell[P])]
) extends FwPartitions[P, I]
  with Persist[(I, Cell[P])] {
  def add(id: I, partition: U[Cell[P]]): U[(I, Cell[P])] = data ++ (partition.map { case c => (id, c) })

  type ForAllTuners[T] = T In OneOf[Default[Execution]]#Or[Default[Pair[Reducers, Execution]]]
  def forAll[
    Q <: Nat,
    T <: Tuner : ForAllTuners
  ](
    fn: (I, U[Cell[P]]) => U[Cell[Q]],
    exclude: List[I],
    tuner: T
  )(implicit
    ev1: ClassTag[I]
  ): U[(I, Cell[Q])] = {
    val (context, identifiers) = tuner.parameters match {
      case Execution(ctx) => (ctx, ids(Default()))
      case Pair(r @ Reducers(_), Execution(ctx)) => (ctx, ids(Default(r)))
    }

    val keys = identifiers
      .collect { case i if !exclude.contains(i) => List(i) }
      .sum
      .getExecution
      .waitFor(context.config, context.mode)
      .getOrElse(throw new Exception("unable to get ids list"))

    forEach(keys, fn)
  }

  def forEach[Q <: Nat](ids: List[I], fn: (I, U[Cell[P]]) => U[Cell[Q]]): U[(I, Cell[Q])] = ids
    .map { case i => fn(i, get(i)).map { case c => (i, c) } }
    .reduce[U[(I, Cell[Q])]]((x, y) => x ++ y)

  def get(id: I): U[Cell[P]] = data.collect { case (i, c) if (id == i) => c }

  type IdsTuners[T] = TP1[T]
  def ids[T <: Tuner : IdsTuners](tuner: T = Default())(implicit ev1: ClassTag[I]): U[I] = data
    .map { case (i, _) => i }
    .tunedDistinct(tuner)

  def merge(ids: List[I]): U[Cell[P]] = data.collect { case (i, c) if (ids.contains(i)) => c }

  def remove(id: I): U[(I, Cell[P])] = data.filter { case (i, _) => i != id }

  type SaveAsTextTuners[T] = PersistParition[T]
  def saveAsText[
    T <: Tuner : SaveAsTextTuners
  ](
    ctx: C,
    file: String,
    writer: TextWriter,
    tuner: T = Default()
  ): U[(I, Cell[P])] = saveText(ctx, file, writer, tuner)
}

