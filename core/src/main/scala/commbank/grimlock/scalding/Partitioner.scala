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

import commbank.grimlock.framework.{ Cell, Persist => FwPersist }
import commbank.grimlock.framework.environment.tuner.{ Default, Pair, Reducers, Tuner }
import commbank.grimlock.framework.partition.{ Partitions => FwPartitions }

import commbank.grimlock.scalding.environment._
import commbank.grimlock.scalding.environment.tuner.Execution
import commbank.grimlock.scalding.environment.tuner.ScaldingImplicits._
import commbank.grimlock.scalding.Persist

import scala.reflect.ClassTag

import shapeless.Nat

/** Rich wrapper around a `TypedPipe[(I, Cell[P])]`. */
case class Partitions[
  P <: Nat,
  I : Ordering
](
  context: Context,
  data: Context.U[(I, Cell[P])]
) extends FwPartitions[P, I, Context.U, Context.E, Context]
  with Persist[(I, Cell[P])] {
  def add(id: I, partition: Context.U[Cell[P]]): Context.U[(I, Cell[P])] = data ++ (partition.map { case c => (id, c) })

  def forAll[
    Q <: Nat,
    T <: Tuner
  ](
    fn: (I, Context.U[Cell[P]]) => Context.U[Cell[Q]],
    exclude: List[I],
    tuner: T
  )(implicit
    ev1: ClassTag[I],
    ev2: FwPartitions.ForAllTuners[Context.U, T]
  ): Context.U[(I, Cell[Q])] = {
    val (context, identifiers) = tuner.parameters match {
      case Execution(ctx) => (ctx, ids(Default()))
      case Pair(Reducers(r), Execution(ctx)) => (ctx, ids(Default(r)))
    }

    val keys = identifiers
      .collect { case i if !exclude.contains(i) => List(i) }
      .sum
      .getExecution
      .waitFor(context.config, context.mode)
      .getOrElse(throw new Exception("unable to get ids list"))

    forEach(keys, fn)
  }

  def forEach[Q <: Nat](ids: List[I], fn: (I, Context.U[Cell[P]]) => Context.U[Cell[Q]]): Context.U[(I, Cell[Q])] = ids
    .map { case i => fn(i, get(i)).map { case c => (i, c) } }
    .reduce[Context.U[(I, Cell[Q])]]((x, y) => x ++ y)

  def get(id: I): Context.U[Cell[P]] = data.collect { case (i, c) if (id == i) => c }

  def ids[
    T <: Tuner
  ](
    tuner: T = Default()
  )(implicit
    ev1: ClassTag[I],
    ev2: FwPartitions.IdsTuners[Context.U, T]
  ): Context.U[I] = data.map { case (i, _) => i }.tunedDistinct(tuner)

  def merge(ids: List[I]): Context.U[Cell[P]] = data.collect { case (i, c) if (ids.contains(i)) => c }

  def remove(id: I): Context.U[(I, Cell[P])] = data.filter { case (i, _) => i != id }

  def saveAsText[
    T <: Tuner
  ](
    file: String,
    writer: FwPersist.TextWriter[(I, Cell[P])],
    tuner: T = Default()
  )(implicit
    ev: FwPersist.SaveAsTextTuners[Context.U, T]
  ): Context.U[(I, Cell[P])] = saveText(file, writer, tuner)
}

