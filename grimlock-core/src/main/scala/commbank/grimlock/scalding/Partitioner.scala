// Copyright 2014,2015,2016,2017,2018,2019 Commonwealth Bank of Australia
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
import commbank.grimlock.framework.environment.tuner.{ Default, Tuner }
import commbank.grimlock.framework.partition.{ Partitions => FwPartitions }

import commbank.grimlock.scalding.environment.Context
import commbank.grimlock.scalding.environment.tuner.ScaldingImplicits._
import commbank.grimlock.scalding.Persist

import scala.reflect.ClassTag

import shapeless.HList

/** Rich wrapper around a `TypedPipe[(I, Cell[P])]`. */
case class Partitions[
  P <: HList,
  I : Ordering
](
  data: Context.U[(I, Cell[P])]
) extends FwPartitions[P, I, Context]
  with Persist[(I, Cell[P])] {
  def add(id: I, partition: Context.U[Cell[P]]): Context.U[(I, Cell[P])] = data ++ (partition.map { case c => (id, c) })

  def forAll[
    Q <: HList,
    T <: Tuner
  ](
    context: Context,
    fn: (I, Context.U[Cell[P]]) => Context.U[Cell[Q]],
    exclude: List[I],
    tuner: T = Default()
  )(implicit
    ev1: ClassTag[I],
    ev2: FwPartitions.ForAllTuner[Context.U, T]
  ): Context.U[(I, Cell[Q])] = {
    val ids = data
      .collect { case (i, _) if !exclude.contains(i) => i }
      .tunedDistinct(tuner)
      .toIterableExecution
      .waitFor(context.config, context.mode)
      .getOrElse(Iterable.empty)
      .toList

    forEach(ids, fn)
  }

  def forEach[
    Q <: HList
  ](
    ids: List[I],
    fn: (I, Context.U[Cell[P]]) => Context.U[Cell[Q]]
  ): Context.U[(I, Cell[Q])] = ids
    .map { case i => fn(i, get(i)).map { case c => (i, c) } }
    .reduce[Context.U[(I, Cell[Q])]]((x, y) => x ++ y)

  def get(id: I): Context.U[Cell[P]] = data.collect { case (i, c) if (id == i) => c }

  def ids[
    T <: Tuner
  ](
    tuner: T = Default()
  )(implicit
    ev1: ClassTag[I],
    ev2: FwPartitions.IdsTuner[Context.U, T]
  ): Context.U[I] = data
    .map { case (i, _) => i }
    .tunedDistinct(tuner)

  def merge(ids: List[I]): Context.U[Cell[P]] = data.collect { case (i, c) if (ids.contains(i)) => c }

  def remove(id: I): Context.U[(I, Cell[P])] = data.filter { case (i, _) => i != id }

  def saveAsText[
    T <: Tuner
  ](
    context: Context,
    file: String,
    writer: FwPersist.TextWriter[(I, Cell[P])],
    tuner: T = Default()
  )(implicit
    ev: FwPersist.SaveAsTextTuner[Context.U, T]
  ): Context.U[(I, Cell[P])] = saveText(context, file, writer, tuner)
}

