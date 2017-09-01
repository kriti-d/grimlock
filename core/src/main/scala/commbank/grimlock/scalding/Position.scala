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

package commbank.grimlock.scalding.position

import commbank.grimlock.framework.{ Persist => FwPersist }
import commbank.grimlock.framework.environment.tuner.{ Default, Tuner }
import commbank.grimlock.framework.position.{ Position, Positions => FwPositions, Slice }

import commbank.grimlock.scalding.environment.Context
import commbank.grimlock.scalding.environment.tuner.ScaldingImplicits._
import commbank.grimlock.scalding.Persist

import shapeless.{ =:!=, Nat }
import shapeless.nat._0

/** Rich wrapper around a `TypedPipe[Position[P]]`. */
case class Positions[P <: Nat](data: Context.U[Position[P]]) extends FwPositions[P, Context] with Persist[Position[P]] {
  def names[
    T <: Tuner
  ](
    slice: Slice[P],
    tuner: T = Default()
  )(implicit
    ev1: slice.S =:!= _0,
    ev2: FwPositions.NamesTuner[Context.U, T]
  ): Context.U[Position[slice.S]] = data.map { case p => slice.selected(p) }.tunedDistinct(tuner)(Position.ordering())

  def saveAsText[
    T <: Tuner
  ](
    context: Context,
    file: String,
    writer: FwPersist.TextWriter[Position[P]],
    tuner: T = Default()
  )(implicit
    ev: FwPersist.SaveAsTextTuner[Context.U, T]
  ): Context.U[Position[P]] = saveText(context, file, writer, tuner)

  protected def slice(keep: Boolean, f: Position[P] => Boolean): Context.U[Position[P]] = data
    .filter { case p => !keep ^ f(p) }
}

