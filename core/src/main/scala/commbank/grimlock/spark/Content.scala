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

package commbank.grimlock.spark.content

import commbank.grimlock.framework.{ Persist => FwPersist }
import commbank.grimlock.framework.content.{
  Contents => FwContents,
  IndexedContents => FwIndexedContents,
  Content
}
import commbank.grimlock.framework.environment.tuner.{ Default, Tuner }
import commbank.grimlock.framework.position.Position

import commbank.grimlock.spark.environment.Context
import commbank.grimlock.spark.Persist

import shapeless.Nat

/** Rich wrapper around a `RDD[Content]`. */
case class Contents(
  context: Context,
  data: Context.U[Content]
) extends FwContents[Context.U, Context.E, Context]
  with Persist[Content] {
  def saveAsText[
    T <: Tuner
  ](
    file: String,
    writer: FwPersist.TextWriter[Content],
    tuner: T = Default()
  )(implicit
    ev: FwPersist.SaveAsTextTuners[Context.U, T]
  ): Context.U[Content] = saveText(file, writer, tuner)
}

/** Rich wrapper around a `RDD[(Position[P], Content)]`. */
case class IndexedContents[
  P <: Nat
](
  context: Context,
  data: Context.U[(Position[P], Content)]
) extends FwIndexedContents[P, Context.U, Context.E, Context]
  with Persist[(Position[P], Content)] {
  def saveAsText[
    T <: Tuner
  ](
    file: String,
    writer: FwPersist.TextWriter[(Position[P], Content)],
    tuner: T = Default()
  )(implicit
    ev: FwPersist.SaveAsTextTuners[Context.U, T]
  ): Context.U[(Position[P], Content)] = saveText(file, writer, tuner)
}

