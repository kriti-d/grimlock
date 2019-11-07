// Copyright 2015,2016,2017,2018,2019 Commonwealth Bank of Australia
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

import commbank.grimlock.framework.{ Persist => FwPersist, SaveStringsAsText => FwSaveStringsAsText }
import commbank.grimlock.framework.environment.tuner.{ Default, Tuner }

import commbank.grimlock.spark.environment.Context
import commbank.grimlock.spark.environment.tuner.SparkImplicits._

/** Trait for peristing a `RDD`. */
trait Persist[X] extends FwPersist[X, Context] {
  /** The underlying data. */
  val data: Context.U[X]

  protected def saveText[
    T <: Tuner
  ](
    context: Context,
    file: String,
    writer: FwPersist.TextWriter[X],
    tuner: T
  ): Context.U[X] = {
    data
      .flatMap { case x => writer(x) }
      .tunedSaveAsText(context, tuner, file)

    data
  }
}

/** Case class that enriches a `RDD` of strings with saveAsText functionality. */
case class SaveStringsAsText(data: Context.U[String]) extends FwSaveStringsAsText[Context] with Persist[String] {
  def saveAsText[
    T <: Tuner
  ](
    context: Context,
    file: String,
    tuner: T = Default()
  )(implicit
    ev: FwPersist.SaveAsTextTuner[Context.U, T]
  ): Context.U[String] = saveText(context, file, Option(_), tuner)
}

