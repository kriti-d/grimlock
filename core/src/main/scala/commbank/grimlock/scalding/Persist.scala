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

package commbank.grimlock.scalding

import commbank.grimlock.framework.{ Persist => FwPersist, Tuner }

import commbank.grimlock.scalding.environment.{ DistributedData, Environment }
import commbank.grimlock.scalding.ScaldingImplicits._

/** Trait for peristing a Scalding `TypedPipe`. */
trait Persist[X] extends FwPersist[X] with DistributedData with Environment {
  protected def saveText[T <: Tuner : PersistParition](ctx: C, file: String, writer: TextWriter, tuner: T): U[X] = {
    data
      .flatMap { case x => writer(x) }
      .tunedSaveAsText(ctx, tuner, file)

    data
  }
}

