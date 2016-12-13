// Copyright 2014,2015,2016 Commonwealth Bank of Australia
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

package commbank.grimlock.scalding.content

import commbank.grimlock.framework.{ Default, Tuner }
import commbank.grimlock.framework.content.{
  Contents => FwContents,
  IndexedContents => FwIndexedContents,
  Content
}
import commbank.grimlock.framework.position.Position

import commbank.grimlock.scalding.Persist

import com.twitter.scalding.typed.TypedPipe

import shapeless.Nat

/**
 * Rich wrapper around a `TypedPipe[Content]`.
 *
 * @param data The `TypedPipe[Content]`.
 */
case class Contents(data: TypedPipe[Content]) extends FwContents with Persist[Content] {
  type SaveAsTextTuners[T] = PersistParition[T]
  def saveAsText[
    T <: Tuner : SaveAsTextTuners
  ](
    ctx: C,
    file: String,
    writer: TextWriter,
    tuner: T = Default()
  ): U[Content] = saveText(ctx, file, writer, tuner)
}

/**
 * Rich wrapper around a `TypedPipe[(Position[P], Content]`.
 *
 * @param data The `TypedPipe[(Position[P], Content]`.
 */
case class IndexedContents[
  P <: Nat
](
  data: TypedPipe[(Position[P], Content)]
) extends FwIndexedContents[P]
  with Persist[(Position[P], Content)] {
  type SaveAsTextTuners[T] = PersistParition[T]
  def saveAsText[
    T <: Tuner : SaveAsTextTuners
  ](
    ctx: C,
    file: String,
    writer: TextWriter,
    tuner: T = Default()
  ): U[(Position[P], Content)] = saveText(ctx, file, writer, tuner)
}

