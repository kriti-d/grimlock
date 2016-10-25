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

package commbank.grimlock.spark.content

import commbank.grimlock.framework.content.{
  Contents => FwContents,
  IndexedContents => FwIndexedContents,
  Content
}
import commbank.grimlock.framework.position.Position

import commbank.grimlock.spark.Persist

import org.apache.spark.rdd.RDD

import shapeless.Nat

/**
 * Rich wrapper around a `RDD[Content]`.
 *
 * @param data The `RDD[Content]`.
 */
case class Contents(data: RDD[Content]) extends FwContents with Persist[Content] {
  def saveAsText(ctx: C, file: String, writer: TextWriter): U[Content] = saveText(ctx, file, writer)
}

/**
 * Rich wrapper around a `RDD[(Position[P], Content)]`.
 *
 * @param data The `RDD[(Position[P], Content)]`.
 */
case class IndexedContents[
  P <: Nat
](
  data: RDD[(Position[P], Content)]
) extends FwIndexedContents[P]
  with Persist[(Position[P], Content)] {
  def saveAsText(ctx: C, file: String, writer: TextWriter): U[(Position[P], Content)] = saveText(ctx, file, writer)
}

