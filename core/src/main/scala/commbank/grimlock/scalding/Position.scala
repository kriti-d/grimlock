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

import commbank.grimlock.framework.{ Default, Tuner }
import commbank.grimlock.framework.DefaultTuners.TP1
import commbank.grimlock.framework.position.{
  Position,
  Positions => FwPositions,
  Slice
}
import commbank.grimlock.framework.utility.=:!=

import commbank.grimlock.scalding.Persist
import commbank.grimlock.scalding.ScaldingImplicits._

import com.twitter.scalding.typed.TypedPipe

import scala.reflect.ClassTag

import shapeless.Nat
import shapeless.nat.{ _0, _1 }
import shapeless.ops.nat.Diff

/**
 * Rich wrapper around a `TypedPipe[Position[P]]`.
 *
 * @param data The `TypedPipe[Position[P]]`.
 */
case class Positions[
  L <: Nat,
  P <: Nat
](
  data: TypedPipe[Position[P]]
) extends FwPositions[L, P]
  with Persist[Position[P]] {
  type NamesTuners[T] = TP1[T]
  def names[
    T <: Tuner : NamesTuners
  ](
    slice: Slice[L, P],
    tuner: T = Default()
  )(implicit
    ev1: slice.S =:!= _0,
    ev2: ClassTag[Position[slice.S]],
    ev3: Diff.Aux[P, _1, L]
  ): U[Position[slice.S]] = data.map { case p => slice.selected(p) }.tunedDistinct(tuner)(Position.ordering())

  type SaveAsTextTuners[T] = PersistParition[T]
  def saveAsText[
    T <: Tuner : SaveAsTextTuners
  ](
    ctx: C,
    file: String,
    writer: TextWriter,
    tuner: T = Default()
  ): U[Position[P]] = saveText(ctx, file, writer, tuner)

  protected def slice(
    keep: Boolean,
    f: Position[P] => Boolean
  )(implicit
    ev: ClassTag[Position[P]]
  ): U[Position[P]] = data.filter { case p => !keep ^ f(p) }
}

