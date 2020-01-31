// Copyright 2019,2020 Commonwealth Bank of Australia
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

package commbank.grimlock.scala.environment

import commbank.grimlock.framework.Persist
import commbank.grimlock.framework.environment.{ Encoder => FwEncoder, MatrixContext }

import scala.util.{ Failure, Success }

/** Scala operating context state. */
case class Context() extends MatrixContext[Context] {
  type D[T] = Context.D[T]

  type E[T] = Context.E[T]

  type U[T] = Context.U[T]

  /** Implicit Scala encoder defined for all types `T`. */
  implicit def encoder[T]: D[T] = Context.encoder

  def read[
    X,
    T
  ](
    file: String,
    loader: Persist.Loader[X, Context],
    parser: Persist.Parser[X, T]
  )(implicit
    enc: D[T]
  ): (U[T], U[Throwable]) = {
    val list = loader.load(this, file).flatMap(parser)

    (list.collect { case Success(c) => c }, list.collect { case Failure(e) => e })
  }

  val implicits = Implicits

  val library = Library

  def empty[T : D]: Context.U[T] = List.empty

  def from[T : D](seq: Seq[T]): Context.U[T] = seq.toList

  def nop(): Unit = ()
}

/** Companion object to `Context` with additional constructors and implicits. */
object Context {
  /** Type class needed to encode data as a distributed list. */
  type D[T] = Encoder[T]

  /** Type for user defined data. */
  type E[T] = T

  /** Type for distributed data. */
  type U[T] = List[T]

  /** Implicit Scala encoder defined for all types `T`. */
  implicit def encoder[T]: D[T] = new Encoder[T] { }
}

/** Type class constraints for encoding a type `T` in a Scala `List[T]`. Trait is empty as there are no constraints. */
trait Encoder[T] extends FwEncoder[T]

