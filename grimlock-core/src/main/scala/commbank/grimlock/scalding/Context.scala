// Copyright 2016,2017,2018,2019,2020 Commonwealth Bank of Australia
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

package commbank.grimlock.scalding.environment

import cascading.flow.FlowDef

import com.twitter.scalding.{ Config, Mode }
import com.twitter.scalding.source.NullSink
import com.twitter.scalding.typed.{ TypedPipe, ValuePipe }

import commbank.grimlock.framework.Persist
import commbank.grimlock.framework.environment.{ Encoder => FwEncoder, MatrixContext }

import scala.util.{ Failure, Success }

/**
 * Scalding operating context state.
 *
 * @param flow   The job `FlowDef`.
 * @param mode   The job `Mode`.
 * @param config The job `Config`.
 */
case class Context(flow: FlowDef, mode: Mode, config: Config) extends MatrixContext[Context] {
  type D[T] = Context.D[T]

  type E[T] = Context.E[T]

  type U[T] = Context.U[T]

  /** Implicit Scalding encoder defined for all types `T`. */
  implicit def encoder[T]: D[T] = Context.encoder

  def read[
    X,
    T
  ](
    location: String,
    loader: Persist.Loader[X, Context],
    parser: Persist.Parser[X, T]
  )(implicit
    enc: D[T]
  ): (U[T], U[Throwable]) = {
    val pipe = loader.load(this, location).flatMap(parser)

    (pipe.collect { case Success(c) => c }, pipe.collect { case Failure(e) => e })
  }

  val implicits = Implicits

  val library = Library

  def empty[T : D]: Context.U[T] = TypedPipe.empty

  def from[T : D](seq: Seq[T]): Context.U[T] = TypedPipe.from(seq)

  def nop(): Unit = {
    val _ = TypedPipe.empty.write(NullSink)(flow, mode)

    ()
  }
}

/** Companion object to `Context` with additional constructors and implicits. */
object Context {
  /** Type class needed to encode data as a distributed list. */
  type D[T] = Encoder[T]

  /** Type for user defined data. */
  type E[T] = ValuePipe[T]

  /** Type for distributed data. */
  type U[T] = TypedPipe[T]

  /** Implicit Scalding encoder defined for all types `T`. */
  implicit def encoder[T]: D[T] = new Encoder[T] { }

  /** Create context using implicitly defined environment variables. */
  def apply()(implicit config: Config, flow: FlowDef, mode: Mode): Context = Context(flow, mode, config)
}

/**
 * Type class constraints for encoding a type `T` in a Scalding `TypedPipe[T]`. This trait is empty as there are no
 * constraints.
 */
trait Encoder[T] extends FwEncoder[T]

