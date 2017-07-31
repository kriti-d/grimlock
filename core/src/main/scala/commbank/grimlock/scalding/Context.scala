// Copyright 2016,2017 Commonwealth Bank of Australia
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

import au.com.cba.omnia.ebenezer.scrooge.ParquetScroogeSource

import cascading.flow.FlowDef

import com.twitter.scalding.{ Config, Mode }
import com.twitter.scalding.{ TextLine, WritableSequenceFile }
import com.twitter.scalding.source.NullSink
import com.twitter.scalding.typed.{ TypedPipe, ValuePipe }
import com.twitter.scrooge.ThriftStruct

import commbank.grimlock.framework.Cell
import commbank.grimlock.framework.environment.{ Context => FwContext }

import commbank.grimlock.scalding.environment.implicits.Implicits

import org.apache.hadoop.io.Writable

import scala.reflect.ClassTag

import shapeless.Nat

/**
 * Scalding operating context state.
 *
 * @param flow   The job `FlowDef`.
 * @param mode   The job `Mode`.
 * @param config The job `Config`.
 */
case class Context(flow: FlowDef, mode: Mode, config: Config) extends FwContext[Context.U, Context.E] {
  def loadText[P <: Nat](file: String, parser: Cell.TextParser[P]): (Context.U[Cell[P]], Context.U[String]) = {
    val pipe = TypedPipe.from(TextLine(file)).flatMap { parser(_) }

    (pipe.collect { case Right(c) => c }, pipe.collect { case Left(e) => e })
  }

  def loadSequence[
    K <: Writable : Manifest,
    V <: Writable : Manifest,
    P <: Nat
  ](
    file: String,
    parser: Cell.SequenceParser[K, V, P]
  ): (Context.U[Cell[P]], Context.U[String]) = {
    val pipe = TypedPipe.from(WritableSequenceFile[K, V](file)).flatMap { case (k, v) => parser(k, v) }

    (pipe.collect { case Right(c) => c }, pipe.collect { case Left(e) => e })
  }

  def loadParquet[
    T <: ThriftStruct : Manifest,
    P <: Nat
  ](
    file: String,
    parser: Cell.ParquetParser[T, P]
  ): (Context.U[Cell[P]], Context.U[String]) = {
    val pipe = TypedPipe.from(ParquetScroogeSource[T](file)).flatMap { case s => parser(s) }

    (pipe.collect { case Right(c) => c }, pipe.collect { case Left(e) => e })
  }

  val implicits = Implicits(this)

  def empty[T : ClassTag]: Context.U[T] = TypedPipe.empty

  def from[T : ClassTag](seq: Seq[T]): Context.U[T] = TypedPipe.from(seq)

  def nop(): Unit = {
    import implicits.environment._

    val _ = TypedPipe.empty.write(NullSink)

    ()
  }
}

/** Companion object to `Context` with additional constructors and implicit. */
object Context {
  /** Type for user defined data. */
  type E[A] = ValuePipe[A]

  /** Type for distributed data. */
  type U[A] = TypedPipe[A]

  def apply()(implicit config: Config, flow: FlowDef, mode: Mode): Context = Context(flow, mode, config)
}

