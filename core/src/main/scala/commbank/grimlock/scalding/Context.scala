// Copyright 2016,2017,2018,2019 Commonwealth Bank of Australia
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
import com.twitter.scalding.{ TextLine, WritableSequenceFile }
import com.twitter.scalding.parquet.scrooge.FixedPathParquetScrooge
import com.twitter.scalding.parquet.tuple.scheme.ParquetReadSupport
import com.twitter.scalding.parquet.tuple.TypedParquet
import com.twitter.scalding.source.NullSink
import com.twitter.scalding.typed.{ TypedPipe, ValuePipe }

import com.twitter.scrooge.ThriftStruct

import commbank.grimlock.framework.{ ParquetConfig, Persist }
import commbank.grimlock.framework.environment.{ Context => FwContext }

import org.apache.hadoop.io.Writable

import scala.reflect.ClassTag
import scala.util.{ Failure, Success }

import shapeless.<:!<

/**
 * Scalding operating context state.
 *
 * @param flow   The job `FlowDef`.
 * @param mode   The job `Mode`.
 * @param config The job `Config`.
 */
case class Context(flow: FlowDef, mode: Mode, config: Config) extends FwContext[Context] {
  type E[T] = Context.E[T]

  type U[T] = Context.U[T]

  def loadText[
    T : ClassTag
  ](
    file: String,
    parser: Persist.TextParser[T]
  ): (Context.U[T], Context.U[Throwable]) = {
    val pipe = TypedPipe.from(TextLine(file)).flatMap { parser(_) }

    (pipe.collect { case Success(c) => c }, pipe.collect { case Failure(e) => e })
  }

  def loadSequence[
    K <: Writable : Manifest,
    V <: Writable : Manifest,
    T : ClassTag
  ](
    file: String,
    parser: Persist.SequenceParser[K, V, T]
  ): (Context.U[T], Context.U[Throwable]) = {
    val pipe = TypedPipe.from(WritableSequenceFile[K, V](file)).flatMap { case (k, v) => parser(k, v) }

    (pipe.collect { case Success(c) => c }, pipe.collect { case Failure(e) => e })
  }

  def loadParquet[
    X,
    T : ClassTag
  ](
    file: String,
    parser: Persist.ParquetParser[X, T]
   )(implicit
     cfg: ParquetConfig[X, Context]
   ): (Context.U[T], Context.U[Throwable]) = {
    val pipe = cfg.load(this, file).flatMap(parser)

    (pipe.collect { case Success(c) => c }, pipe.collect { case Failure(e) => e })
  }

  val implicits = Implicits

  val library = Library

  def empty[T : ClassTag]: Context.U[T] = TypedPipe.empty

  def from[T : ClassTag](seq: Seq[T]): Context.U[T] = TypedPipe.from(seq)

  def nop(): Unit = {
    val _ = TypedPipe.empty.write(NullSink)(flow, mode)

    ()
  }
}

/** Companion object to `Context` with additional constructors and implicits. */
object Context {
  /** Type for user defined data. */
  type E[T] = ValuePipe[T]

  /** Type for distributed data. */
  type U[T] = TypedPipe[T]

  /**
   * Implicit function that provides a `TypedParquet` implementation, which uses case class for the
   * parquet data definition.
   */
  implicit def toTypedParquet[
    T
  ](implicit
    ev1: T <:!< ThriftStruct,
    ev2: ParquetReadSupport[T]
   ): ParquetConfig[T, Context] = new ParquetConfig[T, Context] {
    def load(context: Context, file: String): Context.U[T] = TypedPipe.from(TypedParquet[T](file))
  }

  /**
   * Implicit function that provides a `ParquetScrooge` implementation, which uses a `ThriftStruct` for
   * the parquet data definition.
   */
  implicit def toScroogeParquet[
    T <: ThriftStruct : Manifest
  ]: ParquetConfig[T, Context] = new ParquetConfig[T, Context] {
    def load(context: Context, file: String): Context.U[T] = TypedPipe.from(new FixedPathParquetScrooge[T](file))
  }

  /** Create context using implicitly defined environment variables. */
  def apply()(implicit config: Config, flow: FlowDef, mode: Mode): Context = Context(flow, mode, config)
}

