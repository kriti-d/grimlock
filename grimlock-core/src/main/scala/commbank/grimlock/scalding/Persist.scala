// Copyright 2015,2016,2017,2018,2019,2020 Commonwealth Bank of Australia
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

import com.twitter.scalding.{ TextLine, WritableSequenceFile }
import com.twitter.scalding.parquet.scrooge.FixedPathParquetScrooge
import com.twitter.scalding.parquet.tuple.scheme.ParquetReadSupport
import com.twitter.scalding.parquet.tuple.TypedParquet
import com.twitter.scalding.typed.TypedPipe

import com.twitter.scrooge.ThriftStruct

import commbank.grimlock.framework.{ Persist => FwPersist, SaveStringsAsText => FwSaveStringsAsText }
import commbank.grimlock.framework.environment.tuner.{ Default, Tuner }

import commbank.grimlock.scalding.environment.Context
import commbank.grimlock.scalding.environment.tuner.ScaldingImplicits._

import org.apache.hadoop.io.Writable

import shapeless.<:!<

/** Trait for peristing a `TypedPipe`. */
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

/** Companion object to `Persist` with additional methods. */
object Persist {
  /** Scalding text file loader implementation. */
  val textLoader = new FwPersist.Loader[String, Context] {
    def load(context: Context, file: String): Context.U[String] = TypedPipe.from(TextLine(file))
  }

  /**
   * Function that provides a `ParquetScrooge` implementation, which uses a `ThriftStruct` for the parquet data
   * definition.
   */
  def parquetScroogeLoader[
    T <: ThriftStruct : Manifest
  ]: FwPersist.Loader[T, Context] = new FwPersist.Loader[T, Context] {
    def load(context: Context, file: String): Context.U[T] = TypedPipe.from(new FixedPathParquetScrooge[T](file))
  }

  /** Function that provides a `TypedParquet` implementation, which uses case class for the parquet data definition. */
  def typedParquetLoader[
    T
  ](implicit
    ev1: T <:!< ThriftStruct,
    ev2: ParquetReadSupport[T]
   ): FwPersist.Loader[T, Context] = new FwPersist.Loader[T, Context] {
    def load(context: Context, file: String): Context.U[T] = TypedPipe.from(TypedParquet[T](file))
  }

  /** Function that provides scalding sequence file loader implementation.*/
  def sequenceLoader[
    K <: Writable : Manifest,
    V <: Writable : Manifest
  ] = new FwPersist.Loader[(K, V), Context] {
    def load(context: Context, file: String): Context.U[(K, V)] = TypedPipe.from(WritableSequenceFile[K, V](file))
  }
}

/** Case class that enriches a `TypedPipe` of strings with saveAsText functionality. */
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

