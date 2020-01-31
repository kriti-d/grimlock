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

package commbank.grimlock.scala

import commbank.grimlock.framework.{ Persist => FwPersist, SaveStringsAsText => FwSaveStringsAsText }
import commbank.grimlock.framework.environment.tuner.{ Default, Tuner }

import commbank.grimlock.scala.environment.Context
import commbank.grimlock.scala.environment.tuner.ScalaImplicits._

import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{ SequenceFile, Writable }
import org.apache.parquet.avro.AvroParquetReader

import scala.io.Source

/** Trait for peristing a `List`. */
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
  /** Scala parquet loader implementation. The method use `AvroParquetReader` to read parquet. */
  val parquetGenericRecordLoader = new FwPersist.Loader[GenericRecord, Context] {
    def load(context: Context, file: String): Context.U[GenericRecord] = {
      val reader = AvroParquetReader.builder[GenericRecord](new Path(file)).build()

      Iterator.continually(reader.read).takeWhile(_ != null).toList
    }
  }

  /** Scala text file loader implementation. */
  val textLoader = new FwPersist.Loader[String, Context] {
    def load(context: Context, file: String): Context.U[String] = {
      val src = Source.fromFile(file)
      val list = src.getLines.toList

      src.close
      list
    }
  }

  /** Function that provides scala parquet loader implementation. The method use AvroParquetReader` to read parquet. */
  def parquetLoader[T](implicit ev: (GenericRecord) => T) = new FwPersist.Loader[T, Context] {
    def load(context: Context, file: String): Context.U[T] = parquetGenericRecordLoader
      .load(context, file)
      .map(ev)
  }

  /** Function that provides scala sequence file loader implementation. */
  def sequenceLoader[
    K <: Writable : Manifest,
    V <: Writable : Manifest
  ] = new FwPersist.Loader[(K, V), Context] {
    def load(context: Context, file: String): Context.U[(K, V)] = {
      val key = implicitly[Manifest[K]].runtimeClass.newInstance.asInstanceOf[K]
      val value = implicitly[Manifest[V]].runtimeClass.newInstance.asInstanceOf[V]
      val reader = new SequenceFile.Reader(new Configuration(), SequenceFile.Reader.file(new Path(file)))

      val list = Iterator.continually {
        val result = reader.next(key, value)

        (result, if (result) List((key, value)) else List())
      }
        .takeWhile { case (r, _) => r != false }
        .flatMap { case (_, l) => l }
        .toList

      if (reader != null) reader.close()

      list
    }
  }
}

/** Case class that enriches a `List` of strings with saveAsText functionality. */
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

