// Copyright 2019 Commonwealth Bank of Australia
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

import commbank.grimlock.framework.{ ParquetConfig, Persist }
import commbank.grimlock.framework.environment.{ Context => FwContext }

import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{ SequenceFile, Writable }
import org.apache.parquet.avro.AvroParquetReader

import scala.io.Source
import scala.reflect.ClassTag
import scala.util.{ Failure, Success }

/** Scala operating context state. */
case class Context() extends FwContext[Context] {
  type E[T] = Context.E[T]

  type U[T] = Context.U[T]

  def loadText[
    T : ClassTag
  ](
    file: String,
    parser: Persist.TextParser[T]
  ): (Context.U[T], Context.U[Throwable]) = {
    val src = Source.fromFile(file)
    val list = src.getLines.toList.flatMap { case s => parser(s) }

    src.close

    (list.collect { case Success(c) => c }, list.collect { case Failure(e) => e })
  }

  def loadSequence[
    K <: Writable : Manifest,
    V <: Writable : Manifest,
    T : ClassTag
  ](
    file: String,
    parser: Persist.SequenceParser[K, V, T]
  ): (Context.U[T], Context.U[Throwable]) = { // TODO: test this
    val key = implicitly[Manifest[K]].runtimeClass.newInstance.asInstanceOf[K]
    val value = implicitly[Manifest[V]].runtimeClass.newInstance.asInstanceOf[V]
    val reader = new SequenceFile.Reader(new Configuration(), SequenceFile.Reader.file(new Path(file)))

    val list = Iterator.continually {
      val result = reader.next(key, value)

      (result, if (result) parser(key, value) else List())
    }
      .takeWhile { case (r, _) => r != false }
      .flatMap { case (_, l) => l }
      .toList

    if (reader != null) reader.close()

    (list.collect { case Success(c) => c }, list.collect { case Failure(e) => e })
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
    val list = cfg.load(this, file).flatMap(v => parser(v))

    (list.collect { case Success(c) => c }, list.collect { case Failure(e) => e })
  }

  val implicits = Implicits

  val library = Library

  def empty[T : ClassTag]: Context.U[T] = List.empty

  def from[T : ClassTag](seq: Seq[T]): Context.U[T] = seq.toList

  def nop(): Unit = ()
}

/** Companion object to `Context` with additional constructors and implicits. */
object Context {
  /** Type for user defined data. */
  type E[T] = T

  /** Type for distributed data. */
  type U[T] = List[T]

  /**
   * Implicit function that provides scala parquet reader implementation. The method uses
   * `AvroParquetReader` to read parquet.
   */
  implicit val toScalaGenericRecordParquet = new ParquetConfig[GenericRecord, Context] {
    def load(context: Context, file: String): Context.U[GenericRecord] = {
      val reader = AvroParquetReader.builder[GenericRecord](new Path(file)).build()

      Iterator.continually(reader.read).takeWhile(_ != null).toList
    }
  }

  /**
   * Implicit function that provides scala parquet reader implementation. The method uses
   * `AvroParquetReader` to read parquet.
   */
  implicit def toScalaParquet[T](implicit ev: (GenericRecord) => T) = new ParquetConfig[T, Context] {
    def load(context: Context, file: String): Context.U[T] = toScalaGenericRecordParquet
      .load(context, file)
      .map(ev)
  }
}

