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

package commbank.grimlock.spark

import commbank.grimlock.framework.{ Persist => FwPersist, SaveStringsAsText => FwSaveStringsAsText }
import commbank.grimlock.framework.environment.tuner.{ Default, Tuner }

import commbank.grimlock.spark.environment.Context
import commbank.grimlock.spark.environment.tuner.SparkImplicits._

import org.apache.hadoop.io.Writable

import org.apache.spark.sql.{ Encoder => SparkEncoder, Row }

import scala.reflect.ClassTag

/** Trait for peristing a `RDD`. */
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
  /** Spark parquet loader implementation using `DataFrameReader`. */
  val parquetRowLoader = new FwPersist.Loader[Row, Context] {
    def load(context: Context, file: String): Context.U[Row] = context.session.sqlContext.read.parquet(file).rdd
  }

  /** Spark text file loader implementation. */
  val textLoader = new FwPersist.Loader[String, Context] {
    def load(context: Context, file: String): Context.U[String] = context.session.sparkContext.textFile(file)
  }

  /** Function that provides spark parquet loader implementation. The method uses `DataFrameReader` to read parquet. */
  def parquetLoader[T : ClassTag](implicit ev: SparkEncoder[T]) = new FwPersist.Loader[T, Context] {
    def load(
      context: Context,
      file: String
    ): Context.U[T] = context.session.sqlContext.read.parquet(file).as[T].rdd
  }

  /** Function that provides spark sequence file loader implementation. */
  def sequenceLoader[
    K <: Writable : ClassTag,
    V <: Writable : ClassTag
  ] = new FwPersist.Loader[(K, V), Context] {
    def load(
      context: Context,
      file: String
    ): Context.U[(K, V)] = context.session.sparkContext.sequenceFile[K, V](file)
  }
}

/** Case class that enriches a `RDD` of strings with saveAsText functionality. */
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

