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

package commbank.grimlock.spark.environment

import au.com.cba.omnia.ebenezer.scrooge.ScroogeReadSupport

import com.twitter.scrooge.ThriftStruct

import commbank.grimlock.framework.Cell
import commbank.grimlock.framework.environment.{ Context => FwContext }

import commbank.grimlock.spark.environment.implicits.Implicits

import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import parquet.hadoop.ParquetInputFormat

import scala.reflect.{ classTag, ClassTag }

import shapeless.Nat

/**
 * Spark operating context state.
 *
 * @param spark The Spark context.
 */
case class Context(spark: SparkContext) extends FwContext[Context.U, Context.E] {
  def loadText[P <: Nat](file: String, parser: Cell.TextParser[P]): (Context.U[Cell[P]], Context.U[String]) = {
    val rdd = spark.textFile(file).flatMap { case s => parser(s) }

    (rdd.collect { case Right(c) => c }, rdd.collect { case Left(e) => e })
  }

  def loadSequence[
    K <: Writable : Manifest,
    V <: Writable : Manifest,
    P <: Nat
  ](
    file: String,
    parser: Cell.SequenceParser[K, V, P]
  ): (Context.U[Cell[P]], Context.U[String]) = {
    val rdd = spark.sequenceFile[K, V](file).flatMap { case (k, v) => parser(k, v) }

    (rdd.collect { case Right(c) => c }, rdd.collect { case Left(e) => e })
  }

  def loadParquet[
    T <: ThriftStruct : Manifest,
    P <: Nat
  ](
    file: String,
    parser: Cell.ParquetParser[T, P]
  ): (Context.U[Cell[P]], Context.U[String]) = {
    val job = new Job()

    ParquetInputFormat.setReadSupportClass(job, classOf[ScroogeReadSupport[T]])

    val rdd = spark.newAPIHadoopFile(
      file,
      classOf[ParquetInputFormat[T]],
      classOf[Void],
      classTag[T].runtimeClass.asInstanceOf[Class[T]],
      job.getConfiguration
    ).flatMap { case (_, v) => parser(v) }

    (rdd.collect { case Right(c) => c }, rdd.collect { case Left(e) => e })
  }

  type C = Context

  val implicits = Implicits(this)

  def empty[T : ClassTag]: Context.U[T] = spark.parallelize(List.empty[T])

  def from[T : ClassTag](seq: Seq[T]): Context.U[T] = spark.parallelize(seq)

  def nop(): Unit = ()
}

/** Companion object to `Context` with implicit. */
object Context {
  /** Type for user defined data. */
  type E[A] = A

  /** Type for distributed data. */
  type U[A] = RDD[A]
}

