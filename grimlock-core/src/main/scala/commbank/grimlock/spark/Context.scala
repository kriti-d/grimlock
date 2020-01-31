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

package commbank.grimlock.spark.environment

import commbank.grimlock.framework.Persist
import commbank.grimlock.framework.environment.{ Encoder => FwEncoder, MatrixContext }

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.reflect.ClassTag
import scala.util.{ Failure, Success }

/**
 * Spark operating context state.
 *
 * @param session The Spark session.
 */
case class Context(session: SparkSession) extends MatrixContext[Context] {
  type D[T] = Context.D[T]

  type E[T] = Context.E[T]

  type U[T] = Context.U[T]

  /** Implicit Spark Encoder for `T` defined given a `ClassTag[T]`. */
  implicit def encoder[T](implicit ev: ClassTag[T]): D[T] = Context.encoder

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
    import enc.ct

    val rdd = loader.load(this, location).flatMap(parser)

    (rdd.collect { case Success(c) => c }, rdd.collect { case Failure(e) => e })
  }

  val implicits = Implicits

  val library = Library

  def empty[T](implicit enc: Encoder[T]): Context.U[T] = {
    import enc.ct

    session.sparkContext.parallelize(List.empty[T])
  }

  def from[T](seq: Seq[T])(implicit enc: Encoder[T]): Context.U[T] = {
    implicit val ct = enc.ct

    session.sparkContext.parallelize(seq)
  }

  def nop(): Unit = ()
}

/** Companion object to `Context` with additional constructors and implicits. */
object Context {
  /** Type class needed to encode data as a distributed list. */
  type D[T] = Encoder[T]

  /** Type for user defined data. */
  type E[T] = T

  /** Type for distributed data. */
  type U[T] = RDD[T]

  /** Implicit Spark Encoder for `T` defined given a `ClassTag[T]`. */
  implicit def encoder[T](implicit ev: ClassTag[T]): D[T] = new Encoder[T] {
    implicit def ct: ClassTag[T] = ev
  }
}

/** Type class constraints for encoding a type `T` in a Spark `RDD[T]`. */
trait Encoder[T] extends FwEncoder[T] {
  /** `ClassTag` for `T`. */
  implicit def ct: ClassTag[T]
}

