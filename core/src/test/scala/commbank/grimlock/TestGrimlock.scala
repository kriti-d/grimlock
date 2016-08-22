// Copyright 2015,2016 Commonwealth Bank of Australia
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

package commbank.grimlock

import commbank.grimlock.framework.position._

import cascading.flow.FlowDef

import com.twitter.scalding.{ Config, Local }
import com.twitter.scalding.typed.{ IterablePipe, TypedPipe }

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._

import org.scalatest._

import scala.reflect.ClassTag

import shapeless.Nat

trait TestGrimlock extends FlatSpec with Matchers {

  private implicit val flow = new FlowDef
  private implicit val mode = Local(true)
  private implicit val config = Config.defaultFrom(mode)

  implicit val scaldingCtx = commbank.grimlock.scalding.environment.Context()
  implicit val sparkCtx = commbank.grimlock.spark.environment.Context(TestGrimlock.spark)

  implicit def positionOrdering[T <: Nat] = Position.ordering[T]()

  def toRDD[T](list: List[T])(implicit ev: ClassTag[T]): RDD[T] = TestGrimlock.spark.parallelize(list)
  def toPipe[T](list: List[T]): TypedPipe[T] = IterablePipe(list)

  implicit def toList[T](rdd: RDD[T]): List[T] = rdd.toLocalIterator.toList
  implicit def toList[T](pipe: TypedPipe[T]): List[T] = pipe
    .toIterableExecution
    .waitFor(config, mode)
    .getOrElse(throw new Exception("couldn't get pipe as list"))
    .toList
}

object TestGrimlock {

  val spark = new SparkContext("local", "Test Spark", new SparkConf())

  Logger.getRootLogger().setLevel(Level.WARN);
}

