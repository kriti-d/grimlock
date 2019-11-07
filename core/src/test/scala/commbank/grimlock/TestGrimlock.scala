// Copyright 2015,2016,2017,2018,2019 Commonwealth Bank of Australia
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

package commbank.grimlock.test

import commbank.grimlock.framework.position._

import cascading.flow.FlowDef

import com.twitter.scalding.{ Config, Local }
import com.twitter.scalding.typed.{ IterablePipe, TypedPipe }

import org.apache.log4j.{ Level, Logger }

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import org.scalatest.{ FlatSpec, Matchers }

import scala.reflect.ClassTag

import shapeless.HList

trait TestGrimlock extends FlatSpec with Matchers {
  implicit def positionOrdering[P <: HList] = Position.ordering[P]()
}

trait TestScala extends TestGrimlock {
  implicit val ctx = commbank.grimlock.scala.environment.Context()

  def toU[T](list: List[T]): List[T] = list

  implicit def toList[T](list: List[T]): List[T] = list
}

trait TestScalding extends TestGrimlock {
  private implicit val flow = new FlowDef
  private implicit val mode = Local(true)
  private implicit val config = Config.defaultFrom(mode)

  implicit val ctx = commbank.grimlock.scalding.environment.Context()

  def toU[T](list: List[T]): TypedPipe[T] = IterablePipe(list)

  implicit def toList[T](pipe: TypedPipe[T]): List[T] = pipe
    .toIterableExecution
    .waitFor(config, mode)
    .getOrElse(throw new Exception("couldn't get pipe as list"))
    .toList
}

trait TestSpark extends TestGrimlock {
  implicit val ctx = commbank.grimlock.spark.environment.Context(TestSpark.session)

  def toU[T](list: List[T])(implicit ev: ClassTag[T]): RDD[T] = TestSpark.session.sparkContext.parallelize(list)

  implicit def toList[T](rdd: RDD[T]): List[T] = rdd.toLocalIterator.toList
}

object TestSpark {
  val session = SparkSession.builder().master("local").appName("Test Spark").getOrCreate()

  Logger.getRootLogger().setLevel(Level.WARN);
}

