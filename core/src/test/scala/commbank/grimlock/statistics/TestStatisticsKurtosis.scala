// Copyright 2017,2018,2019 Commonwealth Bank of Australia
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

import commbank.grimlock.framework._
import commbank.grimlock.framework.content._
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.environment.tuner._
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.position._

import commbank.grimlock.library.aggregate._

import com.twitter.scalding.typed.TypedPipe

import org.apache.spark.rdd.RDD

import shapeless.HList
import shapeless.nat.{ _0, _1, _2 }

class TestScalaStatisticsKurtosis extends TestStatistics with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  val data1 = toU(num1)
  val data2 = toU(num2)
  val data3 = toU(num3)

  implicit class MapNaN[P <: HList](list: List[Cell[P]]) {
    def mapNaN() = list
      .map {
        case c if c.content.value.as[Double].map(_.isNaN) == Option(true) =>
          c.mutate(_ => Content(NominalSchema[Boolean](), true))
        case c => c
      }
  }

  "A kurtosis" should "return its correct statistic" in {
    data1
      .kurtosis(Over(_0), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data1
        .summarise(Over(_0))(Kurtosis(true))
        .mapNaN.toList.sortBy(_.position)

    data1
      .kurtosis(Along(_0), Default())(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data1
        .summarise(Along(_0))(Kurtosis())
        .mapNaN.toList.sortBy(_.position)

    data2
      .kurtosis(Over(_0), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data2
        .summarise(Over(_0))(Kurtosis(true))
        .mapNaN.toList.sortBy(_.position)

    data2
      .kurtosis(Along(_0), Default())(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data2
        .summarise(Along(_0))(Kurtosis())
        .mapNaN.toList.sortBy(_.position)

    data2
      .kurtosis(Over(_1), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data2
        .summarise(Over(_1))(Kurtosis(true))
        .mapNaN.toList.sortBy(_.position)

    data2
      .kurtosis(Along(_1), Default())(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data2
        .summarise(Along(_1))(Kurtosis())
        .mapNaN.toList.sortBy(_.position)

    data3
      .kurtosis(Over(_0), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_0))(Kurtosis(true))
        .mapNaN.toList.sortBy(_.position)

    data3
      .kurtosis(Along(_0), Default())(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_0))(Kurtosis())
        .mapNaN.toList.sortBy(_.position)

    data3
      .kurtosis(Over(_1), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_1))(Kurtosis(true))
        .mapNaN.toList.sortBy(_.position)

    data3
      .kurtosis(Along(_1), Default())(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_1))(Kurtosis())
        .mapNaN.toList.sortBy(_.position)

    data3
      .kurtosis(Over(_2), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_2))(Kurtosis(true))
        .mapNaN.toList.sortBy(_.position)

    data3
      .kurtosis(Along(_2), Default())(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_2))(Kurtosis())
        .mapNaN.toList.sortBy(_.position)
  }
}

class TestScaldingStatisticsKurtosis extends TestStatistics with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  val data1 = toU(num1)
  val data2 = toU(num2)
  val data3 = toU(num3)

  implicit class MapNaN[P <: HList](pipe: TypedPipe[Cell[P]]) {
    def mapNaN() = pipe
      .map {
        case c if c.content.value.as[Double].map(_.isNaN) == Option(true) =>
          c.mutate(_ => Content(NominalSchema[Boolean](), true))
        case c => c
      }
  }

  "A kurtosis" should "return its correct statistic" in {
    data1
      .kurtosis(Over(_0), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data1
        .summarise(Over(_0))(Kurtosis(true))
        .mapNaN.toList.sortBy(_.position)

    data1
      .kurtosis(Along(_0), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data1
        .summarise(Along(_0))(Kurtosis())
        .mapNaN.toList.sortBy(_.position)

    data2
      .kurtosis(Over(_0), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data2
        .summarise(Over(_0))(Kurtosis(true))
        .mapNaN.toList.sortBy(_.position)

    data2
      .kurtosis(Along(_0), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data2
        .summarise(Along(_0))(Kurtosis())
        .mapNaN.toList.sortBy(_.position)

    data2
      .kurtosis(Over(_1), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data2
        .summarise(Over(_1))(Kurtosis(true))
        .mapNaN.toList.sortBy(_.position)

    data2
      .kurtosis(Along(_1), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data2
        .summarise(Along(_1))(Kurtosis())
        .mapNaN.toList.sortBy(_.position)

    data3
      .kurtosis(Over(_0), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_0))(Kurtosis(true))
        .mapNaN.toList.sortBy(_.position)

    data3
      .kurtosis(Along(_0), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_0))(Kurtosis())
        .mapNaN.toList.sortBy(_.position)

    data3
      .kurtosis(Over(_1), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_1))(Kurtosis(true))
        .mapNaN.toList.sortBy(_.position)

    data3
      .kurtosis(Along(_1), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_1))(Kurtosis())
        .mapNaN.toList.sortBy(_.position)

    data3
      .kurtosis(Over(_2), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_2))(Kurtosis(true))
        .mapNaN.toList.sortBy(_.position)

    data3
      .kurtosis(Along(_2), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_2))(Kurtosis())
        .mapNaN.toList.sortBy(_.position)
  }
}

class TestSparkStatisticsKurtosis extends TestStatistics with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  val data1 = toU(num1)
  val data2 = toU(num2)
  val data3 = toU(num3)

  implicit class MapNaN[P <: HList](rdd: RDD[Cell[P]]) {
    def mapNaN() = rdd
      .map {
        case c if c.content.value.as[Double].map(_.isNaN) == Option(true) =>
          c.mutate(_ => Content(NominalSchema[Boolean](), true))
        case c => c
      }
  }

  "A kurtosis" should "return its correct statistic" in {
    data1
      .kurtosis(Over(_0), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data1
        .summarise(Over(_0))(Kurtosis(true))
        .mapNaN.toList.sortBy(_.position)

    data1
      .kurtosis(Along(_0), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data1
        .summarise(Along(_0))(Kurtosis())
        .mapNaN.toList.sortBy(_.position)

    data2
      .kurtosis(Over(_0), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data2
        .summarise(Over(_0))(Kurtosis(true))
        .mapNaN.toList.sortBy(_.position)

    data2
      .kurtosis(Along(_0), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data2
        .summarise(Along(_0))(Kurtosis())
        .mapNaN.toList.sortBy(_.position)

    data2
      .kurtosis(Over(_1), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data2
        .summarise(Over(_1))(Kurtosis(true))
        .mapNaN.toList.sortBy(_.position)

    data2
      .kurtosis(Along(_1), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data2
        .summarise(Along(_1))(Kurtosis())
        .mapNaN.toList.sortBy(_.position)

    data3
      .kurtosis(Over(_0), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_0))(Kurtosis(true))
        .mapNaN.toList.sortBy(_.position)

    data3
      .kurtosis(Along(_0), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_0))(Kurtosis())
        .mapNaN.toList.sortBy(_.position)

    data3
      .kurtosis(Over(_1), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_1))(Kurtosis(true))
        .mapNaN.toList.sortBy(_.position)

    data3
      .kurtosis(Along(_1), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_1))(Kurtosis())
        .mapNaN.toList.sortBy(_.position)

    data3
      .kurtosis(Over(_2), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_2))(Kurtosis(true))
        .mapNaN.toList.sortBy(_.position)

    data3
      .kurtosis(Along(_2), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_2))(Kurtosis())
        .mapNaN.toList.sortBy(_.position)
  }
}

