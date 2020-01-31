// Copyright 2017,2018,2019,2020 Commonwealth Bank of Australia
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

import commbank.grimlock.framework.content._
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.environment.tuner._
import commbank.grimlock.framework.position._

import commbank.grimlock.library.aggregate._

import shapeless.nat.{ _0, _1, _2 }

class TestScalaStatisticsPredicateCounts extends TestStatistics with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  val data1 = toU(num1)
  val data2 = toU(num2)
  val data3 = toU(num3)

  val predicate = (con: Content) => con.value gtr 7.0

  "A predicateCounts" should "return its correct statistic" in {
    data1
      .predicateCounts(Over(_0), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data1
        .summarise(Over(_0))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data1
      .predicateCounts(Along(_0), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data1
        .summarise(Along(_0))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data2
      .predicateCounts(Over(_0), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data2
        .summarise(Over(_0))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data2
      .predicateCounts(Along(_0), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data2
        .summarise(Along(_0))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data2
      .predicateCounts(Over(_1), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data2
        .summarise(Over(_1))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data2
      .predicateCounts(Along(_1), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data2
        .summarise(Along(_1))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data3
      .predicateCounts(Over(_0), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_0))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data3
      .predicateCounts(Along(_0), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_0))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data3
      .predicateCounts(Over(_1), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_1))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data3
      .predicateCounts(Along(_1), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_1))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data3
      .predicateCounts(Over(_2), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_2))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data3
      .predicateCounts(Along(_2), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_2))(PredicateCounts(predicate))
        .toList.sortBy(_.position)
  }
}

class TestScaldingStatisticsPredicateCounts extends TestStatistics with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  val data1 = toU(num1)
  val data2 = toU(num2)
  val data3 = toU(num3)

  val predicate = (con: Content) => con.value gtr 7.0

  "A predicateCounts" should "return its correct statistic" in {
    data1
      .predicateCounts(Over(_0), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data1
        .summarise(Over(_0))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data1
      .predicateCounts(Along(_0), Default(12))(predicate)
      .toList.sortBy(_.position) shouldBe data1
        .summarise(Along(_0))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data2
      .predicateCounts(Over(_0), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data2
        .summarise(Over(_0))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data2
      .predicateCounts(Along(_0), Default(12))(predicate)
      .toList.sortBy(_.position) shouldBe data2
        .summarise(Along(_0))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data2
      .predicateCounts(Over(_1), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data2
        .summarise(Over(_1))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data2
      .predicateCounts(Along(_1), Default(12))(predicate)
      .toList.sortBy(_.position) shouldBe data2
        .summarise(Along(_1))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data3
      .predicateCounts(Over(_0), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_0))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data3
      .predicateCounts(Along(_0), Default(12))(predicate)
      .toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_0))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data3
      .predicateCounts(Over(_1), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_1))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data3
      .predicateCounts(Along(_1), Default(12))(predicate)
      .toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_1))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data3
      .predicateCounts(Over(_2), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_2))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data3
      .predicateCounts(Along(_2), Default(12))(predicate)
      .toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_2))(PredicateCounts(predicate))
        .toList.sortBy(_.position)
  }
}

class TestSparkStatisticsPredicateCounts extends TestStatistics with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  val data1 = toU(num1)
  val data2 = toU(num2)
  val data3 = toU(num3)

  val predicate = (con: Content) => con.value gtr 7.0

  "A predicateCounts" should "return its correct statistic" in {
    data1
      .predicateCounts(Over(_0), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data1
        .summarise(Over(_0))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data1
      .predicateCounts(Along(_0), Default(12))(predicate)
      .toList.sortBy(_.position) shouldBe data1
        .summarise(Along(_0))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data2
      .predicateCounts(Over(_0), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data2
        .summarise(Over(_0))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data2
      .predicateCounts(Along(_0), Default(12))(predicate)
      .toList.sortBy(_.position) shouldBe data2
        .summarise(Along(_0))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data2
      .predicateCounts(Over(_1), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data2
        .summarise(Over(_1))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data2
      .predicateCounts(Along(_1), Default(12))(predicate)
      .toList.sortBy(_.position) shouldBe data2
        .summarise(Along(_1))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data3
      .predicateCounts(Over(_0), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_0))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data3
      .predicateCounts(Along(_0), Default(12))(predicate)
      .toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_0))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data3
      .predicateCounts(Over(_1), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_1))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data3
      .predicateCounts(Along(_1), Default(12))(predicate)
      .toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_1))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data3
      .predicateCounts(Over(_2), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_2))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data3
      .predicateCounts(Along(_2), Default(12))(predicate)
      .toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_2))(PredicateCounts(predicate))
        .toList.sortBy(_.position)
  }
}

