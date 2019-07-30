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

import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.environment.tuner._
import commbank.grimlock.framework.position._

import shapeless.nat.{ _0, _1, _2 }

class TestScalaDistanceMutualInformation extends TestDistanceMutualInformation with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A mutualInformation" should "return its second over in 2D" in {
    val res = toU(data1)
      .mutualInformation(Over(_1), Default())(TestDistanceMutualInformation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result1.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result1(i).position
      res(i).content.value.as[Double].get shouldBe result1(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "return its first along in 2D" in {
    val res = toU(data1)
      .mutualInformation(Along(_0), Default())(TestDistanceMutualInformation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result1.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result1(i).position
      res(i).content.value.as[Double].get shouldBe result1(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "return its first along in 3D" in {
    val res = toU(data2)
      .mutualInformation(Along(_2), Default())(TestDistanceMutualInformation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result2.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result2(i).position
      res(i).content.value.as[Double].get shouldBe result2(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "return its second over in 3D" in {
    val res = toU(data3)
      .mutualInformation(Over(_1), Default())(TestDistanceMutualInformation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result3.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result3(i).position
      res(i).content.value.as[Double].get shouldBe result3(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "filter correctly" in {
    val res = toU(data4)
      .mutualInformation(Over(_1), Default())(TestDistanceMutualInformation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result4.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result4(i).position
      res(i).content.value.as[Double].get shouldBe result4(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "not filter correctly" in {
    val res = toU(data4)
      .mutualInformation(Over(_1), Default())(TestDistanceMutualInformation.name, false)
      .toList.sortBy(_.position)

    res.size shouldBe result1.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result1(i).position
      res(i).content.value.as[Double].get shouldBe result1(i).content.value.as[Double].get +- 1e-8
    }
  }
}

class TestScaldingDistanceMutualInformation extends TestDistanceMutualInformation with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A mutualInformation" should "return its second over in 2D" in {
    val res = toU(data1)
      .mutualInformation(Over(_1), InMemory())(TestDistanceMutualInformation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result1.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result1(i).position
      res(i).content.value.as[Double].get shouldBe result1(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "return its first along in 2D" in {
    val res = toU(data1)
      .mutualInformation(Along(_0), Default())(TestDistanceMutualInformation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result1.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result1(i).position
      res(i).content.value.as[Double].get shouldBe result1(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "return its first along in 3D" in {
    val res = toU(data2)
      .mutualInformation(
        Along(_2),
        Ternary(Default(12), InMemory(), Default())
      )(
        TestDistanceMutualInformation.name,
        true
      )
      .toList.sortBy(_.position)

    res.size shouldBe result2.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result2(i).position
      res(i).content.value.as[Double].get shouldBe result2(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "return its second over in 3D" in {
    val res = toU(data3)
      .mutualInformation(
        Over(_1),
        Ternary(Default(12), InMemory(12), Default(12))
      )(
        TestDistanceMutualInformation.name,
        true
      )
      .toList.sortBy(_.position)

    res.size shouldBe result3.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result3(i).position
      res(i).content.value.as[Double].get shouldBe result3(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "filter correctly" in {
    val res = toU(data4)
      .mutualInformation(
        Over(_1),
        Ternary(Default(12), Default(), Unbalanced(12))
      )(
        TestDistanceMutualInformation.name,
        true
      )
      .toList.sortBy(_.position)

    res.size shouldBe result4.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result4(i).position
      res(i).content.value.as[Double].get shouldBe result4(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "not filter correctly" in {
    val res = toU(data4)
      .mutualInformation(
        Over(_1),
        Ternary(Default(12), Unbalanced(12), Unbalanced(12))
      )(
        TestDistanceMutualInformation.name,
        false
      )
      .toList.sortBy(_.position)

    res.size shouldBe result1.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result1(i).position
      res(i).content.value.as[Double].get shouldBe result1(i).content.value.as[Double].get +- 1e-8
    }
  }
}

class TestSparkDistanceMutualInformation extends TestDistanceMutualInformation with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A mutualInformation" should "return its second over in 2D" in {
    val res = toU(data1)
      .mutualInformation(Over(_1), InMemory())(TestDistanceMutualInformation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result1.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result1(i).position
      res(i).content.value.as[Double].get shouldBe result1(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "return its first along in 2D" in {
    val res = toU(data1)
      .mutualInformation(Along(_0), Default())(TestDistanceMutualInformation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result1.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result1(i).position
      res(i).content.value.as[Double].get shouldBe result1(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "return its first along in 3D" in {
    val res = toU(data2)
      .mutualInformation(
        Along(_2),
        Ternary(Default(12), InMemory(), Default())
      )(
        TestDistanceMutualInformation.name,
        true
      )
      .toList.sortBy(_.position)

    res.size shouldBe result2.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result2(i).position
      res(i).content.value.as[Double].get shouldBe result2(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "return its second over in 3D" in {
    val res = toU(data3)
      .mutualInformation(
        Over(_1),
        Ternary(Default(12), InMemory(12), Default(12))
      )(
        TestDistanceMutualInformation.name,
        true
      )
      .toList.sortBy(_.position)

    res.size shouldBe result3.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result3(i).position
      res(i).content.value.as[Double].get shouldBe result3(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "filter correctly" in {
    val res = toU(data4)
      .mutualInformation(
        Over(_1),
        Ternary(Default(12), Default(), Default(12))
      )(
        TestDistanceMutualInformation.name,
        true
      )
      .toList.sortBy(_.position)

    res.size shouldBe result4.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result4(i).position
      res(i).content.value.as[Double].get shouldBe result4(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "not filter correctly" in {
    val res = toU(data4)
      .mutualInformation(
        Over(_1),
        Ternary(Default(12), Default(12), Default(12))
      )(
        TestDistanceMutualInformation.name,
        false
      )
      .toList.sortBy(_.position)

    res.size shouldBe result1.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result1(i).position
      res(i).content.value.as[Double].get shouldBe result1(i).content.value.as[Double].get +- 1e-8
    }
  }
}

