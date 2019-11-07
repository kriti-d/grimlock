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

import shapeless.nat.{ _1, _2 }

class TestScalaDistanceCorrelation extends TestDistanceCorrelation with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A correlation" should "return its second over in 2D" in {
    val res = toU(data1)
      .correlation(Over(_1), Default())(TestDistanceCorrelation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result1.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result1(i).position
      res(i).content.value.as[Double].get shouldBe result1(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "return its multiple second over in 2D" in {
    val res = toU(data2)
      .correlation(Over(_1), Default())(TestDistanceCorrelation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result2.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result2(i).position
      res(i).content.value.as[Double].get shouldBe result2(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "return its first along in 3D" in {
    val res = toU(data3)
      .correlation(Along(_2), Default())(TestDistanceCorrelation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result3.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result3(i).position
      res(i).content.value.as[Double].get shouldBe result3(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "return its second over in 3D" in {
    val res = toU(data4)
      .correlation(Over(_1), Default())(TestDistanceCorrelation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result4.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result4(i).position
      res(i).content.value.as[Double].get shouldBe result4(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "filter correctly" in {
    val res = toU(data5)
      .correlation(Over(_1), Default())(TestDistanceCorrelation.name, true, true)
      .toList.sortBy(_.position)

    res.size shouldBe result2.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result2(i).position
      res(i).content.value.as[Double].get shouldBe result2(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "!filter,strict correctly" in {
    val res = toU(data5)
      .correlation(Over(_1), Default())(TestDistanceCorrelation.name, false, true)
      .toList.sortBy(_.position)

    res.size shouldBe result5.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result5(i).position

      val ref = result5(i).content.value.as[Double].get

      if (ref.isNaN)
        res(i).content.value.as[Double].get.compare(ref) shouldBe 0
      else
        res(i).content.value.as[Double].get - ref shouldBe 0.0 +- 1e-8
    }
  }

  it should "!filter,!strict correctly" in {
    val res = toU(data5)
      .correlation(Over(_1), Default())(TestDistanceCorrelation.name, false, false)
      .toList.sortBy(_.position)

    res.size shouldBe result6.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result6(i).position
      res(i).content.value.as[Double].get shouldBe result6(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "!filter,strict correctly take 2" in {
    val res = toU(data6)
      .correlation(Over(_1), Default())(TestDistanceCorrelation.name, false, false)
      .toList.sortBy(_.position)

    res.size shouldBe result7.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result7(i).position

      val ref = result7(i).content.value.as[Double].get

      if (ref.isNaN)
        res(i).content.value.as[Double].get.compare(ref) shouldBe 0
      else
        res(i).content.value.as[Double].get - ref shouldBe 0.0 +- 1e-8
    }
  }
}

class TestScaldingDistanceCorrelation extends TestDistanceCorrelation with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A correlation" should "return its second over in 2D" in {
    val res = toU(data1)
      .correlation(Over(_1), InMemory())(TestDistanceCorrelation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result1.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result1(i).position
      res(i).content.value.as[Double].get shouldBe result1(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "return its multiple second over in 2D" in {
    val res = toU(data2)
      .correlation(Over(_1), Default())(TestDistanceCorrelation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result2.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result2(i).position
      res(i).content.value.as[Double].get shouldBe result2(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "return its first along in 3D" in {
    val res = toU(data3)
      .correlation(Along(_2), Ternary(Default(12), InMemory(), Default()))(TestDistanceCorrelation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result3.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result3(i).position
      res(i).content.value.as[Double].get shouldBe result3(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "return its second over in 3D" in {
    val res = toU(data4)
      .correlation(Over(_1), Ternary(Default(12), InMemory(12), Default(12)))(TestDistanceCorrelation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result4.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result4(i).position
      res(i).content.value.as[Double].get shouldBe result4(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "filter correctly" in {
    val res = toU(data5)
      .correlation(Over(_1), Ternary(Default(12), Default(), Unbalanced(12)))(TestDistanceCorrelation.name, true, true)
      .toList.sortBy(_.position)

    res.size shouldBe result2.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result2(i).position
      res(i).content.value.as[Double].get shouldBe result2(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "!filter,strict correctly" in {
    val res = toU(data5)
      .correlation(Over(_1), Ternary(Default(12), Default(12), Default(12)))(TestDistanceCorrelation.name, false, true)
      .toList.sortBy(_.position)

    res.size shouldBe result5.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result5(i).position

      val ref = result5(i).content.value.as[Double].get

      if (ref.isNaN)
        res(i).content.value.as[Double].get.compare(ref) shouldBe 0
      else
        res(i).content.value.as[Double].get - ref shouldBe 0.0 +- 1e-8
    }
  }

  it should "!filter,!strict correctly" in {
    val res = toU(data5)
      .correlation(
        Over(_1),
        Ternary(Default(12), Unbalanced(12), Default(12))
      )(
        TestDistanceCorrelation.name,
        false,
        false
      )
      .toList.sortBy(_.position)

    res.size shouldBe result6.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result6(i).position
      res(i).content.value.as[Double].get shouldBe result6(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "!filter,strict correctly take 2" in {
    val res = toU(data6)
      .correlation(Over(_1), Default())(TestDistanceCorrelation.name, false, false)
      .toList.sortBy(_.position)

    res.size shouldBe result7.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result7(i).position

      val ref = result7(i).content.value.as[Double].get

      if (ref.isNaN)
        res(i).content.value.as[Double].get.compare(ref) shouldBe 0
      else
        res(i).content.value.as[Double].get - ref shouldBe 0.0 +- 1e-8
    }
  }
}

class TestSparkDistanceCorrelation extends TestDistanceCorrelation with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A correlation" should "return its second over in 2D" in {
    val res = toU(data1)
      .correlation(Over(_1), InMemory())(TestDistanceCorrelation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result1.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result1(i).position
      res(i).content.value.as[Double].get shouldBe result1(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "return its multiple second over in 2D" in {
    val res = toU(data2)
      .correlation(Over(_1), Default())(TestDistanceCorrelation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result2.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result2(i).position
      res(i).content.value.as[Double].get shouldBe result2(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "return its first along in 3D" in {
    val res = toU(data3)
      .correlation(Along(_2), Ternary(Default(12), InMemory(), Default()))(TestDistanceCorrelation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result3.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result3(i).position
      res(i).content.value.as[Double].get shouldBe result3(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "return its second over in 3D" in {
    val res = toU(data4)
      .correlation(Over(_1), Ternary(Default(12), InMemory(), Default(12)))(TestDistanceCorrelation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result4.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result4(i).position
      res(i).content.value.as[Double].get shouldBe result4(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "filter correctly" in {
    val res = toU(data5)
      .correlation(Over(_1), Ternary(Default(12), InMemory(12), Default(12)))(TestDistanceCorrelation.name, true, true)
      .toList.sortBy(_.position)

    res.size shouldBe result2.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result2(i).position
      res(i).content.value.as[Double].get shouldBe result2(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "!filter,strict correctly" in {
    val res = toU(data5)
      .correlation(Over(_1), Ternary(Default(12), Default(), Default()))(TestDistanceCorrelation.name, false, true)
      .toList.sortBy(_.position)

    res.size shouldBe result5.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result5(i).position

      val ref = result5(i).content.value.as[Double].get

      if (ref.isNaN)
        res(i).content.value.as[Double].get.compare(ref) shouldBe 0
      else
        res(i).content.value.as[Double].get - ref shouldBe 0.0 +- 1e-8
    }
  }

  it should "!filter,!strict correctly" in {
    val res = toU(data5)
      .correlation(Over(_1), Ternary(Default(12), Default(), Default(12)))(TestDistanceCorrelation.name, false, false)
      .toList.sortBy(_.position)

    res.size shouldBe result6.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result6(i).position
      res(i).content.value.as[Double].get shouldBe result6(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "!filter,strict correctly take 2" in {
    val res = toU(data6)
      .correlation(Over(_1), Ternary(Default(12), Default(12), Default(12)))(TestDistanceCorrelation.name, false, false)
      .toList.sortBy(_.position)

    res.size shouldBe result7.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result7(i).position

      val ref = result7(i).content.value.as[Double].get

      if (ref.isNaN)
        res(i).content.value.as[Double].get.compare(ref) shouldBe 0
      else
        res(i).content.value.as[Double].get - ref shouldBe 0.0 +- 1e-8
    }
  }
}

