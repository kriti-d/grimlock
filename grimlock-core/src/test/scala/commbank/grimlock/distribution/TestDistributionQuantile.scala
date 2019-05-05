// Copyright 2016,2017,2018,2019 Commonwealth Bank of Australia
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

import commbank.grimlock.framework.distribution._
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.environment.tuner._
import commbank.grimlock.framework.position._

import shapeless.nat.{ _0, _1 }

class TestScalaQuantile extends TestQuantile with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A quantile" should "return its first along 1 value in 1D" in {
    toU(data1)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type1, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data1)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type2, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data1)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type3, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data1)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type4, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data1)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type5, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data1)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type6, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data1)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type7, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data1)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type8, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data1)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type9, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first along 3 values in 1D" in {
    toU(data2)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type1, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result2

    toU(data2)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type2, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result2

    toU(data2)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type3, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result3

    toU(data2)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type4, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result4

    toU(data2)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type5, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result5

    toU(data2)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type6, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result6

    toU(data2)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type7, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result7

    toU(data2)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type8, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result8

    toU(data2)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type9, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its first along 3 equal values in 1D" in {
    toU(data3)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type1, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data3)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type2, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data3)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type3, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data3)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type4, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data3)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type5, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data3)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type6, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data3)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type7, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data3)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type8, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data3)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type9, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first along values in 2D" in {
    toU(data4)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type1, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result10

    toU(data4)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type2, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result10

    toU(data4)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type3, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result11

    toU(data4)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type4, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result12

    toU(data4)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type5, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result13

    toU(data4)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type6, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result14

    toU(data4)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type7, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result15

    toU(data4)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type8, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result16

    toU(data4)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type9, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its first over values in 2D" in {
    toU(data5)
      .quantiles(Over(_0), Default())(probs, Quantiles.Type1, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result10

    toU(data5)
      .quantiles(Over(_0), Default())(probs, Quantiles.Type2, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result10

    toU(data5)
      .quantiles(Over(_0), Default())(probs, Quantiles.Type3, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result11

    toU(data5)
      .quantiles(Over(_0), Default())(probs, Quantiles.Type4, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result12

    toU(data5)
      .quantiles(Over(_0), Default())(probs, Quantiles.Type5, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result13

    toU(data5)
      .quantiles(Over(_0), Default())(probs, Quantiles.Type6, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result14

    toU(data5)
      .quantiles(Over(_0), Default())(probs, Quantiles.Type7, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result15

    toU(data5)
      .quantiles(Over(_0), Default())(probs, Quantiles.Type8, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result16

    toU(data5)
      .quantiles(Over(_0), Default())(probs, Quantiles.Type9, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its second along values in 2D" in {
    toU(data5)
      .quantiles(Along(_1), Default())(probs, Quantiles.Type1, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result10

    toU(data5)
      .quantiles(Along(_1), Default())(probs, Quantiles.Type2, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result10

    toU(data5)
      .quantiles(Along(_1), Default())(probs, Quantiles.Type3, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result11

    toU(data5)
      .quantiles(Along(_1), Default())(probs, Quantiles.Type4, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result12

    toU(data5)
      .quantiles(Along(_1), Default())(probs, Quantiles.Type5, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result13

    toU(data5)
      .quantiles(Along(_1), Default())(probs, Quantiles.Type6, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result14

    toU(data5)
      .quantiles(Along(_1), Default())(probs, Quantiles.Type7, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result15

    toU(data5)
      .quantiles(Along(_1), Default())(probs, Quantiles.Type8, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result16

    toU(data5)
      .quantiles(Along(_1), Default())(probs, Quantiles.Type9, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its second over values in 2D" in {
    toU(data4)
      .quantiles(Over(_1), Default())(probs, Quantiles.Type1, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result10

    toU(data4)
      .quantiles(Over(_1), Default())(probs, Quantiles.Type2, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result10

    toU(data4)
      .quantiles(Over(_1), Default())(probs, Quantiles.Type3, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result11

    toU(data4)
      .quantiles(Over(_1), Default())(probs, Quantiles.Type4, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result12

    toU(data4)
      .quantiles(Over(_1), Default())(probs, Quantiles.Type5, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result13

    toU(data4)
      .quantiles(Over(_1), Default())(probs, Quantiles.Type6, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result14

    toU(data4)
      .quantiles(Over(_1), Default())(probs, Quantiles.Type7, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result15

    toU(data4)
      .quantiles(Over(_1), Default())(probs, Quantiles.Type8, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result16

    toU(data4)
      .quantiles(Over(_1), Default())(probs, Quantiles.Type9, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return with non-numeric data" in {
    val res1 = toU(data6)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type1, TestQuantile.name, false, true)
      .toList.sortBy(_.position)
    res1(0) shouldBe result18(0)
    res1(1) shouldBe result18(1)
    res1(2) shouldBe result18(2)
    res1(3).position shouldBe Position("quantile=0.800000")
    res1(3).content.value.as[Double].map(_.compare(Double.NaN)) shouldBe Some(0)

    toU(data6)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type1, TestQuantile.name, false, false)
      .toList.sortBy(_.position) shouldBe result18
  }
}

class TestScaldingQuantile extends TestQuantile with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A quantile" should "return its first along 1 value in 1D" in {
    toU(data1)
      .quantiles(Along(_0), InMemory())(probs, Quantiles.Type1, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data1)
      .quantiles(Along(_0), InMemory(12))(probs, Quantiles.Type2, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data1)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type3, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data1)
      .quantiles(Along(_0), Default(12))(probs, Quantiles.Type4, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data1)
      .quantiles(Along(_0), InMemory())(probs, Quantiles.Type5, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data1)
      .quantiles(Along(_0), InMemory(12))(probs, Quantiles.Type6, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data1)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type7, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data1)
      .quantiles(Along(_0), Default(12))(probs, Quantiles.Type8, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data1)
      .quantiles(Along(_0), InMemory())(probs, Quantiles.Type9, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first along 3 values in 1D" in {
    toU(data2)
      .quantiles(Along(_0), Unbalanced(12))(probs, Quantiles.Type1, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result2

    toU(data2)
      .quantiles(Along(_0), InMemory())(probs, Quantiles.Type2, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result2

    toU(data2)
      .quantiles(Along(_0), InMemory(12))(probs, Quantiles.Type3, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result3

    toU(data2)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type4, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result4

    toU(data2)
      .quantiles(Along(_0), Default(12))(probs, Quantiles.Type5, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result5

    toU(data2)
      .quantiles(Along(_0), Unbalanced(12))(probs, Quantiles.Type6, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result6

    toU(data2)
      .quantiles(Along(_0), InMemory())(probs, Quantiles.Type7, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result7

    toU(data2)
      .quantiles(Along(_0), InMemory(12))(probs, Quantiles.Type8, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result8

    toU(data2)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type9, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its first along 3 equal values in 1D" in {
    toU(data3)
      .quantiles(Along(_0), Default(12))(probs, Quantiles.Type1, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data3)
      .quantiles(Along(_0), Unbalanced(12))(probs, Quantiles.Type2, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data3)
      .quantiles(Along(_0), InMemory())(probs, Quantiles.Type3, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data3)
      .quantiles(Along(_0), InMemory(12))(probs, Quantiles.Type4, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data3)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type5, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data3)
      .quantiles(Along(_0), Default(12))(probs, Quantiles.Type6, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data3)
      .quantiles(Along(_0), Unbalanced(12))(probs, Quantiles.Type7, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data3)
      .quantiles(Along(_0), InMemory())(probs, Quantiles.Type8, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data3)
      .quantiles(Along(_0), InMemory(12))(probs, Quantiles.Type9, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first along values in 2D" in {
    toU(data4)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type1, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result10

    toU(data4)
      .quantiles(Along(_0), Default(12))(probs, Quantiles.Type2, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result10

    toU(data4)
      .quantiles(Along(_0), Unbalanced(12))(probs, Quantiles.Type3, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result11

    toU(data4)
      .quantiles(Along(_0), InMemory())(probs, Quantiles.Type4, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result12

    toU(data4)
      .quantiles(Along(_0), InMemory(12))(probs, Quantiles.Type5, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result13

    toU(data4)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type6, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result14

    toU(data4)
      .quantiles(Along(_0), Default(12))(probs, Quantiles.Type7, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result15

    toU(data4)
      .quantiles(Along(_0), Unbalanced(12))(probs, Quantiles.Type8, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result16

    toU(data4)
      .quantiles(Along(_0), InMemory())(probs, Quantiles.Type9, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its first over values in 2D" in {
    toU(data5)
      .quantiles(Over(_0), InMemory(12))(probs, Quantiles.Type1, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result10

    toU(data5)
      .quantiles(Over(_0), Default())(probs, Quantiles.Type2, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result10

    toU(data5)
      .quantiles(Over(_0), Default(12))(probs, Quantiles.Type3, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result11

    toU(data5)
      .quantiles(Over(_0), Unbalanced(12))(probs, Quantiles.Type4, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result12

    toU(data5)
      .quantiles(Over(_0), InMemory())(probs, Quantiles.Type5, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result13

    toU(data5)
      .quantiles(Over(_0), InMemory(12))(probs, Quantiles.Type6, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result14

    toU(data5)
      .quantiles(Over(_0), Default())(probs, Quantiles.Type7, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result15

    toU(data5)
      .quantiles(Over(_0), Default(12))(probs, Quantiles.Type8, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result16

    toU(data5)
      .quantiles(Over(_0), Unbalanced(12))(probs, Quantiles.Type9, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its second along values in 2D" in {
    toU(data5)
      .quantiles(Along(_1), InMemory())(probs, Quantiles.Type1, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result10

    toU(data5)
      .quantiles(Along(_1), InMemory(12))(probs, Quantiles.Type2, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result10

    toU(data5)
      .quantiles(Along(_1), Default())(probs, Quantiles.Type3, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result11

    toU(data5)
      .quantiles(Along(_1), Default(12))(probs, Quantiles.Type4, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result12

    toU(data5)
      .quantiles(Along(_1), Unbalanced(12))(probs, Quantiles.Type5, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result13

    toU(data5)
      .quantiles(Along(_1), InMemory())(probs, Quantiles.Type6, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result14

    toU(data5)
      .quantiles(Along(_1), InMemory(12))(probs, Quantiles.Type7, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result15

    toU(data5)
      .quantiles(Along(_1), Default())(probs, Quantiles.Type8, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result16

    toU(data5)
      .quantiles(Along(_1), Default(12))(probs, Quantiles.Type9, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its second over values in 2D" in {
    toU(data4)
      .quantiles(Over(_1), Unbalanced(12))(probs, Quantiles.Type1, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result10

    toU(data4)
      .quantiles(Over(_1), InMemory())(probs, Quantiles.Type2, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result10

    toU(data4)
      .quantiles(Over(_1), InMemory(12))(probs, Quantiles.Type3, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result11

    toU(data4)
      .quantiles(Over(_1), Default())(probs, Quantiles.Type4, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result12

    toU(data4)
      .quantiles(Over(_1), Default(12))(probs, Quantiles.Type5, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result13

    toU(data4)
      .quantiles(Over(_1), Unbalanced(12))(probs, Quantiles.Type6, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result14

    toU(data4)
      .quantiles(Over(_1), InMemory())(probs, Quantiles.Type7, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result15

    toU(data4)
      .quantiles(Over(_1), InMemory(12))(probs, Quantiles.Type8, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result16

    toU(data4)
      .quantiles(Over(_1), Default())(probs, Quantiles.Type9, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return with non-numeric data" in {
    val res1 = toU(data6)
      .quantiles(Along(_0), Default(12))(probs, Quantiles.Type1, TestQuantile.name, false, true)
      .toList.sortBy(_.position)
    res1(0) shouldBe result18(0)
    res1(1) shouldBe result18(1)
    res1(2) shouldBe result18(2)
    res1(3).position shouldBe Position("quantile=0.800000")
    res1(3).content.value.as[Double].map(_.compare(Double.NaN)) shouldBe Some(0)

    toU(data6)
      .quantiles(Along(_0), Unbalanced(12))(probs, Quantiles.Type1, TestQuantile.name, false, false)
      .toList.sortBy(_.position) shouldBe result18
  }
}

class TestSparkQuantile extends TestQuantile with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A quantile" should "return its first along 1 value in 1D" in {
    toU(data1)
      .quantiles(Along(_0), InMemory())(probs, Quantiles.Type1, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data1)
      .quantiles(Along(_0), InMemory(12))(probs, Quantiles.Type2, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data1)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type3, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data1)
      .quantiles(Along(_0), Default(12))(probs, Quantiles.Type4, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data1)
      .quantiles(Along(_0), InMemory())(probs, Quantiles.Type5, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data1)
      .quantiles(Along(_0), InMemory(12))(probs, Quantiles.Type6, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data1)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type7, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data1)
      .quantiles(Along(_0), Default(12))(probs, Quantiles.Type8, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data1)
      .quantiles(Along(_0), InMemory())(probs, Quantiles.Type9, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first along 3 values in 1D" in {
    toU(data2)
      .quantiles(Along(_0), InMemory(12))(probs, Quantiles.Type1, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result2

    toU(data2)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type2, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result2

    toU(data2)
      .quantiles(Along(_0), Default(12))(probs, Quantiles.Type3, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result3

    toU(data2)
      .quantiles(Along(_0), InMemory())(probs, Quantiles.Type4, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result4

    toU(data2)
      .quantiles(Along(_0), InMemory(12))(probs, Quantiles.Type5, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result5

    toU(data2)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type6, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result6

    toU(data2)
      .quantiles(Along(_0), Default(12))(probs, Quantiles.Type7, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result7

    toU(data2)
      .quantiles(Along(_0), InMemory())(probs, Quantiles.Type8, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result8

    toU(data2)
      .quantiles(Along(_0), InMemory(12))(probs, Quantiles.Type9, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its first along 3 equal values in 1D" in {
    toU(data3)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type1, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data3)
      .quantiles(Along(_0), Default(12))(probs, Quantiles.Type2, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data3)
      .quantiles(Along(_0), InMemory())(probs, Quantiles.Type3, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data3)
      .quantiles(Along(_0), InMemory(12))(probs, Quantiles.Type4, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data3)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type5, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data3)
      .quantiles(Along(_0), Default(12))(probs, Quantiles.Type6, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data3)
      .quantiles(Along(_0), InMemory())(probs, Quantiles.Type7, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data3)
      .quantiles(Along(_0), InMemory(12))(probs, Quantiles.Type8, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1

    toU(data3)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type9, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first along values in 2D" in {
    toU(data4)
      .quantiles(Along(_0), Default(12))(probs, Quantiles.Type1, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result10

    toU(data4)
      .quantiles(Along(_0), InMemory())(probs, Quantiles.Type2, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result10

    toU(data4)
      .quantiles(Along(_0), InMemory(12))(probs, Quantiles.Type3, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result11

    toU(data4)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type4, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result12

    toU(data4)
      .quantiles(Along(_0), Default(12))(probs, Quantiles.Type5, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result13

    toU(data4)
      .quantiles(Along(_0), InMemory())(probs, Quantiles.Type6, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result14

    toU(data4)
      .quantiles(Along(_0), InMemory(12))(probs, Quantiles.Type7, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result15

    toU(data4)
      .quantiles(Along(_0), Default())(probs, Quantiles.Type8, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result16

    toU(data4)
      .quantiles(Along(_0), Default(12))(probs, Quantiles.Type9, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its first over values in 2D" in {
    toU(data5)
      .quantiles(Over(_0), InMemory())(probs, Quantiles.Type1, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result10

    toU(data5)
      .quantiles(Over(_0), InMemory(12))(probs, Quantiles.Type2, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result10

    toU(data5)
      .quantiles(Over(_0), Default())(probs, Quantiles.Type3, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result11

    toU(data5)
      .quantiles(Over(_0), Default(12))(probs, Quantiles.Type4, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result12

    toU(data5)
      .quantiles(Over(_0), InMemory())(probs, Quantiles.Type5, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result13

    toU(data5)
      .quantiles(Over(_0), InMemory(12))(probs, Quantiles.Type6, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result14

    toU(data5)
      .quantiles(Over(_0), Default())(probs, Quantiles.Type7, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result15

    toU(data5)
      .quantiles(Over(_0), Default(12))(probs, Quantiles.Type8, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result16

    toU(data5)
      .quantiles(Over(_0), InMemory())(probs, Quantiles.Type9, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its second along values in 2D" in {
    toU(data5)
      .quantiles(Along(_1), InMemory(12))(probs, Quantiles.Type1, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result10

    toU(data5)
      .quantiles(Along(_1), Default())(probs, Quantiles.Type2, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result10

    toU(data5)
      .quantiles(Along(_1), Default(12))(probs, Quantiles.Type3, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result11

    toU(data5)
      .quantiles(Along(_1), InMemory())(probs, Quantiles.Type4, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result12

    toU(data5)
      .quantiles(Along(_1), InMemory(12))(probs, Quantiles.Type5, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result13

    toU(data5)
      .quantiles(Along(_1), Default())(probs, Quantiles.Type6, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result14

    toU(data5)
      .quantiles(Along(_1), Default(12))(probs, Quantiles.Type7, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result15

    toU(data5)
      .quantiles(Along(_1), InMemory())(probs, Quantiles.Type8, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result16

    toU(data5)
      .quantiles(Along(_1), InMemory(12))(probs, Quantiles.Type9, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its second over values in 2D" in {
    toU(data4)
      .quantiles(Over(_1), Default())(probs, Quantiles.Type1, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result10

    toU(data4)
      .quantiles(Over(_1), Default(12))(probs, Quantiles.Type2, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result10

    toU(data4)
      .quantiles(Over(_1), InMemory())(probs, Quantiles.Type3, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result11

    toU(data4)
      .quantiles(Over(_1), InMemory(12))(probs, Quantiles.Type4, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result12

    toU(data4)
      .quantiles(Over(_1), Default())(probs, Quantiles.Type5, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result13

    toU(data4)
      .quantiles(Over(_1), Default(12))(probs, Quantiles.Type6, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result14

    toU(data4)
      .quantiles(Over(_1), InMemory())(probs, Quantiles.Type7, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result15

    toU(data4)
      .quantiles(Over(_1), InMemory(12))(probs, Quantiles.Type8, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result16

    toU(data4)
      .quantiles(Over(_1), Default())(probs, Quantiles.Type9, TestQuantile.name, true, true)
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return with non-numeric data" in {
    val res1 = toU(data6)
      .quantiles(Along(_0), Default(12))(probs, Quantiles.Type1, TestQuantile.name, false, true)
      .toList.sortBy(_.position)
    res1(0) shouldBe result18(0)
    res1(1) shouldBe result18(1)
    res1(2) shouldBe result18(2)
    res1(3).position shouldBe Position("quantile=0.800000")
    res1(3).content.value.as[Double].map(_.compare(Double.NaN)) shouldBe Some(0)

    toU(data6)
      .quantiles(Along(_0), InMemory())(probs, Quantiles.Type1, TestQuantile.name, false, false)
      .toList.sortBy(_.position) shouldBe result18
  }
}

