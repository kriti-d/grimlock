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

package commbank.grimlock.test

import commbank.grimlock.framework._
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.environment.tuner._
import commbank.grimlock.framework.position._

import shapeless.nat.{ _0, _1, _2 }

class TestScalaHistogram extends TestDistribution with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A histogram" should "return its first over in 1D" in {
    toU(data1)
      .histogram(Over(_0), Default())(Locate.AppendContentString, true)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first along in 1D" in {
    toU(data1)
      .histogram(Along(_0), Default())(Locate.AppendContentString, true)
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first over in 2D" in {
    toU(data2)
      .histogram(Over(_0), Default())(Locate.AppendContentString, true)
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first along in 2D" in {
    toU(data2)
      .histogram(Along(_0), Default())(Locate.AppendContentString, true)
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second over in 2D" in {
    toU(data2)
      .histogram(Over(_1), Default())(Locate.AppendContentString, false)
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its second along in 2D" in {
    toU(data2)
      .histogram(Along(_1), Default())(Locate.AppendContentString, false)
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first over in 3D" in {
    toU(data3)
      .histogram(Over(_0), Default())(Locate.AppendContentString, false)
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its first along in 3D" in {
    toU(data3)
      .histogram(Along(_0), Default())(Locate.AppendContentString, false)
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second over in 3D" in {
    toU(data3)
      .histogram(Over(_1), Default())(Locate.AppendContentString, true)
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its second along in 3D" in {
    toU(data3)
      .histogram(Along(_1), Default())(Locate.AppendContentString, true)
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third over in 3D" in {
    toU(data3)
      .histogram(Over(_2), Default())(Locate.AppendContentString, true)
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its third along in 3D" in {
    toU(data3)
      .histogram(Along(_2), Default())(Locate.AppendContentString, true)
      .toList.sortBy(_.position) shouldBe result12
  }
}

class TestScaldingHistogram extends TestDistribution with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A histogram" should "return its first over in 1D" in {
    toU(data1)
      .histogram(Over(_0), Default())(Locate.AppendContentString, true)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first along in 1D" in {
    toU(data1)
      .histogram(Along(_0), Default(12))(Locate.AppendContentString, true)
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first over in 2D" in {
    toU(data2)
      .histogram(Over(_0), Default())(Locate.AppendContentString, true)
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first along in 2D" in {
    toU(data2)
      .histogram(Along(_0), Default(12))(Locate.AppendContentString, true)
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second over in 2D" in {
    toU(data2)
      .histogram(Over(_1), Default())(Locate.AppendContentString, false)
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its second along in 2D" in {
    toU(data2)
      .histogram(Along(_1), Default(12))(Locate.AppendContentString, false)
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first over in 3D" in {
    toU(data3)
      .histogram(Over(_0), Default())(Locate.AppendContentString, false)
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its first along in 3D" in {
    toU(data3)
      .histogram(Along(_0), Default(12))(Locate.AppendContentString, false)
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second over in 3D" in {
    toU(data3)
      .histogram(Over(_1), Default())(Locate.AppendContentString, true)
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its second along in 3D" in {
    toU(data3)
      .histogram(Along(_1), Default(12))(Locate.AppendContentString, true)
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third over in 3D" in {
    toU(data3)
      .histogram(Over(_2), Default())(Locate.AppendContentString, true)
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its third along in 3D" in {
    toU(data3)
      .histogram(Along(_2), Default(12))(Locate.AppendContentString, true)
      .toList.sortBy(_.position) shouldBe result12
  }
}

class TestSparkHistogram extends TestDistribution with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A histogram" should "return its first over in 1D" in {
    toU(data1)
      .histogram(Over(_0), Default())(Locate.AppendContentString, true)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first along in 1D" in {
    toU(data1)
      .histogram(Along(_0), Default(12))(Locate.AppendContentString, true)
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first over in 2D" in {
    toU(data2)
      .histogram(Over(_0), Default())(Locate.AppendContentString, true)
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first along in 2D" in {
    toU(data2)
      .histogram(Along(_0), Default(12))(Locate.AppendContentString, true)
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second over in 2D" in {
    toU(data2)
      .histogram(Over(_1), Default())(Locate.AppendContentString, false)
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its second along in 2D" in {
    toU(data2)
      .histogram(Along(_1), Default(12))(Locate.AppendContentString, false)
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first over in 3D" in {
    toU(data3)
      .histogram(Over(_0), Default())(Locate.AppendContentString, false)
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its first along in 3D" in {
    toU(data3)
      .histogram(Along(_0), Default(12))(Locate.AppendContentString, false)
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second over in 3D" in {
    toU(data3)
      .histogram(Over(_1), Default())(Locate.AppendContentString, true)
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its second along in 3D" in {
    toU(data3)
      .histogram(Along(_1), Default(12))(Locate.AppendContentString, true)
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third over in 3D" in {
    toU(data3)
      .histogram(Over(_2), Default())(Locate.AppendContentString, true)
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its third along in 3D" in {
    toU(data3)
      .histogram(Along(_2), Default(12))(Locate.AppendContentString, true)
      .toList.sortBy(_.position) shouldBe result12
  }
}

