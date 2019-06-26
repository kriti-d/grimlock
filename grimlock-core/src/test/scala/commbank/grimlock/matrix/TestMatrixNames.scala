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

import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.environment.tuner._
import commbank.grimlock.framework.position._

import shapeless.nat.{ _0, _1, _2 }

class TestScalaMatrixNames extends TestMatrix with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A Matrix.names" should "return its first over names in 1D" in {
    toU(data1)
      .names(Over(_0), Default())
      .toList.sorted shouldBe List(Position("bar"), Position("baz"), Position("foo"), Position("qux"))
  }

  it should "return its first over names in 2D" in {
    toU(data2)
      .names(Over(_0), Default())
      .toList.sorted shouldBe List(Position("bar"), Position("baz"), Position("foo"), Position("qux"))
  }

  it should "return its first along names in 2D" in {
    toU(data2)
      .names(Along(_0), Default())
      .toList.sorted shouldBe List(Position(1), Position(2), Position(3), Position(4))
  }

  it should "return its second over names in 2D" in {
    toU(data2)
      .names(Over(_1), Default())
      .toList.sorted shouldBe List(Position(1), Position(2), Position(3), Position(4))
  }

  it should "return its second along names in 2D" in {
    toU(data2)
      .names(Along(_1), Default())
      .toList.sorted shouldBe List(Position("bar"), Position("baz"), Position("foo"), Position("qux"))
  }

  it should "return its first over names in 3D" in {
    toU(data3)
      .names(Over(_0), Default())
      .toList.sorted shouldBe List(Position("bar"), Position("baz"), Position("foo"), Position("qux"))
  }

  it should "return its first along names in 3D" in {
    toU(data3)
      .names(Along(_0), Default())
      .toList.sorted shouldBe List(Position(1, "xyz"), Position(2, "xyz"), Position(3, "xyz"), Position(4, "xyz"))
  }

  it should "return its second over names in 3D" in {
    toU(data3)
      .names(Over(_1), Default())
      .toList.sorted shouldBe List(Position(1), Position(2), Position(3), Position(4))
  }

  it should "return its second along names in 3D" in {
    toU(data3)
      .names(Along(_1), Default())
      .toList.sorted shouldBe List(
        Position("bar", "xyz"),
        Position("baz", "xyz"),
        Position("foo", "xyz"),
        Position("qux", "xyz")
      )
  }

  it should "return its third over names in 3D" in {
    toU(data3)
      .names(Over(_2), Default())
      .toList.sorted shouldBe List(Position("xyz"))
  }

  it should "return its third along names in 3D" in {
    toU(data3)
      .names(Along(_2), Default())
      .toList.sorted shouldBe List(
        Position("bar", 1),
        Position("bar", 2),
        Position("bar", 3),
        Position("baz", 1),
        Position("baz", 2),
        Position("foo", 1),
        Position("foo", 2),
        Position("foo", 3),
        Position("foo", 4),
        Position("qux", 1)
      )
  }
}

class TestSparkMatrixNames extends TestMatrix with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A Matrix.names" should "return its first over names in 1D" in {
    toU(data1)
      .names(Over(_0), Default())
      .toList.sorted shouldBe List(Position("bar"), Position("baz"), Position("foo"), Position("qux"))
  }

  it should "return its first over names in 2D" in {
    toU(data2)
      .names(Over(_0), Default(12))
      .toList.sorted shouldBe List(Position("bar"), Position("baz"), Position("foo"), Position("qux"))
  }

  it should "return its first along names in 2D" in {
    toU(data2)
      .names(Along(_0), Default())
      .toList.sorted shouldBe List(Position(1), Position(2), Position(3), Position(4))
  }

  it should "return its second over names in 2D" in {
    toU(data2)
      .names(Over(_1), Default(12))
      .toList.sorted shouldBe List(Position(1), Position(2), Position(3), Position(4))
  }

  it should "return its second along names in 2D" in {
    toU(data2)
      .names(Along(_1), Default())
      .toList.sorted shouldBe List(Position("bar"), Position("baz"), Position("foo"), Position("qux"))
  }

  it should "return its first over names in 3D" in {
    toU(data3)
      .names(Over(_0), Default(12))
      .toList.sorted shouldBe List(Position("bar"), Position("baz"), Position("foo"), Position("qux"))
  }

  it should "return its first along names in 3D" in {
    toU(data3)
      .names(Along(_0), Default())
      .toList.sorted shouldBe List(Position(1, "xyz"), Position(2, "xyz"), Position(3, "xyz"), Position(4, "xyz"))
  }

  it should "return its second over names in 3D" in {
    toU(data3)
      .names(Over(_1), Default(12))
      .toList.sorted shouldBe List(Position(1), Position(2), Position(3), Position(4))
  }

  it should "return its second along names in 3D" in {
    toU(data3)
      .names(Along(_1), Default())
      .toList.sorted shouldBe List(
        Position("bar", "xyz"),
        Position("baz", "xyz"),
        Position("foo", "xyz"),
        Position("qux", "xyz")
      )
  }

  it should "return its third over names in 3D" in {
    toU(data3)
      .names(Over(_2), Default(12))
      .toList.sorted shouldBe List(Position("xyz"))
  }

  it should "return its third along names in 3D" in {
    toU(data3)
      .names(Along(_2), Default())
      .toList.sorted shouldBe List(
        Position("bar", 1),
        Position("bar", 2),
        Position("bar", 3),
        Position("baz", 1),
        Position("baz", 2),
        Position("foo", 1),
        Position("foo", 2),
        Position("foo", 3),
        Position("foo", 4),
        Position("qux", 1)
      )
  }
}

