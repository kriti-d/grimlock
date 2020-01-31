// Copyright 2015,2016,2017,2018,2019,2020 Commonwealth Bank of Australia
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

trait TestMatrixGet extends TestMatrix {
  val result1 = List(Cell(Position("qux"), Content(OrdinalSchema[String](), "12.56")))

  val result2 = List(
    Cell(Position("foo", 3), Content(NominalSchema[String](), "9.42")),
    Cell(Position("qux", 1), Content(OrdinalSchema[String](), "12.56"))
  )

  val result3 = List(
    Cell(Position("foo", 3, "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(Position("qux", 1, "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )
}

class TestScalaMatrixGet extends TestMatrixGet with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A Matrix.get" should "return its cells in 1D" in {
    toU(data1)
      .get("qux", Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its cells in 2D" in {
    toU(data2)
      .get(List(Position("foo", 3), Position("qux", 1), Position("baz", 4)), Default())
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its cells in 3D" in {
    toU(data3)
      .get(List(Position("foo", 3, "xyz"), Position("qux", 1, "xyz"), Position("baz", 4, "xyz")), Default())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return empty data - Default" in {
    toU(data3)
      .get(List(), Default())
      .toList.sortBy(_.position) shouldBe List()
  }
}

class TestScaldingMatrixGet extends TestMatrixGet with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A Matrix.get" should "return its cells in 1D" in {
    toU(data1)
      .get("qux", InMemory())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its cells in 2D" in {
    toU(data2)
      .get(List(Position("foo", 3), Position("qux", 1), Position("baz", 4)), Default())
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its cells in 3D" in {
    toU(data3)
      .get(List(Position("foo", 3, "xyz"), Position("qux", 1, "xyz"), Position("baz", 4, "xyz")), Unbalanced(12))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return empty data - InMemory" in {
    toU(data3)
      .get(List(), InMemory())
      .toList.sortBy(_.position) shouldBe List()
  }

  it should "return empty data - Default" in {
    toU(data3)
      .get(List(), Default(12))
      .toList.sortBy(_.position) shouldBe List()
  }
}

class TestSparkMatrixGet extends TestMatrixGet with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A Matrix.get" should "return its cells in 1D" in {
    toU(data1)
      .get("qux", Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its cells in 2D" in {
    toU(data2)
      .get(List(Position("foo", 3), Position("qux", 1), Position("baz", 4)), Default(12))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its cells in 3D" in {
    toU(data3)
      .get(List(Position("foo", 3, "xyz"), Position("qux", 1, "xyz"), Position("baz", 4, "xyz")), Default())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return empty data - Default" in {
    toU(data3)
      .get(List(), Default())
      .toList.sortBy(_.position) shouldBe List()
  }
}

